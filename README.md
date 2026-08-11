# wb-orders

`wb-orders` — мой учебный backend-сервис для обработки заказов маркетплейса. Я сделал его после обучения в WB Tech School, чтобы на практике разобраться с event-driven архитектурой, Kafka, Transactional Outbox и конкурентным программированием в Go.

Это независимый учебный проект, не связанный с внутренними системами Wildberries.

## Архитектура

В проекте три Go-приложения:

| Приложение | Задача |
| --- | --- |
| `order-service` | REST API, валидация, создание и получение заказов |
| `outbox-publisher` | Чтение событий из PostgreSQL и публикация в Kafka |
| `order-worker` | Обработка Kafka-событий, изменение статуса заказа |

```mermaid
flowchart TD
    Client[HTTP-клиент] --> API[order-service]
    API -->|orders + outbox в одной транзакции| DB[(PostgreSQL)]
    DB --> Publisher[outbox-publisher]
    Publisher -->|OrderCreated| Kafka[(Kafka)]
    Kafka --> Worker[order-worker]
    Worker --> Pipeline[Worker Pool и Fan-Out/Fan-In]
    Pipeline -->|итоговый статус| DB
```

PostgreSQL является источником текущего состояния заказа. Kafka используется для доставки событий между компонентами.

## Как работает проект

### Создание заказа

Клиент отправляет `POST /api/v1/orders`. `order-service` проверяет данные и в одной транзакции PostgreSQL создает:

- заказ и его позиции;
- событие `OrderCreated` в таблице `outbox_events`.

Сервис не отправляет событие в Kafka напрямую. Если транзакция откатится, не сохранится ни заказ, ни событие. Если commit прошел успешно, событие гарантированно остается в Outbox и может быть опубликовано позже.

### Публикация в Kafka

`outbox-publisher` забирает неопубликованные события через `FOR UPDATE SKIP LOCKED` и публикует их ограниченным Worker Pool. Поэтому несколько workers или pods могут работать параллельно, не обрабатывая одну строку одновременно.

После успешной публикации событие отмечается в PostgreSQL. Если процесс упадет после отправки в Kafka, но до обновления Outbox, сообщение будет отправлено повторно. Проект использует at-least-once delivery и не пытается выдавать ее за exactly-once.

Kafka key равен `order_id`, поэтому события одного заказа сохраняют порядок внутри partition.

### Обработка заказа

`order-worker` получает `OrderCreated` и параллельно запускает три операции:

- резервирование товара;
- расчет доставки;
- подготовку уведомления.

Fan-Out запускает операции, Fan-In собирает результаты. Количество одновременно обрабатываемых сообщений ограничено Worker Pool, а все операции поддерживают `context.Context` и отмену.

После обработки одна транзакция PostgreSQL:

1. записывает `event_id` в `processed_events`;
2. переводит заказ в `CONFIRMED` или `FAILED`;
3. создает событие `OrderStatusChanged` в Outbox.

Уникальный ключ `(consumer_name, event_id)` делает consumer идемпотентным. Повторная доставка того же события не изменит заказ второй раз.

Для временных ошибок предусмотрены exponential backoff и topic `wb.orders.created.retry`. После исчерпания попыток или при неисправимом сообщении событие попадает в `wb.orders.created.dlq`.

## Быстрый запуск на Windows 11

Понадобятся [Git](https://git-scm.com/download/win) и [Docker Desktop](https://www.docker.com/products/docker-desktop/). В Docker Desktop должен быть запущен Linux containers engine.

Открой PowerShell и выполни:

```powershell
git clone https://github.com/eztwokey/wb-orders.git
cd wb-orders
docker compose up -d --build
docker compose ps
```

Docker Compose поднимет PostgreSQL, Kafka, применит миграции и запустит все три приложения.

Проверка API:

```powershell
Invoke-RestMethod http://localhost:8080/health
Invoke-RestMethod http://localhost:8080/ready
```

Создание заказа:

```powershell
$body = @{
    customer_id  = "3f2b598f-bcaf-41f0-a4f4-7e80dc93d49a"
    warehouse_id = "1e734f9d-1855-4b60-a428-1645946878e1"
    items = @(
        @{ sku = "keyboard-01"; quantity = 1; unit_price_minor = 129900 }
        @{ sku = "mouse-01"; quantity = 2; unit_price_minor = 49900 }
    )
} | ConvertTo-Json -Depth 4

$order = Invoke-RestMethod `
    -Method Post `
    -Uri "http://localhost:8080/api/v1/orders" `
    -Headers @{ "X-Request-ID" = "local-demo-1" } `
    -ContentType "application/json" `
    -Body $body

$order
```

Через несколько секунд можно проверить результат:

```powershell
Start-Sleep -Seconds 3
Invoke-RestMethod "http://localhost:8080/api/v1/orders/$($order.id)"
```

Заказ сначала создается в статусе `PENDING`, а затем переходит в `CONFIRMED` или `FAILED`.

Посмотреть логи:

```powershell
docker compose logs -f
```

Остановить проект:

```powershell
docker compose down
```

Более подробная инструкция для Windows находится в [`WINDOWS_11_RUNBOOK_RU.md`](WINDOWS_11_RUNBOOK_RU.md).

## Тесты

Если Go установлен локально:

```bash
go test ./...
go test -race ./...
```

Запуск тестов через Docker из PowerShell:

```powershell
docker run --rm -v "${PWD}:/app" -w /app golang:1.26 go test ./...
docker run --rm -v "${PWD}:/app" -w /app golang:1.26 go test -race ./...
```

## Где смотреть детали

- [`api/openapi.yaml`](api/openapi.yaml) — описание REST API;
- [`docs/architecture.md`](docs/architecture.md) — подробная архитектура и потоки данных;
- [`docs/adr`](docs/adr) — принятые архитектурные решения;
- [`migrations`](migrations) — схема PostgreSQL;
- [`deploy/k8s`](deploy/k8s) — Kubernetes-манифесты;
- [`.gitlab-ci.yml`](.gitlab-ci.yml) — pipeline `lint → test → build → docker → deploy`.

В проекте также есть structured JSON logs, Prometheus-метрики, health/readiness endpoints и graceful shutdown по `SIGTERM`/`SIGINT`.
