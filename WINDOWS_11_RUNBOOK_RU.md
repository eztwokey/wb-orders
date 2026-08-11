# Запуск и проверка `wb-orders` на Windows 11

Инструкция рассчитана на PowerShell и Docker Desktop. Устанавливать Go, PostgreSQL, Kafka и `make` на Windows для первого запуска не нужно: приложения и инфраструктура запускаются в Linux-контейнерах.

## 1. Что нужно установить

### Обязательно

1. Windows 11 x64 с включенной аппаратной виртуализацией.
2. WSL 2.
3. Docker Desktop с WSL 2 backend.
4. Распакованный проект, например в `C:\dev\wb-orders`.

Официальные инструкции:

- WSL: <https://learn.microsoft.com/windows/wsl/install>
- Docker Desktop: <https://docs.docker.com/desktop/setup/install/windows-install/>
- Docker Desktop и WSL 2: <https://docs.docker.com/desktop/features/wsl/>

Если WSL еще не установлен, откройте PowerShell **от имени администратора**:

```powershell
wsl --install
```

Перезагрузите компьютер. После установки Docker Desktop проверьте в `Settings → General`, что включен `Use WSL 2 based engine`.

Для комфортной работы рекомендуется выделить Docker Desktop хотя бы 4 CPU и 6 GB RAM. Это не жесткое требование проекта, но одновременный запуск PostgreSQL, Kafka, сборки трех Go-приложений и тестов требует памяти.

## 2. Проверка Docker

Запустите Docker Desktop и дождитесь состояния `Engine running`. Откройте обычный PowerShell:

```powershell
docker version
docker compose version
wsl --status
```

Команды `docker version` и `docker compose version` должны завершиться без ошибки. Если видна только Client-секция и ошибка подключения к engine, Docker Desktop еще не запущен.

## 3. Распаковка проекта

Рекомендуемый путь:

```text
C:\dev\wb-orders
```

Лучше не использовать OneDrive, сетевой диск и очень длинный путь: Docker bind mounts и антивирус могут заметно замедлять сборку.

Перейдите в каталог, где находится `docker-compose.yml`:

```powershell
cd C:\dev\wb-orders
Get-ChildItem
```

В списке должны быть `docker-compose.yml`, `go.mod`, `README.md`, каталоги `cmd`, `internal`, `migrations` и `docker`.

## 4. Первый запуск

Выполните:

```powershell
docker compose up --build -d
```

При первом запуске Docker скачает образы и Go dependencies, поэтому операция может занять несколько минут. Compose должен:

1. запустить PostgreSQL;
2. применить SQL migrations;
3. запустить Kafka в KRaft mode;
4. создать четыре Kafka topics;
5. собрать и запустить три Go-приложения.

Посмотрите состояние:

```powershell
docker compose ps
```

Ожидается:

- `postgres`, `kafka`, `order-service`, `outbox-publisher`, `order-worker` — `Up`;
- `migrate` и `kafka-init` — `Exited (0)` или `Completed`, потому что это одноразовые задачи.

Если какой-либо контейнер имеет `Exited (1)`, сразу смотрите его logs:

```powershell
docker compose logs --tail=200 ИМЯ_СЕРВИСА
```

Например:

```powershell
docker compose logs --tail=200 order-worker
```

## 5. Проверка health и readiness

В PowerShell:

```powershell
Invoke-RestMethod http://localhost:8080/health
Invoke-RestMethod http://localhost:8080/ready
Invoke-RestMethod http://localhost:8081/health
Invoke-RestMethod http://localhost:8081/ready
Invoke-RestMethod http://localhost:8082/health
Invoke-RestMethod http://localhost:8082/ready
```

Ожидаются значения `ok` и `ready`.

Назначение портов:

| Порт | Компонент |
| --- | --- |
| `8080` | Order HTTP API, health и metrics |
| `8081` | Outbox Publisher health и metrics |
| `8082` | Order Worker health и metrics |
| `5432` | PostgreSQL |
| `29092` | Kafka для программ с Windows-host |

## 6. Создание тестового заказа

Скопируйте блок целиком в PowerShell:

```powershell
$body = @{
    customer_id = "3f2b598f-bcaf-41f0-a4f4-7e80dc93d49a"
    warehouse_id = "1e734f9d-1855-4b60-a428-1645946878e1"
    items = @(
        @{
            sku = "keyboard-01"
            quantity = 1
            unit_price_minor = 129900
        },
        @{
            sku = "mouse-01"
            quantity = 2
            unit_price_minor = 49900
        }
    )
} | ConvertTo-Json -Depth 5

$order = Invoke-RestMethod `
    -Method Post `
    -Uri "http://localhost:8080/api/v1/orders" `
    -Headers @{ "X-Request-ID" = "win11-demo-1" } `
    -ContentType "application/json" `
    -Body $body

$order | ConvertTo-Json -Depth 5
$orderId = $order.id
$orderId
```

Первый ответ обычно содержит статус `PENDING`, валюту `RUB` и выбранный `warehouse_id`: HTTP API зафиксировал заказ, но асинхронный worker еще мог не закончить обработку.

Через несколько секунд прочитайте заказ снова:

```powershell
Start-Sleep -Seconds 3
$result = Invoke-RestMethod "http://localhost:8080/api/v1/orders/$orderId"
$result | ConvertTo-Json -Depth 5
```

Для предложенных позиций ожидается `CONFIRMED`.

Чтобы увидеть business failure inventory, создайте заказ с `quantity = 101`. Технического retry не будет: валидный заказ должен перейти в `FAILED`, потому что лимит заглушки inventory равен 100 единицам одного SKU.

## 7. Проверка сквозных логов

Покажите logs трех приложений:

```powershell
docker compose logs --tail=200 order-service outbox-publisher order-worker
```

Или следите в реальном времени:

```powershell
docker compose logs -f --tail=100 order-service outbox-publisher order-worker
```

Остановить просмотр logs можно сочетанием `Ctrl+C`; контейнеры при этом продолжат работать.

Найдите `win11-demo-1`. В связанных записях должны присутствовать:

- `request_id` — корреляция исходного HTTP-запроса;
- `order_id` — aggregate ID;
- `event_id` — ID конкретного события;
- имя `service`;
- Kafka topic или итоговый order status.

## 8. Проверка PostgreSQL

Посмотреть заказы:

```powershell
docker compose exec -T postgres psql -U wb_orders -d wb_orders -c 'SELECT id, warehouse_id, status, currency, total_amount_minor, created_at FROM orders ORDER BY created_at DESC LIMIT 10;'
```

Посмотреть Outbox:

```powershell
docker compose exec -T postgres psql -U wb_orders -d wb_orders -c 'SELECT id, aggregate_id, event_type, status, attempts, published_at, last_error FROM outbox_events ORDER BY created_at DESC LIMIT 20;'
```

Посмотреть таблицу идемпотентности:

```powershell
docker compose exec -T postgres psql -U wb_orders -d wb_orders -c 'SELECT event_id, consumer_name, processed_at FROM processed_events ORDER BY processed_at DESC LIMIT 20;'
```

После успешного happy path должны быть:

- одна строка заказа со статусом `CONFIRMED`;
- опубликованное `OrderCreated`;
- опубликованное `OrderStatusChanged`;
- processed marker для `OrderCreated` и `wb-orders-worker-v1`.

## 9. Проверка Kafka

Список topics:

```powershell
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
```

Ожидаются:

- `wb.orders.created`;
- `wb.orders.status-changed`;
- `wb.orders.created.retry`;
- `wb.orders.created.dlq`.

Состояние consumer group:

```powershell
docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:9092 --group wb-orders-worker-v1 --describe
```

`LAG` должен стремиться к нулю после завершения обработки.

Прочитать ранее опубликованные status events:

```powershell
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic wb.orders.status-changed --from-beginning --max-messages 1
```

Если topic пока пуст, эта команда будет ждать. Остановите ее через `Ctrl+C`, создайте заказ и повторите.

## 10. Проверка Prometheus-метрик

```powershell
(Invoke-WebRequest http://localhost:8080/metrics).Content | Select-String "wb_orders_http"
(Invoke-WebRequest http://localhost:8081/metrics).Content | Select-String "wb_orders_outbox"
(Invoke-WebRequest http://localhost:8082/metrics).Content | Select-String "wb_orders_kafka|wb_orders_order_processing"
```

После создания заказа счетчики HTTP, Outbox и Kafka должны быть ненулевыми.

## 11. Проверка поведения при недоступной Kafka

Это полезный ручной reliability test.

Остановите Kafka:

```powershell
docker compose stop kafka
```

Создайте еще один заказ тем же PowerShell-запросом, изменив `X-Request-ID` и при желании SKU. HTTP API должен продолжить принимать заказы, потому что он атомарно пишет в PostgreSQL и Outbox, а не вызывает Kafka.

Проверьте накопившуюся Outbox-строку:

```powershell
docker compose exec -T postgres psql -U wb_orders -d wb_orders -c "SELECT event_type, status, attempts, next_attempt_at, last_error FROM outbox_events WHERE status <> 'PUBLISHED' ORDER BY created_at DESC;"
```

Верните Kafka:

```powershell
docker compose start kafka
```

Дождитесь readiness и проверьте, что событие было опубликовано, а заказ обработан:

```powershell
Start-Sleep -Seconds 15
docker compose ps
docker compose logs --tail=100 outbox-publisher order-worker
```

Если `kafka-init` потребуется повторно, выполните:

```powershell
docker compose run --rm kafka-init
```

## 12. Unit tests без установки Go на Windows

Остановить работающие приложения не обязательно для unit tests.

Из корня проекта выполните одной строкой:

```powershell
docker run --rm --mount "type=bind,source=$($PWD.Path),target=/src" -w /src golang:1.26-bookworm go test ./...
```

Ожидается завершение с exit code 0 и строки `ok` для пакетов с тестами.

## 13. Race detector без установки Go

```powershell
docker run --rm --mount "type=bind,source=$($PWD.Path),target=/src" -w /src golang:1.26-bookworm bash -lc "go test -race ./..."
```

Команда медленнее обычных unit tests. Результат не должен содержать `DATA RACE`.

## 14. Integration tests PostgreSQL и Kafka

Integration tests следует запускать без трех application containers: живой Outbox Publisher мог бы забрать тестовые строки и создать гонку с тестом repository.

Остановите все контейнеры, не удаляя данные:

```powershell
docker compose down
```

Запустите только инфраструктуру и примените migrations:

```powershell
docker compose up -d postgres kafka
docker compose run --rm migrate
```

Запустите integration suite в одноразовом Go-контейнере, подключенном к Compose network:

```powershell
docker run --rm --network wb-orders_default --mount "type=bind,source=$($PWD.Path),target=/src" -w /src -e TEST_DATABASE_URL="postgres://wb_orders:wb_orders@postgres:5432/wb_orders?sslmode=disable" -e TEST_KAFKA_BROKERS="kafka:9092" golang:1.26-bookworm go test -tags=integration ./internal/order ./internal/consumer ./internal/outbox ./internal/platform/kafka
```

После тестов верните полную систему:

```powershell
docker compose up --build -d
```

## 15. Полная минимальная проверка перед публикацией

```powershell
docker run --rm --mount "type=bind,source=$($PWD.Path),target=/src" -w /src golang:1.26-bookworm bash -lc 'test -z "$(find . -type f -name "*.go" -exec gofmt -l {} +)" && go vet ./... && go test ./...'
docker run --rm --mount "type=bind,source=$($PWD.Path),target=/src" -w /src golang:1.26-bookworm bash -lc "go test -race ./..."
docker compose build
```

Одинарные кавычки вокруг первой shell-команды важны: они не дают PowerShell интерпретировать выражения `$()` раньше Linux shell внутри контейнера. Если хотите выполнять проверки раздельно:

```powershell
docker run --rm --mount "type=bind,source=$($PWD.Path),target=/src" -w /src golang:1.26-bookworm bash -lc 'find . -type f -name "*.go" -exec gofmt -l {} +'
docker run --rm --mount "type=bind,source=$($PWD.Path),target=/src" -w /src golang:1.26-bookworm go vet ./...
docker run --rm --mount "type=bind,source=$($PWD.Path),target=/src" -w /src golang:1.26-bookworm go test ./...
```

Первая раздельная команда только перечисляет неправильно отформатированные `.go`-файлы и ничего не изменяет. При корректном форматировании она не печатает ничего.

## 16. Остановка и сброс

Остановить систему, сохранив PostgreSQL volume:

```powershell
docker compose down
```

Повторный запуск:

```powershell
docker compose up -d
```

Полностью удалить контейнеры **и все локальные данные PostgreSQL**:

```powershell
docker compose down -v
```

Последняя команда разрушительна. Используйте ее только когда действительно хотите получить чистую БД.

## 17. Типичные проблемы Windows 11

### `docker` не является именем командлета

Docker Desktop не установлен либо PowerShell был открыт до установки. Установите Docker Desktop и откройте новое окно терминала.

### `Cannot connect to the Docker daemon` или named pipe error

Запустите Docker Desktop, дождитесь `Engine running` и повторите `docker version`. Проверьте, что используется Linux containers / WSL 2 backend.

### WSL не установлен или устарел

PowerShell от администратора:

```powershell
wsl --install
wsl --update
wsl --status
wsl -l -v
```

У distribution должна быть версия 2.

### Порт `8080`, `5432` или `29092` занят

Проверить процесс:

```powershell
Get-NetTCPConnection -State Listen | Where-Object LocalPort -In 8080,8081,8082,5432,29092
```

Остановите конфликтующую программу/контейнер или измените левую часть port mapping в `docker-compose.yml`.

### `order-service` не стартует после старого неудачного запуска

Посмотрите:

```powershell
docker compose ps -a
docker compose logs --tail=300 migrate postgres order-service
```

Если локальные данные не нужны, выполните чистый сброс `docker compose down -v`, затем `docker compose up --build -d`.

### Заказ долго остается `PENDING`

Проверьте последовательно:

```powershell
docker compose ps
docker compose logs --tail=200 outbox-publisher
docker compose logs --tail=200 order-worker
docker compose exec -T postgres psql -U wb_orders -d wb_orders -c "SELECT event_type, status, attempts, next_attempt_at, last_error FROM outbox_events ORDER BY created_at DESC LIMIT 20;"
```

Если Outbox остается `PENDING`, проблема до Kafka publish. Если `OrderCreated` уже `PUBLISHED`, проблема находится в consumer/processing path.

### Docker build очень медленный

Не храните проект на сетевом диске. Проверьте ресурсы Docker Desktop. Первый build медленнее из-за скачивания images/modules; последующие используют cache.

### Image pull не работает через корпоративную сеть/VPN

Проверьте proxy settings Docker Desktop и доступ к Docker Hub/GCR. Проект использует образы PostgreSQL, Apache Kafka, Go, migration tool и distroless runtime.

## 18. Когда запуск считается полностью успешным

Все пункты должны выполняться:

- `docker compose ps` показывает пять работающих долгоживущих сервисов;
- health и readiness endpoints отвечают успешно;
- `POST /api/v1/orders` возвращает заказ;
- через несколько секунд GET показывает `CONFIRMED` или ожидаемый business `FAILED`;
- Outbox events имеют `PUBLISHED`;
- `processed_events` содержит event ID;
- Kafka consumer group имеет нулевой или уменьшающийся lag;
- Prometheus metrics изменяются после запроса;
- unit и race tests проходят;
- integration tests проходят отдельно от application containers.

После этого локальная система на Windows 11 воспроизводит полный поток:

`HTTP → PostgreSQL transaction → Outbox → Kafka → Worker Pool → Fan-Out/Fan-In → idempotent PostgreSQL transaction`.
