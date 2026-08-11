wb-orders

wb-orders — мой учебный backend-сервис для обработки заказов маркетплейса. Я сделал его после обучения в WB Tech School, чтобы на практике разобраться с event-driven архитектурой, Kafka, Transactional Outbox и конкурентным программированием в Go.

Это независимый учебный проект, не связанный с внутренними системами Wildberries.

Архитектура

В проекте три Go-приложения:

Приложение

Задача

order-service

REST API, валидация, создание и получение заказов

outbox-publisher

Чтение событий из PostgreSQL и публикация в Kafka

order-worker

Обработка Kafka-событий, изменение статуса заказа

flowchart TD
    Client[HTTP-клиент] --> API[order-service]
    API -->|orders + outbox в одной транзакции| DB[(PostgreSQL)]
    DB --> Publisher[outbox-publisher]
    Publisher -->|OrderCreated| Kafka[(Kafka)]
    Kafka --> Worker[order-worker]
    Worker --> Pipeline[Worker Pool и Fan-Out/Fan-In]
    Pipeline -->|итоговый статус| DB

PostgreSQL является источником текущего состояния заказа. Kafka используется для доставки событий между компонентами.

Как работает проект

Создание заказа

Клиент отправляет POST /api/v1/orders. order-service проверяет данные и в одной транзакции PostgreSQL создает:

заказ и его позиции;

событие OrderCreated в таблице outbox_events.

Сервис не отправляет событие в Kafka напрямую. Если транзакция откатится, не сохранится ни заказ, ни событие. Если commit прошел успешно, событие гарантированно остается в Outbox и может быть опубликовано позже.

Публикация в Kafka

outbox-publisher забирает неопубликованные события через FOR UPDATE SKIP LOCKED и публикует их ограниченным Worker Pool. Поэтому несколько workers или pods могут работать параллельно, не обрабатывая одну строку одновременно.

После успешной публикации событие отмечается в PostgreSQL. Если процесс упадет после отправки в Kafka, но до обновления Outbox, сообщение будет отправлено повторно. Проект использует at-least-once delivery и не пытается выдавать ее за exactly-once.

Kafka key равен order_id, поэтому события одного заказа сохраняют порядок внутри partition.

Обработка заказа

order-worker получает OrderCreated и параллельно запускает три операции:

резервирование товара;

расчет доставки;

подготовку уведомления.

Fan-Out запускает операции, Fan-In собирает результаты. Количество одновременно обрабатываемых сообщений ограничено Worker Pool, а все операции поддерживают context.Context и отмену.

После обработки одна транзакция PostgreSQL:

записывает event_id в processed_events;

переводит заказ в CONFIRMED или FAILED;

создает событие OrderStatusChanged в Outbox.

Уникальный ключ (consumer_name, event_id) делает consumer идемпотентным. Повторная доставка того же события не изменит заказ второй раз.

Для временных ошибок предусмотрены exponential backoff и topic wb.orders.created.retry. После исчерпания попыток или при неисправимом сообщении событие попадает в wb.orders.created.dlq.
