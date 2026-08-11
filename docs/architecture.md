# Architecture notes

## Consistency boundaries

There are two local ACID boundaries:

1. order creation: `orders + order_items + OrderCreated outbox`;
2. consumer completion: `processed_events + order status + OrderStatusChanged outbox`.

Kafka publication is outside both boundaries. Consequently, delivery is at-least-once and consumers must tolerate duplicates.

The HTTP `request_id` is persisted in the event envelope and carried through Kafka headers and derived events. It is correlation metadata, not an idempotency key; `event_id` remains the deduplication identity.

## Why the consumer commits a finite batch

Processing Kafka records concurrently and committing each record independently can commit a higher offset while a lower offset in the same partition is still running. A crash would then lose the lower record. The worker therefore:

1. polls a bounded batch;
2. blocks rebalancing;
3. processes the batch through a bounded pool;
4. commits only after every record reaches a durable terminal result;
5. allows rebalancing again.

If one record cannot be durably handled, the batch is not committed. Other completed records may be delivered again and are handled through idempotency.

## Scaling boundaries

- `order-service` scales as ordinary stateless HTTP pods.
- `outbox-publisher` scales through `SKIP LOCKED` claims and leases.
- `order-worker` scales up to the number of Kafka partitions in its consumer group.
- Worker counts protect local dependencies; they do not create more Kafka partition parallelism by themselves.
- An Outbox lease covers the worst-case time that a claimed row can wait behind other rows in its local worker pool. Invalid internal events enter terminal `FAILED` state rather than becoming poison retries.

## Retry trade-off

The retry topic prevents a poison/transient record from indefinitely blocking the source partition. The cost is relaxed cross-topic ordering. The implementation retains the original key and stable event ID, uses guarded state transitions, and accepts duplicate retry records.

The `not_before` header implements delay without adding a scheduling subsystem. One worker slot waits for that time. For substantially longer delays or higher throughput, use tiered retry topics or a persistent scheduler.
