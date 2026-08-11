# ADR-0003: At-least-once delivery and idempotent consumers

- Status: Accepted
- Date: 2026-08-07

## Context

The Outbox/Kafka/database boundary contains unavoidable crash windows without a distributed transaction.

## Decision

Use at-least-once delivery. Keep a stable `event_id` across retries. Each logical consumer inserts `(consumer_name, event_id)` under a unique constraint in the same transaction as its state change and resulting Outbox event.

## Consequences

Duplicates are expected and safe. An early read check reduces repeated work but the unique constraint is the actual concurrency guard. Arbitrary external side effects are not made atomic by this table and require their own idempotency key or another Outbox boundary.

