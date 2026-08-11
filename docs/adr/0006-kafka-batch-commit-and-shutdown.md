# ADR-0006: Finite Kafka batches, offset commits, and shutdown

- Status: Accepted
- Date: 2026-08-07

## Context

Concurrent record handling can commit beyond unfinished offsets in the same partition. Rebalances and SIGTERM can also interrupt in-flight work.

## Decision

Disable auto-commit, block rebalancing for a finite polled batch, process it with bounded concurrency, and commit after every record has a durable outcome. On cancellation, stop polling, propagate context cancellation, shut down health/HTTP servers within a timeout, and leave uncommitted work for redelivery.

## Consequences

No lower in-flight offset is skipped by a higher concurrent commit. A failed batch can replay already completed records, which is safe through idempotency. `PollRecords` bounds the batch, and startup validation keeps its conservative worst-case processing duration below franz-go's configured rebalance timeout. Shutdown also has a final process-level timeout.
