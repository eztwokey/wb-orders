# ADR-0005: Retry topic and Dead Letter Queue

- Status: Accepted
- Date: 2026-08-07

## Context

Transient dependency failures should not permanently lose an event, while malformed or exhausted events must not poison normal consumption forever.

## Decision

Republish transient failures to `wb.orders.created.retry` with an attempt counter, capped exponential backoff and `not_before`. Send malformed headers, malformed payloads, unsupported versions/types, or exhausted events to `wb.orders.created.dlq`. Commit the source offset only after the new record is acknowledged.

## Consequences

Republish and offset commit are still two operations, so duplicate retry records are possible and handled through the same event ID. Cross-topic ordering is relaxed. The simple delayed consumer occupies a bounded worker slot; a persistent scheduler is a future scaling option.
