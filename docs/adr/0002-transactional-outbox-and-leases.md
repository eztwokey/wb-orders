# ADR-0002: Transactional Outbox with lease-based claiming

- Status: Accepted
- Date: 2026-08-07

## Context

Writing an order to PostgreSQL and publishing to Kafka are two independent operations. A crash between them can lose an event or publish an event for a rolled-back order.

## Decision

Write the aggregate and its event to PostgreSQL in one transaction. A separate publisher claims rows with `FOR UPDATE SKIP LOCKED`, records an expiring lease, commits the claim, and only then calls Kafka. Validate that the lease covers the worst-case local queueing time for the configured batch and worker count. Mark structurally invalid rows as terminal `FAILED`.

## Consequences

The HTTP path does not depend on Kafka availability. Multiple publishers can safely claim different rows. A database transaction is not held during Kafka I/O. A crash after Kafka accepts a message but before `published_at` creates a duplicate, so downstream idempotency remains mandatory. Failed rows require an explicit operator repair/replay decision and are excluded from normal claims.
