# ADR-0001: Three applications with a shared PostgreSQL

- Status: Accepted
- Date: 2026-08-07

## Context

The project must demonstrate event-driven reliability without manufacturing a microservice for each small operation.

## Decision

Deploy `order-service`, `outbox-publisher`, and `order-worker` independently. Keep inventory, delivery and notification preparation as internal components. Use one PostgreSQL schema as the source of truth in the first version.

## Consequences

The design has clear runtime responsibilities and independent scaling while retaining simple local transactions. It is not a strict database-per-service architecture. If teams, security boundaries or independent ownership require it later, components can be extracted behind events with their own idempotency contracts.

