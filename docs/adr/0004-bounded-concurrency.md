# ADR-0004: Bounded Worker Pools and Fan-Out/Fan-In

- Status: Accepted
- Date: 2026-08-07

## Context

Unbounded goroutine creation can exhaust database connections, memory, Kafka request capacity, or downstream services.

## Decision

Use fixed-size Worker Pools for Outbox events and Kafka records. For one order, run the fixed set of independent operations concurrently and collect all results through a buffered channel. Pass one cancelable context to every operation.

## Consequences

Concurrency is explicit and configurable. Technical errors cancel sibling work. Fan-Out is limited to a known small number rather than an unbounded goroutine per item. Race tests focus on these packages.

