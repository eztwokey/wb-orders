# ADR-0007: Marketplace order scope and fulfillment warehouse

- Status: Accepted
- Date: 2026-08-11

## Context

A completely generic order example is easy to run but says little about domain boundaries. At the same time, adding sellers, payments, couriers, warehouse networks and external APIs at once would hide the reliability mechanisms this project is intended to demonstrate.

## Decision

Require every order to reference one fulfillment `warehouse_id`. Store monetary values as integer kopecks and support only the explicitly recorded `RUB` currency in version 1. Keep inventory reservation, delivery planning and notification preparation as in-process operations behind a small interface. Namespace Kafka topics under `wb.orders.*`.

## Consequences

The API, event payload and PostgreSQL row all carry the warehouse and currency, so consumers can validate the same business facts at every boundary. A warehouse/status index supports fulfillment-oriented reads. Version 1 cannot split one order across warehouses or currencies; introducing that behavior would require a deliberate aggregate and event-schema change. External warehouse side effects still need their own idempotency contract before replacing the current controlled operation.
