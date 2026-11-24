---
type: "topic-research"
id: "brief"
title: "PostgreSQL Push-Based Consumer Delivery"
date: "2026-09-18T14:49:27+00:00"
author: "Toby Clemson"
producer: "research-topic"
status: "complete"
kind: "brief"
source_profiles: ["web", "openalex"]
tags: ["research", "topic-research", "brief"]
last_updated: "2026-09-18T14:49:27+00:00"
last_updated_by: "Toby Clemson"
schema_version: 1
---

# PostgreSQL Push-Based Consumer Delivery

## Subject
Alternatives to the current polling-based consumer infrastructure in this event
sourcing library — replacing periodic database polling with push/notification-based
delivery of new work to consumers, while remaining backed solely by PostgreSQL.

## Motivation
Every subscriber is currently driven by wrapping its `consume_all` in a
`PollingService` that fires on a fixed interval (200ms default,
`services/polling.py:17`). On each tick the consumer loads its last-consumed
position, turns it into a query constraint, and issues a paginated `SELECT` scan
against the `events` table through `PostgresEventStorageAdapter.scan()`. The
singleton and distributed brokers run their own interval loops (10–30s) that poll
Postgres subscriber/subscription state stores to distribute sources. No
push/notification mechanism exists in the codebase today.

This polling design carries two costs. First, constant query load against the
database, proportional to (consumer count × poll frequency), the large majority
of which returns no new work. Second, a baseline read latency of, on average, half
the poll interval for every event before any consumer observes it. As consumer
count and event throughput grow, both the wasted query load and the latency floor
become material.

The goal is to determine whether a push/notification approach — where PostgreSQL
signals consumers as new events arrive — can eliminate wasteful polling and reduce
baseline latency, while keeping PostgreSQL as the sole backing store (no additional
broker infrastructure such as Kafka or Redis). There is an existing Postgres
advisory-lock coordination layer (`locks/postgres.py`) that a push redesign could
build on.

## Scope
In scope:
- PostgreSQL-native notification mechanisms: `LISTEN`/`NOTIFY`, `pg_notify`, and
  trigger-driven signalling.
- Hybrid push+poll designs: notify-as-hint with a polling safety net,
  adaptive/backoff polling, long-polling, and queue-style claiming
  (e.g. `SELECT ... FOR UPDATE SKIP LOCKED`).
- Operational concerns: connection limits and connection-pooler (PgBouncer)
  interaction, missed-notification durability and at-least-once delivery
  guarantees, ordering, and scaling to many consumers/subscriptions.

Out of scope:
- Logical replication / change data capture (logical decoding, replication slots,
  wal2json, Debezium-style pipelines) — deliberately deprioritised for this round.
- Non-PostgreSQL backing stores and external message brokers.
- Concrete implementation or code changes — this is decision-informing research,
  not a design.

## Questions
- What PostgreSQL-native mechanisms exist for pushing new-work notifications to
  consumers, and how does each work?
- What are the delivery guarantees and failure modes of `LISTEN`/`NOTIFY` —
  notifications lost when no listener is connected, transaction/commit semantics,
  payload size limits, queue overflow?
- How does a notify-based approach interact with connection pooling (PgBouncer
  transaction vs session mode) and per-listener connection cost/limits?
- What hybrid push+poll patterns reconcile push's efficiency with polling's
  durability, and how are missed notifications detected and recovered?
- How do these approaches scale as the number of consumers, subscriptions, and
  event throughput grow?
- Across the candidate approaches, what are the trade-offs in baseline latency,
  steady-state load, complexity, durability, and operability, and which best fits
  an event sourcing library that must remain PostgreSQL-backed?

## Success Criteria
A dossier that:
- Catalogues the viable PostgreSQL-native and hybrid approaches with a clear
  explanation of how each works.
- Presents a comparison matrix across the axes that matter here: baseline latency,
  steady-state query load, delivery durability / at-least-once guarantees,
  ordering, pooler compatibility, connection cost, operational complexity, and
  scalability to many consumers.
- Surfaces the operational gotchas (missed notifications, pooler incompatibility,
  connection exhaustion) explicitly rather than burying them.
- Concludes with a recommended direction for this library's constraints
  (PostgreSQL-only, potentially many subscribers, at-least-once processing already
  tracked via consumer state stores), giving the reasoning and the conditions under
  which the recommendation holds — a high-level feasibility read first, converging
  on a concrete mechanism to pursue.
