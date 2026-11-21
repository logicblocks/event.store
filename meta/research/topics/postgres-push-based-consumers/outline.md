---
type: "topic-research"
id: "outline"
title: "PostgreSQL Push-Based Consumer Delivery"
date: "2026-10-05T10:09:02+00:00"
author: "Toby Clemson"
producer: "research-topic"
status: "complete"
kind: "outline"
tags: ["research", "topic-research", "outline"]
last_updated: "2026-10-05T10:09:02+00:00"
last_updated_by: "Toby Clemson"
schema_version: 1
---

# PostgreSQL Push-Based Consumer Delivery

## Focus Areas

One focus area per line; each becomes a finding per assigned profile. This is
the working log — checkboxes track which focus areas have been researched.

## Round 1

- [x] What PostgreSQL-native mechanisms can signal new work to consumers — `LISTEN`/`NOTIFY`, `pg_notify()`, row- versus statement-level trigger-driven notification, and advisory-lock or other wake-up primitives — and how does each work internally, from the notifying transaction's commit to delivery on a listening connection? — profiles: web
- [x] What delivery guarantees and failure modes does `LISTEN`/`NOTIFY` have — notifications lost while no listener is connected or during reconnection, commit-time delivery and duplicate collapsing within a transaction, the 8000-byte payload limit, the notification queue's size limit and behaviour when it fills, and the global lock taken when committing a transaction that issued `NOTIFY`? — profiles: web
- [x] How does `LISTEN`/`NOTIFY` interact with connection poolers and connection limits — PgBouncer session versus transaction mode, other poolers and managed proxies (such as RDS Proxy pinning, Supavisor and PgCat), and the cost of a dedicated session connection per listener — and how does psycopg 3 expose asynchronous notifications and support keeping a long-lived listener connection healthy (keepalives, idle timeouts, reconnection)? — profiles: web
- [x] What hybrid push-and-poll patterns reconcile notification efficiency with polling durability — notify as a wake-up hint over a durable log, safety-net polling, adaptive and backoff polling, long-polling, and coalescing or debouncing of notification bursts — and how are missed notifications detected and recovered from? — profiles: web, openalex
- [x] How do established PostgreSQL-backed event stores and job queues — such as Marten's async daemon, Message DB, River, Oban, Graphile Worker, GoodJob, pg-boss, Solid Queue and pgmq — deliver new work to consumers, and what design choices, problems and lessons have their maintainers published? — profiles: web
- [x] In an append-only PostgreSQL event log whose positions come from a `BIGSERIAL` sequence, how can a consumer woken by a notification avoid skipping events that concurrent transactions commit out of position order — transaction-id or snapshot-based high-water marks (`pg_current_xact_id`, `pg_snapshot_xmin`), gap detection with timeouts, or serialised appends — and how does notification order relate to commit order? — profiles: web
- [x] How do `SELECT ... FOR UPDATE SKIP LOCKED` and advisory-lock-based claiming distribute work among competing consumers, how do they combine with notification-driven wake-ups, and how do they preserve per-stream or per-partition ordering? — profiles: web
- [x] How do notification-driven and polling-based delivery scale as listeners, channels, subscriptions and event throughput grow — measured notification latency and throughput, commit-time contention, thundering-herd wake-ups and fan-out cost — and what does published research on push versus pull delivery and adaptive polling say about the latency-versus-load trade-off? — profiles: web, openalex
