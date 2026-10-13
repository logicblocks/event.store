---
type: "topic-research"
id: "04-what-hybrid-push-and-poll-patterns-reconcile-notification-ef-web"
title: "What hybrid push-and-poll patterns reconcile notification efficiency with polling durability — notify as a wake-up hint over a durable log, safety-net polling, adaptive and backoff polling, long-polling, and coalescing or debouncing of notification bursts — and how are missed notifications detected and recovered from?"
date: "2026-10-05T13:56:47+00:00"
author: "Toby Clemson"
producer: "research-topic"
status: "complete"
kind: "finding"
round: 1
depth: 3
question: "What hybrid push-and-poll patterns reconcile notification efficiency with polling durability — notify as a wake-up hint over a durable log, safety-net polling, adaptive and backoff polling, long-polling, and coalescing or debouncing of notification bursts — and how are missed notifications detected and recovered from?"
source_profile: "web"
tags: ["research", "topic-research", "finding"]
last_updated: "2026-10-05T13:56:47+00:00"
last_updated_by: "Toby Clemson"
schema_version: 1
---

# What hybrid push-and-poll patterns reconcile notification efficiency with polling durability — notify as a wake-up hint over a durable log, safety-net polling, adaptive and backoff polling, long-polling, and coalescing or debouncing of notification bursts — and how are missed notifications detected and recovered from?

## Question

Which hybrid designs give a PostgreSQL-backed consumer the low latency of `LISTEN`/`NOTIFY` while keeping the durability of polling? The motivating case is the subscriber side of an event store built on psycopg 3. Five patterns are in scope:

- treating a notification as a wake-up hint over a durable log;
- safety-net polling;
- adaptive and backoff polling;
- long-polling, meaning a bounded wait instead of a sleep;
- coalescing or debouncing bursts of notifications.

The question also asks how a consumer finds out that it has missed notifications, and how it recovers.

## Findings

### Summary

Every mature PostgreSQL-backed consumer examined that uses `LISTEN`/`NOTIFY` follows the same hybrid design. That covers the job queues River, Oban, Graphile Worker, GoodJob, Que, Procrastinate, PgQueuer and PGMQ, and the event stores Commanded's Elixir EventStore and the eugene-khyst reference implementation. The design has five parts:

1. **The table holds the truth; `NOTIFY` is only a lossy hint.** A notification means "look now". It never carries the work itself, so losing one costs latency, not correctness.
2. **Every read starts from the consumer's own durable checkpoint.** Any number of hints therefore collapse into one read, and a duplicate costs one empty read. Recovering from any outage uses the same read.
3. **The consumer waits on the notification socket with a timeout instead of sleeping.** A hint ends the wait early. The timeout doubles as the safety-net poll and leaves room for a heartbeat.
4. **Bursts are absorbed in three places.** PostgreSQL folds identical notifications within one transaction. Senders debounce. Receivers limit how often they fetch, or drop redundant hints.
5. **Missed notifications are mostly recovered by design rather than detected.** On every connect and reconnect the consumer issues `LISTEN` and then catches up from its checkpoint. Together with the safety-net poll, this caps the cost of any loss. Explicit detection is layered on top:
   - gaps in positions carried in payloads;
   - liveness probes on the listener, most usefully a canary `NOTIFY` sent from another connection;
   - TCP-level failure detection;
   - checks on how full the notification queue is.

Two constraints decide whether this design is both correct and fast for an event store:

- **A notification proves that something committed. It does not prove that everything below it committed.** Global positions drawn from a sequence are reserved before commit, so they can become visible out of order. The read a hint triggers must follow a gap-safe rule. Some gaps also close without any notification, so a consumer that is holding at a gap needs a short timed re-check.
- **`NOTIFY` is not free for writers.** Every notifying commit takes a cluster-wide lock and holds it through its WAL flush and any wait for a synchronous standby. Before PostgreSQL 19, every listening connection in the database is woken as well. Debouncing at the sender and keeping one listener connection per process are the main ways to reduce the cost.

### Why a notification can only ever be a hint

PostgreSQL's notification rules make `NOTIFY` unusable as a delivery mechanism on its own:

- **Nothing is stored for listeners that are absent.** `LISTEN` takes effect only when its transaction commits, and a session receives only notifications committed after that point. Registrations end with the session. Postgrex's documentation says notifications sent while a listener is disconnected "are not queued and cannot be recovered".
- **Notifications are delivered only between transactions.** A listening session that is inside a transaction receives nothing until the transaction ends. A listener left in a long transaction also blocks queue cleanup for the whole server.
  - The shared queue holds about 8 GB by default (`max_notify_queue_pages`).
  - Once it is full, "transactions calling `NOTIFY` will fail at commit".
  - So a single stalled listener can eventually break writers.
- **Poolers and proxies can disable or pin `LISTEN`.**
  - PgBouncer marks `LISTEN` as "Never" supported in transaction and statement pooling modes, and Neon's pooled endpoints exclude it too.
  - Graphile Worker warns that this setup can lose events, and Oban turns its notifications off under it.
  - Session pooling works, but each listener permanently occupies one server connection.
  - Amazon RDS Proxy accepts `LISTEN` but pins the client session to one database connection.
- **Idle limits can kill the listener.**
  - `idle_session_timeout` (PostgreSQL 14+) ends sessions that are "waiting for a client query" outside a transaction. A parked listener matches that definition. That incoming notifications do not reset the timer is an inference from the definition; it is not documented.
  - Pooler and proxy limits can end the session too: RDS Proxy's `IdleClientTimeout` and PgBouncer's `client_idle_timeout`.
  - So can load-balancer idle timeouts: 350 s on an AWS Network Load Balancer and 4 minutes on Azure Standard Load Balancer.
- **Failover throws everything away.** Hot standbys reject both `LISTEN` and `NOTIFY`. A standby promoted to primary has no listener registrations and none of the old primary's queued notifications. That second point is an inference from the documented rules.
- **A silently dead connection looks exactly like a quiet system.** An idle listener has nothing in flight, so with Linux defaults it can take more than two hours to notice that the peer is gone.

PostgreSQL's own documentation describes notifications in these terms. A notification on a channel named after a table "essentially means, 'I changed this table, take a look at it to see what's new'". The documentation suggests sending it from a statement trigger so that no writer can forget to.

### The core pattern: wake up, then read from a durable checkpoint

#### Connect, LISTEN, catch up, wait

The `LISTEN` documentation sets out the sequence that every implementation follows. Postgrex recommends the same sequence for recovering after a reconnect:

1. Execute and commit `LISTEN`. In autocommit mode this happens as soon as the statement completes.
2. In a new transaction, read the log from the stored checkpoint.
3. Wait for notifications.

The order matters. Suppose the catch-up read ran before `LISTEN`. An append committed between the two would be invisible to both, and would wait for the next poll. With `LISTEN` first, such an append is either seen by the read or produces a notification. The worst case is a redundant read, which the documentation calls "usually harmless".

Since psycopg 3.2.4, a notification that arrives between `LISTEN` and the start of the `notifies()` generator is buffered rather than lost. The changelog warns that this can produce duplicates in code that used notifications to bridge that gap.

#### How production systems apply it

- **Job queues.** In Graphile Worker, River, Oban, GoodJob, Que, Procrastinate and PgQueuer, a notification tells an idle worker to run its normal fetch query.
  - Procrastinate's worker "polls the database regardless of" its `listen_notify` setting. A notification only sets an `asyncio.Event`. The worker waits for whichever comes first: that event, the 5-second polling interval, or a stop signal.
  - Que's locker always runs a `Listener` alongside per-queue `Poller`s.
- **Commanded's Elixir EventStore.**
  - A row-level trigger on the `streams` table sends `stream_uuid,stream_id,first_version,last_version` (for example `stream-12345,1,1,5`) on the channel `<schema>.events`.
  - Each append updates its own stream's row and the `$all` row, so each append sends one notification for each.
  - One listener per node reads exactly the notified range once, on a pooled connection, and broadcasts it to subscriptions in the same process.
  - Persistent subscriptions store their last acknowledged position, which gives at-least-once delivery. Advisory locks make each named subscription run only once across a cluster.
- **Outbox and event-sourcing reference code.** The eugene-khyst reference implementation pairs `LISTEN`/`NOTIFY` wake-ups with a 1-second fallback poll.

#### One listener connection per process

Brandur Leach's "notifier pattern", which River uses, keeps one `LISTEN` connection per process. Any number of components in that process can subscribe to topics through it. Commanded's EventStore does the same ("One connection per node is used"), as do Oban and the Rails Action Cable adapter. The pattern saves cost in three places:

- **Fan-out on the server.** Before PostgreSQL 19, every notifying commit signals every listening backend in the database, whatever channel it listens on. Each woken backend starts a transaction, takes a snapshot, scans the queue and filters by channel.
  - Joel Jacobson ran a ping-pong benchmark with added idle listeners. Throughput fell from 9,126 TPS with none to 6,233 with 10, 2,020 with 100 and 238 with 1,000.
  - That works out to roughly 4 µs per listening connection per round trip, on hardware the benchmark does not describe.
  - With P processes each running C consumers, one dispatcher per process cuts the number of woken backends from P × C to P.
- **Connections.** Each process holds one long-lived session-level connection instead of one per consumer. The rest of the application can then go through a transaction-mode pooler.
- **Reads.** A dispatcher can read a notified range once and share it, instead of every consumer re-querying after the same wake-up.

The pattern does nothing for the commit lock that writers take, and fan-out still grows with the number of processes.

Commanded's pipeline shows how to fan out without letting one slow subscriber hold up the rest. It has three stages:

1. **A listener that never blocks.** It drains every notification off the connection into an in-memory queue, whatever the downstream demand, so nothing backs up into PostgreSQL. That queue is unbounded. A psycopg version can bound memory by merging pending notifications into "highest notified position per category".
2. **A dispatcher that never waits.**
   - It routes by key through a registry local to the node (an Elixir `Registry` with duplicate keys).
   - It applies each subscriber's filter, then sends without blocking.
   - Commanded dropped an earlier design built on `GenStage.BroadcastDispatcher`, which by definition paces every consumer to the slowest one.
3. **Back-pressure inside each subscription.** Each subscription has a bounded buffer. When the buffer overflows, it stops accepting pushed events and catches up from storage.

Action Cable shows some further details:

- It keeps one listener per process on its own connection.
- It issues `LISTEN` when a channel gets its first subscriber and `UNLISTEN` when the last one leaves.
- It loops on `wait_for_notify(1)`. The one-second timeout lets the same thread handle subscribe, unsubscribe and shutdown commands.

Oban keeps its registry outside the notifier process, so "Registration is durable across notifier restarts".

A psycopg version can deliver in one of two ways:

- **Read once, then push the events.** Put them into each matching subscription's bounded `asyncio.Queue` with `put_nowait`, and flag the subscription as overflowed on `QueueFull`. Awaiting `put()` instead would bring back pacing by the slowest consumer.
- **Wake up only.** Set a per-subscription flag or `asyncio.Event`, and let each subscription read from its own checkpoint. Duplicate wake-ups merge naturally and the code is simpler, but every wake-up costs one query per subscription.

`asyncio.Queue` is not thread-safe. If a synchronous listener runs on its own thread, it must hand work to the event loop through a thread-safe path.

### Making a notification-triggered read gap-safe

#### Why the wake-up does not make the read safe

A PostgreSQL sequence hands out its value when `nextval` runs, not when the transaction commits. Aborted transactions never give their numbers back, and `INSERT ... ON CONFLICT` can use one up without aborting at all. The documentation says sequences "cannot be used to obtain 'gapless' sequences".

Two concurrent appenders can therefore draw 11 and 12 and commit 12 first. Notifications are delivered in commit order, so the notification for 12 can reach a consumer while 11 is still in flight. A consumer that reads everything after its checkpoint, then moves the checkpoint to the highest position it saw, will never see 11. Protean's ADR-0025 calls this a "permanent, silent hole", and it happens while the consumer appears to have no lag. Marten's async daemon had the same bug under concurrent appends until version 9.16.1.

The hybrid therefore needs a rule for which visible prefix of the log is safe to consume. That rule must apply to reads triggered by hints and to polled reads alike.

#### Four families of safe-prefix rule

There are four families of rule; the high-water mark comes in two variants.

| Approach | Examples | Read latency added | Write-side cost | Residual risk |
|---|---|---|---|---|
| Commit-ordered allocation | Commanded's `$all` counter row; an exclusive `pg_advisory_xact_lock` taken "as the very last thing before commit" (Kevin Grittner); Message DB's per-category advisory lock; logical decoding | None | All appends in the lock's scope are serialised through their commit flush | A throughput ceiling. A per-category scope leaves cross-category reads unsafe. Logical decoding needs replication-slot management and WAL retention |
| Transaction-ID horizon | Each row stamped with `pg_current_xact_id()`; reads limited to rows below `pg_snapshot_xmin(pg_current_snapshot())`, ordered and checkpointed by `(transaction_id, position)` (Oskar Dudycz, eugene-khyst, Emmett) | Until every writing transaction with a lower ID, anywhere in the cluster, has ended | One `xid8` column | An unbounded stall behind any long-running, idle-in-transaction or prepared transaction anywhere in the cluster |
| High-water mark with a wall-clock timeout | Protean (`gap_timeout_seconds`, 5 s); Marten before 9.16.1 (3 s) | Usually milliseconds; up to the timeout for dead gaps | None | Loses any append that takes longer than the timeout |
| High-water mark with evidence-based proof | Marten 9.16.1's gap-liveness probe; Sequin's advisory-lock ceiling | Usually milliseconds; otherwise until candidate reservers end | Catalog queries, or advisory locks on every insert | Stalls behind a reserver that really is still running, unless an opt-in override is set |
| Gap-aware tracking token | Axon (`gapTimeout` 60 s, `maxGapOffset` 10,000) | None; events that fill a gap late arrive out of order | None | Loses events that commit after their gap has timed out |

**Commit-ordered allocation moves the whole cost onto writers.**

- **Why the lock is held so long.** Transaction-level advisory locks and row locks are released only after three things have happened: the commit record has been flushed, any synchronous standby has acknowledged it, and the transaction has become visible. The time each append holds the lock (H) can therefore never be shorter than a durable commit.
- **The ceiling.** Throughput tops out at roughly B / H events per second, where B is the number of events per transaction, however many writers there are. Appends also cannot share WAL flushes through group commit.
- **Measured effect.** Commanded's README benchmark (100-event batches, local database) shows the shape. Going from 1 writer to 50 raised throughput only from 4,929 to 8,586 events per second, while per-writer latency rose from about 20 ms to about 580 ms.
- **Practitioner reports.**
  - Sequin saw lock waits of several seconds on a counter row.
  - Oskar Dudycz calls the singleton counter the "nuke option".
  - Laurenz Albe (Cybertec) says it will "serialize all transactions".
- **Ways to reduce the loss.**
  - Write more events per transaction.
  - Take the lock as late as possible, and send `COMMIT` in the same round trip, for example with psycopg's pipeline mode.
  - Narrow the lock's scope, as Message DB does by locking per category.
  - Bound the queue of waiting writers with `lock_timeout`, as the Python `eventsourcing` library does.

One interaction matters for a push-based store. `NOTIFY` already serialises notifying commits through their flush, as described below. So a store that issues `NOTIFY` in every append transaction already pays most of the cost of a store-wide allocation lock. Adding one mainly makes the locked window start earlier in each transaction. This is an inference from the PostgreSQL source.

**The transaction-ID horizon needs no gap detection.** Rolled-back or burned numbers stop mattering once the horizon has passed the transaction that finished with them. However, `pg_current_snapshot()` is built from every backend on the server, with no filter by database. A migration in another database, a session left idle in a transaction, or an unresolved prepared transaction can therefore hold back every consumer. Sequin rejected the approach as "a non-starter" for this reason. The transaction holding the horizon back may also never send a wake-up on the event channel. A consumer driven by hints that is stuck behind the horizon therefore needs a timed re-check.

#### Proving a high-water-mark gap is dead

Looking at a hole cannot show whether it is dead. The only test is whether any transaction that could have reserved the missing numbers is still running. Once none is, a fresh read that still finds the hole proves it is permanent. Marten's daemon works this way:

- **Observation.** When the mark gets stuck, the daemon records three things:
  - the time it first saw the gap, taken from the server's `transaction_timestamp()`;
  - `pg_snapshot_xmax(pg_current_snapshot())`;
  - an "allocation fence": the latest server time at which the sequence's highest allocated value was still at or below the stuck mark.
- **Grace period.** Nothing is probed until the gap is older than `StaleSequenceThreshold` (3 s by default), because most holes "fill in within milliseconds". The daemon polls the high-water mark every 250 ms under load and every 1 s when quiet.
- **Probe.** A single statement counts three kinds of possible live reserver:
  - sessions holding a `RowExclusiveLock` on the events tables whose transaction started before the gap was first seen;
  - any client backend in the current database whose transaction started before then;
  - in-progress transaction IDs below the recorded `xmax`.

  Two kinds of session are excluded: sessions idle in a transaction since before the allocation fence, and the daemon's own lock connections. Without the `pg_read_all_stats` role, the data used for exclusions is hidden, and the probe then errs towards holding the mark.
- **Decision.**
  - If any count is non-zero, the mark holds.
  - If all are zero, the mark advances to the highest committed position, "never the reserved last_value", and skipped spans can be recorded.
  - An opt-in setting, `SkipStaleGapsDespiteLiveTransactionsAfter`, trades safety for progress. The documentation recommends `idle_in_transaction_session_timeout` as the better backstop.

Marten discussion #4953 found four ways in which the older wall-clock design skipped committed events:

- checks spread over several statements, whose READ COMMITTED snapshots disagreed;
- unconditional skips during projection rebuilds;
- a fallback on idle stores that jumped to `HighestSequence - 32`;
- skip decisions based on elapsed time rather than evidence.

How narrowly the evidence is scoped decides whether unrelated work stalls the consumer:

- The snapshot's `xmin` and its in-progress list (`xip`) cover the whole cluster.
- `pg_stat_activity` can be limited to `current_database()`.
- `pg_locks` on the events table covers only that table, but it misses a reserver that has called `nextval` and not yet inserted.

An inference from `sequence.c`, not yet tested, would be narrower still. `nextval` takes a `RowExclusiveLock` on the sequence itself, before it returns any value, and holds it until the end of the top-level transaction. If the sequence uses `CACHE 1`, the transactions holding that lock would be exactly the set of possible reservers.

All of these proofs need the sequence to use `CACHE 1`, which is the default. With a larger cache, a session can hand out a low number in a later transaction. Two more rules apply to probes:

- Run each probe in a fresh transaction, because `pg_stat_activity` takes one snapshot per transaction.
- Run probes against the primary, where the appenders are.

#### What this means for the hybrid loop

- **Apply the same safe-prefix rule to every read.** Persist only the safe checkpoint: the contiguous prefix or the horizon tuple, never the highest position seen.
- **Expect some gaps to close silently.** A gap left by an appender that is still running closes with that appender's own notification, because notifications arrive after commit and in commit order. Three kinds of gap produce no notification at all:
  - rollbacks;
  - numbers burned by `ON CONFLICT`;
  - transactions that append nothing but hold back the transaction-ID horizon.

  A consumer holding at a gap therefore needs a short re-check timer, separate from the long safety-net poll.
- **Choose by what the library can change.**
  - With modest append concurrency, commit-ordered allocation is the simplest choice and adds no read latency.
  - If the schema and the checkpoint format can change, the transaction-ID horizon avoids detection logic, at the price of stalls caused by anything in the cluster.
  - To keep a single global position with parallel writers, use a high-water mark with evidence-based gap proof. Skipping gaps on a timer alone is known to lose events.

### Waiting with a timeout instead of sleeping

The simplest way to combine push and poll is to replace the poll loop's sleep with a bounded wait on the notification socket. The consumer reads until it has caught up. It then blocks until a notification arrives or the interval runs out, and reads again in either case.

#### psycopg 3 mechanics

- **Autocommit is required.** psycopg's documentation says to use it to receive notifications "in a timely manner". Without it, "even a simple `SELECT` will start a transaction". The session then sits idle in a transaction, holding back both its own notifications and queue cleanup.
- **Bounded waits.** `Connection.notifies(*, timeout=None, stop_after=None)` gained both parameters in 3.2. When the timeout expires, the generator simply ends; it does not raise. With `stop_after=n` it may yield more than n notifications if several arrive in one packet.
- **The generator holds the connection lock for its whole loop.** No heartbeat query can run on that connection while it waits. The loop must let a bounded generator finish, run its heartbeat, and then start a new generator. Breaking out early leaves a suspended generator holding the lock until it is garbage-collected.
- **Minimum version.** Require psycopg 3.2.10 or later, and use only the generator.
  - Before 3.2.4, notifications that arrived between two `notifies()` calls were lost. Issue #962 reports exactly the wait, health-check, wait pattern.
  - 3.2.4 fixed that, and 3.2.10 fixed the memory leak the fix caused.
  - 3.2.10 also deprecated mixing `add_notify_handler` with the generator. Handlers fire only while the connection is running a query, so they do not suit a blocking wait anyway.
- **Typed disconnect errors.** Since 3.2.4, psycopg raises the server's actual error, such as `AdminShutdown` (57P01) or `IdleSessionTimeout` (57P05). All class-57 errors are subclasses of `OperationalError`, so a single handler covers them.
- **Your own notifications.** `Notify.pid` carries the sending backend's PID. Comparing it with `conn.info.backend_pid` lets a consumer skip notifications it sent itself, as the `NOTIFY` documentation suggests.

#### How production loops are built

- **Procrastinate**
  - opens a standalone autocommit `AsyncConnection`, built from the pool's settings but not taken from the pool;
  - issues `LISTEN` and loops on `notifies()` with a 30-second timeout;
  - runs `SELECT 1` after each wait;
  - on `OperationalError`, reconnects and listens again from an outer loop.
- **River**
  - blocks in `WaitForNotification` on one goroutine, while a select loop pings the connection every 5 seconds so that "dead or flaky connections are now actively detected";
  - reconnects with exponential backoff and re-issues `LISTEN` for every subscription;
  - expects subscribers to re-poll after a reconnect.
- **Que's locker** checks every 50 ms for notifications or an expired timer. Its default poll interval is 5 s.
- **Npgsql** offers `Wait` and `WaitAsync` with a timeout, and recommends keepalives.

#### Long-polling elsewhere

Other messaging systems hold the wait on the server instead of sleeping, which supports the same design:

- **Amazon SQS long polling** (`WaitTimeSeconds` up to 20 s) returns as soon as a message exists, which "reduce[s] the number of empty responses".
- **Kafka fetches** wait up to `fetch.max.wait.ms` (500 ms by default) for `fetch.min.bytes` of data, which both long-polls and batches.
- **KurrentDB's `ES-LongPoll` header** makes a read at the head of a stream wait for new data.
- **PGMQ's `read_with_poll`** runs the loop inside PostgreSQL. It re-checks every `poll_interval_ms` (100 by default) for up to `max_poll_seconds` (5 by default). That is still polling, and it holds a connection for the whole wait.

### Safety-net polling and degraded modes

The safety-net poll does three jobs:

- it caps how late a missed hint can make a consumer;
- it finds work that nothing will ever announce;
- it is the fallback when notifications are unavailable.

Where notifications exist, libraries treat polling as a backstop:

| System | Poll interval | Notes |
|---|---|---|
| River | `FetchPollInterval` 1 s | "provides a fallback"; jobs are normally picked up "~immediately after insert via LISTEN/NOTIFY" |
| Oban | 1 s | Insert notifications exist so queues need not wait "up to 1s" |
| Graphile Worker | 2 s | 30–60 s "is probably fine" |
| Procrastinate | `fetch_job_polling_interval` 5 s | "can reasonably be set to a higher value" when notifications are on |
| Que | 5 s | Listener and pollers always run together |
| eugene-khyst reference implementation | 1 s | Fallback behind `LISTEN`/`NOTIFY` |
| Marten (high-water detection) | 250 ms under load, 1 s when quiet | Polling only |

Some work has no event to announce it. Graphile Worker polls for future-dated and retried jobs, because "there's nothing to generate an event when the time 'ticks over'". GoodJob calls an overflowing scheduled-job cache "another reason to poll".

Several libraries make the fallback an explicit mode:

- **River's `PollOnly`** issues no `LISTEN` at all. River enters it automatically when the driver cannot listen.
- **Oban's job stager** switches from "global mode" to "local mode" when its notifier is found to be isolated. In global mode a leader queries and fans out; in local mode every queue polls every second.
- **PgQueuer**, on listener failure, either shuts the worker down or keeps polling on its dequeue timeout.

### Adaptive and backoff polling

When polling is the only signal, systems scale its rate to recent results:

- **Azure Functions' queue trigger** uses a "random exponential back-off algorithm". It checks again 100 ms after finding a message and about 200 ms after an empty check. The gap grows up to `maxPollingInterval`, one minute by default.
- **River** goes the other way when there is a backlog. From v0.14.0 it fetches again immediately after a full batch, or after it skipped a triggered fetch because it was already full. From v0.24.0 each queue can set its own `FetchCooldown` and `FetchPollInterval`.
- **Oban** backs its liveness checks off from 5 s to 60 s.

In a hybrid loop, the timers do different jobs and have different limits, so they should be kept separate. The table below is a synthesis of the sources above:

| Timer | Purpose | Limit | Adapts? |
|---|---|---|---|
| Heartbeat | Stop idle limits closing the listener; detect a silently dead path | Below every idle timeout on the path (`idle_session_timeout`, pooler and proxy limits, load balancers). Time-based: run whenever no query has been sent within the interval | No |
| Safety-net poll | Recover missed hints and unannounced work | Its ceiling is the worst-case latency for a missed hint, so it must stay within the latency budget | May back off while idle; any notification resets it to an immediate read |
| Gap re-check | Release a hold on a gap that will close without a notification | Short. Marten re-checks its high-water mark every 250 ms under load | Runs only while holding at a gap |
| Refetch | Drain a backlog | Immediately after a full batch | Yes, by batch size |
| Reconnect | Avoid hammering a server that is restarting or failing over | Exponential with a cap (River; psycopg's pool, up to `reconnect_timeout`) | Yes |

### Coalescing and debouncing bursts

#### What each notification costs

Issuing `NOTIFY` on every append has two costs, and they grow with different things.

**The first cost is commit serialisation, which grows with notifying commits per second.**

- **The lock.** `PreCommit_Notify` takes an `AccessExclusiveLock` on a shared object (database 0) "that we hold till after commit", so that queue entries appear in commit order. Because the object is shared, the lock serialises notifying commits across the whole cluster.
- **How long it is held.** The lock is held through `RecordTransactionCommit`, which includes the WAL flush and any wait for synchronous replication. It is released only after the transaction becomes visible. Listeners are signalled afterwards, outside the lock.
- **Measurements.**
  - **pgbench, 100 clients, no listeners (Arseniy Mukhin).** A one-row `INSERT` ran at 30,982 TPS, the same insert plus `NOTIFY` at 818 TPS, and `NOTIFY` alone at 87,751 TPS.
    - 818 TPS is about 1.2 ms per serialised commit, which fits one fsync per commit.
    - Benchmarks of `NOTIFY` alone look fast because those transactions write no WAL.
  - **1,000 clients, one listener (Joel Jacobson).** 7,679 TPS with the lock, against 95,430 TPS in a prototype with the lock removed.
  - **DBOS.** A trigger issuing `NOTIFY` on every stream write "could not sustain more than 2.9K stream writes per second", without visibly using up any resource.
  - **Recall.ai.** Lock waits above 1,000 ms were logged, with hundreds of sessions queued, while CPU, I/O and network use fell. That is the signature of a queue of writers stuck behind one global lock. Recall.ai removed `NOTIFY` from its write path.

`synchronous_commit` and synchronous standbys decide how long the lock is held:

| `synchronous_commit` | Without synchronous standbys | With synchronous standbys |
|---|---|---|
| `off` | No flush, no wait | No flush, no wait |
| `local` | Local WAL flush | Local flush only |
| `on` (default) | Local flush | Local flush, then wait until standbys have flushed the commit |
| `remote_write` | Local flush | Local flush, then wait until standbys have written it |
| `remote_apply` | Local flush | Local flush, then wait until standbys have replayed it |

The documentation says the minimum added wait is one round trip between primary and standby. The ceiling on notifying commits is roughly 1 ÷ the mean hold time, shared across every notifier in the cluster. Adding 1 ms of replication to a 1.2 ms flush would cut it from about 800 to about 450 commits per second. That figure is illustrative arithmetic, not a measurement.

The lock also spreads failures across the cluster:

- A commit stuck waiting on an unavailable synchronous standby keeps the lock.
- Every notifying transaction in every database then queues behind it, including transactions that set `synchronous_commit` to `off`, because they are waiting to acquire the lock, not to commit.
- For diagnosis, the holder shows the wait events `IO`/`WalSync` or `IPC`/`SyncRep`. The waiters show `Lock`/`object` and are logged as "object 0 of class 1262 of database 0".

A transaction whose only action is `NOTIFY` writes no WAL before its commit record. It therefore takes the asynchronous commit path, whatever the setting.

**The second cost is listener fan-out, which grows with the number of listening connections.** Before PostgreSQL 19, `SignalBackends()` signals every listening backend in the sender's database. PostgreSQL 13 had already stopped it signalling other databases. In the benchmark above this cost about 4 µs per listener per round trip. Many writers with few listeners run into the lock first; many listeners run into the signalling cost.

**PostgreSQL 19.** Commit 282b1cde9, Joel Jacobson's patch committed by Tom Lane on 2026-01-15, changes the signalling:

- **What it does.** It adds a shared map from each channel to the backends listening on it. Only interested listeners are woken. Uninterested listeners have their queue positions advanced without being woken.
- **Release status.** The PostgreSQL 19 release notes were still a draft as of September 2026. They describe the change as "Improve NOTIFY to only wake up backends that are listening to specified notifications".
- **Claimed gain.** The commit claims "integer multiples" of throughput when there are many listeners but only a few care about each message.
- **Limits.** "There is no improvement for the simplest case where every listener reads every message", which is exactly the single-global-channel case. The commit lock is untouched.
- **Proposals not in PostgreSQL 19.**
  - A "Write Notifications Through WAL" patch raised insert plus `NOTIFY` from 818 to about 33,800 TPS, but lowered `NOTIFY` alone to 34,000–36,000 TPS.
  - An out-of-order delivery setting was also proposed.

#### Folding inside PostgreSQL

PostgreSQL folds repeated notifications that have the same channel and an identical payload within one transaction. It never folds notifications from different transactions, or notifications with different payloads. It preserves order within a transaction, and commit order across transactions. Since PostgreSQL 13, the duplicate check switches to a hash table once a transaction has 16 pending notifications; before that it degraded to O(N²). A patch to let senders skip de-duplication altogether was returned with feedback and never merged.

Where `NOTIFY` is issued therefore decides how many notifications are sent:

- A row trigger with a constant payload folds to one notification per transaction per channel.
- A row trigger that carries each row's position sends one per event.
- A statement-level `AFTER INSERT ... REFERENCING NEW TABLE ... FOR EACH STATEMENT` trigger can send one per statement, carrying `max(position)`. It must skip statements that inserted no rows, because it fires for them anyway.
- Constraint triggers can be deferred to the end of the transaction, but they must be row-level. This is the form PGMQ uses.
- The library can call `pg_notify` itself, once per append transaction.

Folding cuts queue volume and listener work for appends that write several events. But a transaction that notifies still takes the commit lock and still signals listeners. Constant payloads therefore do almost nothing for single-event appends.

#### Debouncing at the sender

Only transactions that actually execute `NOTIFY` take the lock. Capping the number of notifying commits is therefore the most effective lever:

- **River.**
  - From v0.5.0, insert notifications moved out of triggers into client code, piggy-backed on the insert query.
  - A per-queue limiter sends `{"queue": ...}` only if that queue has not been notified within `FetchCooldown` (100 ms by default). The limiter calls the notify function "no more than once per waitDuration".
  - The total notification rate is roughly processes × queues ÷ cooldown.
- **DBOS.**
  - Writes stopped notifying inside their own transaction. Notifications are buffered in memory and flushed "in a single batch transaction".
  - Stream writes rose from 2,900 to 60,000 per second, with 15–100 ms wake-up latency.
  - Readers poll occasionally to catch notifications lost in a crash.
- **PGMQ (1.8.0+)** throttles to one notification per queue per `throttle_interval_ms` (250 ms by default), tracked in a table.
- **Oban** moved insert notifications into application code. Its scaling guide suggests turning them off when sub-second dispatch is not needed.
- **GoodJob** groups bulk enqueues by queue and scheduled time.

The commit path suggests two further options. Both are inferences, and neither has been benchmarked:

- **Notify from a separate transaction after the append commits.** That transaction writes no WAL, so it holds the lock without any flush or replication wait, and the append keeps full durability. The wake-up is no longer atomic with the append, though: a crash between the two commits loses it, and the safety-net poll has to cover that case.
- **Set `SET LOCAL synchronous_commit TO OFF` on appends.** This removes both waits, but it lets consumers act on events that can disappear in a crash, which is a serious risk for an event store.

`commit_delay` is unlikely to help, because notifiers that are serialised cannot share a flush.

Debouncing has two costs:

- It adds up to one window of wake-up latency.
- A limiter that fires only on the first write in a window can stay silent for the last append of a burst. Correctness then depends on a trailing notification or on the safety-net poll. Every system above keeps a poll for this reason.

#### Coalescing at the receiver

- **River's `FetchCooldown`** is also the minimum gap between fetches: "Jobs will only be fetched *at most* this often".
- **Graphile Worker** drops a notification when no worker is idle, "knowing that when one of the workers finishes they'll ask for the next job anyway".
- **The notifier pattern** delivers to each subscriber through a buffered channel with a non-blocking send, and discards the notification when the buffer is full.
- **A dispatcher** can merge pending notifications into the highest position per category.

All of these are safe only because the next read starts from the durable checkpoint. Any number of notifications between two reads collapse into a single "something changed".

#### When per-append `NOTIFY` becomes the bottleneck

- **Fine:** low hundreds of appending transactions per second, with a handful of listeners.
- **Limiting:** when notifying commits approach 1 ÷ the commit-flush time. That is hundreds per second on slow durable storage, about 800 in the pgbench run and about 3,000 in DBOS's setup. Bursts hit the limit sooner, because queueing delay grows well before the lock is saturated.
- **Listeners, before PostgreSQL 19:** `NOTIFY` throughput drops about 4.5× with 100 listeners and about 38× with 1,000.
- **Mitigations, most effective first:**
  1. Sender-side debouncing or batching, which needs a fallback poll.
  2. PostgreSQL 19's listener tracking, which helps only when many listeners use distinct channels.
  3. Constant payloads, which help only transactions that append several events.

  Keeping the number of listening connections small helps on every version.

### Channel and payload design

The choice of channels and payloads decides how much deduplication, filtering and gap detection a consumer can do without querying.

| Topology | Used by | Strengths | Costs |
|---|---|---|---|
| One global channel | Commanded EventStore (`<schema>.events`), GoodJob, Graphile Worker (`jobs:insert`), River (schema-prefixed topics) | One `LISTEN` per process; constant payloads fold | Every listener hears everything, so filtering moves into the payload or the read; no gain from PostgreSQL 19 |
| Per category or per queue | Procrastinate (`procrastinate_queue_v1#<queue>`), PGMQ | Consumers hear only what concerns them; on PostgreSQL 19 only they are woken | Before 19, every listener is still signalled. Supporting both all-events and per-category consumers costs a second notification per append (Procrastinate also sends to `procrastinate_any_queue_v1`) |
| Per stream | None of the systems surveyed | Exact targeting | Unbounded number of channels, constant `LISTEN`/`UNLISTEN` churn, and the 63-byte name limit |

Payloads come in three kinds:

1. **Empty or constant.** PGMQ sends `NULL`. These are minimal and fold well, but all the consumer can do is re-read.
2. **Routing keys.** Examples are GoodJob's `queue_name` and `count`, River's `{"queue": ...}`, and Graphile Worker's `count` of workers to wake. They allow filtering on the client side over a shared channel.
3. **Positions or position ranges**, as in Commanded. This is the only kind that supports deduplication, coalescing to the maximum position, and gap detection without a query. A bigint is far below the 8,000-byte payload limit.

Positional payloads are only as safe as the ordering of positions:

- **Commit-ordered positions.** Commanded's gap check works because its `$all` positions come from a row update, which serialises them in commit order.
- **Sequence positions.**
  - A jump in position does not prove a loss: the missing number may belong to a transaction that is still running, or to one that aborted.
  - The worse case is a consumer that has advanced its checkpoint naively. If it then drops a notification at or below that checkpoint as a duplicate, it loses the late committer's event for good.
- **Rule.** Treat a payload position as a lower bound on the store's high-water mark. It tells the consumer whether a read is worth issuing, not what to read. Deduplicating against it is safe only when the checkpoint is advanced by a gap-safe read.

Channel naming has two traps:

- **Length.** Identifiers keep at most 63 bytes. `LISTEN` truncates longer names, while `pg_notify` rejects them with "channel name too long".
- **Case.** Unquoted identifiers are folded to lower case, but `pg_notify`'s text argument is not.

River checks its longest schema-prefixed topic name for this reason, and Action Cable hashes long names with SHA-1. Lower-case names well under 63 bytes avoid both traps.

### Detecting and recovering from missed notifications

#### Recovery by design

The baseline is design, not detection:

- Every read starts from a persisted checkpoint and is idempotent.
- Every connect and reconnect runs `LISTEN` and then a catch-up read.
- The safety-net poll limits the cost of any loss to one interval.

After a reconnect, a dispatcher should also tell every subscription to catch up. Commanded's listener does not do this. As far as its code shows, a notification lost during a reconnect is recovered only when a later notification on the same topic reveals the gap.

#### Gap detection from payload positions

Commanded's subscription state machine compares the first event number of each pushed batch with `last_received + 1`:

- A batch that starts earlier is ignored as already seen.
- The expected batch is queued.
- A batch that starts later means "Missed event(s), request catch-up".

Catch-up reads storage from `last_sent + 1` in batches of `max_size` (1,000 by default). It returns to the subscribed state only when a read comes back empty and `last_sent == last_received`, so a notification that arrives during the read triggers another pass. Three other rules complete the picture:

- Overflowing `max_size` also forces a catch-up.
- `buffer_size` (default 1) limits how many unacknowledged events each subscriber may hold.
- Events rejected by a subscriber's filter are acknowledged automatically, so they never hold back the checkpoint.

With sequence-assigned positions, an apparent gap should prompt a read, not raise an alarm.

#### Detecting a dead or deaf listener

Closures that the server starts are easy to detect. `idle_session_timeout`, termination by an administrator and a fast shutdown all send a FATAL error; the socket becomes readable, and psycopg raises. A silently dead path sends nothing, so only a probe finds it. Each kind of probe covers different failures:

| Failure | `SELECT 1` heartbeat | `NOTIFY` to itself on the listener connection | Canary `NOTIFY` sent from another connection |
|---|---|---|---|
| Silent TCP or middlebox loss (with `tcp_user_timeout` set) | Detected | Detected | Detected |
| Listener stuck inside a transaction | Missed | Detected | Detected |
| Notification queue full | Missed | Detected (the commit fails) | Detected |
| `LISTEN` registration lost (a transparent reconnect, `DISCARD ALL`, a proxy session re-attached elsewhere) | Missed | Detected | Detected |
| Stall on the notify lock or commit path | Missed | Detected (as a slow round trip) | Detected |
| Connected to a hot standby | Missed | Detected (rejected) | Detected |
| Proxy that answers queries but drops unprompted messages | Missed | Missed | Detected |
| Transaction-mode pooler | Missed | Unreliable; may pass (inference) | Detected |
| Writers on another server, or writers that stopped notifying | Missed | Missed | Partly; only if the canary is sent through the writers' path |
| Notifications already missed | Missed | Missed | Missed |

A `NOTIFY` sent to itself misses the proxy case for two reasons:

- A session's own notifications come back inside that command's response, just before ReadyForQuery, not as an unprompted message on an idle connection.
- The sending backend only sets a flag on itself, so the signal another backend would send to wake a listener is never exercised.

There is also a trap in the heartbeat itself. On a psycopg connection without autocommit, a `SELECT 1` heartbeat creates the stuck-in-transaction failure while still reporting success.

Probes used in production:

- **Oban's "sonar".**
  - It broadcasts a ping every 5 s, backing off to 60 s, and forgets nodes not heard from for 120 s.
  - It classifies the notifier as isolated (hears nothing), solitary (hears only itself) or clustered.
  - Its Postgres notifier sends through the main connection pool and listens on a dedicated connection, so even its own ping crosses connections.
  - When isolated, it logs that functionality is degraded and switches to local polling.
- **PgQueuer's `listener_healthy()`.** Every 10 s it sends a `health_check_event` with a unique id on the queue's channel and waits up to 10 s for it to arrive. A timeout raises `FailingListenerError`, and the worker then either shuts down or keeps polling.
- **pg-listen's "paranoid checking"** runs `SELECT pg_backend_pid()` every 30 s and listens again when it throws.
- **Npgsql** recommends application-level keepalive pings while waiting for notifications.

Each canary is a full notifying commit. It takes the lock and, before PostgreSQL 19, wakes every listener in the database. N processes probing every T seconds add N/T notifying commits per second, which is negligible at intervals of a few seconds. On PostgreSQL 19, a private channel per process wakes only the prober.

The recommended combination is:

1. Use `SELECT pg_notification_queue_usage()` as the cheap heartbeat, with an alert threshold.
2. Every few seconds, send a canary carrying a nonce from the append-side pool.
3. If the canary does not arrive, reconnect, re-issue `LISTEN`, catch up, and rely on the safety-net poll in the meantime.

A passing probe only shows that the path works now. It never shows that nothing was missed.

#### Transport failures and failover

TCP settings decide how long a silent failure stays hidden:

- **Keepalive defaults.** libpq turns keepalives on by default, but `keepalives_idle=0` means the operating-system default, which is 7,200 s on Linux. With 9 probes sent 75 s apart, a dead peer is found after about 2 h 11 min.
- **Retransmission.** A heartbeat written into a dead connection waits in retransmission for `tcp_retries2` (default 15). That is roughly 13–30 minutes; the kernel documentation gives 924.6 s as a theoretical minimum.
- **`tcp_user_timeout`** caps that wait, and on Linux it also overrides keepalive timing.
- **AWS's settings.** AWS's Aurora fast-failover guidance uses a keepalive idle time of 1 s, an interval of 1 s and a count of 5, which "should notify the application within five seconds". A listener can set the same values on its own connection through libpq. `keepalives_idle` must also stay below middlebox idle timeouts.
- **Server side.** The server's `tcp_keepalives_*` and `tcp_user_timeout` settings reclaim backends whose listener has vanished. RDS defaults are 300 s idle, a 30 s interval and a count of 2. `client_connection_check_interval` checks only while a query is running, so it does not help an idle listener.
- **Limits of keepalives.** Keepalives test only the hop to the nearest TCP peer. Behind a pooler or proxy, only a SQL-level probe tests the whole path. This last point is an inference.

On managed PostgreSQL, an idle listener fails in one of two ways during failover:

- **Planned events end sessions cleanly.** These include switchovers, reboots with failover, maintenance and blue/green cut-overs. PostgreSQL's fast shutdown reports `57P01 admin_shutdown`, and the listener notices immediately.
- **Unplanned failures can be silent.** After a host, zone or network failure, the listener may hear nothing until a keepalive or heartbeat fires.

Recovery time depends mostly on the platform:

| Platform and event | Documented time |
|---|---|
| Aurora failover with a replica | Under 60 s, often under 30 s; the cluster endpoint's DNS update takes under 30 s |
| RDS Multi-AZ DB cluster | Under 35 s |
| RDS Multi-AZ DB instance | 60–120 s |
| Cloud SQL HA failover | About 60 s; the IP address stays the same, so there is no DNS step |
| Cloud SQL maintenance | Under 30 s on Enterprise; under 1 s on Enterprise Plus |
| RDS blue/green switchover | Bounded by a timeout of 30–3,600 s (default 300 s) |

The client then adds three delays: the time to detect the failure, up to one DNS TTL (5 s on RDS), and its own retry interval. Four libpq behaviours shape the reconnect:

- libpq tries each listed host and address in turn, but it does not retry on its own; the consumer's loop must.
- `connect_timeout` applies to each host separately and, if unset, waits indefinitely. An attempt against an address that no longer answers is then bounded only by SYN retries, whose last retransmission comes at about 67 s by default.
- AWS's fast-failover example uses a 2 s connect timeout.
- `57P03 cannot_connect_now`, which appears while the new primary is recovering, should be retried.

`target_session_attrs` does not make reconnecting faster, but it stops a listener from going silently deaf. Use `read-write`, not `primary`:

- After an RDS blue/green switchover, the old primary stays up and read-only, but it is not a hot standby. `primary` would accept it, `LISTEN` would succeed, and the listener would wait forever for notifications that now go to the new environment. `read-write` refuses that server.
- Aurora's cluster endpoint can briefly resolve to the former writer. AWS therefore lists both the cluster and reader endpoints. A libpq connection string naming both hosts, with `read-write`, finds the new primary before DNS has caught up.

Two AWS options can shorten the reconnect further:

- The AWS Advanced Python Wrapper caches the cluster topology and reconnects "typically around 7 seconds or less after the driver notices the failure".
- RDS Proxy keeps a stable address, but it pins `LISTEN` sessions, and its documentation does not say whether a pinned listener survives failover.

In every case, the consumer must re-issue `LISTEN` and catch up from the log.

#### Server-side queue health

`pg_notification_queue_usage()` returns the fraction of the notification queue in use. From half full onwards, PostgreSQL logs warnings (`NOTIFY queue is %.0f%% full`) that name the session blocking cleanup. That session is typically a listener stuck in a long transaction. This is one more reason to keep the listener in autocommit, and separate from the connection used for processing or catch-up reads.

#### Upgrades that silently change channels or payloads

**Channel mismatches fail silently.** If writers and listeners on different versions disagree about the channel name, nothing raises an error; listeners simply stop waking. River's 0.5.0 release renamed its insert channel, adding a schema prefix and moving notification into client code. The changelog describes no period in which both channels were active. Periodic fetching covered the gap, but a push-only consumer would have stalled without any sign.

**Payload mismatches can fail loudly or quietly**, depending on the reader. Commanded parses its comma-separated payload by position into exactly four parts, with no version marker, so any change to it would break older listeners.

Libraries version their notifications in two ways:

- **Version in the channel name.** Procrastinate uses `_v1` channels. Its pre-migration keeps the old trigger alongside the new one, and its post-migration drops the old one.
- **Version in the payload.** Procrastinate uses a `type` field and Que a `message_type` field. Oban adds a `Z:` compression prefix and its decoder accepts several formats.

For changes that cannot be made compatible:

- Graphile Worker announces breaking migrations on a `worker:migrate` channel, and old workers exit with code 57.
- Que's trigger notifies only lockers whose `job_schema_version` matches the job's.

For an event store, this suggests:

- **Channel.** Version the channel name from the start, for example `<schema>_events_v1`.
- **Payload.** Keep it small and self-describing, for example `{"v":1,"category":...,"position":...}`, and only ever add fields.
- **Tolerant readers.** Ignore unknown keys. Treat an unparseable payload or an unknown version as an unfiltered wake-up rather than an error, as Graphile Worker's `tryParseJson` does.
- **Breaking changes.** Roll them out in three steps: expand, migrate, contract. During the expand step, either writers publish to both channels, or upgraded consumers listen on both. Support formats N and N−1 in each release.
- **Who sends the notification.** If a trigger sends `NOTIFY`, the format follows the schema version. If library code sends it, a mixed-version fleet sends mixed formats.

### A composite design for a psycopg 3 event-store consumer

Taken together, the sources point to the design below. It is a synthesis, and anything marked as an inference above remains one.

- **The listener connection.**
  - Use one per process. It should be dedicated, in autocommit mode and not taken from a pool.
  - Give it its own connection string, so that it can bypass a transaction-mode pooler.
  - Set the keepalives low, somewhere between AWS's 1/1/5 and about 30/10/3 (`keepalives_idle`, `keepalives_interval`, `keepalives_count`).
  - Also set `tcp_user_timeout`, a `connect_timeout` of about 2 s and `target_session_attrs=read-write`. Use a multi-host connection string where the platform offers one.
  - Require psycopg 3.2.10 or later, and use only the `notifies()` generator.
- **The order on every connect.** `LISTEN`, then catch up from the safe checkpoint, then wait.
- **The main loop.**
  - Wait with `notifies(timeout=...)`, and let the generator finish rather than breaking out of it.
  - After each wait, run the heartbeat if it is due, then read from the checkpoint until a batch comes back short.
  - Use the short gap re-check timeout while holding at a gap, and the safety-net interval otherwise.
- **Reads.** Run catch-up reads on a pooled connection as short autocommit statements. Apply a gap-safe rule: commit-ordered allocation, the transaction-ID horizon, or an evidence-based high-water mark. Persist only the safe checkpoint.
- **Fan-out.**
  - The listener only parses notifications and hands them off.
  - A registry kept outside the listener routes them by category, merges pending hints into the highest position per category, and delivers through bounded queues or wake-up flags.
  - On overflow, a subscription switches to catching up.
  - After any reconnect, every subscription catches up.
- **Health.**
  - Run the heartbeat as `SELECT pg_notification_queue_usage()`, at an interval below every idle limit on the path.
  - Every few seconds, send a canary carrying a nonce from the append pool. If it does not arrive, reconnect, re-issue `LISTEN`, catch up, and run poll-only until the canary returns.
  - Treat `OperationalError`, including `AdminShutdown` and `IdleSessionTimeout`, as a normal reconnect with capped exponential backoff.
- **The sender.**
  - Send one notification per append transaction, from the library or from a statement-level trigger.
  - Use a lower-case, versioned global channel. Send a small versioned payload carrying the append's highest position, and optionally its category.
  - If appending commits approach the notify-lock ceiling, debounce per process, and either keep a trailing notification or rely on the poll.
  - If synchronous commit or synchronous standbys make the lock window inside the append transaction expensive, consider notifying from a separate transaction after the append commits.

A sketch of the listener loop follows. It is a synchronous version; the asyncio version has the same shape. It leaves out the canary, the gap re-check timeout and backoff on the safety-net interval. `catch_up` should use a separate pooled connection, and the queue-usage result should be checked against a threshold.

```python
import time
import psycopg
from psycopg import sql

CONNECT_KWARGS = dict(
    autocommit=True, connect_timeout=2,
    keepalives=1, keepalives_idle=30, keepalives_interval=10,
    keepalives_count=3, tcp_user_timeout=60_000,
    target_session_attrs="read-write",
)

def listen_forever(listener_conninfo, channel, catch_up, interval=30.0):
    backoff = 0.5
    while True:
        try:
            with psycopg.connect(listener_conninfo, **CONNECT_KWARGS) as conn:
                conn.execute(sql.SQL("LISTEN {}").format(sql.Identifier(channel)))
                backoff = 0.5
                catch_up()                          # gap-safe read from checkpoint
                last_query = time.monotonic()
                while True:
                    for _ in conn.notifies(timeout=interval, stop_after=1):
                        pass                        # a hint only; exhaust, never break
                    if time.monotonic() - last_query >= interval:
                        conn.execute("SELECT pg_notification_queue_usage()")
                        last_query = time.monotonic()
                    catch_up()                      # wake-up or safety-net poll
        except psycopg.OperationalError:           # incl. AdminShutdown, IdleSessionTimeout
            time.sleep(backoff)
            backoff = min(backoff * 2, 30.0)
```

### Limits of the evidence

- **Missing benchmarks.** None of the sources benchmarks any of the following: a store-wide allocation lock on its own; insert plus `NOTIFY` under synchronous replication or with `synchronous_commit = off`; or notifying from a separate transaction after commit. The ceilings above are arithmetic from the commit-path code plus a handful of benchmarks.
- **Narrow benchmarks.** The cost per listener comes from one benchmark on undocumented hardware. Commanded's throughput benchmark has no unserialised control.
- **Untested technique.** Using the sequence's own lock to prove a gap dead is an inference from `sequence.c`. It is neither documented nor tested.
- **Inferences from documented behaviour.** Three claims rest on reasoning rather than tests:
  - that `idle_session_timeout` ignores incoming notifications;
  - that a `NOTIFY` sent to itself can pass through a transaction-mode pooler;
  - that a promoted standby holds no registrations.
- **Undocumented behaviour.** AWS does not document whether an RDS Proxy session pinned by `LISTEN` survives failover.
- **Release status.** PostgreSQL 19's release notes were still a draft as of September 2026. The "Write Notifications Through WAL" patch for `NOTIFY` was still unmerged as of its November 2025 review.

## Sources

- [PostgreSQL: NOTIFY](https://www.postgresql.org/docs/current/sql-notify.html) — tier-1 — postgresql.org
- [PostgreSQL: LISTEN](https://www.postgresql.org/docs/current/sql-listen.html) — tier-1 — postgresql.org
- [PostgreSQL: Client Connection Defaults (idle_session_timeout)](https://www.postgresql.org/docs/current/runtime-config-client.html) — tier-1 — postgresql.org
- [Connections and authentication: tcp_keepalives, tcp_user_timeout, client_connection_check_interval](https://www.postgresql.org/docs/current/runtime-config-connection.html) — tier-1 — postgresql.org (PostgreSQL official documentation)
- [PostgreSQL documentation: Resource consumption (max_notify_queue_pages)](https://www.postgresql.org/docs/current/runtime-config-resource.html) — tier-1 — postgresql.org
- [PostgreSQL: System Information Functions (pg_notification_queue_usage)](https://www.postgresql.org/docs/current/functions-info.html) — tier-1 — postgresql.org
- [PostgreSQL: libpq Connection Parameters (keepalives, tcp_user_timeout)](https://www.postgresql.org/docs/current/libpq-connect.html) — tier-1 — postgresql.org
- [Hot Standby](https://www.postgresql.org/docs/current/hot-standby.html) — tier-1 — postgresql.org (PostgreSQL official documentation)
- [Shutting Down the Server](https://www.postgresql.org/docs/current/server-shutdown.html) — tier-1 — postgresql.org
- [PostgreSQL Error Codes](https://www.postgresql.org/docs/current/errcodes-appendix.html) — tier-1 — postgresql.org
- [PostgreSQL documentation: Message Flow (Asynchronous Operations)](https://www.postgresql.org/docs/current/protocol-flow.html) — tier-1 — postgresql.org
- [PostgreSQL documentation: Lexical structure (identifier length and case folding)](https://www.postgresql.org/docs/current/sql-syntax-lexical.html) — tier-1 — postgresql.org
- [PostgreSQL documentation: CREATE TRIGGER](https://www.postgresql.org/docs/current/sql-createtrigger.html) — tier-1 — postgresql.org
- [Sequence Manipulation Functions](https://www.postgresql.org/docs/current/functions-sequence.html) — tier-1 — postgresql.org (PostgreSQL documentation)
- [PostgreSQL: CREATE SEQUENCE](https://www.postgresql.org/docs/current/sql-createsequence.html) — tier-1 — postgresql.org
- [Transactions and Identifiers](https://www.postgresql.org/docs/current/transaction-id.html) — tier-1 — postgresql.org (PostgreSQL documentation)
- [Explicit Locking](https://www.postgresql.org/docs/current/explicit-locking.html) — tier-1 — postgresql.org (PostgreSQL documentation)
- [PostgreSQL: pg_locks](https://www.postgresql.org/docs/current/view-pg-locks.html) — tier-1 — postgresql.org
- [PostgreSQL: The Cumulative Statistics System (pg_stat_activity)](https://www.postgresql.org/docs/current/monitoring-stats.html) — tier-1 — postgresql.org
- [PostgreSQL: PREPARE TRANSACTION](https://www.postgresql.org/docs/current/sql-prepare-transaction.html) — tier-1 — postgresql.org
- [Logical Decoding Concepts](https://www.postgresql.org/docs/current/logicaldecoding-explanation.html) — tier-1 — postgresql.org (PostgreSQL documentation)
- [PostgreSQL docs: WAL Configuration (commit_delay and group commit)](https://www.postgresql.org/docs/current/wal-configuration.html) — tier-1 — postgresql.org
- [PostgreSQL documentation: Write Ahead Log settings (synchronous_commit, commit_delay)](https://www.postgresql.org/docs/current/runtime-config-wal.html) — tier-1 — postgresql.org
- [PostgreSQL documentation: Asynchronous Commit](https://www.postgresql.org/docs/current/wal-async-commit.html) — tier-1 — postgresql.org
- [PostgreSQL documentation: Log-Shipping Standby Servers (Synchronous Replication)](https://www.postgresql.org/docs/current/warm-standby.html) — tier-1 — postgresql.org
- [PostgreSQL 19 release notes](https://www.postgresql.org/docs/19/release-19.html) — tier-1 — postgresql.org
- [PostgreSQL 13 release notes](https://www.postgresql.org/docs/13/release-13.html) — tier-1 — postgresql.org
- [async.c (PostgreSQL source, master)](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/commands/async.c) — tier-1 — github.com (postgres/postgres)
- [PostgreSQL source: src/backend/commands/async.c (REL_17_STABLE)](https://raw.githubusercontent.com/postgres/postgres/REL_17_STABLE/src/backend/commands/async.c) — tier-1 — github.com (postgres/postgres official repository)
- [PostgreSQL source: src/backend/access/transam/xact.c](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/access/transam/xact.c) — tier-1 — github.com/postgres/postgres
- [PostgreSQL source: src/backend/replication/syncrep.c (REL_17_STABLE)](https://raw.githubusercontent.com/postgres/postgres/REL_17_STABLE/src/backend/replication/syncrep.c) — tier-1 — github.com (postgres/postgres official repository)
- [PostgreSQL source: src/backend/commands/sequence.c](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/commands/sequence.c) — tier-1 — raw.githubusercontent.com (postgres/postgres primary source)
- [PostgreSQL source: src/backend/storage/ipc/procarray.c](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/storage/ipc/procarray.c) — tier-1 — raw.githubusercontent.com (postgres/postgres primary source)
- [pgsql-committers: Use a hash table to de-duplicate NOTIFY events faster](https://www.postgresql.org/message-id/E1hyIW7-0002lw-KJ%40gemulon.postgresql.org) — tier-1 — postgresql.org (pgsql-committers list)
- [pgsql-committers: Optimize LISTEN/NOTIFY via shared channel map and direct advance](https://www.postgresql.org/message-id/E1vgSld-000fFk-2J%40gemulon.postgresql.org) — tier-1 — postgresql.org (pgsql-committers list)
- [Commit 51004c7: Make some efficiency improvements in LISTEN/NOTIFY](https://github.com/postgres/postgres/commit/51004c717) — tier-1 — github.com/postgres/postgres
- [Optimize LISTEN/NOTIFY (Joel Jacobson, initial proposal and benchmarks)](https://www.postgresql.org/message-id/6899c044-4a82-49be-8117-e6f669765f7e%40app.fastmail.com) — tier-1 — postgresql.org (pgsql-hackers)
- [pgsql-hackers: Re: Proposal: Out-of-Order NOTIFY via GUC to Improve LISTEN/NOTIFY Throughput (Joel Jacobson)](https://www.postgresql.org/message-id/ab1b986a-8ae2-469e-a680-11c1ce8fd4e8%40app.fastmail.com) — tier-1 — postgresql.org (pgsql-hackers list)
- [Re: Proposal: Out-of-Order NOTIFY via GUC (Joel Jacobson, 2025-07-30)](https://www.postgresql.org/message-id/7c8c61c2-4dbf-4961-9e0c-a5ec77d8f846%40app.fastmail.com) — tier-2 — postgresql.org (pgsql-hackers mailing list archive)
- [[PATCH] Write Notifications Through WAL (Rishu Bagga, 2025-09-25)](https://www.postgresql.org/message-id/CAK80%3DjjvgWJL5EdzH_Bxc37gn53PJaB5sDXB3VPfJuN7G1xL5Q%40mail.gmail.com) — tier-1 — postgresql.org (pgsql-hackers)
- [Re: [PATCH] Write Notifications Through WAL (Arseniy Mukhin, 2025-10-17 insert-with-notify benchmarks)](https://www.postgresql.org/message-id/CAE7r3MLwRNyvmMd2kaefeNYTRjGD8wOxLzyGn8vexaeTJdASaw%40mail.gmail.com) — tier-1 — postgresql.org (pgsql-hackers)
- [[PATCH] Write Notifications Through WAL (thread, flat view)](https://www.postgresql.org/message-id/flat/CAE7r3MKV7oewo6EGGLPMKQvv74ujPurjjDBU=K8KH8CYJStXaw@mail.gmail.com) — tier-2 — postgresql.org (pgsql-hackers mailing list archive)
- [Re: Sequences, txids, and serial order of transactions (Kevin Grittner)](https://www.postgresql.org/message-id/CACjxUsMKA6k-mDOdkos3k0i-KE4HFRwkd=PXPArYy4UabTd-LA@mail.gmail.com) — tier-2 — postgresql.org (pgsql-general mailing list)
- [NOTIFY and pg_notify performance when deduplicating notifications](https://hackorum.dev/topics/39439) — tier-3 — hackorum.dev (pgsql-hackers archive aggregator)
- [psycopg 3: Asynchronous operations (notifications)](https://www.psycopg.org/psycopg3/docs/advanced/async.html) — tier-1 — psycopg.org
- [psycopg 3: Connection classes (notifies, add_notify_handler)](https://www.psycopg.org/psycopg3/docs/api/connections.html) — tier-1 — psycopg.org
- [psycopg 3: Release notes](https://www.psycopg.org/psycopg3/docs/news.html) — tier-1 — psycopg.org
- [psycopg.errors SQLSTATE exception classes](https://www.psycopg.org/psycopg3/docs/api/errors.html) — tier-1 — psycopg.org (psycopg 3 official documentation)
- [psycopg 3 documentation: Transactions management](https://www.psycopg.org/psycopg3/docs/basic/transactions.html) — tier-1 — psycopg.org
- [psycopg 3 documentation: Other objects (ConnectionInfo, Notify)](https://www.psycopg.org/psycopg3/docs/api/objects.html) — tier-1 — psycopg.org
- [Connection pools](https://www.psycopg.org/psycopg3/docs/advanced/pool.html) — tier-1 — psycopg.org (psycopg 3 official documentation)
- [psycopg 3 docs: Pipeline mode support](https://www.psycopg.org/psycopg3/docs/advanced/pipeline.html) — tier-1 — psycopg.org
- [psycopg Connection source (notifies implementation)](https://raw.githubusercontent.com/psycopg/psycopg/master/psycopg/psycopg/connection.py) — tier-1 — github.com (psycopg official repository)
- [psycopg issue 962: notifications lost between notifies() calls](https://github.com/psycopg/psycopg/issues/962) — tier-2 — github.com (psycopg issue tracker, user report)
- [Python asyncio Queues documentation](https://docs.python.org/3/library/asyncio-queue.html) — tier-1 — docs.python.org
- [Postgrex.Notifications](https://postgrex.hexdocs.pm/Postgrex.Notifications.html) — tier-1 — postgrex.hexdocs.pm
- [Npgsql: Waiting for notifications](https://npgsql.org/doc/wait.html) — tier-1 — npgsql.org
- [Npgsql documentation: Keepalive](https://www.npgsql.org/doc/keepalive.html) — tier-1 — npgsql.org
- [pg-listen source: src/index.ts](https://raw.githubusercontent.com/andywer/pg-listen/master/src/index.ts) — tier-1 — github.com/andywer/pg-listen
- [River client.go (Config: FetchCooldown, FetchPollInterval, PollOnly)](https://raw.githubusercontent.com/riverqueue/river/master/client.go) — tier-1 — raw.githubusercontent.com (riverqueue/river source)
- [River CHANGELOG](https://raw.githubusercontent.com/riverqueue/river/master/CHANGELOG.md) — tier-1 — raw.githubusercontent.com (riverqueue/river source)
- [River notifier source](https://raw.githubusercontent.com/riverqueue/river/master/internal/notifier/notifier.go) — tier-1 — github.com (River official repository)
- [notifylimiter/limiter.go (River source)](https://raw.githubusercontent.com/riverqueue/river/master/internal/notifylimiter/limiter.go) — tier-1 — github.com (riverqueue/river)
- [River migration 004_pending_and_more.up.sql](https://raw.githubusercontent.com/riverqueue/river/master/riverdriver/riverpgxv5/migration/main/004_pending_and_more.up.sql) — tier-1 — github.com/riverqueue
- [Oban: Scaling Applications](https://oban.hexdocs.pm/2.24.1/scaling.html) — tier-1 — oban.hexdocs.pm
- [Oban: Troubleshooting](https://oban.hexdocs.pm/troubleshooting.html) — tier-1 — oban.hexdocs.pm
- [Oban.Notifier](https://oban.hexdocs.pm/Oban.Notifier.html) — tier-1 — oban.hexdocs.pm
- [Oban source: lib/oban/sonar.ex](https://raw.githubusercontent.com/oban-bg/oban/main/lib/oban/sonar.ex) — tier-1 — github.com/oban-bg/oban
- [Oban source: lib/oban/stager.ex](https://raw.githubusercontent.com/oban-bg/oban/main/lib/oban/stager.ex) — tier-1 — github.com/oban-bg/oban
- [Oban source: lib/oban/notifiers/postgres.ex](https://raw.githubusercontent.com/oban-bg/oban/main/lib/oban/notifiers/postgres.ex) — tier-1 — github.com/oban-bg/oban
- [Oban lib/oban/notifier.ex](https://raw.githubusercontent.com/oban-bg/oban/main/lib/oban/notifier.ex) — tier-1 — github.com/oban-bg
- [Graphile Worker FAQ](https://worker.graphile.org/docs/faq) — tier-1 — worker.graphile.org
- [Graphile Worker source: main.ts](https://raw.githubusercontent.com/graphile/worker/main/src/main.ts) — tier-1 — github.com (graphile/worker)
- [Graphile Worker src/migrate.ts](https://raw.githubusercontent.com/graphile/worker/main/src/migrate.ts) — tier-1 — github.com/graphile
- [GoodJob README](https://github.com/bensheldon/good_job/blob/main/README.md) — tier-1 — github.com (bensheldon/good_job)
- [GoodJob source: notifier.rb](https://raw.githubusercontent.com/bensheldon/good_job/main/lib/good_job/notifier.rb) — tier-1 — github.com (bensheldon/good_job)
- [GoodJob source: adapter.rb](https://raw.githubusercontent.com/bensheldon/good_job/main/lib/good_job/adapter.rb) — tier-1 — github.com (bensheldon/good_job)
- [Que::Locker API documentation](https://www.rubydoc.info/gems/que/Que/Locker) — tier-1 — rubydoc.info (generated gem docs)
- [Que migration 5 up.sql](https://raw.githubusercontent.com/que-rb/que/master/lib/que/migrations/5/up.sql) — tier-1 — github.com/que-rb
- [Procrastinate API Reference](https://procrastinate.readthedocs.io/en/stable/reference.html) — tier-1 — procrastinate.readthedocs.io
- [Procrastinate connector source (LISTEN_TIMEOUT)](https://raw.githubusercontent.com/procrastinate-org/procrastinate/main/procrastinate/connector.py) — tier-1 — github.com (Procrastinate official repository)
- [Procrastinate psycopg connector source](https://raw.githubusercontent.com/procrastinate-org/procrastinate/main/procrastinate/psycopg_connector.py) — tier-1 — github.com (Procrastinate official repository)
- [Procrastinate worker source](https://raw.githubusercontent.com/procrastinate-org/procrastinate/main/procrastinate/worker.py) — tier-1 — github.com (Procrastinate official repository)
- [Procrastinate source: sql/schema.sql](https://raw.githubusercontent.com/procrastinate-org/procrastinate/main/procrastinate/sql/schema.sql) — tier-1 — github.com (procrastinate-org/procrastinate)
- [Procrastinate documentation: Migrations](https://procrastinate.readthedocs.io/en/stable/howto/production/migrations.html) — tier-1 — procrastinate.readthedocs.io
- [Procrastinate migration 03.00.00_01_pre_cancel_notification.sql](https://raw.githubusercontent.com/procrastinate-org/procrastinate/main/procrastinate/sql/migrations/03.00.00_01_pre_cancel_notification.sql) — tier-1 — github.com/procrastinate-org
- [PgQueuer source: pgqueuer/core/qm.py](https://raw.githubusercontent.com/janbjorge/pgqueuer/main/pgqueuer/core/qm.py) — tier-1 — github.com/janbjorge/pgqueuer
- [pgmq source: pgmq.sql](https://raw.githubusercontent.com/pgmq/pgmq/main/pgmq-extension/sql/pgmq.sql) — tier-1 — github.com (pgmq/pgmq)
- [PGMQ Extension](https://supabase.com/docs/guides/queues/pgmq.md) — tier-2 — supabase.com (vendor docs for the PGMQ extension)
- [PgFlow.Signal.Notify](https://pgflow.hexdocs.pm/PgFlow.Signal.Notify.html) — tier-3 — pgflow.hexdocs.pm (small third-party library docs)
- [Action Cable PostgreSQL subscription adapter source](https://raw.githubusercontent.com/rails/rails/main/actioncable/lib/action_cable/subscription_adapter/postgresql.rb) — tier-1 — github.com/rails/rails
- [The Notifier Pattern for Applications That Use Postgres](https://brandur.org/notifier) — tier-3 — brandur.org (personal blog)
- [Postgres LISTEN/NOTIFY does not scale](https://recall.ai/blog/postgres-listen-notify-does-not-scale) — tier-3 — recall.ai (company engineering blog)
- [Postgres LISTEN/NOTIFY Actually Scales](https://dbos.dev/blog/postgres-listen-notify-scalability) — tier-3 — dbos.dev (vendor engineering blog)
- [EventStore (Elixir): Subscriptions](https://eventstore.hexdocs.pm/subscriptions.html) — tier-1 — eventstore.hexdocs.pm
- [EventStore (Elixir): Running on a cluster of nodes](https://eventstore.hexdocs.pm/cluster.html) — tier-1 — eventstore.hexdocs.pm
- [EventStore Subscriptions guide](https://raw.githubusercontent.com/commanded/eventstore/master/guides/Subscriptions.md) — tier-1 — github.com/commanded/eventstore
- [EventStore (Elixir): subscription_fsm.ex](https://raw.githubusercontent.com/commanded/eventstore/master/lib/event_store/subscriptions/subscription_fsm.ex) — tier-1 — raw.githubusercontent.com (commanded/eventstore source)
- [EventStore Notifications Listener source](https://raw.githubusercontent.com/commanded/eventstore/master/lib/event_store/notifications/listener.ex) — tier-1 — github.com/commanded/eventstore
- [EventStore Notifications Publisher source](https://raw.githubusercontent.com/commanded/eventstore/master/lib/event_store/notifications/publisher.ex) — tier-1 — github.com/commanded/eventstore
- [EventStore Notifications Supervisor source](https://raw.githubusercontent.com/commanded/eventstore/master/lib/event_store/notifications/supervisor.ex) — tier-1 — github.com/commanded/eventstore
- [EventStore Notification payload parser source](https://raw.githubusercontent.com/commanded/eventstore/master/lib/event_store/notifications/notification.ex) — tier-1 — github.com/commanded/eventstore
- [EventStore PubSub source](https://raw.githubusercontent.com/commanded/eventstore/master/lib/event_store/pubsub.ex) — tier-1 — github.com/commanded/eventstore
- [EventStore schema initialisation SQL (notify_events trigger)](https://raw.githubusercontent.com/commanded/eventstore/master/lib/event_store/sql/init.ex) — tier-1 — github.com/commanded/eventstore
- [Commanded EventStore insert_events.sql.eex](https://github.com/commanded/eventstore/blob/master/lib/event_store/sql/statements/insert_events.sql.eex) — tier-1 — github.com (commanded/eventstore source)
- [Commanded EventStore README (benchmarks)](https://raw.githubusercontent.com/commanded/eventstore/master/README.md) — tier-1 — github.com/commanded/eventstore
- [GenStage.BroadcastDispatcher documentation](https://gen-stage.hexdocs.pm/GenStage.BroadcastDispatcher.html) — tier-1 — hexdocs.pm (gen_stage)
- [Registry — Elixir documentation](https://elixir.hexdocs.pm/Registry.html) — tier-1 — hexdocs.pm (Elixir)
- [Marten: Async Daemon (high water mark and gap handling)](https://martendb.io/events/projections/async-daemon.html) — tier-1 — martendb.io (official Marten documentation)
- [Async daemon can advance projection progress past committed events during concurrent appends #4953](https://github.com/JasperFx/marten/discussions/4953) — tier-2 — github.com (JasperFx/marten discussions)
- [Marten source: GapLivenessProbe.cs](https://raw.githubusercontent.com/JasperFx/marten/master/src/Marten/Events/Daemon/HighWater/GapLivenessProbe.cs) — tier-1 — raw.githubusercontent.com (JasperFx/marten primary source)
- [Marten source: HighWaterDetector.cs](https://raw.githubusercontent.com/JasperFx/marten/master/src/Marten/Events/Daemon/HighWater/HighWaterDetector.cs) — tier-1 — raw.githubusercontent.com (JasperFx/marten primary source)
- [JasperFx.Events source: DaemonSettings.cs](https://raw.githubusercontent.com/JasperFx/jasperfx/main/src/JasperFx.Events/Daemon/DaemonSettings.cs) — tier-1 — raw.githubusercontent.com (JasperFx/jasperfx primary source)
- [ADR-0025: All-subscription gap safety](https://docs.proteanhq.com/adr/0025-all-subscription-gap-safety/) — tier-1 — docs.proteanhq.com (Protean official documentation)
- [Emmett PostgreSQL readMessagesBatch](https://github.com/event-driven-io/emmett/blob/main/src/packages/emmett-postgresql/src/eventStore/schema/readMessagesBatch.ts) — tier-1 — github.com (event-driven-io/emmett source)
- [Message DB acquire_lock function](https://github.com/message-db/message-db/blob/master/database/functions/acquire-lock.sql) — tier-1 — github.com (message-db/message-db source)
- [Message DB write_message function](https://github.com/message-db/message-db/blob/master/database/functions/write-message.sql) — tier-1 — github.com (message-db/message-db source)
- [Ordering in Postgres Outbox: why transaction id matters](https://event-driven.io/en/ordering_in_postgres_outbox/) — tier-2 — event-driven.io (Oskar Dudycz)
- [postgresql-event-sourcing reference implementation](https://github.com/eugene-khyst/postgresql-event-sourcing) — tier-3 — github.com/eugene-khyst
- [Postgres sequences can commit out-of-order](https://blog.sequinstream.com/postgres-sequences-can-commit-out-of-order/) — tier-3 — blog.sequinstream.com (Sequin engineering blog)
- [GapAwareTrackingToken](https://apidocs.axoniq.io/4.10/org/axonframework/eventhandling/GapAwareTrackingToken.html) — tier-1 — apidocs.axoniq.io (Axon Framework API documentation)
- [JdbcEventStorageEngine.Builder](https://apidocs.axoniq.io/4.10/org/axonframework/eventsourcing/eventstore/jdbc/JdbcEventStorageEngine.Builder.html) — tier-1 — apidocs.axoniq.io (Axon Framework API documentation)
- [eventsourcing docs: Persistence (PostgreSQL table lock)](https://eventsourcing.readthedocs.io/en/stable/topics/persistence.html) — tier-1 — eventsourcing.readthedocs.io
- [eventsourcing source: eventsourcing/postgres.py](https://raw.githubusercontent.com/pyeventsourcing/eventsourcing/main/eventsourcing/postgres.py) — tier-1 — github.com/pyeventsourcing/eventsourcing
- [Gaps in sequences in PostgreSQL](https://www.cybertec-postgresql.com/en/gaps-in-sequences-postgresql/) — tier-2 — cybertec-postgresql.com
- [PgBouncer: Features](https://www.pgbouncer.org/features.html) — tier-1 — pgbouncer.org
- [PgBouncer configuration](https://www.pgbouncer.org/config.html) — tier-1 — pgbouncer.org (PgBouncer official documentation)
- [Neon connection pooling](https://neon.com/docs/connect/connection-pooling) — tier-1 — neon.com (Neon official documentation)
- [Avoiding pinning an RDS Proxy](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/rds-proxy-pinning.html) — tier-1 — docs.aws.amazon.com
- [RDS Proxy concepts and terminology](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/rds-proxy.howitworks.html) — tier-1 — docs.aws.amazon.com
- [CreateDBProxy API reference (IdleClientTimeout)](https://docs.aws.amazon.com/AmazonRDS/latest/APIReference/API_CreateDBProxy.html) — tier-1 — docs.aws.amazon.com (AWS official documentation)
- [Dead connection handling in PostgreSQL (RDS)](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Appendix.PostgreSQL.CommonDBATasks.DeadConnectionHandling.html) — tier-1 — docs.aws.amazon.com (AWS official documentation)
- [Network Load Balancers: connection idle timeout](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/network-load-balancers.html) — tier-1 — docs.aws.amazon.com (AWS official documentation)
- [Configure load balancer TCP reset and idle timeout](https://learn.microsoft.com/en-us/azure/load-balancer/load-balancer-tcp-idle-timeout) — tier-1 — learn.microsoft.com (Microsoft official documentation)
- [tcp(7) Linux manual page](https://man7.org/linux/man-pages/man7/tcp.7.html) — tier-1 — man7.org (Linux man-pages project)
- [IP Sysctl (Linux kernel networking documentation)](https://docs.kernel.org/networking/ip-sysctl.html) — tier-1 — docs.kernel.org
- [Failing over a Multi-AZ DB instance for Amazon RDS](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.MultiAZ.Failover.html) — tier-1 — docs.aws.amazon.com
- [Failing over a Multi-AZ DB cluster for Amazon RDS](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/multi-az-db-clusters-concepts-failover.html) — tier-1 — docs.aws.amazon.com
- [High availability for Amazon Aurora](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Concepts.AuroraHighAvailability.html) — tier-1 — docs.aws.amazon.com
- [Fast failover with Amazon Aurora PostgreSQL](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/AuroraPostgreSQL.BestPractices.FastFailover.html) — tier-1 — docs.aws.amazon.com
- [Amazon Aurora endpoint connections](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Aurora.Overview.Endpoints.html) — tier-1 — docs.aws.amazon.com
- [Switching a blue/green deployment in Amazon RDS](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/blue-green-deployments-switching.html) — tier-1 — docs.aws.amazon.com
- [About high availability (Cloud SQL for PostgreSQL)](https://docs.cloud.google.com/sql/docs/postgres/high-availability) — tier-1 — docs.cloud.google.com
- [About maintenance on Cloud SQL instances](https://docs.cloud.google.com/sql/docs/postgres/maintenance) — tier-1 — docs.cloud.google.com
- [AWS Advanced Python Wrapper README](https://github.com/aws/aws-advanced-python-wrapper) — tier-1 — github.com/aws
- [Introducing the Advanced Python Wrapper Driver for Amazon Aurora](https://aws.amazon.com/blogs/database/introducing-the-advanced-python-wrapper-driver-for-amazon-aurora/) — tier-2 — aws.amazon.com (AWS Database Blog)
- [Amazon SQS short and long polling](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-short-and-long-polling.html) — tier-1 — docs.aws.amazon.com
- [Apache Kafka consumer configs (fetch.min.bytes, fetch.max.wait.ms)](https://kafka.apache.org/0102/configuration/consumer-configs) — tier-1 — kafka.apache.org
- [KurrentDB HTTP API: Optional HTTP headers (ES-LongPoll)](https://docs.kurrent.io/server/v5/http-api/optional-http-headers) — tier-1 — docs.kurrent.io
- [Azure Queue storage trigger for Azure Functions (polling algorithm)](https://learn.microsoft.com/en-us/azure/azure-functions/functions-bindings-storage-queue-trigger) — tier-1 — learn.microsoft.com
- [Confluent documentation: Schema Evolution and Compatibility](https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html) — tier-1 — docs.confluent.io
- [Martin Fowler: Parallel Change](https://martinfowler.com/bliki/ParallelChange.html) — tier-2 — martinfowler.com
- [Martin Fowler: Tolerant Reader](https://martinfowler.com/bliki/TolerantReader.html) — tier-2 — martinfowler.com
