---
type: "topic-research"
id: "05-how-do-established-postgresql-backed-event-stores-and-job-qu-web"
title: "How do established PostgreSQL-backed event stores and job queues — such as Marten's async daemon, Message DB, River, Oban, Graphile Worker, GoodJob, pg-boss, Solid Queue and pgmq — deliver new work to consumers, and what design choices, problems and lessons have their maintainers published?"
date: "2026-10-05T13:56:47+00:00"
author: "Toby Clemson"
producer: "research-topic"
status: "complete"
kind: "finding"
round: 1
depth: 3
question: "How do established PostgreSQL-backed event stores and job queues — such as Marten's async daemon, Message DB, River, Oban, Graphile Worker, GoodJob, pg-boss, Solid Queue and pgmq — deliver new work to consumers, and what design choices, problems and lessons have their maintainers published?"
source_profile: "web"
tags: ["research", "topic-research", "finding"]
last_updated: "2026-10-05T13:56:47+00:00"
last_updated_by: "Toby Clemson"
schema_version: 1
---

# How do established PostgreSQL-backed event stores and job queues — such as Marten's async daemon, Message DB, River, Oban, Graphile Worker, GoodJob, pg-boss, Solid Queue and pgmq — deliver new work to consumers, and what design choices, problems and lessons have their maintainers published?

## Question

How do mature PostgreSQL-backed event stores and job queues get newly written work to their consumers? The systems covered are Marten's async daemon, Message DB with its Eventide consumers, River, Oban, Graphile Worker, GoodJob, pg-boss, Solid Queue and pgmq. What design choices have their maintainers made, which problems have they hit, and what lessons have they published?

Those choices are shaped by how PostgreSQL itself behaves, so this finding also covers three underlying questions:

- What does `NOTIFY` cost a writing transaction?
- What does it take to keep a listening connection alive?
- Why can a wake-up never, on its own, tell an event-store consumer that it is safe to read up to a given position?

## Findings

### Summary

None of the nine systems uses a notification to carry the work itself. They fall into three groups:

1. **Polling, sped up by notifications.**
   - River, Oban, Graphile Worker and GoodJob use PostgreSQL `LISTEN`/`NOTIFY` by default. pg-boss and pgmq offer it as an opt-in.
   - In all of them the notification is only a wake-up hint. It carries at most a queue name.
   - The consumer then re-reads the table, usually with `FOR UPDATE SKIP LOCKED`.
   - A periodic poll keeps running underneath as the guarantee that nothing is missed.
2. **Pure polling.** Solid Queue, Message DB with its Eventide consumers, and Marten's async daemon do not use `LISTEN`/`NOTIFY` for delivery. Marten's creator wrote in 2016 that `NOTIFY`/`LISTEN` "was not adequate by itself". The project later closed a request for listen/notify support as "wontfix".
3. **Server-side long-polling.** pgmq's `read_with_poll` loops inside the database with `pg_sleep` until a message arrives or a timeout expires.

The lessons these projects publish point the same way:

- **Polling is what guarantees correctness.** Every system that listens also polls. One reason is that nothing emits an event when a scheduled job's due time arrives.
- **Notify per channel and per interval, never per row.**
  - River and Oban both replaced per-insert triggers with notifications sent from application code, debounced or de-duplicated. pgmq added a time-based throttle to its trigger.
  - The underlying cause is in PostgreSQL. Every transaction that has issued `NOTIFY` takes a cluster-wide lock at commit and holds it through its WAL flush, so notifying commits run one at a time.
  - In one pgsql-hackers benchmark, adding a single `NOTIFY` to a single-row insert transaction cut throughput from about 31,000 to about 818 transactions per second.
  - PostgreSQL 19 does not remove this lock. A patch that would largely remove it has stalled.
- **The listener needs its own session connection, and the library needs a poll-only mode.** Transaction-mode poolers break `LISTEN`, and every system that listens ships an escape hatch. Keepalive, reconnection and health checks are part of each published design.
- **Notification failures are silent.**
  - Oban added a health check after isolated nodes stopped processing jobs.
  - pgmq's notifications never fired for queues with mixed-case names until a 2026 fix. The fallback poll hid the bug.
- **For event stores, ordering is the hard problem.**
  - A consumer that advances by global sequence number can skip an event whose transaction took a lower number but committed later.
  - A notification says only that *something* committed. It does not say that every lower number is visible.
  - Marten detects gaps (and fixed a skipping bug in July 2026). Message DB serialises writes per category. Emmett, Watermill and eugene-khyst's reference implementation filter by the transaction-snapshot horizon.
  - Push delivery has to sit on top of one of these mechanisms. It cannot replace them.

### How each system delivers work

| System | Wake-up path | Who sends the notification | Fallback poll (default) | Notable published lesson |
|---|---|---|---|---|
| River (Go) | `LISTEN`/`NOTIFY` | Application code. Debounced per queue and attached to the insert inside the transaction (trigger removed in 0.5.0) | `FetchPollInterval` 1 s; `FetchCooldown` 100 ms | One `NOTIFY` per inserted job was too much overhead at high throughput. `PollOnly` mode added for PgBouncer |
| Oban (Elixir) | Pluggable notifier: Postgres `LISTEN`/`NOTIFY` or Erlang `PG` | Application code since 2.17.0 (trigger removed). De-duplicated; delivered after commit | Stager every 1 s; producer `dispatch_cooldown` 5 ms | Detect a broken notifier and switch every node to "local" polling |
| Graphile Worker (Node) | `LISTEN`/`NOTIFY`, on by default | Database side, when a job is added | `pollInterval` 2 s, for future-dated jobs and retries | Polling is still needed because nothing emits an event when a scheduled time arrives. Events are dropped if no worker is idle |
| GoodJob (Ruby) | `LISTEN`/`NOTIFY` on the `good_job` channel | Application code: `NOTIFY good_job` with a JSON payload | `poll_interval` 10 s in production | Dedicated connection with keepalive and reconnect. `:skiplocked` plus polling for PgBouncer |
| pg-boss (Node) | Polling by default; opt-in `useListenNotify` | Sent inside the insert transaction, so it fires on commit. Only for jobs available immediately | `pollingIntervalSeconds` 2 s, or `notifyPollingIntervalSeconds` 30 s while listening | Notification is "an opt-in optimization on top of polling, not a replacement for it" |
| pgmq (extension) | Long-poll `read_with_poll`; opt-in insert trigger with `NOTIFY` | Deferred `AFTER INSERT` trigger, throttled per queue (default 250 ms) | Client's choice; `read_with_poll` checks every 100 ms internally | Notifications for mixed-case queue names silently never fired until 1.13.1 |
| Solid Queue (Ruby) | Polling only | None | Workers 0.1 s, dispatchers 1 s | Keep the polled table tiny and use a covering index. About 1,300 polls a second at 110 µs average at 37signals |
| Message DB with Eventide | Polling only | None | Eventide consumer sleeps only when a batch comes back empty | Global position "may have gaps". Writes take an advisory lock on the category |
| Marten async daemon (.NET) | Polling with high-water-mark gap detection | None | `FastPollingTime` 250 ms, `SlowPollingTime` 1 s (per an issue report) | Events still in flight make sequence order unreliable. Events were skipped under concurrent appends until 9.16.1 |

#### River

River's `Config` documentation says new jobs "will be picked up ~immediately after insert via LISTEN/NOTIFY". `FetchPollInterval` (default 1 s) "provides a fallback". `FetchCooldown` (default 100 ms) is "the minimum amount of time to wait between fetches of new jobs".

**Sending notifications.** River's original design used a trigger on job insert that issued `NOTIFY`, so that idle clients could pick up work quickly. The 0.5.0 changelog (May 2024) says this emitted "a large amount of notifications any time there are lots of jobs being inserted". That added overhead, "particularly to high-throughput installations". Notifications now come from the client: "a client-level debouncer ensures that these notifications are not emitted more often than they could be useful". A due notification "is piggy-backed onto the insert query within the transaction".

The source shows how the debouncer works:

- `notifylimiter.Limiter` keeps a `lastSentByTopic` map.
- `ShouldTrigger(topic)` returns true only once `FetchCooldown` has passed since that queue last fired.
- `maybeNotifyInsertForQueues` de-duplicates the affected queues. It then sends one `NotifyMany` call inside the inserting transaction, with a `{"queue": ...}` payload for each queue that is due.

Reading the code gives three trade-offs:

- The limiter is per process, so the cap grows with the number of processes.
- A send is recorded before the transaction commits. If that insert rolls back, the queue's notifications stay suppressed for the rest of the window.
- River has published no throughput measurements for the change. The changelog says only that the extra insert latency "should be small".

**Listening.**

- One `Notifier` per client owns a single listener connection and a `subscriptions` map from topic to subscribers.
- It issues `LISTEN` when the first subscriber for a topic arrives and waits for it to finish before returning. It issues `UNLISTEN` when the last subscriber leaves.
- Because "waiting for a notification locks an underlying connection", the notifier interrupts its own wait through an interrupt channel to apply `LISTEN` and `UNLISTEN` operations.
- It pings the connection after 5 seconds without a notification. On failure it logs "Notifier unhealthy", reconnects with exponential backoff and re-issues `LISTEN` for every active topic.
- A past bug made the ping use an already-cancelled context, so it "always returned context.Canceled" and dead connections went unnoticed.
- Incoming notifications go into a 1,000-slot buffer with a non-blocking send. When it is full, River logs "Dropping notification due to full buffer". A separate goroutine calls subscribers synchronously, so one slow subscriber delays every topic.

**Running without LISTEN.**

- 0.3.0 added `Config.PollOnly`. A new job or a resigning leader "will be noticed less quickly", but River can then run on "systems without listen/notify support, like PgBouncer operating in transaction pooling mode".
- River's PgBouncer guide says `LISTEN`/`NOTIFY` is "required for both listening for new jobs and listening for leadership demotions". It recommends transaction pooling for inserts and workers, and a direct Postgres pool for the coordinators.
- Later releases extend the same idea:
  - 0.23.0 notifies producers in the same process directly when the driver cannot listen.
  - 0.40.0 adds a table-backed "pseudo listen/notify" for SQLite.
  - 0.48.0 (30 September 2026) polls for job cancellations and queue pause or resume changes when `LISTEN`/`NOTIFY` is unavailable.

#### Oban

Oban delivers through a pluggable `Oban.Notifier` with `insert`, `leader`, `signal` and `sonar` channels. Each node has one dedicated notifier connection, which relays notifications to registered processes as Erlang messages. Backpressure therefore lands in unbounded process mailboxes.

**The Postgres notifier.**

- It delivers notifications "only after a transaction completes" and de-duplicates them. Payloads cannot exceed 8 kB.
- Its documentation states that notifications "will not be sent if the transaction is rolled back … this is the only notifier which provides that guarantee".
- It "doesn't work with connection poolers like PgBouncer when configured in transaction or statement mode".
- For that case Oban recommends `Oban.Notifiers.PG`, which uses distributed Erlang. It "scales much better than Oban.Notifiers.Postgres but lacks its transactional guarantees".

**Removing the trigger.** Historically Oban "used database triggers to emit a notification after a job is inserted". Oban 2.17.0 (8 December 2023) moved insert notifications into application code, "so it's possible to disable triggers without running database migrations". The notifications now "work for any notifier, not just Postgres". The `insert_trigger` option (default `true`) can be turned off "if sub-second job execution isn't important or you'd like to reduce PubSub chatter".

**Scheduled and retried jobs (the Stager).**

- The Stager runs every 1,000 ms and stages at most 5,000 jobs per run.
- When notifications work, "only the leader node stages jobs and notifies queues".
- When they do not, "staging switches to a less efficient 'local' mode in which all nodes poll for jobs to run".

**Health checking (Sonar).** Oban 2.17.4 added `Notifier.status/1`, backed by a Sonar process:

- Each node sends a notification on the `sonar` channel and records which nodes it hears from.
- It pings every 5 s after a status change and backs off to 60 s. A node is treated as stale after 120 s.
- The status is `isolated` (hears nothing), `solitary` (hears only itself) or `clustered`.
- When the node is isolated, the Stager signals local queues directly on its 1 s interval, "to ensure jobs keep processing when staging or database driven notifications fail".
- This fixed a bug where "isolated follower nodes wouldn't process jobs".

**Other behaviour.**

- Each queue's producer waits on average `dispatch_cooldown` (5 ms) before fetching again, which smooths out bursts of notifications.
- "Every connection, including a reconnect after a crash, listens on all channels that have registered listeners". Registrations are "durable across notifier restarts".
- Listeners whose process has exited are removed.

#### Graphile Worker

Graphile Worker advertises "typically under 3ms from task schedule to execution" because it "uses `LISTEN`/`NOTIFY` to be informed of jobs as they're inserted". It finds jobs with `SKIP LOCKED`.

Its FAQ explains why polling must remain: "We cannot use `LISTEN`/`NOTIFY` since there's nothing to generate an event when the time 'ticks over'." `pollInterval` (default 2,000 ms) exists "for jobs scheduled in the future/retries". The FAQ suggests 30 to 60 s is acceptable if database load is a concern.

**Dispatch.**

- Each worker pool creates "a client dedicated to listening for new jobs". It reconnects with exponential backoff capped at 60 seconds.
- Dispatch within an instance deliberately drops events: "Each instance will `LISTEN` for events and when it receives one it will pick an idle worker to ask for jobs. If there is no idle worker then it'll drop the event". Busy workers ask for the next job when they finish anyway.

**Local queue.**

- The newer `localQueue` locks a batch of jobs ahead of time to cut database round trips. The documented costs are higher latency and late detection of new high-priority jobs.
- A `refetchDelay` holds back re-fetching after a fetch returns few jobs. In the source, incoming notification "pulses" are counted and can end that delay early.

#### GoodJob

GoodJob sends `NOTIFY good_job` with a JSON payload from application code.

- Each process has one Notifier. It checks a connection out of the ActiveRecord pool, removes it from the pool, and listens on a background thread.
- It calls each recipient in turn on that thread.
- It exposes `connected?` and `listening?` so callers can see whether it is healthy.

Its timing constants are:

| Constant | Value |
|---|---|
| `WAIT_INTERVAL` | 1 s blocking wait for a notification |
| `RECONNECT_INTERVAL` | 5 s |
| `KEEPALIVE_INTERVAL` | 10 s (a no-op `SELECT 1`) |
| `CONNECTION_ERRORS_REPORTING_THRESHOLD` | 6 consecutive errors before reporting |

**Polling.** `poll_interval` defaults to 10 s in production, "in case of a LISTEN/NOTIFY blip". It is disabled by default in development, because "LISTEN/NOTIFY is enabled in both production and development, so polling is not strictly necessary."

**PgBouncer.** GoodJob "is not compatible with PgBouncer in transaction mode", because both its advisory locks and `LISTEN`/`NOTIFY` "require full database connections". Using the `:skiplocked` lock strategy with listening disabled makes it compatible. The README budgets "2 additional connections" for utility work such as listening and cron.

#### pg-boss

pg-boss polls by default every `pollingIntervalSeconds` (default 2 s, minimum 0.5 s).

- A request for `LISTEN`/`NOTIFY` support (issue #24) has been open since March 2017.
- A 2026 suggestion to replace fixed polling with deadline-based sleeping, citing the CPU and connection cost of polling, was closed as not planned.

Recent documentation adds an opt-in `useListenNotify` instance option and a per-queue `notify: true` flag:

- The notification is "emitted transactionally with the insert, so it fires on commit".
- Only "immediately-available jobs emit a notification". Future-dated jobs and jobs blocked by flow dependencies are found by polling.
- While a queue is listening, polling slows to `notifyPollingIntervalSeconds` (default 30 s) but "always keeps running as a safety net".
- One dedicated, session-pinned connection is used, tuned by heartbeat and keepalive settings.
- If the listener cannot be established, pg-boss emits a `listen_notify_unavailable` warning and continues with polling only.

#### Solid Queue

Solid Queue has no `LISTEN`/`NOTIFY` path:

- Workers poll `solid_queue_ready_executions` every 0.1 s.
- Dispatchers poll scheduled executions every 1 s and move due jobs in batches of 500.

The 37signals announcement (Rosa Gutiérrez, December 2023) explains the design. Scheduled, in-progress, failed and blocked jobs are kept in separate tables, so "the table that workers poll" stays "as small as possible". A covering index keeps the polling queries "very fast, non-blocking". In production, "with around 5.6 million jobs executed daily, we perform around 1,300 polling queries per second, with an average query time of 110 µs and examination of 0.02 rows per query".

Users running PostgreSQL have reported CPU spikes from the ready-executions polling query (issue #508). No maintainer response there was visible.

#### pgmq

**Long-polling.** pgmq offers `read_with_poll(queue, vt, qty, max_poll_seconds DEFAULT 5, poll_interval_ms DEFAULT 100)`. This is a PL/pgSQL loop that re-runs the read and calls `pg_sleep` until it finds rows or times out. It holds a database backend for the whole wait.

**Insert notifications.** pgmq 1.7.0 added `enable_notify_insert`, and 1.8.0 added throttling (PR #445, merged November 2025). The current mechanism works as follows:

- `enable_notify_insert(queue_name, throttle_interval_ms DEFAULT 250)` installs `AFTER INSERT DEFERRABLE FOR EACH ROW` constraint triggers.
- Throttle state lives in an UNLOGGED table, `pgmq.notify_insert_throttle`, with columns `queue_name`, `throttle_interval_ms` and `last_notified_at`.
- For each inserted row, the trigger runs `UPDATE ... SET last_notified_at = clock_timestamp()`. The update applies only where the throttle interval is zero or has elapsed.
- Only if a row was updated does the trigger call `pg_notify('pgmq.q_' || queue || '.' || TG_OP, NULL)`.

The PR's review discussion focused on write amplification, which is why the table is unlogged. It contains no benchmarks.

**A silent bug.** For queues with mixed-case names, the trigger rebuilt a lower-cased name. So "the throttle UPDATE touched nothing ... and the PG_NOTIFY it guards was never reached". Notifications never fired, and consumers fell back silently to whatever polling they had. The fix shipped in 1.13.1 in September 2026.

#### Message DB with Eventide

**Reading.** The consumer interface is `get_category_messages(category, position, batch_size DEFAULT 1000, …, consumer_group_member, consumer_group_size, condition)`. It selects `WHERE category(stream_name) = $1 AND global_position >= $2 ORDER BY global_position ASC LIMIT $3`. It has no high-water mark and no gap tracking. The README says "Global position may have gaps", and the reader simply steps over them. Message DB offers category, stream and last-message reads, but no read across the whole store.

**Polling.** Eventide consumers poll this function through the `poll` library:

- A consumer sleeps for `poll_interval_milliseconds` only when a batch comes back empty. Otherwise it loops immediately.
- It saves its read position every 100 messages by default (`position_update_interval`).

**Writing.**

- `write_message` always calls `acquire_lock` first.
- `acquire_lock` derives the category from the stream name, hashes it with `hash_64` (the first 64 bits of an MD5 digest), and takes `pg_advisory_xact_lock` on the result.
- Only then does it read the stream version, check `expected_version` and insert.
- `global_position` is a `bigserial` key, so its value is drawn during the insert, while the lock is held.

**Why the lock exists.** The lock does two jobs:

- It keeps per-stream `position` gapless. The maintainer explained that otherwise "using the `position` as an optimistic concurrency protection would not be guaranteed to work".
- It keeps consumers safe. The Eventide documentation says: "all writes to streams in a given category are queued and processed in sequence. This ensures that write of a message to a stream does not complete after a consumer has already proceeded past its position."

The maintainer's March 2024 issue #54, "Add a high-performance, non-locking write function for purely telemetric use cases", is still open and has no description. That suggests the lock is the known limit on write rate.

#### Marten's async daemon

**Original design.** In 2016 Marten's creator explained how the daemon worked:

- It polls the events table.
- It uses a `SEQUENCE`-backed `bigint` key, because sequential GUIDs could not be ordered reliably "under a heavy parallel load".
- It queries only events older than a threshold ("3 seconds is the default") "to avoid missing events still in flight".
- It "pauses a little bit, and tries again" when it sees gaps.

**Current design.** The documentation now describes a "high water mark": "the point below which the daemon knows every event is committed and safely ordered".

- The daemon stops at any hole. It skips one only when no live transaction could still fill it: it "rules out any session that has provably executed nothing since before the gap's sequence numbers were handed out".
- Skips are recorded in `mt_high_water_skips`.
- An optional `SkipStaleGapsDespiteLiveTransactionsAfter` setting knowingly gives up that guarantee in exchange for progress.
- Marten also reduces gaps at the source. Failed appends write "tombstone" rows into the sequence numbers they reserved. `QuickAppend` mode is "substantially less likely to lead to gaps".
- Leader election uses advisory locks.

**Published problems.**

- **Skipping under slow appends.** The performance guide warns of event "skipping" when appending transactions "become slower than the `StaleSequenceThreshold` (the default is only 3 seconds)". It recommends Quick append mode.
- **Skipped events under concurrent appends (2026).** A maintainer discussion records this failure. Sequence numbers are reserved before rows become visible. The gap detector ran three statements, each with its own READ COMMITTED snapshot, and it "could only see gaps that sit between two visible rows". So the high-water mark could silently pass an event that had committed but was not yet visible to it. Version 9.16.1 (July 2026) fixed this with two changes: gap queries now run in a single snapshot, and the daemon checks `pg_locks` and `pg_stat_activity` for in-flight appends before skipping. It also now logs every skip at Warning level.
- **Over-polling (2021).** Issue #1862 reported the daemon polling up to three times in 10 ms, although the expected `FastPollingTime` and `SlowPollingTime` defaults were 250 ms and 1 s.

### What `NOTIFY` costs the writing transaction

#### A cluster-wide lock held across commit

Issuing `NOTIFY` itself costs almost nothing. The notification sits in backend-local memory until commit. Within one transaction, repeats of the same channel and payload are delivered once. Distinct payloads are always delivered separately, and a hash table is built for duplicate detection once 16 notifications are pending.

The cost comes at commit:

- `PreCommit_Notify` takes a heavyweight `AccessExclusiveLock` through `LockSharedObject(DatabaseRelationId, InvalidOid, 0, ...)`. The source comment says it is held "till after commit", so that "there are never uncommitted queue entries ahead of committed ones".
- The lock is on "database 0", which is a shared object. It therefore serialises notifying commits across every database in the cluster, not just one.
- It sits inside an `if (pendingNotifies)` block, so transactions that issue no `NOTIFY` never take it.

`CommitTransaction` in `xact.c` shows what the lock spans:

1. `PreCommit_Notify` runs before `RecordTransactionCommit`.
2. `RecordTransactionCommit` performs the synchronous `XLogFlush` and, when required, the synchronous-replication wait.
3. `AtCommit_Notify` signals listeners.
4. Locks are released only afterwards.

So a notifying append holds the lock through its own WAL flush and through any synchronous-standby round trip, and every other notifying commit waits behind it. The author of a later patch put it this way: "each must become durable before the next can proceed".

#### Measured cost

**Notifying inserts against plain inserts.** Arseniy Mukhin posted a pgbench run in October 2025, on the master branch before PostgreSQL 19, with 100 clients:

- `insert_only`: about 31,000 TPS.
- `insert_with_notify` (the same transaction plus one `NOTIFY` with a 10-byte payload): 817.9 TPS. That is roughly a 38-fold drop.
- `notify_only` (no insert): 87,751 TPS.

The explanation that fits the commit sequence above: a transaction that has written data must flush its commit record to WAL while holding the lock, so notifying writers cannot share group commits. A NOTIFY-only transaction writes no other WAL and holds the lock only briefly. Mukhin offered no explanation of his own, so this reading is an inference.

**A second measurement.** The patch thread's opening message reported about 30,000 TPS on master against 60,000–70,000 with the patch, using a different script and configuration. The size of the penalty depends on the environment; its direction does not.

**Listener count.** Before PostgreSQL 19, `SignalBackends()` signalled every listening backend in the database, whatever channel it was on, while the lock was held. Joel Jacobson's July 2025 benchmark fell as idle listeners on unrelated channels were added:

- no extra listeners: 9,126 TPS
- 10 listeners: 6,233 TPS
- 100 listeners: 2,020 TPS
- 1,000 listeners: 238 TPS

With 1 listener and 1,000 notifying clients, commenting out the heavyweight lock raised throughput from 7,679 to 95,430 TPS. Jacobson, who had first blamed the signalling, wrote "I was wrong here": which of the two dominates depends on the workload.

**In production.** Recall.ai (July 2025) had "tens of thousands of simultaneous writers". It logged waits such as "acquired AccessExclusiveLock on object 0 of class 1262 of database 0 after 1015.921 ms". Throughput, CPU and I/O all fell as load rose, which is the signature of lock serialisation rather than resource exhaustion. Recall.ai moved the notification logic into its application.

The post was later updated to say that commit 282b1cde "eliminated the bottleneck". That commit keeps the lock (see below), and the post does not say how much of its lock-hold time was signalling.

#### What PostgreSQL 19 changes, and what it does not

PostgreSQL 19 Beta 4 was announced on 24 September 2026; its release notes are dated "as of" 14 September 2026. They contain one NOTIFY item: "Improve NOTIFY to only wake up backends that are listening to specified notifications."

That item is commit 282b1cde, by Joel Jacobson, committed by Tom Lane on 15 January 2026:

- It adds a shared channel map and "direct advancement". Only backends listening on the notified channel are signalled. Idle listeners on other channels have their queue pointers moved forward without being woken.
- The commit message reports "very significant improvements (integer multiples)" when many listeners are on other channels. In the patch thread the 1,000-listener case stayed near 9,000 TPS.
- A follow-up fix, "Fix NOTIFY wakeups for pre-commit LISTEN entries", landed in May 2026.

The design explicitly relies on the lock: "This is possible because all NOTIFY writers are serialized by a heavyweight lock". Master's `async.c` still takes it. PostgreSQL 19 therefore shortens the signalling part of the critical section, but notifying commits are still serialised, WAL flush included.

#### The stalled "write notifications through WAL" patch

The patch that would remove most of the serialisation is not in any release and has not been committed.

**Origin.**

- In July 2025, Rishu Bagga proposed a GUC to allow out-of-order NOTIFY.
- Tom Lane rejected it, saying "GUCs that change query semantics turn out to be very regrettable choices". He also called "queue entries appear in commit order" "a rather fundamental assumption in async.c".
- Lane then sketched an alternative. Notification payloads would be written into a WAL record, its LSN put in the commit record, and only a small (xid, database, LSN) entry added to the queue. Listeners would read payloads back out of WAL. This would also let hot-standby sessions receive notifications, "a feature many have asked for".
- Even in that design, "step 3 would still require a global lock, to ensure that SLRU entries are made in commit order". The design narrows the serialised section; it does not remove serialisation.

**Problems reviewers found, version by version:**

- An early version wrote a notify record *after* the commit record. Mukhin objected: "I don't think we can write anything about the transaction to the wal after we wrote the transaction commit record".
- v4 failed an assertion (`palloc` inside a critical section), and isolation and regression tests failed (Matheus Alcantara).
- Without the `XidInMVCCSnapshot` check, a listener "got the notification but still can't see changes notify transaction done". The check was dropped in v6 and restored in v8.
- A full queue inside the critical section would PANIC, where master raises an ERROR. Later versions reserve a queue slot before entering the critical section.
- Using `NotifyQueueLock` to enforce commit order blocked readers and lost WAL's parallel inserts. v7 fixed the second point.
- WAL retention needed "pinning" so that unread notifications are not recycled. Mukhin found two bugs in it: unbounded WAL growth, and a race with the checkpointer. Both were fixed in v8.
- v8 added LISTEN on hot standbys. Nobody in the thread examined it.

Mukhin's benchmark (100 clients, no listeners, 10-byte payload):

| Workload | master (TPS) | v6 (TPS) | v7 (TPS) |
|---|---|---|---|
| `insert_only` | 30,982 | 34,941 | 30,933 |
| `insert_with_notify` | 817.9 | 33,762 | 33,870 |
| `notify_only` | 87,751 | 36,141 | 34,262 |

The patch brought notifying inserts to near insert-only throughput. It also made bare `NOTIFY` about 2.5 times slower: "master is 2.5 times faster than the patched version. Don't know why it happens." In the messages examined, that regression was never explained or fixed.

**Status.**

- v9 (25 November 2025) is the last message in the thread.
- The patch is absent from the PostgreSQL 19 release notes, and the `async.c` history up to 23 September 2026 has no WAL-transport commit.
- Its commitfest status could not be checked without a login.

A design should treat the commit-time lock as a lasting constraint.

**Documentation.** In July 2026, Nikolay Samokhvalov proposed documenting the global commit lock. His proposal recommends `log_lock_waits` for diagnosis and points to logical decoding as an alternative. Bruce Momjian replied that, given PostgreSQL 19's changes, a wiki page might suit better.

#### Other ways NOTIFY couples to the writer

- **Queue full.** The shared notification queue is 8 GB in a standard install. A session that has run `LISTEN` and then sits in a long transaction stops it from being cleaned up. PostgreSQL warns, naming that session, once the queue is half full, and `pg_notification_queue_usage()` reports how full it is. When it fills, "transactions calling NOTIFY will fail at commit". An idle-in-transaction listener can therefore make appends fail.
- **Two-phase commit.** "A transaction that has executed NOTIFY cannot be prepared for two-phase commit".

### Reducing the cost on the sending side

#### Debouncing inside the transaction

Only transactions that carry a pending notification pay the serialisation cost, so debouncing works by reducing how many such transactions there are.

**River's limiter** allows at most one notifying insert transaction per queue per 100 ms *per process*. That is at most about 10·P·Q lock-taking commits per second for P inserting processes and Q busy queues. Every other insert commits without touching the lock.

**pgmq's throttle** keeps its state in the database. It allows at most one notifying commit per 250 ms *per queue across all producers*: about 4 per second, which is a tighter bound. With `throttle_interval_ms = 0`, every row calls `pg_notify`. Identical channel and NULL payload fold within a transaction, but every sending transaction still takes the commit lock.

Reading pgmq's SQL suggests some contention costs (an inference):

- Every inserted row runs an extra `UPDATE` against one hot row per queue.
- When the window has expired, the first sender's `UPDATE` holds that row lock until its commit, and that commit also holds the NOTIFY lock. Concurrent senders queue behind it, then re-check and skip.
- The timestamp is taken at `UPDATE` time, not at commit.

**How much this helps.** Neither maintainer has published commit-throughput measurements. At 100 ms or 250 ms windows, notifying commits drop to single digits or low tens per second per channel. That is well below the serialised ceiling of hundreds per second seen above, so most of the 818-against-31,000 TPS gap should disappear. Some costs remain:

- Notifying commits are still serialised, and still pay their WAL flush under the lock.
- On PostgreSQL 18 and earlier, each notifying commit still signals every listener in the database.
- pgmq adds waits on its throttle row.
- Consumers can wait up to the debounce interval for a wake-up, or up to the fallback poll when the suppressed notification was the last one.

The consistent advice for an append-only store on every version up to 19 is:

- Never `NOTIFY` per row or per append.
- Debounce per channel, for example per category.
- Use constant payloads so that in-transaction folding works.
- Keep a polling fallback.

#### Notifying after commit, in a separate transaction

The alternative is to send `NOTIFY` in its own short autocommit transaction after the append has committed. This trades atomicity for a shorter serialised section.

**Why the NOTIFY-only transaction is cheap.** `NOTIFY` forces an XID, so the transaction writes a commit record. But `RecordTransactionCommit` flushes synchronously only when the transaction wrote WAL *before* its commit record, and the synchronous-replication wait has the same condition. A transaction that only calls `pg_notify` should therefore take the asynchronous-commit path. It would hold the lock only while queueing its entry and inserting its commit record, not through an fsync or a standby acknowledgement. This comes from reading the source, not from a published benchmark. It is consistent with Mukhin's 87,751 TPS `notify_only` figure.

**Latency.**

- Append latency improves under concurrency: the append commits without touching the lock, and a slow synchronous standby no longer slows every notifying commit.
- Wake-up latency worsens by at least one client-to-server round trip. That is tiny next to any polling interval.
- Pipelining has a trap. psycopg's documentation warns that "even in autocommit, the server wraps the statements sent in pipeline mode in an implicit transaction, which will be only committed when the Sync is received". An insert and a `pg_notify` in one Sync group put `NOTIFY` back inside the append.
- A post-commit notifier can debounce across many appends, not only within one transaction.

**Lost wake-ups.**

- **In-transaction `NOTIFY` is atomic and never early.** Listeners hold back entries from a transaction "still in progress" by checking `XidInMVCCSnapshot` "before we test TransactionIdDidCommit". A consumer that queries after a wake-up therefore sees the append.
- **Post-commit `NOTIFY` is a dual write.** It has the classic outbox failure mode: "if a service sends a message after committing the transaction there's no guarantee that it won't crash before sending the message". The events are durably committed, so a missed wake-up only delays them, until the next poll or the next append's notification. The worst case is the last append before a quiet period, whose events wait a full poll interval.
- **Listeners can miss notifications in either design**, for example on disconnects or a psycopg bug. Every system therefore polls, which bounds the extra risk.
- **Send strictly after the commit reply.** A wake-up sent before the append is visible lets a consumer read nothing and go back to sleep. That is the trade-off Oban describes for its `PG` notifier.
- **Failures are isolated the other way round.** Queue-full and two-phase-commit failures then affect only the wake-up, not the append. The library must log `NOTIFY` errors after commit, not raise them: a caller that saw an exception after a successful commit might retry and append twice. This last point is an inference.
- **Ordering is weaker.** Separate notifying transactions can reach the queue in a different order from their appends' commits. That matters only if a payload is treated as a position.

**What maintainers do.**

- River and Oban's Postgres notifier keep `NOTIFY` inside the insert transaction for atomicity, and both rely on a fallback poll.
- Recall.ai moved off `LISTEN`/`NOTIFY` entirely, "in favor of tracking this logic at the application layer".
- In the Hacker News discussion of that post, commenters proposed a relay process "whose only job is to listen to the wal … and then NOTIFY".

### Keeping a listening connection healthy

#### A dedicated session connection on the primary

Every system that listens gives the listener its own connection, for these reasons:

- **Transactions hold notifications back.** Notifications reach a session only *between* transactions, and `LISTEN` takes effect only at commit. psycopg's documentation therefore says to use autocommit "if you wish to receive or send notifications in a timely manner".
- **Registrations belong to the session.** "A session's listen registrations are automatically cleared when the session ends." Pools are a poor home for them:
  - asyncpg's pool runs `UNLISTEN *` on reset.
  - PgBouncer marks `LISTEN` as "Never" supported in transaction pooling.
  - Every reconnect must re-issue every `LISTEN`.
- **The waiting generator holds the connection lock (psycopg).** `notifies()` runs its whole loop under the connection lock. In psycopg issue #340, `execute()` on the same connection hung while another coroutine iterated `notifies()`. A listening connection cannot double as a query connection.
- **How the prior art does it.**
  - Procrastinate, a psycopg 3 job queue, opens a standalone `autocommit=True` connection outside its pool.
  - GoodJob removes its connection from the pool.
  - Oban uses a dedicated `Postgrex.SimpleConnection`.
- **The listener must reach the primary.** Hot standbys reject `LISTEN` and `NOTIFY`, and notifications exist only on the server where they committed. A multi-host connection string should set `target_session_attrs=read-write` (or `primary`). Notifications in flight on an old primary are lost at failover.
- **Channel names are identifiers, not parameters.** Build the statement with `sql.SQL("LISTEN {}").format(sql.Identifier(channel))`. For sending, psycopg suggests `pg_notify()`.

Every listening system also ships a poll-only escape hatch for poolers:

- River: `PollOnly`.
- GoodJob: `:skiplocked` with listening disabled.
- Oban: the Erlang `PG` notifier.
- pg-boss: falls back to polling automatically.

Managed poolers impose the same constraint:

- RDS Proxy treats "Listening on a notification channel" as a pinning condition, so each listener holds a dedicated database connection.
- Cloud SQL Managed Connection Pooling defaults to transaction mode, in which `LISTEN` is not supported.
- Azure's built-in PgBouncer defaults to `transaction` mode.

#### Silent network death and cloud middleboxes

A waiting listener sends nothing, so in quiet periods its TCP flow is idle in both directions. The defaults barely help:

- libpq enables keepalives by default, but the timings fall back to the operating system. On Linux that is a 7,200 s idle period, then 75 s probe intervals, then 9 probes: about 2 hours 11 minutes before a dead peer is declared.
- `tcp_user_timeout` limits only how long "transmitted data may remain unacknowledged". An idle listener has no such data.
- `client_connection_check_interval` applies only "while running queries".

Every documented cloud middlebox idle timeout is far shorter than that:

| Component | Default idle timeout | Documented behaviour on expiry |
|---|---|---|
| AWS NAT gateway | 350 s (fixed) | Returns an RST "to any resources behind the NAT gateway that attempt to continue the connection (it does not send a FIN packet)"; advises keepalive "less than 350 seconds" |
| AWS Network Load Balancer, TCP | 350 s, configurable 60–6,000 s | Connection "no longer tracked"; later data gets an RST |
| AWS EC2 connection tracking | 350 s on Nitro v6; 5 days on others | Silent drop; long-lived connections such as "database connection pools" should send keepalives "at intervals of less than 5 minutes" |
| Google Cloud VPC firewall tracking | 10 min | Entries removed after 10 idle minutes; Linux keepalive example of 60 s idle, 60 s interval, 5 probes |
| Google Cloud NAT | 20 min, configurable | Mappings removed |
| Azure Load Balancer (Standard) | 4 min, configurable | Session "isn't guaranteed to be maintained"; optional TCP reset |
| Azure NAT Gateway | 4 min, configurable to 120 | "silently dropped"; RST only "when it detects traffic on the dropped connection flow" |

The vendor documents imply a direction problem for listeners; no vendor states it. After a silent drop, the next notification is server-to-client data, so any RST goes to the database, not the client. A waiting client may not learn the flow has gone for about 2 hours, and notifications sent meanwhile are lost to it.

Two measures together close the gap:

- **Client-side keepalives below the shortest timeout on the path**, around 60–120 s idle, both refresh middlebox state and expose dead flows.
- **A periodic application query** creates outbound data, so `tcp_user_timeout` can then bound detection.

#### Managed-service maintenance and failover

Disconnection is routine on managed services, not exceptional:

- **Amazon RDS.**
  - Multi-AZ instance failover typically takes 60–120 s; the two-reader cluster form takes under 35 s. Both require clients to "re-establish any existing connections".
  - Blue/Green switchover "Drops connections to the DB instances in both environments".
  - RDS Proxy closes clients idle beyond `IdleClientTimeout` (default 1,800 s) and enforces a fixed 24-hour maximum client connection life.
- **Aurora PostgreSQL.**
  - Failover is "typically … less than 60 seconds, and often less than 30 seconds".
  - AWS's fast-failover guidance recommends very aggressive keepalives (1 s idle, 1 s interval, 5 probes), low DNS TTLs, and checking `SHOW transaction_read_only` after connecting. During DNS propagation a connection can land on a reader, which rejects `LISTEN`.
  - Zero-downtime patching lists "All sessions are listening on notification channels." among the conditions under which connections are dropped. The wording is ambiguous.
  - In Serverless v2, any open connection prevents auto-pause, so a permanent listener stops a cluster scaling to zero.
- **Google Cloud SQL.**
  - During maintenance "existing connections are dropped": typically under 30 s for Enterprise and under 1 s for Enterprise Plus.
  - With a pooler, the client-to-pooler connection survives maintenance while the pooler-to-database connection is dropped. A listener could therefore stay connected while its registration is gone. This is an inference; the documentation does not discuss `LISTEN`.
  - HA failover closes all connections, with about 60 s of unavailability.
- **Azure Database for PostgreSQL flexible server.**
  - HA failover completes within 60–120 s.
  - Planned maintenance is monthly and "involves some downtime". On HA servers the service "drains primary server connections and triggers a failover".

#### Detecting failure

Four kinds of failure need different detectors:

1. **Clean server-side termination.** This covers restarts, `pg_terminate_backend` and `idle_session_timeout`. The blocked generator wakes and raises. Since psycopg 3.2.4 it raises the specific error, such as `AdminShutdown` or `IdleSessionTimeout`. All SQLSTATE class 08 and 57 errors map to `OperationalError`, so a single `except` catches them. `idle_session_timeout` will end a waiting listener, so disable it for the listener's role or ping more often than it.
2. **A half-open TCP connection.** Handle this with explicit keepalives, `tcp_user_timeout`, and a client-side deadline on the health query (for example `asyncio.wait_for`). psycopg 3.3.6 fixed a case where it waited forever for an interrupted query on an unresponsive server.
3. **A dead connection while idle.** Ping it with a round-trip query:
   - Procrastinate runs `SELECT 1` whenever its 30 s `notifies()` timeout expires.
   - River pings after 5 s without a notification.
   - GoodJob pings every 10 s.

   River's cancelled-context bug shows that a health check must actually reach the server.
4. **A healthy connection with no notifications flowing.** Causes include a missing registration, a publisher that stopped, or a pooler in the path, and `SELECT 1` cannot see any of them. Oban's Sonar is the published answer. A psycopg equivalent is to send `pg_notify` on a heartbeat channel on a schedule and check that it arrives. `Notify.pid` compared with `conn.info.backend_pid` identifies the listener's own notifications.

#### Reconnecting and catching up

The PostgreSQL `LISTEN` documentation gives a startup order with no race:

1. Run and commit `LISTEN`.
2. In a new transaction, read the current state.
3. Rely on notifications for later changes.

The first few notifications "might refer to updates already observed … but this is usually harmless". This sequence must run on *every* reconnect, not only at startup. Anything committed while the listener was disconnected produced a notification it can never receive.

The reconnect loop should:

1. Connect with an explicit `connect_timeout`. psycopg otherwise defaults to 130 s (minimum 2 s), applied per host.
2. Set autocommit, keepalive parameters and `target_session_attrs`.
3. Re-issue every `LISTEN`.
4. Mark the listener healthy and trigger a catch-up read.
5. On error, mark it unhealthy and back off with a capped exponential delay plus jitter.

Prior art on backoff:

- River uses exponential backoff, and Graphile Worker caps its backoff at 60 s.
- GoodJob waits a fixed 5 s.
- Procrastinate reconnects immediately with no backoff, which risks a tight loop while the database is down.
- psycopg_pool's `reconnect_timeout` with a `reconnect_failed` callback is the model for alerting when the listener has been down too long. A long outage silently turns every consumer into a pure poller.

#### psycopg 3 specifics

- **`notifies(timeout=…, stop_after=…)`** exists since 3.2. The deadline is fixed at the start, so the call ends on time even on a busy channel. Without a timeout it blocks indefinitely and never yields to health checks.
- **Notifications between generator runs:**
  - Before 3.2.4, notifications arriving between `LISTEN` and the generator, or between two generator runs, were lost (issue #962).
  - 3.2.4 added a backlog to keep them. That backlog leaked memory on long-lived connections, around 200 MB per hour at 10,000+ notifications per minute (issue #1091).
  - 3.2.10 fixed the leak. It also deprecated mixing notify handlers with the generator on one connection, which now raises a runtime warning.

  A consumer should require psycopg 3.2.10 or later.
- **Notify handlers suit an idle listener poorly.** `add_notify_handler()` callbacks fire only "during a connection operation, such as a query". They also run synchronously inside psycopg's processing.

A minimal single-listener loop, assembled from these sources:

```python
async def run_listener(conninfo, channels, wake, health):
    attempt = 0
    while True:
        try:
            async with await psycopg.AsyncConnection.connect(
                conninfo, autocommit=True, connect_timeout=10,
                keepalives=1, keepalives_idle=15, keepalives_interval=5,
                keepalives_count=3, tcp_user_timeout=30_000,
                target_session_attrs="read-write",
            ) as conn:
                for channel in channels:
                    await conn.execute(
                        sql.SQL("LISTEN {}").format(sql.Identifier(channel)))
                attempt = 0
                health.mark_healthy()
                wake.set()                      # catch-up after (re)LISTEN
                while True:
                    async for _ in conn.notifies(timeout=10.0):
                        wake.set()
                    await asyncio.wait_for(conn.execute("SELECT 1"), 5.0)
        except (psycopg.OperationalError, TimeoutError):
            health.mark_unhealthy()
            await asyncio.sleep(min(30.0, 0.5 * 2 ** attempt) * random.uniform(0.5, 1.0))
            attempt += 1
```

### Sharing one listener across the consumers in a process

The evidence favours one dedicated listener connection per process, or per event loop for asyncio code, with wake-ups routed to consumers by channel:

- **A listening connection cannot run queries.** psycopg serialises everything on a connection, and the waiting generator holds the lock. One listener per consumer multiplies connections for no benefit.
- **Every listening backend is woken today.** On PostgreSQL 18 and earlier, every `NOTIFY` wakes every listening backend in the database: "we don't know which backend is listening on which channel so we must signal them all". N listener connections per process mean N backends woken per append. PostgreSQL 19 removes that cost, but each extra connection still costs a slot.
- **Managed services penalise extra listeners.** Behind RDS Proxy each listener pins a database connection, and any open listener blocks Aurora Serverless v2 from auto-pausing.
- **Mature systems all take this shape.**
  - River: one notifier per client with a subscription map.
  - Oban: one notifier connection per node.
  - GoodJob: one notifier per process.
  - Graphile Worker: one listening client per pool that nudges idle workers.
  - Procrastinate: one listener task per worker that sets an `asyncio.Event`.

psycopg connections "are not process-safe", so each process needs its own notifier. The shared connection is a single point of failure, which is acceptable only because consumers keep polling.

**A single reader loop.** One task or thread owns the connection, iterates `notifies()`, and looks up `notification.channel` in a routing table. PgQueuer's psycopg driver shows the trap to avoid. Each `add_listener()` starts its own watcher iterating `notifies(timeout=1.0, stop_after=1)` and hands every payload to its callback without checking the channel. Watchers then compete for the lock and receive other channels' notifications.

**Subscribing.**

- Keep a reference count per channel. River issues `LISTEN` only for a topic's first subscriber, and Oban filters out channels that already have listeners. Re-issuing `LISTEN` would be harmless anyway.
- Make subscribe return only after `LISTEN` has committed. The consumer then does its catch-up read and waits for wake-ups, which is the race-free order from the `LISTEN` documentation.
- Find a way past the lock, because a subscribe request cannot run `LISTEN` while the reader is waiting. River interrupts its own wait and drains pending `LISTEN` and `UNLISTEN` operations before waiting again. A psycopg notifier could either:
  - queue commands and apply them each time a bounded `notifies(timeout=t)` call returns, so subscribe latency is bounded by `t` and the 3.2.4+ backlog keeps notifications that arrive in between; or
  - cancel the waiting task.

  Neither is a documented psycopg recipe.
- On reconnect, re-`LISTEN` every channel and then wake every subscriber so each does a catch-up read. Oban re-listens on all registered channels. River leaves recovery to its poll interval; waking subscribers is an inference.

**Unsubscribing.** Remove the subscriber from the routing table at once. Send `UNLISTEN` only when the channel's count reaches zero, as River does ("If this is the last subscription on the topic, unlisten if we're connected"). Tie each subscription's lifetime to a context manager or to the consumer task, as Oban removes the listeners of processes that have exited.

**Backpressure.**

- Keep draining. The server queue cannot be cleaned up while a listener lags inside a transaction. psycopg's backlog is an unbounded `deque` that grows if the reader stalls.
- The systems treat fan-out differently:
  - River drops notifications when its 1,000-slot buffer is full.
  - Oban relies on unbounded mailboxes.
  - GoodJob calls recipients in turn on the listening thread.
  - Graphile Worker nudges only idle workers.
  - Procrastinate only sets an event.
- For consumers that read from their own checkpoint, ten notifications mean the same as one: read again. A per-subscriber coalescing signal is bounded and loses nothing. That can be an `asyncio.Event`, a `threading.Event`, or a queue of size one filled with `put_nowait` that ignores "full".
  - The reader sets it without blocking.
  - The consumer clears it *before* each read, so a wake-up that arrives mid-read triggers one more pass.
  - Memory then scales with subscribers, not with notification rate.
- Subscriber code must never run on the reader thread or task.
- A consumer-side cooldown, like River's `FetchCooldown`, caps the query rate under heavy append load.

### Why a wake-up does not make a position-based read safe

#### The cause

**Sequences do not follow commit order.** PostgreSQL hands out sequence values when `nextval` runs, not at commit, and never gives them back. The documentation says "transaction aborts or database crashes can result in gaps". Gaps appear even without aborts; `INSERT ... ON CONFLICT`, for example, draws a value before it detects the conflict. Sequences "cannot be used to obtain 'gapless' sequences".

Two concurrent appenders can therefore commit in the opposite order to their numbers. A processor that reads transaction #2's events first and records #2's position as its checkpoint will skip transaction #1's events when they become visible, "and thus lose the events created by transaction #1". A reader also cannot tell whether a hole came from a rollback or from a slow transaction that is still running: "If we optimistically assume that the gaps were caused by rollbacks, we may lose messages."

**`NOTIFY` cannot close the gap.**

- It is delivered only once the transaction commits.
- Messages "from different transactions are delivered in the order in which the transactions committed", so a later notification can carry a *lower* position than an earlier one.
- A notification says that something committed. It cannot say that everything below a position has committed.

The safety guarantee must therefore come from the read query and the checkpoint rule. The notification decides only *when* to read.

#### Four strategies

| Strategy | Used by | Read rule | Cost |
|---|---|---|---|
| Make commit order match sequence order by serialising appends | Message DB (per category, advisory lock); Python `eventsourcing` (`EXCLUSIVE` table lock); Commanded EventStore (row lock on the `$all` counter); a counter-table variant described by Sequin | `position > checkpoint`; every gap can be skipped at once | Appends serialised per lock key; per-key ceiling of about one commit per commit time |
| Filter by the transaction-snapshot horizon | eugene-khyst's reference implementation, Oskar Dudycz's outbox design and Emmett, Watermill | `(xid, position) > checkpoint AND xid < pg_snapshot_xmin(pg_current_snapshot())` | Any long-running write transaction in the cluster delays every reader; the checkpoint becomes a pair |
| Detect gaps by position and resolve them | Marten (high-water mark with transaction evidence), Axon Framework (`GapAwareTrackingToken`) | Stop at holes; skip one on evidence or after a timeout | A purely time-based timeout can skip a live but slow transaction |
| Writers announce their reserved range | Sequin's recommended scheme | Read below `min(objid)` of appenders' advisory locks in `pg_locks` | Appends stay concurrent and ordered by position, and readers are not held back by unrelated long transactions |

#### Serialising appends per lock key

**Why it works.** Inside `CommitTransaction`, `ProcArrayEndTransaction`, which is the point at which a commit becomes visible to new snapshots, "must be done _before_ releasing locks we hold". Transaction-scoped advisory locks are released only at transaction end.

So the next writer in a category cannot even draw a sequence number until the previous writer's transaction is visible or rolled back. Within one category, `global_position` order then equals visibility order. Every gap a category reader sees is permanent: a number used by another category, an aborted insert, or a lost sequence value. The reader can step over it at once, with no wait. By the same argument, reads from a hot standby should also be safe, because commit records reach the WAL in the same serialised order. That is an inference.

The Python `eventsourcing` library takes the same approach with a table lock. It states that this "avoids a potential issue where insert order and commit order are not the same", so that readers "tailing the application notification log" do not miss events.

**Where the guarantee stops.**

- **Only within one lock key.** Readers that span categories, or the whole store, still need gap handling. Message DB avoids the problem by offering no whole-store read.
- **Advisory means voluntary.** "the system does not enforce their use". A bulk import or a hand-written `INSERT` that skips the lock brings the race back.
- **Hash collisions** between categories, or with other code using the same advisory key, add blocking but do not break ordering.
- **Deadlocks.** A transaction that appends to several categories can deadlock with another that writes them in the opposite order. PostgreSQL advises acquiring locks "in a consistent order".
- **Wake-ups are still needed.** The lock removes gap handling, not the need to poll or be woken.

#### What serialisation costs in write throughput

The lock is held from the first write until after the commit record is flushed. Under synchronous replication it is also held until the standby acknowledges. `RecordTransactionCommit` waits "while we … still show as running in the procarray and continue to hold locks". The synchronous-replication documentation says "transaction locks continue to be held until the transfer is confirmed".

So the per-category ceiling is roughly 1 / (work done under the lock + local WAL flush + any replication wait). Group commit cannot help within a category, because the next writer cannot reach commit until the previous flush returns. It still helps across categories.

No published pgbench comparison of this exact shape exists. Message DB's own benchmark runs many `write_message` calls in one PL/pgSQL transaction with one commit, so it measures statement speed under the lock, not commit rate through it. Ceilings estimated from published device and network latencies (calculated, not measured):

| Commit path inside the lock window | Published latency | Per-category ceiling |
|---|---|---|
| Battery-backed RAID cache | 0.04 ms fsync | ~20,000+/s; statement work and CPU set the limit |
| Enterprise NVMe with power-loss protection | 0.14 ms | ~7,000/s |
| AWS EBS io2 Block Express | under 500 µs per 16 KiB I/O | ~2,000/s or a little more |
| Consumer SATA or NVMe SSD | 0.49–9.3 ms | ~100 to ~2,000/s |
| 7,200 or 5,400 RPM disk | 18–66 ms | ~15 to ~55/s |
| Synchronous replication across zones (`on`) | local flush + up to ~2 ms round trip + standby flush | ~300 to 500/s |
| Synchronous replication with `remote_apply` | adds standby replay ("much larger commit delays") | lower again |

Unserialised inserts on the same hardware are not tied to one flush time. The group-commit work in PostgreSQL 9.2 reported "towards 14,000 write transactions per second" on a laptop disk whose raw `fdatasync()` allowed only about 125 serial commits per second. One serialised category on that disk would have had roughly 1% of the unserialised throughput.

The main managed high-availability setups all put a cross-zone acknowledgement inside the lock window:

- Azure always commits synchronously to the standby; Microsoft calls the cost "a small increase in commit latency" and targets under about 2 ms between zones.
- RDS Multi-AZ instances "can have increased write and commit latency".
- RDS Multi-AZ clusters have "lower write latency" than Multi-AZ instance deployments.

**What pushes the per-category rate lower.**

- More clients add only waiting time. Mean latency grows roughly as clients × hold time; for example, 32 clients sharing a 3 ms hold would wait about 96 ms per append.
- `commit_delay` "increases latency by up to the `commit_delay` for each WAL flush", inside every category's window.
- Every client round trip between taking the lock and `COMMIT` adds to the hold time.
- Large batches lengthen the window. Sequin's serialised counter table produced lock durations "very high (seconds)" and write throughput "much lower than we needed".
- The lock manager itself is rarely the limit.

**Levers.**

- Set `synchronous_commit` per transaction. `local` drops the replication wait; `off` drops the flush, at the risk of losing recent acknowledged commits in a crash.
- Batch several events into one append transaction.
- Use finer lock keys, accepting that each reader must then match exactly one key.
- Do not serialise at all. Practitioners rejected global serialisation because "the performance hit … was too big to accept".

To measure a given deployment, run two pgbench scripts, one with and one without `SELECT pg_advisory_xact_lock(:cat)` before the insert:

- Vary the number of categories and clients across the `synchronous_commit` settings.
- Use `-r` latencies to separate lock queueing from flush time.
- Run `pg_test_fsync` on both servers.
- Run each test for minutes; pgbench's documentation advises "never believe any test that runs for only a few seconds".

Marten takes the opposite trade-off: it does not serialise appends, so the cost falls on readers.

#### Taking the ordering lock at commit time

An event store can take its per-category lock from a `DEFERRABLE INITIALLY DEFERRED` constraint trigger. Such triggers fire in `CommitTransaction` before `PreCommit_Notify` and before the commit record. The lock is then held through flush, replication wait and the point of visibility.

This is the same pattern PostgreSQL uses for its own notification queue: `xact.c` places `PreCommit_Notify` "late in the pre-commit sequence to minimize time spent holding the notify-insertion lock."

**One hard condition.** Positions must be drawn *under* the lock, inside the trigger. If only Message DB's lock were moved, positions would be drawn in insert order but the lock taken in commit order, which reopens the window. Assigning positions late has its own costs:

- **Write amplification.** Updating an indexed position column cannot be a HOT update, so every event becomes two heap tuples.
- **No `NOT NULL`.** The position column must be nullable at insert time.
- **Per-row firing.** Constraint triggers fire per row, so the first firing should number every row in the transaction (an inference).
- **Order within a transaction** must come from an explicit column, not from trigger-firing order.

Sequin recommends this late assignment: "make sure you don't touch `my_seq` until the last possible moment in your transaction".

**One hard limit.** The serialised section can never be shorter than one commit. A deferred lock removes only work that sits between the append and `COMMIT`: client round trips, other statements and application logic. For a single-statement, auto-committed append the gain is close to nothing.

**Hazards.**

- The trigger does not fire under `session_replication_role = replica` unless it is `ENABLE ALWAYS`. A row inserted with the trigger disabled commits with a `NULL` position and is invisible to position-based readers forever.
- `SET CONSTRAINTS ALL IMMEDIATE` fires the trigger early. That is still correct, but the lock is held longer.
- `COMMIT` becomes a lock wait that can fail through `lock_timeout`, `statement_timeout` or deadlock detection.
- Optimistic concurrency then needs a non-deferrable unique index on (stream, position).
- A session-level lock released before an outer transaction's `COMMIT` is no substitute. Read from prooph's code (not a documented claim), its PostgreSQL advisory-lock strategy keeps order only when the insert auto-commits.

Message DB, Commanded, Python `eventsourcing` and prooph all take their ordering lock at append time. None uses a deferred trigger, so this design rests on documented PostgreSQL behaviour rather than on a production store.

#### Filtering by the transaction-snapshot horizon

**The scheme.** Each event row stores `pg_current_xact_id()`, a 64-bit `xid8` that "does not wrap around during the life of an installation". Readers return only rows below `pg_snapshot_xmin(pg_current_snapshot())`. All transaction IDs below that horizon "are either committed and visible, or rolled back and dead". The reference query is:

```sql
WHERE (e.TRANSACTION_ID, e.ID) > (:lastProcessedTransactionId::xid8, :lastProcessedEventId)
  AND e.TRANSACTION_ID < pg_snapshot_xmin(pg_current_snapshot())
ORDER BY e.TRANSACTION_ID ASC, e.ID ASC
```

**The checkpoint must be the pair.** A transaction ID is assigned "when a transaction first writes to the database", not when `nextval` runs. So an event can have a higher position but a lower transaction ID than another. The resulting order is neither sequence order nor commit order. Dudycz describes it as respecting "when the transaction was started". Strictly, it is the order of each transaction's first write.

**Which sessions hold readers back.** Only transactions that hold an XID count:

- Any write transaction anywhere in the cluster. `GetSnapshotData` scans every database's backends.
- `SELECT FOR UPDATE`, which counts as a write. That includes a job-queue worker holding a row lock while it runs.
- Prepared transactions.
- The reader's own XID, if the read runs inside a transaction that has already written.

Some sessions do not hold readers back, although they still block VACUUM:

- read-only transactions, including long reports;
- replication-slot `xmin` and `hot_standby_feedback`;
- vacuum and logical-decoding backends.

Readers on a hot standby are held back by long transactions on the primary.

**How long the delay can be.** With stock settings it has no upper bound. The delay is head-of-line blocking: every later event, in every stream, waits behind the oldest writing transaction.

- `statement_timeout`, `transaction_timeout` and `idle_in_transaction_session_timeout` all default to disabled.
- A client that vanishes while idle in a transaction is noticed only by TCP keepalive: about 2 hours 11 minutes on Linux defaults.
- Prepared transactions are covered by no timeout. The documentation recommends `max_prepared_transactions = 0` unless an external transaction manager is in use.

**What bounds it.**

- `idle_in_transaction_session_timeout` limits each idle gap, not the transaction's whole life.
- `statement_timeout` limits each statement.
- `transaction_timeout` (PostgreSQL 17 and later) is the only setting that bounds the whole transaction. It does not apply to prepared transactions, and the documentation advises setting it per role rather than in `postgresql.conf`.
- All of these are user-settable, so any session can turn them off. Common practice raises them per session for migrations, and those long writing sessions are exactly the ones that stall readers.
- PgBouncer has its own equivalents, all disabled by default.

**Monitoring.**

- Rank `pg_stat_activity` by `age(backend_xid)`. Use `backend_xid`, not `backend_xmin`, which would wrongly flag read-only sessions.
- Check `pg_prepared_xacts` separately.
- An unprivileged library role can see `backend_xid` but not the query text or timings; operators need `pg_monitor` or `pg_read_all_stats`.
- A useful library-side metric would be the count, and the oldest age, of committed rows still held back above the horizon. That is a suggested design, not taken from any source.

Watermill warns that "very long-running transactions may result in delayed message delivery". It recommends its Queue adapter, "which does not depend on the transaction ID", for such workloads.

#### Migrating a store from sequence order to transaction-ID order

**Emmett's migration.** Emmett's PR #336 (merged May 2026 for 0.43.0) is the clearest published record of migrating a live deployment.

- **The bug it fixed.** "in high contention, this could cause some messages to be lost in processors". The 0.42.x reader already filtered and ordered by `(transaction_id, global_position)`, but its cursor advanced on `global_position` alone.
- **The new checkpoint.** It became "a compound checkpoint built from the transaction id and global position", stored as a string: the transaction ID zero-padded to 20 digits, a colon, then the position. That was a breaking change: "they're not always bigint, but they will be always monotonic alphanumeric."
- **Converting old checkpoints.** The migration joins each old checkpoint to the event at that position. If that event is missing, the row stays in the old format, and new code then throws when it reads it.
- **Mixed versions.** For blue/green deployments, 0.42.4 (PR #381, August 2026) taught old nodes to read new checkpoints. `store_processor_checkpoint` performs a format-aware compare-and-set. Tests cover both deploy orders.
- **Indexes.** An index on `(transaction_id, global_position)` replaced the single-column index.

**Watermill's migration.** Watermill made the same change in watermill-sql v2.0.0 (July 2023), which needed "changing the public API". It added `xid8` in v4.0.0. Its offsets table gained `last_processed_transaction_id`. It puts `transaction_id` first in the primary key and uses a subquery to stop the query planner misestimating: one plan scanned 241,157 rows, the other 112.

**Risks inferred from transaction-ID semantics:**

- **The overlap window.** Mapping an old checkpoint `p` to `(xid_of(p), p)` creates two problems. An event with a higher position but a lower XID is skipped forever. An event with a lower position but a higher XID is delivered twice.
  - One fix is to convert at a quiescent point.
  - Another is to backfill all history with one constant XID such as `'0'::xid8`. Adding a column with a non-volatile default is metadata-only, and a later `SET DEFAULT` "will only apply in subsequent INSERT or UPDATE commands". History then keeps its old order exactly.
- **Already-lost events stay lost.** Only a replay can recover them.
- **Integer-position code breaks.** Lag computed as `head - checkpoint`, read-your-writes waits, "read up to N" bounds and single numeric positions in APIs all need tuple semantics.
- **Replays can reorder events.** Rebuilt projections can process cross-stream events in a different order from what live consumers saw. Within a stream, versions can arrive out of order if an append transaction wrote something else first.
- **Restores and cluster moves.** Stored XIDs belong to one cluster's counter. Watermill says to reset `last_processed_transaction_id` when moving data between databases, and Emmett defers helpers for restores from a snapshot or `pg_dump` backup. After a restore into a cluster whose counter is behind, restored rows can be hidden and new appends can sort below existing checkpoints.
- **Version requirement.** `xid8` needs PostgreSQL 13 or later, and the older `txid_*` functions are deprecated.

#### Detecting gaps by position

**Marten** keeps a contiguous high-water mark and skips a hole only on evidence from running transactions, as described above. Its 2026 bug shows that the evidence must be gathered in one snapshot.

**Axon's `GapAwareTrackingToken`** stores the highest index seen "and additionally stores a set of possible gaps". Each fetch re-queries those gap positions. Its JDBC engine has three limits:

- it drops gaps more than `maxGapOffset` (default 10,000) behind the head;
- it stops tracking gaps around events older than `gapTimeout` (default 60 s);
- it cleans up once a token holds `gapCleaningThreshold` (default 250) gaps.

**Sequin's writer-announced range** keeps position order without serialising. Each writer takes a shared advisory lock to mark itself as an appender, plus an exclusive lock keyed on its next sequence value. The reader's safe ceiling is the minimum of those keys in `pg_locks`, or everything when no writer holds a lock.

#### Wiring wake-ups to whichever read is chosen

One loop works with every strategy:

1. Commit `LISTEN` on the dedicated connection, then run a catch-up read from the stored checkpoint.
2. Treat every notification as a hint that can be merged with others, and run the safe read until it returns nothing. Never move the checkpoint to a position taken from a notification payload; under strategies 2 to 4 the payload can name events the read is not yet allowed to return.
3. When a read is held back, arm a short re-check timer. A read is held back when there is a hole, rows above the horizon, or a writer's lock lowering the ceiling. The event that unblocks the read may never send a notification: aborts do not notify, gap timeouts are measured in time, and the long transaction holding back `xmin` may not append anything.
4. Keep a slow fallback poll for notifications that are missed or lost.

eugene-khyst's implementation offers `LISTEN`/`NOTIFY` "instead of polling", as a mutually exclusive mode. In that design a held-back read waits for the next append's notification. That delays events but does not lose them, and the re-check timer in step 3 closes the latency gap.

### Lessons across the projects

1. **A notification is a hint; correctness comes from re-reading the table.** Every system that uses `NOTIFY` keeps a periodic poll. Polling is needed anyway for work that becomes due as time passes (Graphile Worker, Oban's Stager, pg-boss). Notifications carry at most a queue name.
2. **Per-row notifications do not scale.** River and Oban replaced insert triggers with debounced or de-duplicated application-level notifications, and pgmq throttles its trigger. PostgreSQL serialises every notifying commit across the cluster, through its WAL flush. PostgreSQL 19 leaves that in place, and the patch that would remove most of it has stalled. Consumers also merge bursts: River's 100 ms `FetchCooldown`, Oban's 5 ms `dispatch_cooldown`, and Graphile Worker dropping events when no worker is idle.
3. **The listener needs its own session connection on the primary, and the library needs a poll-only mode.** Transaction-mode poolers break `LISTEN`, and every listening system ships an escape hatch. Keepalive, reconnect with backoff, re-`LISTEN` and a catch-up read on every reconnect are published parts of the design for GoodJob, River, Oban, pg-boss and Procrastinate. On cloud networks, default keepalives are far longer than middlebox idle timeouts, and managed services drop connections routinely.
4. **Detect silent failure.** Oban's Sonar exists because isolated nodes stopped processing jobs. pgmq's mixed-case bug and River's cancelled-ping bug both failed without errors while polling hid them.
5. **Cheap polling is a valid design.** Solid Queue sustains about 1,300 polls a second at 110 µs average by keeping the polled table tiny and covered by an index. Eventide's "sleep only when empty" loop reads back to back under load.
6. **Ordering by sequence is the hard problem for event stores.** Marten detects gaps with transaction evidence, and its 2026 bug shows how subtle that is. Message DB serialises writes per category, which costs per-category write rate and leaves readers that span categories unsolved. Emmett and Watermill filter by the transaction horizon, which gives up gap waits in exchange for stalls behind long-running transactions and a two-part checkpoint. Emmett's own migration found message loss from mixing the two orders.

### Implications for a psycopg 3 event-store consumer (synthesis)

This section is synthesis, not something a maintainer published.

- **Keep the position-based poll as the guarantee.** Add a wake-up path on top, and lengthen the idle poll while the listener is healthy. Shorten it again when the listener is down.
- **Send notifications per channel (for example per category), debounced, with constant payloads.** Never send one per event. Two placements are defensible:
  - *Inside the append transaction:* the wake-up is atomic with the append, but every notifying append serialises cluster-wide through its WAL flush.
  - *As an autocommit `pg_notify` after the commit returns:* the append's flush stays out of the lock, at the cost of one round trip and an occasional extra poll interval after a crash. Errors must be logged, not raised.

  Because consumers poll anyway, the post-commit placement removes the larger cost.
- **Run one dedicated autocommit listener per process or event loop**, on psycopg 3.2.10 or later:
  - Explicit keepalives (idle around 15–120 s), `tcp_user_timeout` and `target_session_attrs=read-write`.
  - A bounded `notifies(timeout=…)` loop with a `SELECT 1` health check, reconnect with jittered exponential backoff, re-`LISTEN` and wake-all on reconnect.
  - Optionally, a heartbeat `NOTIFY` to catch a healthy connection that receives nothing.
  - Reference-counted subscriptions, per-subscriber coalescing events, and an exposed health state.
  - Never route it through a transaction-mode pooler.
- **Choose an ordering guarantee explicitly; a wake-up does not provide one.**
  - Per-category consumers can rely on per-category serialisation at write time, ideally with the lock and position assignment as late as possible. The cost is a per-category ceiling of roughly one commit per flush-plus-replication time.
  - Log-wide consumers need gap detection backed by transaction evidence, an `xid8` horizon filter with a two-part checkpoint, or writer-announced ranges.
  - If the store adopts the horizon filter, document `transaction_timeout` for every writing role and expose a held-back-events metric. Plan the checkpoint migration around the overlap window.

## Sources

- [Async Daemon — Marten documentation](https://martendb.io/events/projections/async-daemon) — tier-1 — martendb.io
- [Marten: Async Daemon (high water mark and gap handling)](https://martendb.io/events/projections/async-daemon.html) — tier-1 — martendb.io
- [Optimizing for Performance and Scalability — Marten documentation](https://martendb.io/events/optimizing) — tier-1 — martendb.io
- [Marten: Appending Events (QuickAppend and tombstone events)](https://martendb.io/events/appending.html) — tier-1 — martendb.io
- [Async daemon can advance projection progress past committed events during concurrent appends (Discussion #4953)](https://github.com/JasperFx/marten/discussions/4953) — tier-2 — github.com/JasperFx/marten (discussion thread)
- [Async Daemon polls PostgreSQL too quickly (Issue #1862)](https://github.com/JasperFx/marten/issues/1862) — tier-2 — github.com/JasperFx/marten (issue tracker)
- [Notify/Listen for Documents and Events (Issue #362)](https://github.com/JasperFx/marten/issues/362) — tier-2 — github.com/JasperFx/marten (issue tracker)
- [Building Marten's Async Daemon](https://jeremydmiller.com/2016/08/31/building-martens-async-daemon/) — tier-2 — jeremydmiller.com (Marten creator's blog)
- [Message DB README](https://github.com/message-db/message-db) — tier-1 — github.com/message-db/message-db (README)
- [Message DB README](https://raw.githubusercontent.com/message-db/message-db/master/README.md) — tier-1 — github.com/message-db/message-db
- [Message DB acquire-lock.sql](https://raw.githubusercontent.com/message-db/message-db/master/database/functions/acquire-lock.sql) — tier-1 — github.com/message-db/message-db (source)
- [Message DB write-message.sql](https://raw.githubusercontent.com/message-db/message-db/master/database/functions/write-message.sql) — tier-1 — github.com/message-db/message-db (source)
- [Message DB get_category_messages function source](https://raw.githubusercontent.com/message-db/message-db/master/database/functions/get-category-messages.sql) — tier-1 — github.com/message-db/message-db
- [Message DB messages table definition](https://raw.githubusercontent.com/message-db/message-db/master/database/tables/messages.sql) — tier-1 — github.com/message-db/message-db
- [Message DB hash_64 function source](https://raw.githubusercontent.com/message-db/message-db/master/database/functions/hash-64.sql) — tier-1 — github.com/message-db/message-db
- [Message DB messages_stream unique index](https://raw.githubusercontent.com/message-db/message-db/master/database/indexes/messages-stream.sql) — tier-1 — github.com/message-db/message-db
- [Message DB database functions directory](https://github.com/message-db/message-db/tree/master/database/functions) — tier-1 — github.com/message-db/message-db
- [Message DB issue #5: Why self-maintained counter with explicit lock, not Sequence?](https://github.com/message-db/message-db/issues/5) — tier-1 — github.com/message-db/message-db (maintainer comment)
- [Message DB issue #54: Add a high-performance, non-locking write function for purely telemetric use cases](https://github.com/message-db/message-db/issues/54) — tier-1 — github.com/message-db/message-db (maintainer issue)
- [Message DB: benchmark.sh](https://raw.githubusercontent.com/message-db/message-db/master/database/benchmark.sh) — tier-1 — github.com/message-db
- [Message DB: benchmark_write.sql](https://raw.githubusercontent.com/message-db/message-db/master/database/benchmark_write.sql) — tier-1 — github.com/message-db
- [Eventide consumer subscription.rb](https://raw.githubusercontent.com/eventide-project/consumer/master/lib/consumer/subscription.rb) — tier-1 — github.com/eventide-project/consumer (source)
- [Eventide consumer defaults.rb](https://raw.githubusercontent.com/eventide-project/consumer/master/lib/consumer/defaults.rb) — tier-1 — github.com/eventide-project/consumer (source)
- [Eventide poll library](https://github.com/eventide-project/poll) — tier-1 — github.com/eventide-project/poll (README)
- [Eventide docs: Message DB server functions (Acquire a Lock for a Stream Name)](https://raw.githubusercontent.com/eventide-project/docs/master/user-guide/message-db/server-functions.md) — tier-1 — github.com/eventide-project/docs
- [Eventide docs: Consumers](https://raw.githubusercontent.com/eventide-project/docs/master/user-guide/consumers.md) — tier-1 — github.com/eventide-project/docs
- [River client.go (Config documentation)](https://raw.githubusercontent.com/riverqueue/river/master/client.go) — tier-1 — github.com/riverqueue/river (source)
- [River CHANGELOG](https://raw.githubusercontent.com/riverqueue/river/master/CHANGELOG.md) — tier-1 — github.com/riverqueue/river (changelog)
- [River and PgBouncer](https://riverqueue.com/docs/pgbouncer) — tier-1 — riverqueue.com
- [River internal/notifylimiter/limiter.go](https://raw.githubusercontent.com/riverqueue/river/master/internal/notifylimiter/limiter.go) — tier-1 — github.com/riverqueue/river
- [River notifier.go source](https://raw.githubusercontent.com/riverqueue/river/master/internal/notifier/notifier.go) — tier-1 — raw.githubusercontent.com (riverqueue/river repository, primary source)
- [Oban.Notifiers.Postgres](https://oban.hexdocs.pm/Oban.Notifiers.Postgres.html) — tier-1 — oban.hexdocs.pm
- [Oban.Notifiers.PG](https://oban.hexdocs.pm/Oban.Notifiers.PG.html) — tier-1 — oban.hexdocs.pm (official Oban documentation)
- [Oban.Notifier](https://oban.hexdocs.pm/Oban.Notifier.html) — tier-1 — oban.hexdocs.pm
- [Oban (v2.24.1 configuration options)](https://oban.hexdocs.pm/Oban.html) — tier-1 — oban.hexdocs.pm
- [Changelog for Oban v2.17](https://oban.hexdocs.pm/2.17.8/changelog.html) — tier-1 — oban.hexdocs.pm
- [Oban.Plugins.Stager (v2.13.0)](https://oban.hexdocs.pm/2.13.0/Oban.Plugins.Stager.html) — tier-1 — oban.hexdocs.pm
- [Oban notifier.ex source](https://raw.githubusercontent.com/oban-bg/oban/main/lib/oban/notifier.ex) — tier-1 — raw.githubusercontent.com (oban-bg/oban repository, primary source)
- [Oban sonar.ex source](https://raw.githubusercontent.com/oban-bg/oban/main/lib/oban/sonar.ex) — tier-1 — raw.githubusercontent.com (oban-bg/oban repository, primary source)
- [Oban stager.ex source](https://raw.githubusercontent.com/oban-bg/oban/main/lib/oban/stager.ex) — tier-1 — raw.githubusercontent.com (oban-bg/oban repository, primary source)
- [Oban Postgres notifier source](https://raw.githubusercontent.com/oban-bg/oban/main/lib/oban/notifiers/postgres.ex) — tier-1 — github.com/oban-bg
- [Graphile Worker introduction](https://worker.graphile.org/docs) — tier-1 — worker.graphile.org
- [Graphile Worker FAQ](https://worker.graphile.org/docs/faq) — tier-1 — worker.graphile.org
- [Graphile Worker configuration](https://worker.graphile.org/docs/config) — tier-1 — worker.graphile.org
- [Graphile Worker interfaces.ts](https://raw.githubusercontent.com/graphile/worker/main/src/interfaces.ts) — tier-1 — github.com/graphile/worker (source)
- [Graphile Worker localQueue.ts](https://raw.githubusercontent.com/graphile/worker/main/src/localQueue.ts) — tier-1 — github.com/graphile/worker (source)
- [Graphile Worker main.ts source](https://raw.githubusercontent.com/graphile/worker/main/src/main.ts) — tier-1 — github.com/graphile
- [GoodJob README](https://raw.githubusercontent.com/bensheldon/good_job/main/README.md) — tier-1 — github.com/bensheldon/good_job (README)
- [GoodJob notifier.rb](https://raw.githubusercontent.com/bensheldon/good_job/main/lib/good_job/notifier.rb) — tier-1 — github.com/bensheldon/good_job (source)
- [pg-boss Workers API](https://pgboss.io/api/workers) — tier-1 — pgboss.io
- [pg-boss Constructor API](https://pgboss.io/api/constructor) — tier-1 — pgboss.io
- [pg-boss README](https://raw.githubusercontent.com/timgit/pg-boss/master/README.md) — tier-1 — github.com/timgit/pg-boss (README)
- [LISTEN/NOTIFY support (pg-boss Issue #24)](https://github.com/timgit/pg-boss/issues/24) — tier-2 — github.com/timgit/pg-boss (issue tracker)
- [Suggestion: Consider changing worker polling to deadline-based scheduling (pg-boss Issue #702)](https://github.com/timgit/pg-boss/issues/702) — tier-2 — github.com/timgit/pg-boss (issue tracker)
- [Solid Queue README](https://raw.githubusercontent.com/rails/solid_queue/main/README.md) — tier-1 — github.com/rails/solid_queue (README)
- [Introducing Solid Queue](https://dev.37signals.com/introducing-solid-queue/) — tier-1 — dev.37signals.com
- [Are there any plans to make Solid Queue more performant on Postgres? (Issue #508)](https://github.com/rails/solid_queue/issues/508) — tier-2 — github.com/rails/solid_queue (issue tracker)
- [pgmq extension SQL (pgmq.sql)](https://raw.githubusercontent.com/pgmq/pgmq/main/pgmq-extension/sql/pgmq.sql) — tier-1 — github.com/pgmq/pgmq (source)
- [pgmq releases](https://github.com/pgmq/pgmq/releases) — tier-1 — github.com/pgmq/pgmq (releases)
- [pgmq PR #445: Add notification throttling for insert listeners](https://github.com/pgmq/pgmq/pull/445) — tier-1 — github.com/pgmq/pgmq
- [fix: insert notifications never fire for a queue name that is not all lowercase (pgmq PR #593)](https://github.com/pgmq/pgmq/pull/593) — tier-2 — github.com/pgmq/pgmq (pull request)
- [Procrastinate psycopg_connector.py source (listen_notify, _loop_notify)](https://raw.githubusercontent.com/procrastinate-org/procrastinate/main/procrastinate/psycopg_connector.py) — tier-1 — raw.githubusercontent.com (procrastinate-org/procrastinate repository, primary source)
- [Procrastinate connector.py source (LISTEN_TIMEOUT)](https://raw.githubusercontent.com/procrastinate-org/procrastinate/main/procrastinate/connector.py) — tier-1 — raw.githubusercontent.com (procrastinate-org/procrastinate repository, primary source)
- [Procrastinate worker.py source (notification event plus polling interval)](https://raw.githubusercontent.com/procrastinate-org/procrastinate/main/procrastinate/worker.py) — tier-1 — raw.githubusercontent.com (procrastinate-org/procrastinate repository, primary source)
- [pgqueuer psycopg driver source](https://raw.githubusercontent.com/janbjorge/pgqueuer/main/pgqueuer/adapters/drivers/psycopg.py) — tier-1 — raw.githubusercontent.com (janbjorge/pgqueuer repository, primary source)
- [asyncpg API reference (add_listener, pool reset)](https://magicstack.github.io/asyncpg/current/api/index.html) — tier-1 — magicstack.github.io
- [Axon Framework API: GapAwareTrackingToken](https://apidocs.axoniq.io/4.10/org/axonframework/eventhandling/GapAwareTrackingToken.html) — tier-1 — apidocs.axoniq.io
- [Axon Framework API: JdbcEventStorageEngine.Builder](https://apidocs.axoniq.io/4.10/org/axonframework/eventsourcing/eventstore/jdbc/JdbcEventStorageEngine.Builder.html) — tier-1 — apidocs.axoniq.io
- [Axon Framework source: JdbcEventStorageEngine.java](https://github.com/AxonFramework/AxonFramework/blob/axon-4.10.x/eventsourcing/src/main/java/org/axonframework/eventsourcing/eventstore/jdbc/JdbcEventStorageEngine.java) — tier-1 — github.com (AxonFramework)
- [eventsourcing (Python) documentation: Persistence](https://eventsourcing.readthedocs.io/en/stable/topics/persistence.html) — tier-1 — eventsourcing.readthedocs.io
- [eventsourcing (Python): postgres.py](https://raw.githubusercontent.com/pyeventsourcing/eventsourcing/main/eventsourcing/postgres.py) — tier-1 — github.com/pyeventsourcing/eventsourcing (raw.githubusercontent.com)
- [Commanded EventStore: Subscriptions guide](https://github.com/commanded/eventstore/blob/master/guides/Subscriptions.md) — tier-1 — github.com (commanded)
- [Commanded EventStore source: insert_events.sql.eex](https://github.com/commanded/eventstore/blob/master/lib/event_store/sql/statements/insert_events.sql.eex) — tier-1 — github.com (commanded)
- [Commanded EventStore insert_events SQL template](https://raw.githubusercontent.com/commanded/eventstore/master/lib/event_store/sql/statements/insert_events.sql.eex) — tier-1 — github.com/commanded/eventstore
- [prooph PDO event store: PostgresAdvisoryLockStrategy.php](https://raw.githubusercontent.com/prooph/pdo-event-store/master/src/WriteLockStrategy/PostgresAdvisoryLockStrategy.php) — tier-1 — github.com/prooph/pdo-event-store (raw.githubusercontent.com)
- [prooph PDO event store: PostgresEventStore.php](https://raw.githubusercontent.com/prooph/pdo-event-store/master/src/PostgresEventStore.php) — tier-1 — github.com/prooph/pdo-event-store (raw.githubusercontent.com)
- [eugene-khyst/postgresql-event-sourcing README](https://github.com/eugene-khyst/postgresql-event-sourcing) — tier-3 — github.com (personal repository)
- [postgresql-event-sourcing README](https://raw.githubusercontent.com/eugene-khyst/postgresql-event-sourcing/main/README.md) — tier-3 — github.com/eugene-khyst
- [Emmett PR #336: Fixed race conditions in PostgreSQL event store consumer message polling](https://github.com/event-driven-io/emmett/pull/336) — tier-1 — github.com (event-driven-io/emmett)
- [Emmett PR #381: Support the 0.43.x PostgreSQL checkpoint format](https://github.com/event-driven-io/emmett/pull/381) — tier-1 — github.com (event-driven-io/emmett)
- [Emmett release 0.42.4](https://github.com/event-driven-io/emmett/releases/tag/0.42.4) — tier-1 — github.com (event-driven-io/emmett)
- [Emmett releases](https://github.com/event-driven-io/emmett/releases) — tier-1 — github.com (event-driven-io/emmett)
- [Emmett tags](https://github.com/event-driven-io/emmett/tags) — tier-1 — github.com (event-driven-io/emmett)
- [Emmett checkpoint-related pull requests](https://github.com/event-driven-io/emmett/pulls?q=is%3Apr+checkpoint) — tier-1 — github.com (event-driven-io/emmett)
- [Emmett 0.43.0 migration (0_43_0.migration.ts)](https://raw.githubusercontent.com/event-driven-io/emmett/main/src/packages/emmett-postgresql/src/eventStore/schema/migrations/0_43_0/0_43_0.migration.ts) — tier-1 — github.com (event-driven-io/emmett source)
- [Emmett 0.43.0 migration tests (0_43_0.migration.int.spec.ts)](https://raw.githubusercontent.com/event-driven-io/emmett/main/src/packages/emmett-postgresql/src/eventStore/schema/migrations/0_43_0/0_43_0.migration.int.spec.ts) — tier-1 — github.com (event-driven-io/emmett source)
- [Emmett storeProcessorCheckpoint.ts](https://raw.githubusercontent.com/event-driven-io/emmett/main/src/packages/emmett-postgresql/src/eventStore/schema/storeProcessorCheckpoint.ts) — tier-1 — github.com (event-driven-io/emmett source)
- [Emmett readProcessorCheckpoint.ts](https://raw.githubusercontent.com/event-driven-io/emmett/main/src/packages/emmett-postgresql/src/eventStore/schema/readProcessorCheckpoint.ts) — tier-1 — github.com (event-driven-io/emmett source)
- [Emmett readMessagesBatch.ts (main)](https://raw.githubusercontent.com/event-driven-io/emmett/main/src/packages/emmett-postgresql/src/eventStore/schema/readMessagesBatch.ts) — tier-1 — github.com (event-driven-io/emmett source)
- [Emmett readMessagesBatch.ts (0.42.4)](https://raw.githubusercontent.com/event-driven-io/emmett/0.42.4/src/packages/emmett-postgresql/src/eventStore/schema/readMessagesBatch.ts) — tier-1 — github.com (event-driven-io/emmett source)
- [Emmett tables.ts](https://raw.githubusercontent.com/event-driven-io/emmett/main/src/packages/emmett-postgresql/src/eventStore/schema/tables.ts) — tier-1 — github.com (event-driven-io/emmett source)
- [Watermill SQL Pub/Sub documentation](https://watermill.io/pubsubs/sql/) — tier-1 — watermill.io
- [watermill-sql schema_adapter_postgresql.go](https://raw.githubusercontent.com/ThreeDotsLabs/watermill-sql/master/pkg/sql/schema_adapter_postgresql.go) — tier-1 — github.com (ThreeDotsLabs/watermill-sql source)
- [watermill-sql offsets_adapter_postgresql.go](https://raw.githubusercontent.com/ThreeDotsLabs/watermill-sql/master/pkg/sql/offsets_adapter_postgresql.go) — tier-1 — github.com (ThreeDotsLabs/watermill-sql source)
- [watermill-sql PR #23: Fix potential message loss with small PollInterval](https://github.com/ThreeDotsLabs/watermill-sql/pull/23) — tier-1 — github.com (ThreeDotsLabs/watermill-sql)
- [watermill-sql releases](https://github.com/ThreeDotsLabs/watermill-sql/releases) — tier-1 — github.com (ThreeDotsLabs/watermill-sql)
- [Watermill issue #311: offset ordering in the PostgreSQL schema adapter](https://github.com/ThreeDotsLabs/watermill/issues/311) — tier-1 — github.com (ThreeDotsLabs/watermill issue tracker)
- [PostgreSQL documentation: NOTIFY](https://www.postgresql.org/docs/current/sql-notify.html) — tier-1 — postgresql.org
- [PostgreSQL documentation: LISTEN](https://www.postgresql.org/docs/current/sql-listen.html) — tier-1 — postgresql.org
- [PostgreSQL documentation: Sequence Manipulation Functions](https://www.postgresql.org/docs/current/functions-sequence.html) — tier-1 — postgresql.org
- [PostgreSQL documentation: System Information Functions and Operators (transaction ID and snapshot functions)](https://www.postgresql.org/docs/current/functions-info.html) — tier-1 — postgresql.org
- [PostgreSQL documentation: Transactions and Identifiers](https://www.postgresql.org/docs/current/transaction-id.html) — tier-1 — postgresql.org
- [PostgreSQL documentation: The Cumulative Statistics System (pg_stat_activity)](https://www.postgresql.org/docs/current/monitoring-stats.html) — tier-1 — postgresql.org
- [PostgreSQL documentation: PREPARE TRANSACTION](https://www.postgresql.org/docs/current/sql-prepare-transaction.html) — tier-1 — postgresql.org
- [PostgreSQL documentation: pg_prepared_xacts](https://www.postgresql.org/docs/current/view-pg-prepared-xacts.html) — tier-1 — postgresql.org
- [PostgreSQL documentation: Connections and Authentication (TCP keepalives, client_connection_check_interval)](https://www.postgresql.org/docs/current/runtime-config-connection.html) — tier-1 — postgresql.org
- [PostgreSQL documentation: Client Connection Defaults (statement_timeout, transaction_timeout, idle_in_transaction_session_timeout)](https://www.postgresql.org/docs/current/runtime-config-client.html) — tier-1 — postgresql.org
- [PostgreSQL: Write Ahead Log settings (synchronous_commit)](https://www.postgresql.org/docs/current/runtime-config-wal.html) — tier-1 — postgresql.org
- [PostgreSQL: Explicit Locking (advisory locks, row-level locks, deadlocks)](https://www.postgresql.org/docs/current/explicit-locking.html) — tier-1 — postgresql.org
- [PostgreSQL: Transaction Isolation](https://www.postgresql.org/docs/current/transaction-iso.html) — tier-1 — postgresql.org
- [PostgreSQL: WAL Configuration (group commit, commit_delay)](https://www.postgresql.org/docs/current/wal-configuration.html) — tier-1 — postgresql.org
- [PostgreSQL documentation: Asynchronous Commit](https://www.postgresql.org/docs/current/wal-async-commit.html) — tier-1 — postgresql.org
- [PostgreSQL documentation: Log-Shipping Standby Servers (synchronous replication, planning for performance)](https://www.postgresql.org/docs/current/warm-standby.html) — tier-1 — postgresql.org
- [PostgreSQL documentation — Hot standby](https://www.postgresql.org/docs/current/hot-standby.html) — tier-1 — postgresql.org (official PostgreSQL documentation)
- [PostgreSQL documentation — libpq asynchronous notification](https://www.postgresql.org/docs/current/libpq-notify.html) — tier-1 — postgresql.org (official PostgreSQL documentation)
- [PostgreSQL documentation — libpq connection parameters](https://www.postgresql.org/docs/current/libpq-connect.html) — tier-1 — postgresql.org (official PostgreSQL documentation)
- [PostgreSQL documentation: pgbench](https://www.postgresql.org/docs/current/pgbench.html) — tier-1 — postgresql.org
- [PostgreSQL documentation: pg_test_fsync](https://www.postgresql.org/docs/current/pgtestfsync.html) — tier-1 — postgresql.org
- [ALTER TABLE (PostgreSQL documentation)](https://www.postgresql.org/docs/current/sql-altertable.html) — tier-1 — postgresql.org
- [pg_resetwal (PostgreSQL documentation)](https://www.postgresql.org/docs/current/app-pgresetwal.html) — tier-1 — postgresql.org
- [PostgreSQL documentation: CREATE TRIGGER (constraint triggers)](https://www.postgresql.org/docs/current/sql-createtrigger.html) — tier-1 — postgresql.org
- [PostgreSQL documentation: SET CONSTRAINTS](https://www.postgresql.org/docs/current/sql-set-constraints.html) — tier-1 — postgresql.org
- [PostgreSQL documentation: Overview of Trigger Behavior](https://www.postgresql.org/docs/current/trigger-definition.html) — tier-1 — postgresql.org
- [PostgreSQL documentation: System Administration Functions (advisory lock functions)](https://www.postgresql.org/docs/current/functions-admin.html) — tier-1 — postgresql.org
- [PostgreSQL documentation: Heap-Only Tuples (HOT)](https://www.postgresql.org/docs/current/storage-hot.html) — tier-1 — postgresql.org
- [PostgreSQL documentation: Index Uniqueness Checks](https://www.postgresql.org/docs/current/index-unique-checks.html) — tier-1 — postgresql.org
- [PostgreSQL 13 release notes](https://www.postgresql.org/docs/13/release-13.html) — tier-1 — postgresql.org
- [PostgreSQL 17 release notes](https://www.postgresql.org/docs/release/17.0/) — tier-1 — postgresql.org
- [PostgreSQL 19 release notes (beta)](https://www.postgresql.org/docs/devel/release-19.html) — tier-1 — postgresql.org
- [PostgreSQL 19 release notes](https://www.postgresql.org/docs/19/release-19.html) — tier-1 — postgresql.org (official documentation)
- [PostgreSQL home page: PostgreSQL 19 Beta 4 Released (2026-09-24)](https://www.postgresql.org/) — tier-1 — postgresql.org
- [PostgreSQL versioning policy and release table](https://www.postgresql.org/support/versioning/) — tier-1 — postgresql.org
- [src/backend/commands/async.c (PostgreSQL master)](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/commands/async.c) — tier-1 — github.com/postgres/postgres (official mirror)
- [PostgreSQL async.c, REL_18_STABLE](https://raw.githubusercontent.com/postgres/postgres/REL_18_STABLE/src/backend/commands/async.c) — tier-1 — github.com/postgres (official mirror)
- [PostgreSQL source: xact.c (CommitTransaction, RecordTransactionCommit)](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/access/transam/xact.c) — tier-1 — github.com/postgres/postgres
- [PostgreSQL source: procarray.c (GetSnapshotData)](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/storage/ipc/procarray.c) — tier-1 — github.com (postgres/postgres)
- [PostgreSQL source: guc_parameters.dat (GUC contexts)](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/utils/misc/guc_parameters.dat) — tier-1 — github.com (postgres/postgres)
- [PostgreSQL source: pgstatfuncs.c (pg_stat_get_activity visibility)](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/utils/adt/pgstatfuncs.c) — tier-1 — github.com (postgres/postgres)
- [PostgreSQL source: src/backend/storage/lmgr/README (master)](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/storage/lmgr/README) — tier-1 — github.com/postgres/postgres
- [async.c commit history](https://git.postgresql.org/gitweb/?p=postgresql.git;a=history;f=src/backend/commands/async.c;hb=HEAD) — tier-1 — git.postgresql.org
- [async.c commit history (GitHub mirror)](https://github.com/postgres/postgres/commits/master/src/backend/commands/async.c) — tier-1 — github.com/postgres/postgres (official mirror)
- [Optimize LISTEN/NOTIFY via shared channel map and direct advancement (commit 282b1cde)](https://git.postgresql.org/gitweb/?p=postgresql.git;a=commit;h=282b1cde9dedf456ecf02eb27caf086023a7bb71) — tier-1 — git.postgresql.org
- [Commit 282b1cde: Optimize LISTEN/NOTIFY via shared channel map and direct advancement](https://github.com/postgres/postgres/commit/282b1cde9dedf456ecf02eb27caf086023a7bb71) — tier-1 — github.com (postgres/postgres repository)
- [Commit 282b1cd: Optimize LISTEN/NOTIFY via shared channel map and direct advancement](https://github.com/postgres/postgres/commit/282b1cd) — tier-1 — github.com/postgres/postgres (official source mirror)
- [Optimize LISTEN/NOTIFY (opening message, Joel Jacobson, 2025-07-12)](https://www.postgresql.org/message-id/6899c044-4a82-49be-8117-e6f669765f7e@app.fastmail.com) — tier-1 — postgresql.org (pgsql-hackers archive)
- [Re: Optimize LISTEN/NOTIFY (Joel Jacobson, 2025-11-14)](https://www.postgresql.org/message-id/2eeea4f1-1b4f-430c-8571-544da04f08dc@app.fastmail.com) — tier-1 — postgresql.org (pgsql-hackers archive)
- [PATCH Write Notifications Through WAL (full thread)](https://www.postgresql.org/message-id/flat/CAE7r3MKV7oewo6EGGLPMKQvv74ujPurjjDBU=K8KH8CYJStXaw@mail.gmail.com) — tier-1 — postgresql.org (pgsql-hackers archive)
- [[PATCH] Write Notifications Through WAL (full thread, v6–v9)](https://www.postgresql.org/message-id/flat/CAD21AoBX++gPYYKJb2JsXjDVCy4xwza_Pg9c_75vnGPkbW==5w@mail.gmail.com) — tier-1 — postgresql.org (pgsql-hackers archive)
- [Re: [PATCH] Write Notifications Through WAL — Arseniy Mukhin review of v6, 2025-10-08](https://www.postgresql.org/message-id/CAE7r3MKV7oewo6EGGLPMKQvv74ujPurjjDBU=K8KH8CYJStXaw@mail.gmail.com) — tier-1 — postgresql.org (pgsql-hackers archive)
- [Re: PATCH Write Notifications Through WAL (Arseniy Mukhin benchmark, 2025-10-17)](https://www.postgresql.org/message-id/CAE7r3MLwRNyvmMd2kaefeNYTRjGD8wOxLzyGn8vexaeTJdASaw@mail.gmail.com) — tier-1 — postgresql.org (pgsql-hackers archive)
- [Re: [PATCH] Write Notifications Through WAL — Rishu Bagga v8 with standby LISTEN, 2025-11-03](https://www.postgresql.org/message-id/CAK80=jieMJ4tNEBQwipB+Q472fZP0YEf=EncMxoGqMP70E4uLQ@mail.gmail.com) — tier-1 — postgresql.org (pgsql-hackers archive)
- [Proposal: Out-of-Order NOTIFY via GUC to Improve LISTEN/NOTIFY Throughput (full thread, incl. Tom Lane's WAL design)](https://www.postgresql.org/message-id/flat/CAK80=jiWjMAfjwJymPoG3=FNE=hHygn5dXaoax1BWQT1rJrCCw@mail.gmail.com) — tier-1 — postgresql.org (pgsql-hackers archive)
- [pgsql-hackers archive search: "notifications WAL"](https://www.postgresql.org/search/?m=1&q=notifications+WAL&l=pgsql-hackers&s=d) — tier-1 — postgresql.org (list archive search)
- [Re: docs: LISTEN/NOTIFY performance considerations (Nikolay Samokhvalov, 2026-07-21)](https://www.postgresql.org/message-id/CAM527d8oDVb2K+tTjpjLkd7g14HLDiwwnvv=rcn4BMEs=MatkQ@mail.gmail.com) — tier-1 — postgresql.org (pgsql-hackers archive)
- [Re: docs: LISTEN/NOTIFY performance considerations (Bruce Momjian, 2026-07-29)](https://www.postgresql.org/message-id/amop9PA1KhlY0S0z@momjian.us) — tier-1 — postgresql.org (pgsql-hackers archive)
- [Psycopg 3 — Concurrent operations: asynchronous notifications and detecting disconnections](https://www.psycopg.org/psycopg3/docs/advanced/async.html) — tier-1 — psycopg.org (official psycopg documentation)
- [Psycopg 3 — Connection classes API (notifies, closed, broken, fileno)](https://www.psycopg.org/psycopg3/docs/api/connections.html) — tier-1 — psycopg.org (official psycopg documentation)
- [Psycopg 3 — Other top-level objects (Notify, ConnectionInfo.backend_pid)](https://www.psycopg.org/psycopg3/docs/api/objects.html) — tier-1 — psycopg.org (official psycopg documentation)
- [Psycopg 3 — Errors and SQLSTATE exception mapping](https://www.psycopg.org/psycopg3/docs/api/errors.html) — tier-1 — psycopg.org (official psycopg documentation)
- [Psycopg 3 — Release notes](https://www.psycopg.org/psycopg3/docs/news.html) — tier-1 — psycopg.org (official psycopg documentation)
- [Psycopg 3 — Differences from psycopg2 (server-side binding limits, pg_notify)](https://www.psycopg.org/psycopg3/docs/basic/from_pg2.html) — tier-1 — psycopg.org (official psycopg documentation)
- [Psycopg 3 — Connection pools (check, reconnect_timeout, reconnect_failed)](https://www.psycopg.org/psycopg3/docs/advanced/pool.html) — tier-1 — psycopg.org (official psycopg documentation)
- [Psycopg 3: Pipeline mode support](https://www.psycopg.org/psycopg3/docs/advanced/pipeline.html) — tier-1 — psycopg.org
- [psycopg connection.py source (notifies implementation)](https://raw.githubusercontent.com/psycopg/psycopg/master/psycopg/psycopg/connection.py) — tier-1 — raw.githubusercontent.com (psycopg/psycopg repository, primary source)
- [psycopg connection_async.py source (notifies implementation)](https://raw.githubusercontent.com/psycopg/psycopg/master/psycopg/psycopg/connection_async.py) — tier-1 — github.com/psycopg
- [psycopg _connection_base.py source (notify handlers and backlog)](https://raw.githubusercontent.com/psycopg/psycopg/master/psycopg/psycopg/_connection_base.py) — tier-1 — github.com/psycopg
- [psycopg conninfo.py source (default connect timeout)](https://raw.githubusercontent.com/psycopg/psycopg/master/psycopg/psycopg/conninfo.py) — tier-1 — raw.githubusercontent.com (psycopg/psycopg repository, primary source)
- [psycopg issue 340: notifies generator blocks execute](https://github.com/psycopg/psycopg/issues/340) — tier-2 — github.com (psycopg issue tracker)
- [psycopg issue #962 — notifications lost while notifies() is not running](https://github.com/psycopg/psycopg/issues/962) — tier-1 — github.com (psycopg/psycopg issue tracker, primary source)
- [psycopg issue 1091: leak receiving notifications if notifies() is not called](https://github.com/psycopg/psycopg/issues/1091) — tier-2 — github.com (psycopg issue tracker)
- [tcp(7) — Linux manual page](https://man7.org/linux/man-pages/man7/tcp.7.html) — tier-1 — man7.org (Linux man-pages project)
- [Linux kernel documentation: IP sysctl (tcp_keepalive_time, probes, intvl)](https://www.kernel.org/doc/html/latest/networking/ip-sysctl.html) — tier-1 — kernel.org
- [PgBouncer configuration reference](https://www.pgbouncer.org/config.html) — tier-1 — pgbouncer.org
- [PgBouncer features (SQL feature map for pooling modes)](https://www.pgbouncer.org/features.html) — tier-1 — pgbouncer.org
- [Troubleshoot NAT gateways](https://docs.aws.amazon.com/vpc/latest/userguide/nat-gateway-troubleshooting.html) — tier-1 — docs.aws.amazon.com
- [Network Load Balancers (connection idle timeout)](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/network-load-balancers.html) — tier-1 — docs.aws.amazon.com
- [Amazon EC2 security group connection tracking](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/security-group-connection-tracking.html) — tier-1 — docs.aws.amazon.com
- [Best practices for TCP connection management on EC2](https://aws.amazon.com/blogs/networking-and-content-delivery/best-practices-for-tcp-connection-management-on-ec2/) — tier-1 — aws.amazon.com (AWS Networking and Content Delivery Blog)
- [Avoiding pinning an RDS Proxy](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/rds-proxy-pinning.html) — tier-1 — docs.aws.amazon.com
- [RDS Proxy connection considerations](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/rds-proxy-connections.html) — tier-1 — docs.aws.amazon.com
- [Failing over a Multi-AZ DB instance for Amazon RDS](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.MultiAZ.Failover.html) — tier-1 — docs.aws.amazon.com
- [Failing over a Multi-AZ DB cluster for Amazon RDS](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/multi-az-db-clusters-concepts-failover.html) — tier-1 — docs.aws.amazon.com
- [Maintaining a DB instance](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_UpgradeDBInstance.Maintenance.html) — tier-1 — docs.aws.amazon.com
- [Switching a blue/green deployment in Amazon RDS](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/blue-green-deployments-switching.html) — tier-1 — docs.aws.amazon.com
- [Multi-AZ DB instance deployments for Amazon RDS](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.MultiAZSingleStandby.html) — tier-1 — docs.aws.amazon.com
- [Multi-AZ DB cluster deployments for Amazon RDS](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/multi-az-db-clusters-concepts.html) — tier-1 — docs.aws.amazon.com
- [High availability for Amazon Aurora](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Concepts.AuroraHighAvailability.html) — tier-1 — docs.aws.amazon.com
- [Fast failover with Amazon Aurora PostgreSQL](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/AuroraPostgreSQL.BestPractices.FastFailover.html) — tier-1 — docs.aws.amazon.com
- [Performing a minor version upgrade (Aurora PostgreSQL zero-downtime patching)](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/USER_UpgradeDBInstance.PostgreSQL.MinorUpgrade.html) — tier-1 — docs.aws.amazon.com
- [Scaling to Zero ACUs with automatic pause and resume for Aurora serverless](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-serverless-v2-auto-pause.html) — tier-1 — docs.aws.amazon.com
- [AWS Advanced Python Wrapper](https://github.com/aws/aws-advanced-python-wrapper) — tier-1 — github.com/aws (AWS official repository)
- [Amazon EBS volume types](https://docs.aws.amazon.com/ebs/latest/userguide/ebs-volume-types.html) — tier-1 — docs.aws.amazon.com
- [Cloud SQL for PostgreSQL: About maintenance](https://docs.cloud.google.com/sql/docs/postgres/maintenance) — tier-1 — docs.cloud.google.com
- [Cloud SQL for PostgreSQL: High availability](https://docs.cloud.google.com/sql/docs/postgres/high-availability) — tier-1 — docs.cloud.google.com
- [Cloud SQL for PostgreSQL: Manage database connections](https://docs.cloud.google.com/sql/docs/postgres/manage-connections) — tier-1 — docs.cloud.google.com
- [Cloud SQL for PostgreSQL: Managed Connection Pooling](https://docs.cloud.google.com/sql/docs/postgres/managed-connection-pooling) — tier-1 — docs.cloud.google.com
- [Connect from Cloud Run to Cloud SQL for PostgreSQL](https://docs.cloud.google.com/sql/docs/postgres/connect-run) — tier-1 — docs.cloud.google.com
- [Compute Engine troubleshooting: general tips (idle connections)](https://docs.cloud.google.com/compute/docs/troubleshooting/general-tips) — tier-1 — docs.cloud.google.com
- [VPC firewall rules](https://docs.cloud.google.com/vpc/docs/firewalls) — tier-1 — docs.cloud.google.com
- [Tune Cloud NAT configuration](https://docs.cloud.google.com/nat/docs/tune-nat-configuration) — tier-1 — docs.cloud.google.com
- [High availability in Azure Database for PostgreSQL flexible server](https://learn.microsoft.com/en-us/azure/postgresql/high-availability/concepts-high-availability) — tier-1 — learn.microsoft.com
- [Planned maintenance in Azure Database for PostgreSQL](https://learn.microsoft.com/en-us/azure/postgresql/flexible-server/concepts-maintenance) — tier-1 — learn.microsoft.com
- [Overview of business continuity in Azure Database for PostgreSQL flexible server](https://learn.microsoft.com/en-us/azure/postgresql/backup-restore/concepts-business-continuity) — tier-1 — learn.microsoft.com
- [PgBouncer in Azure Database for PostgreSQL flexible server](https://learn.microsoft.com/en-us/azure/postgresql/connectivity/concepts-pgbouncer) — tier-1 — learn.microsoft.com
- [Configure load balancer TCP reset and idle timeout](https://learn.microsoft.com/en-us/azure/load-balancer/load-balancer-tcp-idle-timeout) — tier-1 — learn.microsoft.com
- [NAT gateway resource (TCP idle timeout and reset)](https://learn.microsoft.com/en-us/azure/nat-gateway/nat-gateway-resource) — tier-1 — learn.microsoft.com
- [Reliability in Azure Database for PostgreSQL](https://learn.microsoft.com/en-us/azure/reliability/reliability-postgresql-flexible-server) — tier-1 — learn.microsoft.com
- [What are Azure Availability Zones? (inter-zone networking)](https://learn.microsoft.com/en-us/azure/reliability/availability-zones-overview) — tier-1 — learn.microsoft.com
- [Towards 14,000 write transactions per second on my laptop (Peter Geoghegan, 2012)](https://pgeoghegan.blogspot.com/2012/06/towards-14000-write-transactions-on-my.html) — tier-2 — pgeoghegan.blogspot.com (PostgreSQL committer's blog)
- [Fsync Performance on Storage Devices (Yves Trudeau, 2018)](https://www.percona.com/blog/2018/02/08/fsync-performance-storage-devices/) — tier-2 — percona.com
- [Four reasons why VACUUM won't remove dead rows from a table (Laurenz Albe)](https://www.cybertec-postgresql.com/en/reasons-why-vacuum-wont-remove-dead-rows/) — tier-2 — cybertec-postgresql.com
- [Gaps in sequences in PostgreSQL (Laurenz Albe)](https://www.cybertec-postgresql.com/en/gaps-in-sequences-postgresql/) — tier-2 — cybertec-postgresql.com
- [Control Runaway Postgres Queries With Statement Timeout (Craig Kerstiens)](https://www.crunchydata.com/blog/control-runaway-postgres-queries-with-statement-timeout) — tier-2 — crunchydata.com
- [Pattern: Transactional outbox](https://microservices.io/patterns/data/transactional-outbox.html) — tier-2 — microservices.io (Chris Richardson)
- [How Postgres sequences issues can impact your messaging guarantees](https://event-driven.io/en/ordering_in_postgres_outbox/) — tier-3 — event-driven.io (Oskar Dudycz)
- [Postgres sequences can commit out-of-order](https://blog.sequinstream.com/postgres-sequences-can-commit-out-of-order/) — tier-3 — blog.sequinstream.com
- [Postgres LISTEN/NOTIFY does not scale](https://www.recall.ai/blog/postgres-listen-notify-does-not-scale) — tier-3 — recall.ai (company engineering blog)
- [Hacker News discussion: Postgres LISTEN/NOTIFY does not scale](https://news.ycombinator.com/item?id=44490510) — tier-3 — news.ycombinator.com
