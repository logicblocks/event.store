---
type: "topic-research"
id: "08-how-do-notification-driven-and-polling-based-delivery-scale-web"
title: "How do notification-driven and polling-based delivery scale as listeners, channels, subscriptions and event throughput grow — measured notification latency and throughput, commit-time contention, thundering-herd wake-ups and fan-out cost — and what does published research on push versus pull delivery and adaptive polling say about the latency-versus-load trade-off?"
date: "2026-10-05T13:56:47+00:00"
author: "Toby Clemson"
producer: "research-topic"
status: "complete"
kind: "finding"
round: 1
depth: 3
question: "How do notification-driven and polling-based delivery scale as listeners, channels, subscriptions and event throughput grow — measured notification latency and throughput, commit-time contention, thundering-herd wake-ups and fan-out cost — and what does published research on push versus pull delivery and adaptive polling say about the latency-versus-load trade-off?"
source_profile: "web"
tags: ["research", "topic-research", "finding"]
last_updated: "2026-10-05T13:56:47+00:00"
last_updated_by: "Toby Clemson"
schema_version: 1
---

# How do notification-driven and polling-based delivery scale as listeners, channels, subscriptions and event throughput grow — measured notification latency and throughput, commit-time contention, thundering-herd wake-ups and fan-out cost — and what does published research on push versus pull delivery and adaptive polling say about the latency-versus-load trade-off?

## Question

A PostgreSQL-backed event store can tell its consumers about new events in two ways: LISTEN/NOTIFY can wake them, or they can poll the event table. As the store gains listeners, channels, subscriptions and write throughput, how does each approach scale? The question covers four things:

- measured notification latency and throughput
- contention at commit time
- thundering-herd wake-ups
- the cost of fanning one change out to many consumers

It also asks what published research on push versus pull delivery, and on adaptive polling, says about trading delivery latency against system load.

## Findings

### Summary

Notification-driven and polling-based delivery hit their limits along different axes. Both sets of limits are now well measured or well understood.

- **LISTEN/NOTIFY costs fall on the write path and on each listening backend.**
  - Every transaction that issues NOTIFY takes a single cluster-wide lock and holds it until its commit is durably flushed, so notifying commits are serialised. Measured ceilings include about 2,900 notifying writes per second in DBOS's trigger benchmark, and 7,679 TPS with 1,000 concurrent notifiers in a pgsql-hackers benchmark.
  - Before PostgreSQL 19, every NOTIFY also signalled every listening backend in the database, whatever channel it listened on. Throughput collapsed as idle listeners were added: 9,126 round trips per second with none, 238 with 1,000.
  - PostgreSQL 19 signals only interested listeners. That recovers about 127x the throughput at 1,000 idle listeners, but it keeps the commit lock and still wakes every listener on the same channel.
- **Polling costs fall on idle reads and on latency.**
  - Idle load is roughly the number of subscriptions divided by the poll interval. Mean latency is about half the interval plus query time.
  - When several writers commit concurrently, a correct poller must also wait at a "safe frontier", because sequence positions commit out of order. Any long-running writing transaction anywhere in the cluster can hold that frontier back.
- **Fan-out cost is mostly an application design choice.** Three measures cut per-commit database work from one read per subscription to one read per process:
  - one listening connection per process
  - coalescing wake-ups
  - a shared head reader

  Without them, one notification becomes N identical reads and N connection checkouts.
- **Published research agrees across fields.** The fields covered are web data dissemination, feed materialisation, kernel networking, storage I/O and message brokers. In all of them:
  - Push gives the best latency and wastes the least work at low load.
  - Pull degrades gracefully, batches well and gives natural back-pressure under overload.
  - The best designs are hybrids that adapt to load.

  The interrupt-starts-polling hybrid from Mogul and Ramakrishnan, and from Linux NAPI, maps directly onto "a notification wakes a drain loop".
- **Production polling policies agree with each other, but nobody has measured which is best.** No source publishes a side-by-side benchmark of adaptive polling policies for hundreds of database-backed subscriptions. Graphile Worker's maintainers say they "don't know the best value" for their own notification-suppression threshold.

### How the notification path works, and where it spends

The PostgreSQL documentation fixes the contract that drives the costs:

- A notification is delivered only when the sending transaction commits. Notifications from different transactions arrive in the order the transactions committed.
- Identical channel-and-payload pairs within one transaction are folded into one notification. Notifications from different transactions are never folded, and distinct payloads are always delivered separately. A payload is limited to 8000 bytes or less.
- A listening session receives notifications only between transactions. A backend handles the notify interrupt at once only when idle, and otherwise just before `ReadyForQuery`. psycopg's documentation therefore says to keep the listening connection in autocommit mode.
- All notifications pass through one shared queue, held in the `pg_notify` SLRU. The queue is 8GB in a standard installation and can be truncated only up to the position of the slowest listening backend. A listener that sits in a long transaction blocks cleanup. When the queue is full, "transactions calling NOTIFY will fail at commit". `pg_notification_queue_usage()` reports how full the queue is.
- Notifications are not written to WAL. The design comment in `async.c` says they "are not expected to survive database crashes", so they are cleared at restart and never reach a standby.

Two implementation details turn this contract into scaling costs:

- **Commit serialisation.** To guarantee commit order, `PreCommit_Notify` takes an `AccessExclusiveLock` on "object 0 of class 1262 of database 0". This is one lock for the whole cluster, and it is held until the commit has been flushed to disk. Recall.ai's incident write-up, DBOS's analysis and the pgsql-hackers patch that tries to shorten the hold all confirm it.
- **Broadcast signalling before PostgreSQL 19.** The server kept no central record of which backend listened to which channel. `SignalBackends()` therefore sent `PROCSIG_NOTIFY_INTERRUPT` (a `kill(pid, SIGUSR1)`) to every registered listener in the same database. Each woken backend read the queue pages, threw away the entries it did not want, and moved its read position forward.

PostgreSQL 13 had already removed several older costs:

- Signals go only to listeners in the same database.
- The notifier, not every listener, advances the queue tail, once every four pages.
- Duplicate detection within a transaction used to be an O(N²) linear scan. It now switches to a hash table once a transaction queues more than 16 distinct notifications.

On an unloaded server, delivery itself is fast:

- In a pgsql-hackers review, 67.6% of deliveries on the master branch landed in the 0.1–1 ms latency bucket.
- A two-connection ping-pong benchmark recorded a worst case of 893 µs with no idle listeners.

For an event store, though, a notification is only a wake-up. Every delivery is followed by a query against the event table.

### Commit-time contention as notifying writers grow

The commit lock is the first limit to bite as the number of notifying writers grows.

- **Production failure (Recall.ai, March 2025).** Recall.ai had tens of thousands of concurrent writers on an endpoint that issued NOTIFY.
  - With `log_lock_waits` enabled, the logs showed one process holding the lock and 256 or more waiting for it, each for about 1 second.
  - Database CPU, disk I/O and network traffic all fell as throughput collapsed. That pattern means work was being serialised, not that resources ran out.
  - Recall.ai moved the notification logic into the application layer in under a day.
- **Benchmark ceiling and workaround (DBOS, July 2026).**
  - With a trigger firing NOTIFY on every stream write, throughput peaked at 2.9K writes per second, with no visible CPU, memory or IOPS saturation.
  - DBOS then buffered notifications in memory and flushed them in periodic batch transactions. That reached 60K writes per second (20x), with Postgres CPU fully used, and delivery latency of 15–100 ms. A low-frequency fallback poll recovers notifications lost in crashes.
  - The trade-off is that notifications are no longer atomic with the write. This is acceptable only because the table remains the source of truth.
- **Developer measurements (Joel Jacobson, pgsql-hackers, July 2025).**
  - With 1,000 concurrent notifiers and one listener, keeping the commit-order lock gave 7,679 TPS. An experimental out-of-order variant without the lock gave 95,430 TPS, about 12.4x more.
  - An earlier single-machine run with local storage showed a bigger gain for NOTIFY alone (about 160K to 340K TPS) than for combined LISTEN and NOTIFY (about 73K to 83K TPS).
  - Jacobson first concluded that the lock was not the bottleneck, then withdrew that conclusion ("much more complex than I initially thought"). Which cost dominates depends on the workload.
- **Inference, not a direct measurement.** The lock spans the commit flush, so notifying transactions commit roughly one durable flush at a time. On storage with millisecond-scale fsync latency, that would cap them in the low thousands per second, which fits DBOS's 2.9K/s.

**The lock is still present in PostgreSQL 19.**

- **The committed patch.** The patch committed for 19 changes only how listeners are woken. Its design depends on the heavyweight lock to identify which queue entries belong to the committing transaction.
- **A separate patch to shorten the lock.** Rishu Bagga's patch writes notification payloads to WAL and holds the lock only briefly.
  - It measured about 30K TPS rising to 60–70K TPS with 100 clients.
  - Reviewers raised open correctness questions, such as whether a listener could see a notification before it can see the transaction's data.
  - At the January 2026 commitfest, the last one for PostgreSQL 19, the patch was listed as "Needs review" and needed a rebase.
- **Recall.ai's update.** Recall.ai's article now carries an update saying the bottleneck was eliminated in core. Given what was actually committed, that is best read as referring to the wake-up work.
- **Documentation proposal.** Nikolay Samokhvalov proposed documenting the global lock, duplicate checking, `log_lock_waits` diagnosis and logical decoding as an alternative. The proposal was not committed. Bruce Momjian replied by pointing to the PostgreSQL 19 wake-up improvement.

Two design consequences follow:

- The lock covers the whole cluster, so heavy notifying in one database slows notifying commits in every other database on the same cluster.
- Folding happens only within one transaction. A constant or position-free payload on a per-stream or per-category channel lets repeated notifications in one transaction collapse into one. A payload that carries a sequence number prevents that.

### Listener count, channel count and thundering-herd wake-ups

Jacobson's ping-pong benchmark has two connections notifying each other while extra idle listeners sit on unrelated channels. On the master branch, round trips per second fell steeply as listeners were added:

| Idle listeners | Round trips per second (master) |
|---|---|
| 0 | 9,126 |
| 10 | 6,233 |
| 100 | 2,020 |
| 1,000 | 238 |

The cause is one `kill()` syscall, plus the wake-up and queue scan that follow, for every listener on every NOTIFY. A later run on an Apple M3 Max compared master with the patched build:

| Idle listeners | Master TPS | Master max latency | Patched TPS | Patched max latency |
|---|---|---|---|---|
| 0 | 32,123.7 | 893 µs | 31,832.6 | about 1 ms |
| 1,000 | 202.5 | 6,212 µs | 25,720.6 | about 1 ms |

At 1,000 idle listeners the patched build had about 127x the throughput and about 6x lower worst-case latency.

The patch was committed on 2026-01-15 for PostgreSQL 19. It adds two things:

- A shared hash map from (database OID, channel) to the backends listening on that channel, so that only interested backends are signalled.
- "Direct advance": an uninterested listener sitting at the old queue head has its read pointer moved past the new entries without being woken.

The commit message claims "integer multiples" more throughput, and lower latency, when there are many listeners but few care about a given message. It reports no gain, and no measurable loss, when every listener reads every message. The PostgreSQL 19 release notes list the change ("previously most backends were woken by NOTIFY"). PostgreSQL 19 was at Beta 4 on 24 September 2026.

**Channels per listener are a cost in their own right.** Heikki Linnakangas reviewed an intermediate version of the patch with listeners subscribed to 10,000 extra channels:

- The notify rate fell from 3,296/s to 1,274/s, because each listener checked its channel list linearly while holding the queue's SLRU lock.
- On master, 67.6% of deliveries fell in the 0.1–1 ms bucket. With the regressing version, 99.7% fell in the 1–10 ms bucket.
- His fix moved the check outside the lock, and he suggested replacing the list with a hash table.

**What PostgreSQL 19 does not fix (inference from the commit message).** Each interested listener still costs one wake-up and one queue read per notification. If N consumers listen on the same channel, a NOTIFY still wakes all N, even when only one of them can claim the work. This is the classic thundering herd, and PostgreSQL 19 leaves it in place. The remedies sit in the application.

### Fan-out: keeping N subscriptions from becoming N database operations

**At the notification layer.** Production libraries make the number of database listeners independent of the number of subscriptions:

- **Brandur Leach's "notifier" pattern.**
  - It keeps listen/notify connections "down to one per program", rather than "one connection per topic per program".
  - It issues one `LISTEN` per topic, however many components subscribe.
  - It hands notifications to each subscriber with a non-blocking send into a bounded buffer. When a buffer is full, "the notification is discarded if the channel is full", so a slow subscriber never blocks the notifier.
  - Everything else can go through PgBouncer in transaction-pooling mode.
- **Graphile Worker.**
  - Each process LISTENs. On each event, it asks one idle worker to fetch jobs. If no worker is idle, it drops the event, because busy workers fetch the next job when they finish. This avoids the herd entirely.
  - With prefetching into a local queue and batched completion writes, it reports about 183,000 jobs per second, with 4.16 ms average (13.84 ms maximum) latency from enqueue to start, on an i9-14900K. Without batching the figure is about 15,600 jobs per second. Batching cuts queries per second and WAL churn.
  - Its fallback poll defaults to 2 seconds. The docs say 30–60 seconds is "probably fine" if polling causes performance problems.

With these designs, the per-listener cost that pre-19 PostgreSQL charges grows with the number of processes, not the number of subscriptions. PostgreSQL never folds notifications from different commits, so merging wake-ups across commits has to happen in the application. A level-triggered flag such as `asyncio.Event` merges any number of `set()` calls into one pending wake-up.

**At the read layer, the herd moves into the application.**

- **One commit, N identical statements.** `asyncio.Event.set()` wakes every waiting task ("All tasks waiting for event to be set will be immediately awakened"). Suppose every subscription waits on the same "head changed" event and then runs `SELECT … WHERE position > :cursor ORDER BY position LIMIT :n`. One commit then produces N statements at once, all asking for the same few rows at the tail of the table.
- **No server-side sharing.** PostgreSQL's only built-in scan sharing, `synchronize_seqscans`, applies to sequential scans of large tables. It does not help small index range scans.
- **The cost is per statement, not per page.** The tail pages are almost certainly in shared buffers, so each read mostly pays fixed overhead: a round trip, a snapshot, executor start-up and sending the rows. This is an inference from how PostgreSQL works, not a measurement.
- **Connection pressure.** N simultaneous checkouts grow a psycopg 3 pool to `max_size` and queue the rest. psycopg's own advice is that pools should be "probably not as large as you imagine".
- **What many connections cost the server.** If each subscription holds its own connection instead, the server pays for many backends:
  - Andres Freund measured that, before PostgreSQL 14, 5,000 idle connections made `GetSnapshotData()` take about half the CPU time and cut throughput by about 30%.
  - In the same measurements, 5,000 mostly-idle connections dropped one active connection's throughput from 33,457 to 9,807 TPS.
  - PostgreSQL 14 sped up snapshot computation "on systems with many CPUs and high session counts", which reduces the problem but does not remove it.
  - The PostgreSQL wiki puts the useful number of active connections at roughly `(core_count * 2) + effective_spindle_count`. Beyond that, contention lowers throughput.

Job queues and event stores differ here:

- **Job queues have competing consumers.** Every pool woken by a new job may ask for it, "multiplying up the load" in Graphile's words, but only one wins.
- **Event-store subscriptions fan out.** Every woken subscription genuinely needs every event. The waste is therefore not lost races. It is N reads per burst of events, plus N idle polls per interval.

Three ways to control the read herd appear in production:

- **Concurrency limit.** A shared limiter, such as `asyncio.Semaphore(L)`, caps how many reads run at once.
  - Marten found that "an unbounded daemon can drive the connection pool's high-water mark". It now defaults `MaxConcurrentEventLoadsPerDatabase` and `MaxConcurrentBatchWritesPerDatabase` to 4, "collapsing the steady-state connection footprint to *O(databases)* with no measured throughput cost".
  - Keeping a separate limiter smaller than the pool leaves spare connections for appends and checkpoint writes.
- **Staggering and cooldowns.** Each reader waits a random delay or a minimum gap, and any further head changes during the wait are folded into the same read.
  - Graphile's `refetchDelay` uses a randomised abort threshold so that "different workers will abort refetch delay at different times".
  - Oban's `dispatch_cooldown` (default 5 ms) exists because "a cooldown period prevents a producer from flooding with messages and thrashing the database".
  - Marc Brooker's AWS analysis found that, without jitter, callers arrive in "clusters of calls". With 100 contending clients, jitter cut the number of calls by more than half.
  - For a single head change, staggering spreads the N reads over time rather than removing them. The saving comes under a sustained write rate: each subscription reads at most once per window W, and gets bigger batches.
- **One shared head reader.** One reader per process reads new events once and hands them out in memory. The general technique is request coalescing, which Go's `singleflight` implements: "only one execution is in-flight for a given key at a time". Mature event stores use it:
  - **Elixir EventStore (used by Commanded).** "A single process will connect to the database to listen for these notifications. It fetches the event data and broadcasts to all interested subscriptions." A subscription that falls behind its buffer switches to reading storage itself until it catches up.
  - **Axon `EmbeddedEventStore`.** It caches recent events (default 10,000) "to prevent that each event processor needs to read from the storage engine individually". A processor whose position falls out of the cache is moved to private reads (checked every `cleanupDelay`, default 10 s). The shared producer polls for other nodes' commits every `fetchDelay` (default 1 s). Axon's Pooled Streaming Event Processor opens "a single event stream" for all segments, whereas the Tracking Event Processor "will use more I/O resources". The cost is that pooled segments "will at least process as fast as the slowest segment in the set".
  - **Marten composite projections.** They read "the event range *once*" for all their constituent projections.
  - **Akka Persistence R2DBC.** It publishes new events to running queries over cluster messaging, so they arrive "without waiting for next query poll".

| Approach | Reads per head change | Peak concurrent statements | Rows sent | Added latency |
|---|---|---|---|---|
| Unbounded individual reads | N | N | N·k | none, but contention at high N |
| Concurrency-limited | N | L | N·k | up to ⌈N/L⌉ × one read |
| Staggered or cooldown | N, or fewer under bursts | about N × read time ÷ W | N·k, in bigger batches | about W/2 on average |
| Shared read | 1 per process | 1 per process | k per process | in-memory dispatch only |

Here N is the number of subscriptions, L the concurrency limit, k the number of new events and W the stagger window. The figures are derived from the mechanisms. No source published a PostgreSQL benchmark comparing these options, and Marten's "no measured throughput cost" is the only empirical data point. The shared read also has costs:

- It starts from the lowest cursor among its subscribers, so it returns a superset of rows and pushes filtering into application CPU.
- It needs memory for the shared buffer.
- It needs a way to evict slow subscribers to private catch-up, as Axon's cleanup and EventStore's `max_capacity` state do.
- Correctness depends on handing subscribers between the shared stream and private catch-up without skipping or duplicating events at the boundary.

### Polling: idle load, latency floor and long polling

A fixed-interval poller sends 1/interval queries per subscription whether anything has changed or not. Its mean delay is about half the interval plus query time, and its worst case is the full interval. Without a shared check, idle load is about N divided by the interval:

- 300 subscriptions polling every second issue about 300 queries per second while idle.
- Backing off to a 30-second cap cuts that to about 10 queries per second, but worst-case latency becomes about 30 seconds.

Liu, Ramasubramanian and Sirer's RSS study (IMC 2005) shows what independent pollers cost in practice:

- 58% of automated clients kept the default hourly poll.
- Clients polled independently, "imposing a high load on the servers of popular feeds".
- Fetching only the changes would have saved about 93% of bandwidth.

Production defaults show where systems land:

| System | Polling | Notes |
|---|---|---|
| Solid Queue (Rails) | 0.1 s workers, 1 s dispatchers, using `FOR UPDATE SKIP LOCKED` | Recommends running in a separate database |
| Eventide | 100 ms cycle | |
| Marten / JasperFx.Events | 250 ms when active, 1 s when caught up | |
| Akka Persistence R2DBC `eventsBySlices` | 3 s refresh interval, adapted to batch fullness | |
| River | 1 s fallback | Behind notifications |
| Graphile Worker | 2 s fallback | Behind notifications |
| GoodJob | 10 s production poll interval | Exists "in case of a LISTEN/NOTIFY blip" |

Long polling removes most empty polls by holding a request open until work arrives:

- **pgmq `read_with_poll`** moves the long poll into the database. It checks every 100 ms for up to 5 s, at the cost of holding a connection for the whole wait.
- **Amazon SQS** recommends waits of up to 20 s, to cut empty and falsely empty receives, each of which is billed.
- **Kafka** notes that naive pull busy-waits. It fixes this with blocking "long poll" fetch requests.
- **psycopg 3 (since 3.2).** `Connection.notifies(timeout=…, stop_after=…)` blocks until a notification arrives or the timeout expires. It gives the same shape on the client side: a notification cuts the wait short, and the timeout acts as the fallback poll. Judging from the source, the wait is on the client's socket, so an empty timeout costs the server nothing.

### Correctness costs that grow with concurrent writers

Both pollers and notification-woken consumers carry a correctness cost, and it grows with write concurrency.

**Sequence positions commit out of order.** PostgreSQL sequences are deliberately non-transactional:

- A value from `nextval` "is not reclaimed for re-use if the calling transaction later aborts".
- Sequence state changes "are immediately visible to other transactions".
- A sequence default is evaluated when the row is written, not at commit.

Two appenders can therefore draw positions 3 and 4 and commit 4 first. A reader that sees 1, 2 and 4 and advances its cursor to the maximum skips 3 for good.

**Notifications do not help.** They arrive in commit order, which is exactly the order that diverges from sequence order. A notification can only say "re-run the frontier query". Its payload must never be used as a cursor.

**Every safe design computes a safe frontier.** A safe frontier is a point below which no further rows can become visible. Marten notes the difficulty: most holes fill within milliseconds, but holes left by rolled-back appends never fill, and looking at the hole alone cannot tell the two apart.

| Approach | Writer change | Exact? | Main cost |
|---|---|---|---|
| Transaction-ID horizon: stamp an `xid8` column from `pg_current_xact_id()`, read `transaction_id < pg_snapshot_xmin(pg_current_snapshot())`, checkpoint `(transaction_id, position)` | One column, no locks | Yes | Order follows each transaction's first write. Long writing transactions hold the frontier back. |
| Hold at the gap until no live transaction could fill it (Marten async daemon) | None | Yes, unless the optional stale-gap cap is used | More expensive reads. Same exposure to long transactions. |
| Tolerate gaps for a bounded time (Pekko/Akka Persistence JDBC: about 10 s; Axon: 60 s or 10,000 positions) | None | No | Events that commit after the window are lost. Axon also delivers late events out of order. |
| Writers register their in-flight sequence floor through advisory locks (Sequin) | A protocol on every writer | Yes | Complexity. Two-argument advisory-lock keys accept only `int`. |
| Narrow the serialisation (Message DB category lock; allocating late, just before commit) | Writers serialised per category, or briefly | Per category only | Write concurrency |
| Assign global positions after commit with a single sequencer (adapted from the "transactionally staged job drain"; an inference) | A second write per row | Yes | A singleton process and one extra cycle of latency |

Marten's daemon is the clearest hold-at-gap example:

- It asks whether "any transaction [is] still running that could have reserved it". Once none is, it skips the dead span in one step.
- An optional `SkipStaleGapsDespiteLiveTransactionsAfter` cap guards against leaked sessions. Past the cap, it skips on suspicion, "so an append that commits inside the skipped range will never be projected".
- JasperFx.Events' settings also carry a `StaleSequenceThreshold` of 3 s for gaps.

**How far the transaction-ID frontier lags.** No published measurements exist, but the PostgreSQL source shows what holds the horizon back. `GetSnapshotData` takes the minimum running XID over every backend that has one. It skips backends without an XID, lazy vacuum and logical-decoding walsenders. Five consequences follow:

- **Read-only work never holds it back.** Reports, `pg_dump` and idle read-only transactions have no XID, though they still hold back vacuum.
- **Row locks count as writes.** `SELECT FOR UPDATE` takes an XID. A consumer that holds a locked lease row open while its handler runs stalls every other subscription.
- **The horizon spans the whole cluster.** XIDs come from "a global counter used by all databases within the PostgreSQL cluster". A batch job in an unrelated database therefore stalls the event store's consumers.
- **Prepared transactions hold it back indefinitely.** This continues even after their session has gone, and no session timeout ends it.
- **The delay is the remaining lifetime of the oldest writer.** An event's delay is the remaining life of the longest-lived older XID-holding transaction in the cluster. Sequin's example: a two-minute transaction on another table leaves the last two minutes of events unreadable. Sequin rejected the approach for that reason.

**Server timeouts can bound the lag, with limits.**

- **Settings that bound it.**
  - `transaction_timeout` (PostgreSQL 17+) is the only setting that caps total transaction duration.
  - `idle_in_transaction_session_timeout` handles only abandoned transactions.
  - `statement_timeout` does not bound the lag.
  - `max_prepared_transactions = 0` removes the prepared-transaction case.
- **Limits on applying them.**
  - All of these are user-settable, so any session can override them. They must cover every role on every database in the cluster for the bound to hold.
  - PostgreSQL advises scoping them by role, not setting them in `postgresql.conf`.
  - Before PostgreSQL 17, only an external watchdog can bound total transaction duration.
- **Monitoring.** The horizon's age can be observed through `pg_stat_activity.backend_xid` and `xact_start`.

A consumer stuck behind the frontier may not be released by a later notification: the transaction blocking it may not be an appender, or may roll back without notifying. As an inference, a notification-woken consumer therefore needs a re-check timer that runs while the frontier trails the newest committed row.

**Ordering within each stream.** A feed ordered by transaction ID follows first-write order, not commit order.

- **When version order holds.** Per-stream version order holds whenever the writer of version N+1 gets its XID after it has read version N, which optimistic concurrency normally ensures.
- **When it breaks.** It breaks when a transaction already holds an XID before it reads the stream head. Examples are multi-stream or outbox writes that come first, calling `pg_current_xact_id()` early, or retrying through a savepoint.
- **Write-side fixes.**
  - Make the append the transaction's first write.
  - Run appends at REPEATABLE READ or SERIALIZABLE. This is derived from the documented snapshot rules rather than stated by any source.
  - Use a per-stream monotonic key, like Akka R2DBC's `GREATEST(…, predecessor + 1 µs)` timestamp.
  - Serialise writers per category, as Message DB does.
- **Consumer-side fix.** Akka places the responsibility on the consumer to "make sure that events are processed in exact sequence number order for each persistence id". A consumer can fetch missing predecessors directly from the stream.

Commit-ordered sources such as logical decoding avoid both the frontier problem and this inversion.

### Adaptive polling policies used in production

Mature PostgreSQL-backed consumers have converged on a four-part policy. They include Akka Persistence R2DBC, Marten/JasperFx.Events, River, Graphile Worker, Oban, Solid Queue, Eventide, GoodJob and Elixir EventStore. Each part bounds a different term.

**1. Drain while full, gated by downstream capacity.**

- **Akka R2DBC** re-queries "immediately" when the last query returned at least 90% of `buffer-size` (default 1000). It waits half the 3 s refresh interval between 10% and 90%, and the full interval at 10% or below.
- **Solid Queue's dispatcher** returns `batch.zero? ? polling_interval : 0.seconds`.
- **Eventide** sleeps only after an empty result.
- **River** sets `fetchWhenSlotsAreAvailable` when a fetch returns its maximum, "implying there may be more in the queue".

A full batch signals backlog in the database, not capacity in the handlers, so immediate re-query must be bounded by demand:

- River fetches at most `MaxWorkers` minus active jobs.
- Oban computes `demand = limit - running`.
- Elixir EventStore caps unacknowledged events at `buffer_size` and drops to catch-up reads on overflow.
- Kafka advises `pause()` while slow records process.
- Reactive Streams makes it a rule: delivery "MUST be less than or equal to the total number of elements requested".

The rule that follows: fetch `n = min(batch_limit, free_capacity)` only when capacity is free. Treat a result of at least about 90% of the requested `n` as a fast-path hit. Under sustained backlog the consumer then settles at handler throughput by construction.

**2. Wait on notifications when idle, with a capped, jittered timeout.**

- **The timeout's two jobs.** The timeout bounds latency when a notification is missed and sets the idle query rate. When notifications are the primary signal, the timer only needs to cover a missed notification, so it can be long.
- **Jitter.** Brooker's analysis found "Full Jitter", `random(0, min(cap, base·2^attempt))`, gave the best balance of total work and completion time for contending retries. The lesson that carries over to hundreds of subscriptions is to de-synchronise them, for example after a deploy.
- **Mean-preserving jitter.** Oban's `dispatch_cooldown` is "the average number of milliseconds", randomised between about 0 and 2x. Graphile's `refetchDelay` is likewise an "on average" wait. Unlike Full Jitter, this does not halve the mean interval, and therefore does not double the idle query rate. That is an inference from the formulas.
- **Two-step schedules.** Marten uses a simple fast/slow pair rather than an exponential ramp.

**3. Treat notifications as edge-triggered hints, and suppress or coalesce them under load.**

- **River** fetches "*at most*" once per `FetchCooldown` (default 100 ms), however many notifications arrive.
- **Graphile Worker** waits `durationMs` after a short fetch and "(mostly) ignore[s]" notifications, abandoning the wait after a random number of them up to `maxAbortThreshold`. Its idle load becomes about `(1000/durationMs) × INSTANCE_COUNT` queries per second, independent of the notification rate.
- **Akka R2DBC** turns its published-events fast path off when the exponentially weighted throughput exceeds 400 events per second. Polling continues regardless, because published delivery is not guaranteed.
- **Oban** documents that insert-trigger notifications become a bottleneck at thousands of inserts per second, and that they can safely be disabled.

"Ignoring" must still mean reading notifications off the connection and discarding them. Otherwise the shared queue cannot be truncated.

**4. Share the "is there anything new?" check across subscriptions.** One head watcher per process removes N from the idle term. Two production examples:

- Marten/JasperFx.Events run one high-water agent per database, which publishes the safe mark to every projection shard.
- Elixir EventStore runs a single listener-reader per node.

Taken together, the latency bounds become:

- when idle: notification delivery plus cooldown plus query time
- when behind: backlog divided by throughput
- when a notification is missed: the timeout T

Idle load is about 1/T queries per second for the whole process, rather than N/T.

**Batch sizing.**

- **Little's law.** The Netflix concurrency-limits library states it as "Limit = Average RPS * Average Latency". For a batch consumer that gives `batch_limit ≈ observed_handler_rate × target_batch_duration`.
- **Kafka's history.** Unbounded `poll()` results made processing time unpredictable. KIP-41 added `max.poll.records`, and KIP-62 set defaults of 500 records and a 5-minute `max.poll.interval.ms`.
- **Batch-closing rules.** AWS Lambda closes a batch on whichever comes first: count, window or 6 MB. Akka Projections groups by 20 envelopes or 500 ms.
- **No self-tuning precedent.** No database-polling consumer in the sources self-tunes batch size; all use static sizes plus demand gating.
- **Controllers that could carry over (an inference).** The concurrency-limit controllers could be applied to batch size: AIMD (multiply by 0.9 on a drop, add 1 only when in-flight is at least half the limit), Gradient (Envoy, Gradient2) and Vegas.

**Never let handler pace set the length of a read transaction.**

- Fetch each batch with a keyset predicate in autocommit mode or a short transaction. Load it into memory, and end the transaction before handlers run.
- psycopg 3 starts a transaction even for a plain `SELECT` outside autocommit, leaving sessions "idle in transaction".
- Server-side cursors without `WITH HOLD`, and `Cursor.stream()`, tie the statement to handler speed.
- Statement, idle-in-transaction and (PostgreSQL 17+) transaction timeouts are backstops, set per role, not control mechanisms.

### What published research says about push, pull and adaptive polling

- **Web data dissemination: Bhide, Deolasee, Katkar, Panchbudhe, Ramamritham and Shenoy, "Adaptive Push-Pull" (WWW10 2001; IEEE Transactions on Computers 2002).**
  - Push keeps clients coherent with low message overhead. It costs per-client server state and connections, and loses that state when the server fails.
  - Pull is stateless and resilient, but needs two messages per poll. It reached only 70–80% fidelity under stringent coherency, even with adaptive time-to-refresh.
  - The PaP hybrid (pull primarily, push what the pull would miss) reached 98–100% fidelity.
  - The PoP hybrid assigns each client to push or pull according to server resources, and degrades gracefully instead of failing abruptly.
- **Feed materialisation: Silberstein, Terrace, Cooper and Ramakrishnan, "Feeding Frenzy" (SIGMOD 2010).** The lowest system load comes from deciding push or pull separately for each producer–consumer pair, based on the ratio of producer event rate to consumer read rate. Events from high-rate producers are pulled at read time. Events from low-rate producers are pushed, meaning materialised in advance.
- **Broadcast: Acharya, Franklin and Zdonik, "Balancing Push and Pull for Data Broadcast" (SIGMOD 1997), via a secondary summary.** A pull back-channel improves on pure broadcast push, but it can saturate the server as load grows. The paper proposes techniques to delay that point.
- **Publish-subscribe on the web: Liu, Ramasubramanian and Sirer (IMC 2005).** Independent pollers impose high load on popular feeds. The authors recommend that publishers state poll rates, and point to asynchronous update notification as the direction to take.
- **Network stacks: Mogul and Ramakrishnan, "Eliminating Receive Livelock in an Interrupt-Driven Kernel" (USENIX 1996; ACM TOCS 1997).**
  - Interrupt-driven systems "provide low overhead and good latency at low offered load, but degrade significantly at higher arrival rates". An unmodified kernel livelocked at about 6,000 packets per second.
  - The fix uses "interrupts only to initiate polling". Polling handles bounded quotas of 10–20 packets, and interrupts are re-enabled only "when no work is pending".
- **Linux NAPI.** Builds this hybrid into the kernel. Interrupts stay masked while polling. `napi_defer_hard_irqs` counts consecutive empty polls before re-arming them. Busy polling trades "CPU cycles for lower latency".
- **Storage I/O: Yang, Minturn and Hady, "When Poll Is Better than Interrupt" (FAST 2012).** With ultra-low-latency devices, spinning on completion beats interrupts. Polling wins when the expected wait is very short.
- **Interrupt coalescing: Tai, Smolyar, Wei and Tsafrir, "Calibrated Interrupts" (OSDI 2021).**
  - They frame the choice as firing an interrupt at once for latency, versus delaying it to spread its cost over more completions.
  - Adaptive coalescing heuristics can misbehave without knowing which requests are latency-sensitive.
  - Marking latency-sensitive requests gave up to 35% more throughput, 30% less CPU and 37% lower latency.
- **Message brokers: Kafka's design rationale.** Push "has difficulty dealing with diverse consumers", because a consumer "tends to be overwhelmed when its rate of consumption falls below the rate of production". Pull lets a consumer "fall behind and catch up" and gives "optimal batching without introducing unnecessary latency". Long polls fix pull's busy-waiting.
- **Standards: IETF RFC 6202 on HTTP long polling.** Under load, long polling "has a natural tendency to gracefully degrade in performance at a cost of message latency", delivering queued messages in batches. It costs a held connection and request per client, and up to three network transits of latency.

The common conclusions:

1. Push minimises latency and wasted work when events are sparse. It costs per-subscriber state and behaves worst under overload.
2. Pull is stateless, sets its own pace and batches well under load. It wastes queries and adds latency when traffic is low.
3. The most efficient designs adapt to load:
   - a push signal triggers pull-based draining
   - the push-or-pull choice is made per producer–consumer pair from relative rates
   - pushes stop while there is a backlog
   - push resumes only after several consecutive empty polls

These map onto PostgreSQL directly:

| Research result | PostgreSQL analogue |
|---|---|
| Interrupts only start polling; re-arm after empty polls (Mogul and Ramakrishnan, NAPI) | A notification moves a subscription into draining. Notifications are ignored while batches are full, and re-armed after short or empty batches. |
| Coalesce to spread fixed cost, but know what is latency-sensitive (Calibrated Interrupts) | DBOS's batched NOTIFY; River and Oban cooldowns |
| Choose push or pull per producer–consumer pair by rate (Feeding Frenzy) | Notify for low-rate streams and poll high-rate ones; Akka's throughput switch |
| Pull primarily, push to fill what pull would miss (Adaptive Push-Pull PaP) | Notifications plus a slow fallback poll |
| Pull with long poll and consumer-controlled batching (Kafka, RFC 6202) | `notifies(timeout=…)` as a long poll; demand-gated fetch sizes |
| Independent pollers overload the source (RSS study) | One shared head watcher per process |
| Spinning wins when the expected wait is short (FAST 2012) | Immediate re-query while batches come back full |

### Logical decoding as an alternative push source

Logical decoding through a replication slot is the only one of the three mechanisms that truly pushes committed data from the server. Its scaling limits are different again.

| Dimension | Logical decoding (slot) | LISTEN/NOTIFY | Table polling |
|---|---|---|---|
| What arrives | Committed row changes or emitted messages, in commit order | A wake-up with a payload of 8000 bytes or less; consumer then queries | Whatever the query returns |
| Latency floor | Commit plus WAL flush plus decode, streamed continuously | Commit plus delivery plus a follow-up query | About half the poll interval, plus any wait at the safe frontier |
| Write-path cost | Extra WAL from server-wide `wal_level = logical`; decoding off the commit path | Cluster-wide lock held through commit | None |
| Retention risk | WAL and catalog rows kept for a lagging slot, up to disk-full or wraparound shutdown | 8GB queue blocked by a listener in a long transaction; NOTIFY commits then fail | None |
| Operations | Replication role, slot and walsender limits, failover-slot setup, monitoring, client protocol support | One dedicated listening session | Simplest; gap handling needed |

**Latency and fan-out evidence.** Published figures come from products rather than bare slots, and include WebSocket fan-out, so they bound slot latency only loosely.

- **Supabase "Broadcast from Database".** Sustained 10,000 messages per second to 80,000 concurrent users, with a median latency of 46 ms, p95 of 132 ms and p99 of 159 ms.
- **Supabase "Postgres Changes".** Polls a slot and authorises each change per subscriber. It reports p95 latencies of 228–618 ms at 30–40 changes per second. Supabase blames single-threaded ordered processing and per-subscriber authorisation, not the slot.
- **Supabase's advice.** Beyond about 3,000 subscribers, use Broadcast, which "sends each change once and fans it out". This is the same lesson as the shared head reader.

**Write overhead.** `wal_level = logical` increases WAL volume, mainly through `REPLICA IDENTITY FULL` updates and deletes. An insert-only event table is the favourable case, though no figure is published for it. The commit path takes no lock; Morling notes that CDC "has no latency impact on writing transactions".

**Scaling limits.**

- `max_replication_slots` and `max_wal_senders` both default to 10 and need a restart to change.
- Only "one receiver may consume changes from a slot at any given time".
- A slot per subscription therefore does not scale. The practical shape is one slot per process or elected leader, fanned out in memory.
- Up to PostgreSQL 18, `wal_level = logical` needs a restart. PostgreSQL 19 can raise `effective_wal_level` automatically when the first logical slot is created.

**Retention risk.**

- **What a slot holds.** Slots "will prevent removal of required resources even when there is no connection using them", in extreme cases forcing a wraparound shutdown.
- **Guard rails.** `max_slot_wal_keep_size` (13+) and `idle_replication_slot_timeout` (18+) turn disk exhaustion into slot invalidation instead.
- **Delivery guarantee.** At least once: a slot's position "is persisted only at checkpoint".
- **No history.** A slot cannot replay from before its confirmed position.

**Temporary slots** "are not saved to disk and are automatically dropped on error or when the session has finished". They remove the abandoned-slot risk, at the cost of a table read on every reattachment. That read can be made exact. When created over the replication protocol, `CREATE_REPLICATION_SLOT` can export a snapshot that "will show exactly the state of the database after which all changes will be included in the change stream". A consumer can then:

1. Store its checkpoint as a table-readable `pg_snapshot`, not a slot LSN.
2. On attachment, read the delta through the exported snapshot.
3. Advance the checkpoint during streaming with a watermark. The options are a `pg_logical_emit_message` carrying `pg_current_snapshot()` (an inferred ordering argument that still needs confirming), or an LSN fence using `pg_current_wal_insert_lsn()`.

Redelivery after slot loss is then bounded by checkpoint frequency. Slot creation waits for all earlier transactions to finish, so a long transaction stalls attachment. This design is assembled from documented behaviour. PostgreSQL does not publish it as a recipe. DBLog's watermark technique and Debezium's re-snapshot and incremental-snapshot modes are the nearest precedents.

**Operations.**

- **Privileges.** A role with the `REPLICATION` attribute is required; the docs describe it as "a very highly privileged role". Amazon RDS also needs a static parameter and a reboot.
- **PgBouncer.** Replication connections work through PgBouncer only from 1.23.0 (July 2024).
- **Failover.** Slots survive failover only from PostgreSQL 17, with failover slots. Temporary slots never do.
- **Python client support.** There is no mature asyncio `pgoutput` consumer as of October 2026:
  - psycopg 3 has no replication API. Issue #71 has been open since 2021. psycopg 3.3 only lets `START_REPLICATION` run (PR #1219). A fuller proof of concept, PR #1311, was closed in May 2026 because the maintainer has a different design in progress, which had not been announced by the 3.3.6 release.
  - psycopg2's replication cursor is mature and can be driven asynchronously through `read_message()` and `fileno()`, but it decodes nothing inside `pgoutput`.
  - aiopg's replication PR #880 is unmerged. asyncpg and psqlpy have no support. Standalone decoders such as pypgoutput are prototypes.
  - Polling a slot over SQL (`pg_logical_slot_peek_binary_changes`) works from psycopg 3, but it is pull, not push.
  - For an insert-only table, an in-house decoder needs only Begin, Relation, Insert, Commit and Type messages.

### Reliability limits a notification-driven design must absorb

Scaling a notification design also means scaling its recovery paths, because notifications are best-effort from the consumer's point of view.

- **Session scope.** `LISTEN` takes effect at commit, and registrations vanish when the session ends. Anything committed while the listener is disconnected is never delivered. Recovery must go in this order: reconnect, re-issue every `LISTEN`, then wake every subscriber to re-query from its stored position. Querying before listening leaves a window that neither the query nor a notification covers.
- **Half-open connections.** A dead peer with no reset leaves an idle listener waiting forever. libpq keepalives, `tcp_user_timeout` and a periodic cheap query defend against this.
- **Poolers.**
  - PgBouncer lists `LISTEN` as "Never" under transaction pooling.
  - Session pooling's default `server_reset_query`, `DISCARD ALL`, includes `UNLISTEN *`.
  - RDS Proxy pins a listening client to one connection.
  - NOTIFY itself works through any mode.
  - The usual split is a direct or session-mode connection for the listener, and transaction pooling for everything else.
- **Standbys and failover.** Hot standbys reject `LISTEN`. Use `target_session_attrs=read-write` so the listener always connects to the primary. Notifications are not WAL-logged, so a failover drops every unread notification and every registration.
- **Asynchronous failover can lose committed events.** The loss is "proportional to the replication delay at the time of failover". The lost transactions form a suffix in commit order, not in position order. As an inference, the new primary can reissue the same positions, and the same XIDs, to different events. A consumer whose checkpoint lives outside the cluster can then silently skip new events.
  - **Detection.** Store a window of recent `(position, event_id)` fingerprints and verify it on reconnect. Optionally, treat the timeline ID as an epoch, following Kafka's KIP-320.
  - **De-duplication.** Key it on event ID, never on position.
  - **Prevention.** Synchronous replication shrinks the window but does not close it. On PostgreSQL 17+, `synchronized_standby_slots` gates logical delivery on standby receipt.
- **psycopg 3 listener mechanics.**
  - `notifies()` holds the connection lock for its whole run, so `LISTEN` changes must happen between iterations.
  - Notifications that arrive between iterations are kept in a backlog from 3.2.4. 3.2.10 fixed a related memory leak and deprecated mixing handlers with the generator.
  - `add_notify_handler()` fires only during other operations, so it cannot drive an idle listener.
  - Since 3.2, cancelling a task blocked in `notifies()` leaves the connection idle and usable. This holds only if the generator is closed deterministically, for example with `contextlib.aclosing()`. The rest of a partly yielded batch is lost.
  - The 3.1 series sends a pointless server cancel and then blocks until the next notification, so it is unfit for this pattern.
  - The maintainer recommends a control channel: `LISTEN` on an extra channel and NOTIFY it to wake the loop. Alternatively, use a bounded `timeout`.

All of these failure modes lead to the same rule. A notification means only "new data may exist". The table is the source of truth, and a low-frequency safety poll stays on even when notifications are healthy.

### Implications for a psycopg 3 event store

The following is synthesis from the evidence above, not a measured result.

- **Notifications as hints.** Use a notification only as a wake-up hint to re-run a frontier-bounded table read. Keep a slow fallback poll, as DBOS, Graphile Worker, River, GoodJob and Akka do.
- **One listener per process.** Hold one listening connection per process: autocommit, outside the pool, direct or via a session-mode pooler, using `target_session_attrs=read-write`, with keepalives, on psycopg 3.2.10 or later. Map channels to subscribers in memory, and wake them with non-blocking, coalescing signals. This avoids both the pre-19 per-listener signalling cost and the same-channel herd that PostgreSQL 19 keeps.
- **Fold notifications.** Use channels per category or stream with constant payloads, so repeats in one transaction fold.
- **Account for the commit lock.**
  - On every release up to and including PostgreSQL 19, NOTIFY inside a write transaction serialises all notifying commits across the cluster.
  - Under high concurrent write throughput, either NOTIFY from a separate batched transaction, giving up atomicity as DBOS does, or leave high-rate streams to polling.
  - This applies Feeding Frenzy's per-pair rate rule and the load-dependent switch in NAPI and Akka.
- **Share the head read.** Run one head watcher and one shared tail reader per process. Put a small separate semaphore, about Marten's 4, on lagging subscriptions' catch-up reads, and jitter catch-up entry after deploys and failovers.
- **Drain adaptively.** Fetch `min(limit, free capacity)` rows. Re-query at once while results are at least about 90% of the request, never faster than a cooldown floor. Go back to waiting after a short or empty batch, and treat further notifications while draining as a dirty flag. Read in short autocommit transactions with keyset paging.
- **Choose a safe frontier deliberately.**
  - The `xid8` horizon is the simplest exact option, but it changes the ordering contract.
  - Its latency depends on the longest writing transaction in the cluster. Bound that with role-scoped `transaction_timeout` (17+), `idle_in_transaction_session_timeout` and `max_prepared_transactions = 0`, and monitor horizon age.
  - Add a re-check timer while the frontier trails the newest committed row.
- **Keep logical decoding opt-in.** It removes NOTIFY's commit serialisation and the poller's gap problem for the live tail. But its operational demands, its 10-slot defaults and the lack of psycopg 3 protocol support make it a backend option rather than the default.

## Sources

- [Postgres LISTEN/NOTIFY does not scale](https://recall.ai/blog/postgres-listen-notify-does-not-scale) — tier-3 — recall.ai (company engineering blog)
- [Postgres LISTEN/NOTIFY Actually Scales](https://dbos.dev/blog/postgres-listen-notify-scalability) — tier-3 — dbos.dev (company engineering blog)
- [PostgreSQL Documentation: NOTIFY](https://www.postgresql.org/docs/current/sql-notify.html) — tier-1 — postgresql.org (official documentation)
- [PostgreSQL: LISTEN](https://www.postgresql.org/docs/current/sql-listen.html) — tier-1 — postgresql.org
- [PostgreSQL 19 Release Notes](https://www.postgresql.org/docs/19/release-19.html) — tier-1 — postgresql.org (official release notes)
- [PostgreSQL 13 Release Notes](https://www.postgresql.org/docs/release/13.0/) — tier-1 — postgresql.org (official release notes)
- [PostgreSQL 14 release notes](https://www.postgresql.org/docs/release/14.0/) — tier-1 — postgresql.org
- [PostgreSQL 17 release notes (transaction_timeout added)](https://www.postgresql.org/docs/release/17.0/) — tier-1 — postgresql.org
- [pgsql: Optimize LISTEN/NOTIFY via shared channel map and direct advance](https://www.postgresql.org/message-id/E1vgSld-000fFk-2J%40gemulon.postgresql.org) — tier-1 — postgresql.org (pgsql-committers archive)
- [Optimize LISTEN/NOTIFY (original proposal and ping-pong benchmark)](https://www.postgresql.org/message-id/6899c044-4a82-49be-8117-e6f669765f7e%40app.fastmail.com) — tier-1 — postgresql.org (pgsql-hackers archive)
- [Re: Optimize LISTEN/NOTIFY (idle-listener benchmark table)](https://www.postgresql.org/message-id/8c71183a-0d28-4bcf-a806-78446ff95404%40app.fastmail.com) — tier-1 — postgresql.org (pgsql-hackers archive)
- [Re: Optimize LISTEN/NOTIFY (targeted signalling and direct advance design)](https://www.postgresql.org/message-id/2eeea4f1-1b4f-430c-8571-544da04f08dc%40app.fastmail.com) — tier-1 — postgresql.org (pgsql-hackers archive)
- [Re: Optimize LISTEN/NOTIFY (review with many channels per listener)](https://www.postgresql.org/message-id/66213fee-00ff-4952-802d-c06454e521ac%40iki.fi) — tier-1 — postgresql.org (pgsql-hackers archive)
- [Proposal: Out-of-Order NOTIFY via GUC to Improve LISTEN/NOTIFY Throughput](https://www.postgresql.org/message-id/ab1b986a-8ae2-469e-a680-11c1ce8fd4e8%40app.fastmail.com) — tier-1 — postgresql.org (pgsql-hackers archive)
- [Re: Proposal: Out-of-Order NOTIFY (NOTIFY-only vs LISTEN+NOTIFY results)](https://www.postgresql.org/message-id/3d7ed3bc-5cdd-4ed7-bed5-b8e4397e11de%40app.fastmail.com) — tier-1 — postgresql.org (pgsql-hackers archive)
- [Re: Proposal: Out-of-Order NOTIFY (1,000-notifier lock benchmark)](https://www.postgresql.org/message-id/7c8c61c2-4dbf-4961-9e0c-a5ec77d8f846%40app.fastmail.com) — tier-1 — postgresql.org (pgsql-hackers archive)
- [[PATCH] Write Notifications Through WAL](https://www.postgresql.org/message-id/CAK80%3DjjvgWJL5EdzH_Bxc37gn53PJaB5sDXB3VPfJuN7G1xL5Q%40mail.gmail.com) — tier-1 — postgresql.org (pgsql-hackers archive)
- [Re: [PATCH] Write Notifications Through WAL (review)](https://www.postgresql.org/message-id/CAE7r3MKV7oewo6EGGLPMKQvv74ujPurjjDBU%3DK8KH8CYJStXaw%40mail.gmail.com) — tier-1 — postgresql.org (pgsql-hackers archive)
- [Commitfest entry: Write Notifications Through WAL](https://commitfest.postgresql.org/patch/6205) — tier-1 — commitfest.postgresql.org (official PostgreSQL commitfest)
- [Use a hash table to de-duplicate NOTIFY events faster](https://git.postgresql.org/gitweb/?p=postgresql.git;a=commit;h=bb5ae8f6c) — tier-1 — git.postgresql.org (official source repository)
- [Make some efficiency improvements in LISTEN/NOTIFY](https://git.postgresql.org/gitweb/?p=postgresql.git;a=commit;h=51004c717) — tier-1 — git.postgresql.org (official source repository)
- [docs: LISTEN/NOTIFY performance considerations](https://hackorum.dev/topics/51899) — tier-3 — hackorum.dev (third-party mailing-list archive)
- [PostgreSQL source: src/backend/commands/async.c (master)](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/commands/async.c) — tier-1 — github.com (postgres/postgres)
- [The Notifier Pattern for Applications That Use Postgres](https://brandur.org/notifier) — tier-3 — brandur.org (personal blog)
- [Transactionally Staged Job Drains in Postgres](https://brandur.org/job-drain) — tier-2 — brandur.org
- [Graphile Worker: Performance](https://worker.graphile.org/docs/performance) — tier-1 — worker.graphile.org (official project documentation)
- [Graphile Worker: FAQ](https://worker.graphile.org/docs/faq) — tier-1 — worker.graphile.org (official project documentation)
- [Graphile Worker configuration (localQueue, pollInterval)](https://worker.graphile.org/docs/config) — tier-1 — worker.graphile.org
- [Graphile Worker src/index.ts (localQueue.refetchDelay doc comments)](https://raw.githubusercontent.com/graphile/worker/main/src/index.ts) — tier-1 — github.com/graphile/worker
- [Solid Queue README](https://github.com/rails/solid_queue) — tier-1 — github.com/rails (official project repository)
- [Solid Queue dispatcher.rb](https://raw.githubusercontent.com/rails/solid_queue/main/lib/solid_queue/dispatcher.rb) — tier-1 — github.com/rails/solid_queue
- [Solid Queue worker.rb](https://raw.githubusercontent.com/rails/solid_queue/main/lib/solid_queue/worker.rb) — tier-1 — github.com/rails/solid_queue
- [Akka Persistence R2DBC: Query plugin](https://doc.akka.io/libraries/akka-persistence-r2dbc/current/query.html) — tier-1 — doc.akka.io
- [Akka Persistence R2DBC PostgresJournalDao source](https://raw.githubusercontent.com/akka/akka-persistence-r2dbc/main/core/src/main/scala/akka/persistence/r2dbc/internal/postgres/PostgresJournalDao.scala) — tier-1 — github.com (akka/akka-persistence-r2dbc, primary source)
- [Akka Projection core reference.conf](https://raw.githubusercontent.com/akka/akka-projection/main/akka-projection-core/src/main/resources/reference.conf) — tier-1 — github.com (akka/akka-projection)
- [pgmq SQL functions: read_with_poll](https://github.com/pgmq/pgmq/blob/main/docs/api/sql/functions.md) — tier-1 — github.com/pgmq (official project documentation)
- [Amazon SQS short and long polling](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-short-and-long-polling.html) — tier-1 — docs.aws.amazon.com (official documentation)
- [Apache Kafka Design: Push vs. pull](https://kafka.apache.org/081/design/design/) — tier-1 — kafka.apache.org (official project documentation)
- [KafkaConsumer Javadoc (3.6)](https://kafka.apache.org/36/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html) — tier-1 — kafka.apache.org
- [KIP-62: Allow consumer to send heartbeats from a background thread](https://cwiki.apache.org/confluence/display/KAFKA/KIP-62%3A+Allow+consumer+to+send+heartbeats+from+a+background+thread) — tier-1 — cwiki.apache.org
- [KIP-320: Allow fetchers to detect and handle log truncation](https://cwiki.apache.org/confluence/display/KAFKA/KIP-320%3A+Allow+fetchers+to+detect+and+handle+log+truncation) — tier-1 — cwiki.apache.org (Apache Kafka)
- [Adaptive Push-Pull: Disseminating Dynamic Web Data](https://archives.iw3c2.org/www10/cdrom/papers/269/index.html) — tier-1 — archives.iw3c2.org (WWW10 peer-reviewed proceedings)
- [Paper: Feeding Frenzy: Selectively Materializing Users' Event Feeds](https://highscalability.com/paper-feeding-frenzy-selectively-materializing-users-event-f/) — tier-2 — highscalability.com (established technical writing summarising the SIGMOD 2010 paper)
- [Balancing Push and Pull for Data Broadcast](https://rmarcus.info/dbscholar/papers/h14ccd9e74accffd7) — tier-3 — rmarcus.info (paper-index aggregator summarising the SIGMOD 1997 paper)
- [Client Behavior and Feed Characteristics of RSS, a Publish-Subscribe System for Web Micronews](https://www.usenix.org/legacy/event/imc05/tech/full_papers/liu_hongzhou/liu_hongzhou_html/paper.html) — tier-1 — usenix.org (IMC 2005 peer-reviewed proceedings)
- [Eliminating Receive Livelock in an Interrupt-Driven Kernel (abstract)](https://www.usenix.org/legacy/publications/library/proceedings/sd96/mogul.html) — tier-1 — usenix.org (USENIX 1996 peer-reviewed proceedings)
- [Eliminating Receive Livelock in an Interrupt-Driven Kernel (full paper)](https://static.usenix.org/publications/library/proceedings/sd96/full_papers/mogul.txt) — tier-1 — static.usenix.org (USENIX 1996 peer-reviewed proceedings)
- [NAPI — The Linux Kernel documentation](https://docs.kernel.org/networking/napi.html) — tier-1 — docs.kernel.org (official documentation)
- [When Poll Is Better than Interrupt](https://usenix.org/conference/fast12/when-poll-better-interrupt) — tier-1 — usenix.org (FAST 2012 peer-reviewed proceedings)
- [Optimizing Storage Performance with Calibrated Interrupts](https://www.usenix.org/conference/osdi21/presentation/tai) — tier-1 — usenix.org (OSDI 2021 peer-reviewed proceedings)
- [RFC 6202: Known Issues and Best Practices for the Use of Long Polling and Streaming in Bidirectional HTTP](https://www.rfc-editor.org/rfc/rfc6202.html) — tier-1 — rfc-editor.org (IETF informational RFC)
- [Exponential Backoff And Jitter (Marc Brooker)](https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/) — tier-2 — aws.amazon.com (AWS Architecture Blog)
- [River client.go (FetchCooldown, FetchPollInterval, PollOnly)](https://raw.githubusercontent.com/riverqueue/river/master/client.go) — tier-1 — github.com/riverqueue/river
- [River producer.go (fetch limiter, full-batch refetch, slot limiting)](https://raw.githubusercontent.com/riverqueue/river/master/producer.go) — tier-1 — github.com/riverqueue/river
- [Oban lib/oban.ex (dispatch_cooldown, stager, insert_trigger)](https://raw.githubusercontent.com/oban-bg/oban/main/lib/oban.ex) — tier-1 — github.com/oban-bg/oban
- [Oban source — lib/oban/engines/basic.ex](https://raw.githubusercontent.com/oban-bg/oban/main/lib/oban/engines/basic.ex) — tier-1 — github.com (oban-bg/oban)
- [Oban module documentation](https://oban.hexdocs.pm/Oban.html) — tier-1 — oban.hexdocs.pm
- [Eventide consumer subscription.rb](https://raw.githubusercontent.com/eventide-project/consumer/master/lib/consumer/subscription.rb) — tier-1 — github.com/eventide-project/consumer
- [Eventide poll.rb](https://raw.githubusercontent.com/eventide-project/poll/master/lib/poll/poll.rb) — tier-1 — github.com/eventide-project/poll
- [Marten documentation: Async Daemon (high water mark and gap detection)](https://martendb.io/events/projections/async-daemon.html) — tier-1 — martendb.io
- [Composite Projections — Marten documentation](https://martendb.io/events/projections/composite.html) — tier-1 — martendb.io
- [JasperFx.Events HighWaterAgent.cs](https://raw.githubusercontent.com/JasperFx/jasperfx/main/src/JasperFx.Events/Daemon/HighWater/HighWaterAgent.cs) — tier-1 — github.com/JasperFx/jasperfx
- [JasperFx.Events DaemonSettings.cs](https://raw.githubusercontent.com/JasperFx/jasperfx/main/src/JasperFx.Events/Daemon/DaemonSettings.cs) — tier-1 — github.com/JasperFx/jasperfx
- [Elixir EventStore Subscriptions guide](https://raw.githubusercontent.com/commanded/eventstore/master/guides/Subscriptions.md) — tier-1 — github.com/commanded/eventstore
- [EventStore notifications publisher source](https://raw.githubusercontent.com/commanded/eventstore/master/lib/event_store/notifications/publisher.ex) — tier-1 — github.com (commanded/eventstore)
- [EventStore subscription state machine source](https://raw.githubusercontent.com/commanded/eventstore/master/lib/event_store/subscriptions/subscription_fsm.ex) — tier-1 — github.com (commanded/eventstore)
- [EmbeddedEventStore javadoc (Axon 4.10)](https://apidocs.axoniq.io/4.10/org/axonframework/eventsourcing/eventstore/EmbeddedEventStore.html) — tier-1 — apidocs.axoniq.io
- [Streaming Event Processors — Axon Framework reference 4.10](https://docs.axoniq.io/axon-framework-reference/4.10/events/event-processors/streaming/) — tier-1 — docs.axoniq.io
- [Axon Framework API: GapAwareTrackingToken](https://apidocs.axoniq.io/4.10/org/axonframework/eventhandling/GapAwareTrackingToken.html) — tier-1 — apidocs.axoniq.io
- [Axon Framework API: JdbcEventStorageEngine.Builder](https://apidocs.axoniq.io/4.10/org/axonframework/eventsourcing/eventstore/jdbc/JdbcEventStorageEngine.Builder.html) — tier-1 — apidocs.axoniq.io
- [Apache Pekko Persistence JDBC reference.conf](https://raw.githubusercontent.com/apache/pekko-persistence-jdbc/main/core/src/main/resources/reference.conf) — tier-1 — github.com (apache/pekko-persistence-jdbc)
- [GoodJob README (poll_interval, LISTEN/NOTIFY)](https://raw.githubusercontent.com/bensheldon/good_job/main/README.md) — tier-1 — github.com/bensheldon/good_job
- [singleflight package documentation](https://pkg.go.dev/golang.org/x/sync/singleflight) — tier-1 — pkg.go.dev
- [asyncio synchronization primitives](https://docs.python.org/3/library/asyncio-sync.html) — tier-1 — docs.python.org
- [Analyzing the Limits of Connection Scalability in Postgres](https://www.citusdata.com/blog/2020/10/08/analyzing-connection-scalability/) — tier-2 — citusdata.com
- [Number Of Database Connections](https://wiki.postgresql.org/wiki/Number_Of_Database_Connections) — tier-2 — wiki.postgresql.org
- [Version and Platform Compatibility (synchronize_seqscans) — PostgreSQL documentation](https://www.postgresql.org/docs/current/runtime-config-compatible.html) — tier-1 — postgresql.org
- [Reactive Streams JVM specification](https://github.com/reactive-streams/reactive-streams-jvm/blob/master/README.md) — tier-1 — github.com (reactive-streams)
- [Netflix concurrency-limits README](https://github.com/Netflix/concurrency-limits) — tier-1 — github.com (Netflix)
- [Netflix concurrency-limits — AIMDLimit.java](https://raw.githubusercontent.com/Netflix/concurrency-limits/master/concurrency-limits-core/src/main/java/com/netflix/concurrency/limits/limit/AIMDLimit.java) — tier-1 — github.com (Netflix)
- [Envoy — Adaptive Concurrency filter](https://www.envoyproxy.io/docs/envoy/latest/configuration/http/http_filters/adaptive_concurrency_filter) — tier-1 — envoyproxy.io
- [AWS Lambda — How Lambda processes records from stream and queue-based event sources](https://docs.aws.amazon.com/lambda/latest/dg/invocation-eventsourcemapping.html) — tier-1 — docs.aws.amazon.com
- [PostgreSQL — LIMIT and OFFSET](https://www.postgresql.org/docs/current/queries-limit.html) — tier-1 — postgresql.org
- [PostgreSQL — DECLARE](https://www.postgresql.org/docs/current/sql-declare.html) — tier-1 — postgresql.org
- [PostgreSQL — Routine Vacuuming](https://www.postgresql.org/docs/current/routine-vacuuming.html) — tier-1 — postgresql.org
- [PostgreSQL documentation: Sequence Manipulation Functions](https://www.postgresql.org/docs/current/functions-sequence.html) — tier-1 — postgresql.org
- [PostgreSQL documentation: System Information Functions (Transaction ID and Snapshot Information)](https://www.postgresql.org/docs/current/functions-info.html) — tier-1 — postgresql.org
- [PostgreSQL documentation: Transactions and Identifiers](https://www.postgresql.org/docs/current/transaction-id.html) — tier-1 — postgresql.org
- [Transaction Isolation](https://www.postgresql.org/docs/current/transaction-iso.html) — tier-1 — postgresql.org
- [Index Uniqueness Checks](https://www.postgresql.org/docs/current/index-unique-checks.html) — tier-1 — postgresql.org
- [PostgreSQL docs: Client Connection Defaults (statement, lock, idle and transaction timeouts)](https://www.postgresql.org/docs/current/runtime-config-client.html) — tier-1 — postgresql.org
- [PostgreSQL docs: The Cumulative Statistics System (pg_stat_activity columns)](https://www.postgresql.org/docs/current/monitoring-stats.html) — tier-1 — postgresql.org
- [PostgreSQL docs: PREPARE TRANSACTION](https://www.postgresql.org/docs/current/sql-prepare-transaction.html) — tier-1 — postgresql.org
- [PostgreSQL docs: Explicit Locking (row-level locks cause writes)](https://www.postgresql.org/docs/current/explicit-locking.html) — tier-1 — postgresql.org
- [PostgreSQL source: procarray.c (GetSnapshotData)](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/storage/ipc/procarray.c) — tier-1 — github.com/postgres/postgres (official source mirror)
- [PostgreSQL source: twophase.c (prepared transactions' dummy PGPROC)](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/access/transam/twophase.c) — tier-1 — github.com/postgres/postgres (official source mirror)
- [How Postgres sequences issues can impact your messaging guarantees](https://event-driven.io/en/ordering_in_postgres_outbox/) — tier-3 — event-driven.io
- [Postgres sequences can commit out-of-order](https://blog.sequinstream.com/postgres-sequences-can-commit-out-of-order/) — tier-3 — blog.sequinstream.com
- [eugene-khyst/postgresql-event-sourcing README](https://github.com/eugene-khyst/postgresql-event-sourcing) — tier-3 — github.com (individual repository)
- [Emmett emt_append_to_stream source](https://raw.githubusercontent.com/event-driven-io/emmett/main/src/packages/emmett-postgresql/src/eventStore/schema/appendToStream.ts) — tier-1 — github.com (event-driven-io/emmett, primary source)
- [Message DB acquire_lock function source](https://raw.githubusercontent.com/message-db/message-db/master/database/functions/acquire-lock.sql) — tier-1 — github.com (message-db/message-db)
- [PostgreSQL docs: Logical Decoding Concepts](https://www.postgresql.org/docs/current/logicaldecoding-explanation.html) — tier-1 — postgresql.org
- [PostgreSQL docs: Logical Decoding Output Plugins](https://www.postgresql.org/docs/current/logicaldecoding-output-plugin.html) — tier-1 — postgresql.org
- [PostgreSQL docs: Logical Decoding SQL Interface](https://www.postgresql.org/docs/current/logicaldecoding-sql.html) — tier-1 — postgresql.org
- [PostgreSQL docs: Streaming Replication Protocol](https://www.postgresql.org/docs/current/protocol-replication.html) — tier-1 — postgresql.org
- [PostgreSQL docs: Replication configuration](https://www.postgresql.org/docs/current/runtime-config-replication.html) — tier-1 — postgresql.org
- [PostgreSQL docs: Write Ahead Log configuration (wal_level)](https://www.postgresql.org/docs/current/runtime-config-wal.html) — tier-1 — postgresql.org
- [PostgreSQL devel docs: Write Ahead Log configuration (effective_wal_level)](https://www.postgresql.org/docs/devel/runtime-config-wal.html) — tier-1 — postgresql.org
- [PostgreSQL docs: pg_replication_slots](https://www.postgresql.org/docs/current/view-pg-replication-slots.html) — tier-1 — postgresql.org
- [PostgreSQL docs: System Administration Functions (replication management)](https://www.postgresql.org/docs/current/functions-admin.html) — tier-1 — postgresql.org
- [PostgreSQL docs: CREATE ROLE](https://www.postgresql.org/docs/current/sql-createrole.html) — tier-1 — postgresql.org
- [PostgreSQL 18 documentation: Logical Replication Failover](https://www.postgresql.org/docs/current/logical-replication-failover.html) — tier-1 — postgresql.org
- [PostgreSQL source: snapbuild.c](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/replication/logical/snapbuild.c) — tier-1 — raw.githubusercontent.com (postgres/postgres repository)
- [Supabase docs: Postgres Changes](https://supabase.com/docs/guides/realtime/postgres-changes) — tier-1 — supabase.com
- [Supabase docs: Realtime Benchmarks](https://supabase.com/docs/guides/realtime/benchmarks) — tier-1 — supabase.com
- [Mastering Postgres Replication Slots: Preventing WAL Bloat and Other Production Issues (Gunnar Morling, 2025)](https://www.morling.dev/blog/mastering-postgres-replication-slots/) — tier-2 — morling.dev
- [Debezium connector for PostgreSQL](https://debezium.io/documentation/reference/stable/connectors/postgresql.html) — tier-1 — debezium.io
- [DBLog: A Watermark Based Change-Data-Capture Framework](https://arxiv.org/abs/2010.12597) — tier-2 — arxiv.org (preprint)
- [Amazon RDS: Performing logical replication for RDS for PostgreSQL](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/PostgreSQL.Concepts.General.FeatureSupport.LogicalReplication.html) — tier-1 — docs.aws.amazon.com
- [PgBouncer changelog](https://www.pgbouncer.org/changelog.html) — tier-1 — pgbouncer.org
- [psycopg issue #71: Replication support](https://github.com/psycopg/psycopg/issues/71) — tier-1 — github.com (psycopg/psycopg)
- [Don't fail on commands returning COPY_BOTH results (PR 1219)](https://api.github.com/repos/psycopg/psycopg/pulls/1219) — tier-1 — github.com/psycopg/psycopg
- [psycopg pull request #1311: POC replication connection support](https://api.github.com/repos/psycopg/psycopg/pulls/1311) — tier-1 — api.github.com (psycopg/psycopg)
- [psycopg PyPI release metadata](https://pypi.org/pypi/psycopg/json) — tier-1 — pypi.org
- [psycopg2 extras: replication support objects](https://www.psycopg.org/docs/extras.html) — tier-1 — psycopg.org
- [aiopg PR 880: add support for streaming replication protocol](https://api.github.com/repos/aio-libs/aiopg/pulls/880) — tier-1 — github.com/aio-libs/aiopg
- [Add support for logical replication protocol (asyncpg issue 91) comments](https://api.github.com/repos/MagicStack/asyncpg/issues/91/comments?per_page=100) — tier-1 — github.com/MagicStack/asyncpg
- [psqlpy repository README](https://github.com/psqlpy-python/psqlpy) — tier-1 — github.com/psqlpy-python/psqlpy
- [pypgoutput repository and README](https://github.com/dgea005/pypgoutput) — tier-1 — github.com/dgea005/pypgoutput
- [PostgreSQL: Logical streaming replication protocol](https://www.postgresql.org/docs/current/protocol-logical-replication.html) — tier-1 — postgresql.org
- [Psycopg 3: Asynchronous operations and notifications](https://www.psycopg.org/psycopg3/docs/advanced/async.html) — tier-1 — psycopg.org
- [Psycopg 3: Connection classes API (notifies timeout and stop_after)](https://www.psycopg.org/psycopg3/docs/api/connections.html) — tier-1 — psycopg.org
- [Psycopg 3: Connection pools](https://www.psycopg.org/psycopg3/docs/advanced/pool.html) — tier-1 — psycopg.org
- [Psycopg 3: Release notes](https://www.psycopg.org/psycopg3/docs/news.html) — tier-1 — psycopg.org
- [Psycopg 3 docs: Transactions management](https://www.psycopg.org/psycopg3/docs/basic/transactions.html) — tier-1 — psycopg.org
- [psycopg 3 — Cursor types (server-side cursors)](https://www.psycopg.org/psycopg3/docs/advanced/cursors.html) — tier-1 — psycopg.org
- [psycopg source: connection_async.py](https://raw.githubusercontent.com/psycopg/psycopg/master/psycopg/psycopg/connection_async.py) — tier-1 — github.com/psycopg/psycopg
- [psycopg connection_async.py (3.1.20)](https://raw.githubusercontent.com/psycopg/psycopg/3.1.20/psycopg/psycopg/connection_async.py) — tier-1 — github.com/psycopg/psycopg (project source)
- [psycopg issue #962: notifications lost between notifies() calls](https://github.com/psycopg/psycopg/issues/962) — tier-2 — github.com/psycopg/psycopg issue tracker
- [Issue #1310: Recommended pattern for notifies() while also running LISTEN/UNLISTEN on the same connection](https://github.com/psycopg/psycopg/issues/1310) — tier-1 — github.com/psycopg/psycopg (official project issue tracker)
- [Issue #340: AsyncConnection.notifies() generator blocks AsyncConnection.execute()](https://github.com/psycopg/psycopg/issues/340) — tier-1 — github.com/psycopg/psycopg (official project issue tracker)
- [PR #760: lock the connection during a notifies() call](https://github.com/psycopg/psycopg/pull/760) — tier-1 — github.com/psycopg/psycopg (official project issue tracker)
- [contextlib: aclosing](https://docs.python.org/3/library/contextlib.html) — tier-1 — docs.python.org
- [PEP 525: Asynchronous Generators (finalization)](https://peps.python.org/pep-0525/) — tier-1 — peps.python.org
- [PgBouncer: Features](https://www.pgbouncer.org/features.html) — tier-1 — pgbouncer.org
- [PgBouncer: Configuration](https://www.pgbouncer.org/config.html) — tier-1 — pgbouncer.org
- [PostgreSQL: DISCARD](https://www.postgresql.org/docs/current/sql-discard.html) — tier-1 — postgresql.org
- [Amazon RDS Proxy: Avoiding pinning](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/rds-proxy-pinning.html) — tier-1 — docs.aws.amazon.com
- [PostgreSQL: Hot Standby](https://www.postgresql.org/docs/current/hot-standby.html) — tier-1 — postgresql.org
- [PostgreSQL: libpq Database Connection Control Functions](https://www.postgresql.org/docs/current/libpq-connect.html) — tier-1 — postgresql.org
- [PostgreSQL documentation: Log-Shipping Standby Servers (asynchronous and synchronous replication)](https://www.postgresql.org/docs/current/warm-standby.html) — tier-1 — postgresql.org
- [PostgreSQL documentation: Continuous Archiving and Point-in-Time Recovery (Timelines)](https://www.postgresql.org/docs/current/continuous-archiving.html) — tier-1 — postgresql.org
- [PostgreSQL source: src/backend/commands/sequence.c (SEQ_LOG_VALS)](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/commands/sequence.c) — tier-1 — github.com (postgres/postgres)
- [Patroni documentation: Replication modes](https://patroni.readthedocs.io/en/latest/replication_modes.html) — tier-1 — patroni.readthedocs.io
