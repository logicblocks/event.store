---
type: "topic-research"
id: "02-what-delivery-guarantees-and-failure-modes-does-listen-notif-web"
title: "What delivery guarantees and failure modes does `LISTEN`/`NOTIFY` have — notifications lost while no listener is connected or during reconnection, commit-time delivery and duplicate collapsing within a transaction, the 8000-byte payload limit, the notification queue's size limit and behaviour when it fills, and the global lock taken when committing a transaction that issued `NOTIFY`?"
date: "2026-10-05T13:56:47+00:00"
author: "Toby Clemson"
producer: "research-topic"
status: "complete"
kind: "finding"
round: 1
depth: 3
question: "What delivery guarantees and failure modes does `LISTEN`/`NOTIFY` have — notifications lost while no listener is connected or during reconnection, commit-time delivery and duplicate collapsing within a transaction, the 8000-byte payload limit, the notification queue's size limit and behaviour when it fills, and the global lock taken when committing a transaction that issued `NOTIFY`?"
source_profile: "web"
tags: ["research", "topic-research", "finding"]
last_updated: "2026-10-05T13:56:47+00:00"
last_updated_by: "Toby Clemson"
schema_version: 1
---

# What delivery guarantees and failure modes does `LISTEN`/`NOTIFY` have — notifications lost while no listener is connected or during reconnection, commit-time delivery and duplicate collapsing within a transaction, the 8000-byte payload limit, the notification queue's size limit and behaviour when it fills, and the global lock taken when committing a transaction that issued `NOTIFY`?

## Question

What does PostgreSQL's `LISTEN`/`NOTIFY` actually guarantee, and how does it fail? This covers:

- notifications lost while no listener is connected or while a listener reconnects;
- delivery at commit time, and the collapsing of duplicate notifications within one transaction;
- the 8000-byte payload limit;
- the size limit of the shared notification queue, and what happens when it fills;
- the global lock taken when a transaction that issued `NOTIFY` commits.

The question is asked with a psycopg 3 event store in mind, where `NOTIFY` might replace or shorten polling by consumers.

## Findings

### In brief

`LISTEN`/`NOTIFY` is a transient, at-most-once broadcast. A notification reaches only the sessions that are connected and listening when the notifying transaction commits. Sending is transactional: a notification goes out only if its transaction commits, and listeners receive notifications in commit order. Nothing is durable. Nothing is stored for sessions that are not listening, the queue is wiped at every server restart, and notifications never reach standbys.

The main operational costs fall on writers, not readers:

- **One lock for the whole cluster.** Every transaction that issued `NOTIFY` takes the same heavyweight lock just before its commit record is written. It holds that lock until the transaction ends, across the WAL flush. Notifying commits therefore run strictly one at a time and cannot share group-commit flushes. Published measurements put the loss at between about 2x and 38x under concurrent load, and at least one production outage has been traced to it. PostgreSQL 19 makes waking listeners cheaper but keeps the lock.
- **A bounded queue that fails writes when full.** All databases share one notification queue, 8 GB by default, and it can only be truncated up to the slowest listener's read position. Two things can hold it back: a listener stuck in a long transaction, or a server backend blocked writing to a client that has silently vanished. Once the queue is full, every transaction that issued `NOTIFY` fails at `COMMIT`, and its data changes roll back with it.
- **Silent loss on dead connections.** A listener whose connection dies without a FIN or RST packet can stay deaf for minutes to hours on default TCP settings. Meanwhile the server may keep writing notifications into the dead socket, where they are lost.

In an event store, then, `NOTIFY` can only be a "wake up and read" hint on top of the durable events table. Consumers read from their own stored checkpoints, and a fallback poll recovers anything a notification failed to signal. Every established PostgreSQL job library examined (Oban, River, Graphile Worker and pg-boss) works this way.

### Who receives a notification, and when it is lost

**Only sessions that are listening at commit time.** The `LISTEN` reference says a newly listening session "will receive all events committed after an instant during the transaction's commit step". In `async.c`, a new listener sets its queue pointer to the global tail and then skips entries that have already committed. It therefore misses nothing that commits after that point and never receives anything that committed before it. `LISTEN` itself takes effect only when its own transaction commits. If that transaction rolls back, the set of channels is unchanged. "A session's listen registrations are automatically cleared when the session ends."

**Nothing is kept for absent listeners.** A notification committed while a consumer is disconnected, crashed or reconnecting is gone for that consumer. A PostgreSQL contributor put it bluntly on the JDBC list: "You lose notifications during any period you don't have a connection." Client libraries that reconnect automatically document the same thing. sqlx's `PgListener` re-subscribes after reconnecting, but "as notifications are transient, any received while the connection was lost, will not be returned."

**Not crash-safe, not replicated.** The design comment in `async.c` says notifications "are not expected to survive database crashes". The `pg_notify` data is wiped at every restart and is neither WAL-logged nor fsynced. A crash or restart therefore drops notifications that had committed but not yet been read, even for listeners that stayed connected. Hot standbys reject both `LISTEN` and `NOTIFY`. A consumer cannot listen on a replica, and a newly promoted primary has no record of earlier notifications.

**Connection poolers can break subscriptions.** PgBouncer's feature map lists `LISTEN` as supported only in session pooling ("Never" in transaction pooling), while `NOTIFY` works in both modes. Oban, River, Graphile Worker and pg-boss all document this limit. When it bites, pg-boss logs a `listen_notify_unavailable` warning and carries on polling. River enters a `PollOnly` mode automatically when its driver cannot listen.

**Two smaller rules.** A session that listens on a channel it also notifies receives its own notifications. It can drop them by comparing the sender PID with its own backend PID. A transaction that ran `NOTIFY` or `LISTEN` cannot be prepared for two-phase commit.

**The documented recovery order.** The `LISTEN` docs prescribe this sequence: "first execute (and commit!) that command, then in a new transaction inspect the database state as needed by the application logic, then rely on notifications to find out about subsequent changes." The first few notifications may refer to changes the initial read already saw, "but this is usually harmless." The same order applies after every reconnect. For an event store, inspecting the database state means reading from the consumer's stored checkpoint.

### Silent connection death

The hardest reconnection case is a connection that dies without the client ever finding out.

**Why a listener can go deaf.** A psycopg 3 listener spends most of its time blocked in `Connection.notifies()`. That call waits for the socket to become readable before it calls `consume_input()`. If the server shuts down, it sends a FIN; if the peer's kernel crashes, it sends a RST. Either wakes the socket at once, `consume_input()` fails, psycopg raises `OperationalError`, and the connection is marked closed or broken.

A silent failure produces no packet at all. Examples are host power loss, a network partition, or a NAT, firewall or load balancer dropping its idle flow state. Without a timeout, `notifies()` then waits forever.

**Why default TCP settings don't help.** libpq enables keepalives by default but takes their timing from the OS. On Linux that is 7,200 s of idle time followed by 9 probes 75 s apart, so a dead peer is declared after about 2 hours 11 minutes.

There is a second trap. Once the client has written to a dead peer, for example by sending a heartbeat query, keepalives stop and TCP's retransmission rules take over. With Linux's `tcp_retries2 = 15`, Cloudflare measured a lower bound of 924.6 s and saw sockets error after about 15.7 minutes.

**Fixes at the transport layer.** psycopg passes these libpq parameters straight through:

- `keepalives_idle`, `keepalives_interval` and `keepalives_count` set how fast an idle dead connection is detected: idle + interval × count, for example 10 + 5 × 3 ≈ 25 s.
- `tcp_user_timeout` bounds how long sent data may stay unacknowledged. On Linux it also overrides keepalive timing.
- `connect_timeout` bounds each reconnect attempt.

Cloudflare advises setting `TCP_USER_TIMEOUT` to `TCP_KEEPIDLE + TCP_KEEPINTVL × TCP_KEEPCNT`. Several limits apply:

- **Platform support.** `TCP_USER_TIMEOUT` is a Linux socket option. Elsewhere, macOS for example, libpq's `tcp_user_timeout` "has no effect", and the server-side setting is unavailable on Windows.
- **Socket type.** libpq ignores all of these settings on Unix-domain sockets.
- **Middleboxes.** Keepalives only test the nearest TCP hop. Psycopg issue #1371 describes a proxy or load balancer that terminates TCP and keeps acknowledging: "keepalive probes are answered by the middlebox, so they detect nothing, and `tcp_user_timeout` never fires". An application-level check is needed even on Linux.

**Application heartbeats.** psycopg's documentation discourages polling `SELECT 1`, yet every listener implementation examined uses a heartbeat:

- **River** pings after 5 s without a notification, then reconnects with backoff and re-issues `LISTEN`. A past bug made the ping "a no-op that always returned context.Canceled", which shows that the heartbeat itself needs a test.
- **pg-listen** runs `SELECT pg_backend_pid()` every 30 s.
- **pg-boss** queries `pg_listening_channels()` every 10 s and tears the listener down if the query takes longer than 5 s. This also confirms that the subscription still exists.
- **Oban** pings on a dedicated `sonar` channel. Because it receives its own `NOTIFY`, this tests the whole delivery path, including the `LISTEN` registration.

psycopg 3.2 added `notifies(timeout=..., stop_after=...)`, so one connection can wake on schedule and run the heartbeat between iterations. Two later releases matter:

- **3.2.4:** notifications that arrive between `notifies()` calls are buffered and delivered by the next call. Before 3.2.4, notifications arriving between `LISTEN` and the start of the generator were lost.
- **3.2.10:** mixing `add_notify_handler()` with `notifies()` on one connection is deprecated. If a handler is registered, notifications are not captured while the generator is not running. Use one mechanism only.

**Putting a time limit on the heartbeat in asyncio.** `statement_timeout` is enforced by the server, so it cannot help when the server is unreachable. Wrapping the heartbeat in `asyncio.timeout` behaves differently depending on versions:

- **psycopg before 3.3.6: not reliable.** When the task is cancelled, psycopg sends a server cancel and then waits again for the query to finish, with no deadline. Issue #1371 reproduced tasks stuck indefinitely. Procrastinate workers hung for seven hours during failovers of a managed database. A second `task.cancel()` from an outer watchdog does break out.
- **psycopg 3.3.6 or newer with libpq 17 or newer (released 18 September 2026): bounded.** The cancel and the second wait are each limited to 5 s. The timeout fires up to about 10 s after its deadline, and the connection is left closed and marked broken.
- **libpq older than 17: unbounded.** The cancel falls back to the blocking `PQcancel` on a worker thread, and nothing limits it.

`close()` does not block on a half-open socket, because psycopg runs libpq in nonblocking mode. The `async with` exit path is different: if the connection is inside a transaction, it attempts a `ROLLBACK` round trip. That is one more reason to keep listener connections in autocommit and close them explicitly. Waiting in `notifies()` is not an active query, so a cancellation there propagates at once, but the connection's health is then unknown. Keep the listening connection out of any pool. psycopg_pool has needed cancellation fixes (#1401), and a residual race has been reported (#1345).

**Server-side settings.**

- `idle_session_timeout` ends a listener blocked in `notifies()`, because such a session sits idle outside a transaction. Keep the heartbeat interval below the limit, or exempt the listener's role.
- Server-side `tcp_keepalives_*` and `tcp_user_timeout` let the server clean up after clients that vanish. Without them the server notices a lost connection "only at the next interaction with the socket". AWS RDS defaults to 300 s, 30 s and 2 probes.
- `client_connection_check_interval` applies only "while running queries", so it does nothing for an idle listener.

### Commit-time delivery, ordering and duplicate collapsing

**Sending is transactional.** Notifications "are not delivered until and unless the transaction is committed", and an aborted transaction delivers none. In `async.c`, `PreCommit_Notify` writes the transaction's entries into the queue before the commit is recorded in clog. Readers stop at entries from transactions still in progress, deliver committed entries, and skip aborted ones.

**A transaction on the receiving side delays delivery.** A listening session that is inside a transaction sees nothing until "just after the transaction is completed (either committed or aborted)". At the protocol level, `NotificationResponse` is currently sent only outside a transaction block, although clients must be ready to receive it at any time. So a consumer connection sitting idle in a transaction receives nothing until that transaction ends. In psycopg's default non-autocommit mode, a heartbeat `SELECT` opens exactly such a transaction. This is why psycopg recommends autocommit for both sending and receiving connections.

**Ordering.** Apart from dropped duplicates, notifications from one transaction arrive in the order they were sent. Notifications from different transactions arrive in commit order. This follows from the single global queue, whose entries "appear in commit order" because the commit lock (described below) serialises writers.

**Collapsing within a transaction.** "If the same channel name is signaled multiple times with identical payload strings within the same transaction, only one instance of the notification event is delivered to listeners." Notifications with different payloads are always delivered separately.

- This has been a guarantee since PostgreSQL 13. The PostgreSQL 12 docs said only that the server "can decide to deliver a single notification only".
- Once a transaction has 16 or more pending notifications (`MIN_HASHABLE_NOTIFIES`), the implementation uses a hash table to find duplicates.
- Duplicates are also removed when a subtransaction's notifications merge into its parent.

**No collapsing across transactions.** Notifications from different transactions "will never get folded into one notification". A listener facing R committing appends per second therefore receives about R notifications per second, and coalescing across appends is the application's job. The source comment advises applications that need every notification delivered to put a unique string in the payload. Graphile Worker's `add_job` payload includes `random()`. One likely effect is that its count-carrying payloads never collapse, but that is an inference, not a documented reason.

**What commit-time delivery does not guarantee for an event store.** A notification means the sender's transaction has committed. It does not mean the consumer's next read will see the events.

- Positions drawn from a sequence are fixed before commit, so transactions can commit out of position order.
- Readers guard against this, for example by reading only rows whose stored `xid8` is below `pg_snapshot_xmin(pg_current_snapshot())`. Any older in-progress transaction blocks such a guard.
- If that older transaction is unrelated to the event log, it sends nothing on the event channel when it ends. The newer events then become readable only at the next fallback poll.

### Payload and channel-name limits

- **Payload.** "In the default configuration it must be shorter than 8000 bytes."
  - The limit is `NOTIFY_PAYLOAD_MAX_LENGTH = BLCKSZ − NAMEDATALEN − 128`, which is 8192 − 64 − 128 = 8000, so at most 7,999 bytes.
  - It is sized so each entry fits on one queue page. Only a rebuild with a different block size changes it.
  - An oversized payload raises `payload string too long` when `NOTIFY` or `pg_notify()` is called, which aborts the surrounding transaction.
- **Channel name.** It must not be empty (`channel name cannot be empty`) and must be shorter than `NAMEDATALEN` (`channel name too long`), so at most 63 bytes by default.
- **Consequence for design.** A payload is no more durable than its notification. So the usual design sends an identifier or position (a stream name or sequence number), or no payload at all, rather than the event body. Small entries also make the queue far slower to fill (see below).

### The notification queue: size, stalls and failure when full

**Structure and size.** All databases in the cluster share one disk-backed queue under `pg_notify/`, cached through SLRU buffers. Each listening backend keeps its own read position. The queue can only be truncated up to "the minimum of all the per-backend tail pointers". One backend that stops advancing therefore stops truncation for the whole cluster, even while healthy listeners keep receiving.

The docs call the queue "quite large (8GB in a standard installation)". From PostgreSQL 17, two settings control it:

- `max_notify_queue_pages` sets the size: 1,048,576 pages of 8 kB by default, which is 8 GB. Changing it requires a restart.
- `notify_buffers` sizes the shared-memory cache: 16 blocks by default.

PostgreSQL 16 has neither setting, and its limit is fixed.

**First cause of stalls: a listener in a long transaction.** "No cleanup can take place if a session executes `LISTEN` and then enters a transaction for a very long time."

**Second cause: a listener whose client has vanished.** What happens depends on whether the orphaned backend has anything to deliver.

- **It has nothing to deliver.** The backend is not listening on any channel being notified. It is woken, or in PostgreSQL 19 advanced directly without waking, finds nothing for its channels, and moves forward without writing to the socket. It does not hold the queue back. TCP keepalives eventually end its idle connection.
- **It has notifications to deliver.** This is the normal case for an event-store consumer, and it goes through two phases.
  1. *Silent loss.* The backend sends a `NotificationResponse` for each matching entry, but writes its shared queue position back only once, at the end of each pass. The first notifications fill the backend's 8 kB send buffer and the kernel send buffer, and are lost into the dead connection.
  2. *Pinned.* Once those buffers are full, `secure_write()` waits for the socket to become writeable, with no timeout, in the `ClientWrite` wait event. The shared position stays where that pass began, so the backend pins the queue tail.

  Reading the source further suggests that the blocked backend also holds a registered snapshot. If so, it shows a non-null `backend_xmin` and holds back VACUUM. This is an inference, not documented behaviour.

**What ends a pinned backend.**

- While blocked, the backend acts only on termination requests, so `pg_terminate_backend()` frees it.
- `idle_session_timeout` would not be expected to free it (an inference), and `client_connection_check_interval` does not apply.
- Keepalives do not run either, because data is unacknowledged. The kernel's retransmission timeout ends the connection instead: about 15–16 minutes on default Linux settings, or sooner if the server's `tcp_user_timeout` is set.
- Since commit 1b46990 (back-patched to 14), any error while delivering notifications is FATAL. The backend exits rather than silently skipping notifications.

**Can one vanished listener fill the queue?** Each entry takes a 16-byte header plus the channel name, the payload and two terminating NUL bytes, rounded up to an `int` boundary. Entries never span pages. The table estimates what it takes to fill the default 8 GB queue within the roughly 925 s default retransmission window:

| Notification shape | Approx. entry size | Entries to fill 8 GB | Sustained rate needed over ~925 s |
|---|---|---|---|
| No payload, short channel | 24–40 bytes | ~210–360 million | ~230,000–390,000 per second |
| ~100-byte payload | ~128 bytes | ~67 million | ~72,000 per second |
| Near the 8,000-byte maximum | one per page | ~1.05 million | ~1,100 per second (about 580 per second over 30 minutes) |

With small payloads on default Linux settings, a single vanished listener realistically cannot fill the queue before the kernel ends its connection. With large payloads on a busy system, it can. The risk also grows in three situations:

- `tcp_retries2` has been raised.
- The platform lacks `TCP_USER_TIMEOUT`.
- The client is alive but has stopped reading, for example because its event loop has stalled. This is the most dangerous case. The client advertises a zero TCP window and answers the server's window probes, and the server "will keep on sending such probes forever", so the queue could be pinned indefinitely. The sources do not settle whether `tcp_user_timeout` reliably ends such a connection.

**Warnings and monitoring.** From 50% full, the server logs `NOTIFY queue is N% full` at most every 5 s. The log names the PID of a process "among those with the oldest transactions" and says the queue "cannot be emptied until that process ends its current transaction". The wording assumes a long transaction, but the PID it names is also the right one for a backend stuck in `ClientWrite`. `pg_notification_queue_usage()` returns the fraction of the queue in use, from 0 to 1.

**When the queue is full.** "Transactions calling `NOTIFY` will fail at commit." `PreCommit_Notify` raises `too many notifications in the NOTIFY queue` before the commit is recorded, so the whole transaction aborts, data included. An event store might issue `NOTIFY` from a trigger or in the same transaction as its appends. In that case one stuck listener anywhere in the cluster can stop every append.

**A related bug in older minor releases.** VACUUM could truncate commit-status (clog) data that the queue still referenced. Lagging listeners then failed with `could not access status of transaction NNN`, even when issuing `LISTEN` on a quiet system. Commit 8eeb4a0 (November 2025) makes VACUUM freeze the transaction IDs held in queue entries. It was back-patched to 14–18, and a follow-up commit closed a remaining race. Long-lived listeners should run a recent minor release.

### The global lock taken at commit

**What it is.** In `PreCommit_Notify`, any transaction with pending notifications calls `LockSharedObject(DatabaseRelationId, InvalidOid, 0, AccessExclusiveLock)`. This is a heavyweight lock on "database 0". It is visible in `pg_locks`, and in lock-wait logs as `AccessExclusiveLock on object 0 of class 1262 of database 0`. The source comment explains why:

> Serialize writers by acquiring a special lock that we hold till after commit. This ensures that queue entries appear in commit order, and in particular that there are never uncommitted queue entries ahead of committed ones, so an uncommitted transaction can't block delivery of deliverable notifications.

Tom Lane confirmed that when notifications are in use, only one notifying transaction can commit at a time. He chose a heavyweight lock so that it is released automatically on commit or abort. Transactions that do not issue `NOTIFY` never take it.

**What it is held across.** `PreCommit_Notify` runs late in pre-commit, "to minimize time spent holding the notify-insertion lock". Commit then continues in this order:

1. `PreCommit_Notify()` takes the lock and appends the transaction's notifications to the queue.
2. `RecordTransactionCommit()` writes the commit record and, under synchronous commit, flushes WAL.
3. `ProcArrayEndTransaction()` runs.

So every notifying transaction holds the lock through its own WAL flush, and notifying commits cannot share a group-commit flush. A 2013 report found the lock held noticeably longer when `synchronous_commit` waited for a remote synchronous standby. The sources disagree on whether listener signalling (`AtCommit_Notify`, which calls `SignalBackends`) runs before or after the lock is released. See "Open points and unreconciled evidence".

**Scope.** The lock covers the whole cluster, not one database. A 2019 pgsql-general report described PostgreSQL 9.4 with about 100 databases and 40–100 NOTIFYs per second. Commits in one database were blocked by commits in another, with up to 500 waiters and stalls of 1–1.5 s.

**Measured cost.** Every measurement below predates PostgreSQL 19 or used prototypes, and most do not state the hardware or the `synchronous_commit` setting.

| Concurrency | Workload | With the lock (master) | Comparison | Source |
|---|---|---|---|---|
| 8 connections (M2 laptop) | NOTIFY only, no listener | ~158k–190k TPS | ~299k–354k TPS with the lock skipped (~2x) | Bagga, pgsql-hackers, July 2025 |
| 8 connections (M3 Max) | LISTEN + NOTIFY | ~73k TPS | ~83k TPS with the lock skipped (~14%); `kill()` signalling dominated | Jacobson, via hackorum.dev |
| 4, then 16–128 connections (M3 Max) | NOTIFY | Peak of 177,351 TPS at 4; 35k–41k TPS from 16 to 128 | — | Jacobson, pgsql-hackers, July 2025 |
| 100 connections | INSERT + NOTIFY in one transaction | 817.9 TPS | 30,981.6 TPS for INSERT alone (~38x) | Mukhin, pgsql-hackers, October 2025 |
| 100 connections | INSERT + NOTIFY in one transaction | ~29k–31k TPS | ~63k–66k TPS with the WAL-transport prototype (~2x) | Bagga, pgsql-hackers, September 2025 |
| 1000 connections, 1 listener | NOTIFY | 7,679 TPS | 95,430 TPS with the lock removed and commit order lost (~12x) | Jacobson, pgsql-hackers, July 2025 |
| Not published | NOTIFY from a trigger on every write | ~2.9K writes/s | ~60K writes/s with batched notifications (~20x) | DBOS |
| Tens of thousands of writers (production) | Write path issuing NOTIFY | Outage: 300+ queued waiters, ~1 s waits, CPU and I/O falling | Notification logic moved out of the database | Recall.ai |

In Mukhin's benchmark, NOTIFY-only transactions ran at 87,750 TPS. `RecordTransactionCommit()` flushes synchronously only when the transaction has already written WAL (`wrote_xlog`). A notify-only transaction writes none before its commit record, so it should commit asynchronously and hold the lock only briefly. That is a reading of the code. It fits the measurement, but no source confirms it directly. It is the basis for moving `NOTIFY` out of the append transaction.

The pattern fits the mechanism. Notifying commits are capped at roughly one per (flush latency + work done under the lock), however many clients there are. Insert-only throughput keeps rising with concurrency because group commit shares flushes. The relative loss therefore grows with both concurrency and flush latency. It is modest where flushes are nearly free, as on laptops. On durable storage under high concurrency it reaches one to two orders of magnitude, and Bagga argued that cloud deployments with remote storage make it worse. This framing is inferred from the code and the measurements; no source states it as a formula.

Separately, the number of idle listeners also matters. In a two-connection ping-pong benchmark, master fell from 9,126 TPS to 6,233, 2,020 and 238 TPS with 10, 100 and 1000 extra listeners. Early versions of the channel-map patch held about 8,900–9,350 TPS throughout.

**Upstream status.**

- **PostgreSQL 19: commit 282b1cd** (Joel Jacobson, committed by Tom Lane on 15 January 2026).
  - *What it changes.* A shared hash map records which backends listen on which channel. A notifier signals only interested listeners and advances uninterested idle ones directly. The release notes describe it as "Improve NOTIFY to only wake up backends that are listening to specified notifications."
  - *Claimed benefit.* The commit message claims "very significant improvements (integer multiples) ... when there are many listeners but only a few are interested in any specific message". It also says there is "no improvement for the simplest case where every listener reads every message".
  - *The lock stays.* The direct-advance logic depends on it: "Because all writers serialize on a cluster-wide heavyweight lock, no other backend can insert entries between these two points." DBOS's author likewise notes that the change "does not remove the global lock".
  - *Release status.* Recall.ai's May 2026 update nonetheless credits this commit with removing their bottleneck. PostgreSQL 19 was in beta in September 2026.
- **An out-of-order NOTIFY setting** was rejected. Tom Lane objected that it breaks commit-order delivery and that writers would still serialise on `NotifyQueueLock`.
- **"Write Notifications Through WAL"** (Rishu Bagga, September–November 2025).
  - *Design.* Payloads go into WAL, and the lock is held only while a small fixed-size queue entry is written. This follows Tom Lane's suggestion that the lock "only needs to be held for the duration of step 3".
  - *Results.* It roughly doubled INSERT + NOTIFY throughput at 100 clients, and in Mukhin's test it restored about 34k TPS against 818 on master. It also made NOTIFY-only transactions about 2.5x slower.
  - *Status.* Reviewers found assertion failures inside a critical section. The patch is uncommitted, listed as "Needs review" in the closed PG19-4 commitfest, and not part of PostgreSQL 19.

A comment in `async.c` anticipates that the lock could one day be replaced by holding `NotifyQueueLock` exclusively across insertions.

On PostgreSQL 19, then, INSERT + NOTIFY transactions still serialise their commits on one lock held across the WAL flush, and insert-only transactions do not. No published benchmark compares the two on PostgreSQL 19 code at 10, 100 and 1000 writers. The evidence points to:

- **10 writers:** a modest gap that depends on flush latency.
- **100 writers:** about half the throughput lost, or more.
- **1000 writers:** an order of magnitude or worse.

Where every consumer listens on the same channel, the commit message itself predicts no gain.

### Designing around these properties

#### Notifications as hints over a durable table, with a polling floor

All four job libraries treat `NOTIFY` as a hint. The payload carries no work, and a woken consumer runs the same fetch it would run on a timer. pg-boss's notifier source says so directly: "A NOTIFY is only ever a latency hint: it wakes workers so they run their normal locking fetch sooner than the polling interval." Its docs call `LISTEN`/`NOTIFY` "an opt-in optimization on top of polling, not a replacement for it." The PostgreSQL docs frame `NOTIFY` the same way: "I changed this table, take a look at it to see what's new". Used as a hint, commit-time delivery and within-transaction collapsing cause no harm.

| Concern | Oban | River | Graphile Worker | pg-boss |
|---|---|---|---|---|
| Where `NOTIFY` is sent | Application code at insert, runnable jobs only | Insert transaction, runnable jobs only, rate-limited per queue | `add_job`/`add_jobs` SQL, with a count plus a random value in the payload | Insert transaction, runnable jobs only |
| Receiver coalescing | 5 ms dispatch cooldown | 100 ms fetch cooldown | Nudges idle workers only; otherwise sets a re-fetch flag | `beenNotified` flag |
| Fallback | Leader re-announces queues with available work every 1 s; each node polls every second when isolated | Each producer polls every 1 s (jittered), always on | Each idle worker polls every 2 s | Each worker polls every 30 s while the listener is healthy, otherwise every 2 s |
| Health detection | Sonar pings classify the node as isolated, solitary or clustered | Backoff reconnect, healthy/unhealthy logging, poll-only mode | Backoff reconnect | TCP keepalive plus a `pg_listening_channels()` heartbeat with a timeout |
| Catch-up after reconnect | Implicit, through the 1 s re-announcement | Implicit, through the 1 s poll | Implicit, through polling (no nudge on reconnect) | Explicit forced fetch once `LISTEN` succeeds again |

The libraries put the polling floor in different places. River, Graphile Worker and pg-boss poll from every consumer. Oban runs one query per second on an elected leader and fans the results out as hints over the notification channel itself. Only pg-boss lengthens its backstop while the listener is known to be healthy.

**Leader-based re-announcement and its failover gaps.** Oban and River both elect a leader through a lease row in a small `UNLOGGED` table. A node becomes leader when its insert or conditional update survives, and it keeps the lease by renewing before expiry. Neither library uses advisory locks.

| | Oban | River |
|---|---|---|
| Lease length | 30 s | 15 s |
| Renewal interval | 15 s | 5 s |
| Clock that judges expiry | Application node | Database |
| Approximate crash failover (derived from defaults) | ~60 s | ~21 s |

- **River's TTL.** River's documentation states a five-second TTL. In the current code, 5 s is the renewal interval and the TTL is 15 s.
- **Stepping down early.** River's leader also keeps a local trust window shorter than the lease, so an old leader steps down before the database lease expires.
- **Clean shutdown.** Both libraries delete the row and send a `NOTIFY` in the same transaction, which cuts the leadership gap to milliseconds.

The two leaders announce different things. Oban's re-announces every queue that still has available work, so it also recovers consumers that missed a notification. River's scheduler announces only newly due work, so River relies on per-consumer polling. Both keep a path that does not depend on `NOTIFY`, because the leader's announcements travel over the same channel that may be failing. Oban's Sonar can take up to about two minutes to notice that notifications have stopped.

#### Taking `NOTIFY` out of the append transaction

Sending `NOTIFY` in the append transaction gives atomic "notify if and only if committed" semantics. With few concurrent commits this probably costs little, because commits are already serialised on their own flushes; no source measured that case with data writes. With tens to thousands of concurrent appenders, it caps appends near the rate of one flush at a time.

The alternative is to send `NOTIFY` after commit, from a separate transaction. DBOS buffered notifications in memory and flushed them periodically in one batch transaction. Throughput rose from about 2.9K to about 60K writes per second, at a cost of 15–100 ms in added latency. A crash loses buffered notifications, so readers also need a low-frequency fallback poll.

For a psycopg 3 event store, the evidence supports this design:

- **Connection.** Use a dedicated notifier connection with `autocommit=True`.
- **Trigger.** Keep a pending flag, and set it only after an append has committed.
- **Firing pattern.** If no window is open, send at once and open one. If a window is open, mark it pending and send once when it closes. This leading-and-trailing pattern is what River's `DebouncedChan` implements. The simpler alternatives fail:
  - A pure trailing debounce adds the full window to every wake-up and never fires under continuous load.
  - A pure leading throttle leaves the last events of a burst waiting for the fallback poll.
- **Payload and channels.** Send a payloadless `NOTIFY`; PGMQ passes `NULL`. If several channels need signalling, signal them in one transaction so the lock is taken once per flush. Oban, for example, sends many payloads in one `SELECT pg_notify(...) FROM json_array_elements_text(...)` statement.
- **Window size.** Default to about 50 ms. Use 5–10 ms for latency-sensitive single-process deployments, and 100–250 ms where many consumers or processes share the database.

If notify-only commits really skip the WAL flush, the window hardly affects append throughput. It sets two other things:

- **Consumer read load.** Every flush wakes every listener. With 20 consumers, reads are capped at 2,000, 400, 200 or 80 per second for windows of 10, 50, 100 or 250 ms.
- **Latency during bursts.** At most one window, about half a window on average, and nothing extra for the first append after a quiet spell.

Avoid PGMQ-style throttles that update a shared row inside every append transaction. By ordinary row-locking rules, that row stays locked until commit. Choose the window by measurement: sweep 0–250 ms while recording append TPS, p50 and p99 commit-to-handler latency, and consumer reads per second.

#### Consumer-side coalescing, backstop polling and reconnection

A checkpoint consumer reads "events after my checkpoint, up to a safe horizon, in pages". That read is cumulative, so ten wake-ups carry no more information than one, and aggressive coalescing is safe:

- **Throttle wake-ups.** Use the same leading-and-trailing throttle on the consumer, with a cooldown C between 5 ms (Oban) and 100 ms (River). While a read runs, drain buffered notifications into a single dirty flag using a near-zero `notifies()` timeout. Clear the flag *before* reading, so that a notification arriving mid-read causes exactly one more read.
- **Drain backlogs directly.** While pages come back full, read the next page at once. The cooldown should pace only reads triggered by notifications.
- **Keep a backstop poll.** About 1 s with about 10% jitter, matching River's default. Reset it after any read, so it never fires under steady traffic. Size it for the degraded case: lost notifications, reconnects, and safe-horizon stalls caused by unrelated transactions that will never notify. Marten's high-water agent faces the same problem. It polls every 250 ms when busy and every 1 s when quiet, and treats a gap as stale after 3 s.
- **Share one listener per process.** One `LISTEN` connection per process can signal each consumer's throttle, instead of one connection per consumer. The Elixir EventStore takes the opposite, edge-triggered approach: one `NOTIFY` per batch, carrying the stream id and version range, one read per notification, and a broadcast to all subscribers.
- **Expect re-reads after a restart.** How often the checkpoint is persisted decides how much is re-read after a crash; the Elixir EventStore exposes this as `checkpoint_threshold` and `checkpoint_after`. Handlers must therefore be idempotent.

These mechanisms give a simple latency model. Here d is the notification transit time, t_r the read time, and P the backstop interval with jitter fraction j:

- **Normal operation:** latency is at most d + C + t_r.
- **Degraded operation** (lost notification, reconnect or horizon stall): at most P(1 + j) + t_r, plus however long the horizon stays held back.
- **Read rate per consumer:** at most about 1/C under load, and about 1/P when idle.

**Reconnecting.** PostgreSQL's documented order, combined with library practice, gives this procedure:

1. Treat any `OperationalError` from `notifies()` or the heartbeat, or `conn.broken` being true, as fatal. Close the connection and never reuse it.
2. Reconnect with backoff, with `connect_timeout`, keepalives and (on Linux) `tcp_user_timeout` set. Illustrative values that follow Cloudflare's formula are 10, 5 and 3 for the keepalive settings with a 25 s user timeout.
3. Issue `LISTEN` on the new autocommit connection before anything else.
4. Only then read from the durable checkpoint, in a new transaction. pg-boss's forced fetch after `LISTEN` succeeds again is the model.
5. Resume `notifies(timeout=heartbeat)` and treat each notification as "read from the checkpoint". When the timeout expires, run the heartbeat and then a catch-up read; that read doubles as the fallback poll.

Follow the same order at cold start. One gap remains: "read after checkpoint" is gap-free only if log positions become visible in order. Sequence-assigned positions can commit out of order, and this procedure does not solve that.

### Logical decoding as an alternative channel

A logical replication slot avoids most of `NOTIFY`'s failure modes, at a different operational price.

**What a slot provides.** A slot is a server-side cursor over WAL that survives crashes. "Concurrent transactions are decoded in commit order", rolled-back transactions "never get decoded", and a consumer resumes from its confirmed position after a disconnect. Writers do nothing extra at commit. By inference from how decoding works (not from a benchmark), the serialisation point therefore leaves the write path. The cost is the extra WAL that `wal_level = logical` produces.

Other properties:

- **At-least-once delivery.** A slot "will emit each change just once in normal operation". But its position "is persisted only at checkpoint", so after a crash it "might return to an earlier LSN", and clients must deduplicate.
- **One reader.** Only one receiver can read a slot at a time.
- **No history.** A slot covers only changes made after it was created.
- **Bursts.** Large transactions arrive in one burst at commit unless the plugin streams them.
- **Slow creation.** Creating a slot waits for transactions already running to finish.
- **Custom messages.** `pg_logical_emit_message()` can add in-transaction doorbell or heartbeat messages to the stream.

**Operational risks.**

- **WAL retention.** Slots "will prevent removal of required resources even when there is no connection using them". `max_slot_wal_keep_size` is unlimited (-1) by default, so an abandoned slot can fill the disk.
- **Catalog vacuum.** A slot's `catalog_xmin` holds back catalog vacuum, "in extreme cases" risking a wraparound shutdown.
- **Quiet streams.** An idle stream on a busy server still pins WAL unless the consumer confirms progress or sends heartbeats.
- **Invalidation.** The safety valves, `max_slot_wal_keep_size` and (in PostgreSQL 18) `idle_replication_slot_timeout`, invalidate a lagging slot. It must then be dropped, recreated and backfilled from the table.
- **Failover.** Slots survive failover only from PostgreSQL 17, and only with `failover = true`, a physical slot, `hot_standby_feedback` and `sync_replication_slots`.
- **Configuration.**
  - Changing `wal_level` requires a restart up to PostgreSQL 18; PostgreSQL 19 switches to logical automatically when the first logical slot is created.
  - `max_replication_slots` and `max_wal_senders` both default to 10.
  - The role needs the `REPLICATION` attribute and a `pg_hba.conf` entry. On RDS, a static parameter that requires a reboot is also needed.

**Python client support.** psycopg 3 has no replication API. Issue #71 has been open since August 2021. psycopg 3.3 only lets `START_REPLICATION` return a `COPY_BOTH` cursor, after which "the connection will be broken for further usage", and a proof-of-concept PR was closed in May 2026. The options:

- **psycopg2's `LogicalReplicationConnection`.** The consumer calls `send_feedback(flush_lsn=...)` after committing its own work. This is the only route that offers consumer-driven acknowledgement without custom protocol code. psycopg2 advances an idle slot only once every received message has been confirmed.
- **`pg_recvlogical` piped to the consumer.** It confirms positions once bytes reach the pipe, without an fsync. A consumer crash therefore loses events in flight, unless the output goes to a regular file that it fsyncs, or the events table is the recovery source.
- **Sidecars** such as Debezium Server, Sequin or pgstream. They confirm once their own sink or offset store has committed.
- **A hand-written loop** over psycopg 3's `PGconn.get_copy_data` and `put_copy_data`. This is possible but unsupported.

**Checkpointing against a slot.** Store the *end* LSN of the last fully processed transaction, which is what PostgreSQL's own apply worker records. The decoder skips a transaction only when its commit record *starts* before the resume point. Storing the commit LSN, or any change LSN, therefore resends the whole transaction. Commit the checkpoint atomically with the side effects, and send `flush_lsn` only afterwards. Before starting, compare the slot's `confirmed_flush_lsn` with the stored LSN. If the slot is ahead, events have been lost.

Positions from an unserialised sequence arrive out of numeric order, so "highest position seen" fails both as a table cursor and as a duplicate filter. Two designs are consistent:

- **Serialise position assignment until commit**, with an advisory lock or a counter row. Position order then equals commit order, and the consumer stores `(position, end_lsn)` together.
- **Keep the LSN as the only cursor.** Hand over between table and slot only at the `consistent_point` of a slot created with an exported snapshot.

A slot can also serve purely as a doorbell. A temporary slot is released on any error, so a dead consumer cannot pin WAL. Decoded commits then only wake the consumer, which reads the events table from its own checkpoint. This works like `NOTIFY` but without the commit lock.

### Open points and unreconciled evidence

- **How large the same-transaction penalty is at 100 writers.** For INSERT + NOTIFY on master, Mukhin measured 817.9 TPS and Bagga about 29k–31k TPS on the same workload shape. Neither gave hardware or `synchronous_commit` settings, so the thread cannot reconcile the two. The loss evidently depends heavily on flush latency.
- **Whether listener signalling happens under the lock.** Two readings of `xact.c` place `AtCommit_Notify` (which signals listeners) before locks are released, and describe PostgreSQL 19's reduced signalling as shortening how long the lock is held. A third reading places the lock release before `AtCommit_Notify`. All three agree that the lock spans the commit record and its WAL flush. Jacobson found that the bottleneck is "either the kill() syscalls *or* the heavyweight lock", which is consistent with signalling happening under the lock.
- **Whether notify-only transactions skip the WAL flush while holding the lock.** This is inferred from `RecordTransactionCommit()` and fits Mukhin's 87,750 TPS notify-only figure. No source measured lock hold time directly.
- **No PostgreSQL 19 benchmark.** No published benchmark compares INSERT + NOTIFY with insert-only on PostgreSQL 19 code.
- **Recall.ai versus DBOS on PostgreSQL 19.** Recall.ai credits the PostgreSQL 19 change with removing their bottleneck. DBOS and the source code say the global lock remains. Both can be true if Recall.ai's workload had many uninterested listeners, but the sources do not say.
- **Clients that stop reading.** It is not settled whether `tcp_user_timeout` reliably ends a connection whose client is alive but no longer reading. If it does not, such a client could pin the queue indefinitely.
- **Older psycopg stacks.** With psycopg before 3.3.6 or libpq before 17, it is unverified whether closing a stuck connection from a separate watchdog task reliably wakes a coroutine blocked on a half-open socket.

## Sources

- [PostgreSQL Documentation: NOTIFY (current, 18)](https://www.postgresql.org/docs/current/sql-notify.html) — tier-1 — postgresql.org
- [PostgreSQL Documentation: LISTEN (current, 18)](https://www.postgresql.org/docs/current/sql-listen.html) — tier-1 — postgresql.org
- [PostgreSQL 12 Documentation: NOTIFY](https://www.postgresql.org/docs/12/sql-notify.html) — tier-1 — postgresql.org
- [PostgreSQL 13 Documentation: NOTIFY](https://www.postgresql.org/docs/13/sql-notify.html) — tier-1 — postgresql.org
- [NOTIFY reference (devel documentation)](https://www.postgresql.org/docs/devel/sql-notify.html) — tier-1 — postgresql.org (official documentation)
- [PostgreSQL Documentation: Resource Consumption (current, 18)](https://www.postgresql.org/docs/current/runtime-config-resource.html) — tier-1 — postgresql.org
- [PostgreSQL 17 Documentation: Resource Consumption](https://www.postgresql.org/docs/17/runtime-config-resource.html) — tier-1 — postgresql.org
- [PostgreSQL 16 Documentation: Resource Consumption](https://www.postgresql.org/docs/16/runtime-config-resource.html) — tier-1 — postgresql.org
- [PostgreSQL 17.0 Release Notes](https://www.postgresql.org/docs/release/17.0/) — tier-1 — postgresql.org
- [PostgreSQL 18 release notes](https://www.postgresql.org/docs/release/18.0/) — tier-1 — postgresql.org
- [PostgreSQL 19 release notes](https://www.postgresql.org/docs/devel/release-19.html) — tier-1 — postgresql.org (official documentation)
- [PostgreSQL 19 release notes (beta)](https://www.postgresql.org/docs/release/19.0/) — tier-1 — postgresql.org
- [PostgreSQL Documentation: System Information Functions](https://www.postgresql.org/docs/current/functions-info.html) — tier-1 — postgresql.org
- [PostgreSQL Documentation: Hot Standby](https://www.postgresql.org/docs/current/hot-standby.html) — tier-1 — postgresql.org
- [PostgreSQL Documentation: Message Flow (Asynchronous Operations)](https://www.postgresql.org/docs/current/protocol-flow.html) — tier-1 — postgresql.org
- [PostgreSQL Documentation: libpq Asynchronous Notification](https://www.postgresql.org/docs/current/libpq-notify.html) — tier-1 — postgresql.org
- [libpq Database Connection Control Functions — parameter key words](https://www.postgresql.org/docs/current/libpq-connect.html) — tier-1 — postgresql.org
- [Connections and Authentication — TCP settings and client_connection_check_interval](https://www.postgresql.org/docs/current/runtime-config-connection.html) — tier-1 — postgresql.org
- [Client Connection Defaults — idle_session_timeout](https://www.postgresql.org/docs/current/runtime-config-client.html) — tier-1 — postgresql.org
- [PostgreSQL documentation: The Cumulative Statistics System (ClientWrite wait event, pg_stat_activity)](https://www.postgresql.org/docs/current/monitoring-stats.html) — tier-1 — postgresql.org
- [PostgreSQL documentation: Asynchronous Commit](https://www.postgresql.org/docs/current/wal-async-commit.html) — tier-1 — postgresql.org
- [PostgreSQL docs: libpq Canceling Queries in Progress (current)](https://www.postgresql.org/docs/current/libpq-cancel.html) — tier-1 — postgresql.org
- [PostgreSQL 16 docs: libpq Canceling Queries in Progress](https://www.postgresql.org/docs/16/libpq-cancel.html) — tier-1 — postgresql.org
- [PostgreSQL documentation — CREATE TABLE (UNLOGGED)](https://www.postgresql.org/docs/current/sql-createtable.html) — tier-1 — postgresql.org (official PostgreSQL docs)
- [PostgreSQL documentation: Logical Decoding Concepts](https://www.postgresql.org/docs/current/logicaldecoding-explanation.html) — tier-1 — postgresql.org
- [PostgreSQL documentation: Log-Shipping Standby Servers (Replication Slots)](https://www.postgresql.org/docs/current/warm-standby.html) — tier-1 — postgresql.org
- [PostgreSQL documentation: Replication configuration parameters](https://www.postgresql.org/docs/current/runtime-config-replication.html) — tier-1 — postgresql.org
- [PostgreSQL documentation: pg_replication_slots](https://www.postgresql.org/docs/current/view-pg-replication-slots.html) — tier-1 — postgresql.org
- [PostgreSQL documentation: Logical Decoding Output Plugins](https://www.postgresql.org/docs/current/logicaldecoding-output-plugin.html) — tier-1 — postgresql.org
- [PostgreSQL documentation: Streaming of Large Transactions for Logical Decoding](https://www.postgresql.org/docs/current/logicaldecoding-streaming.html) — tier-1 — postgresql.org
- [PostgreSQL documentation: System Administration Functions (replication management)](https://www.postgresql.org/docs/current/functions-admin.html) — tier-1 — postgresql.org
- [PostgreSQL documentation: Write Ahead Log configuration (wal_level)](https://www.postgresql.org/docs/current/runtime-config-wal.html) — tier-1 — postgresql.org
- [PostgreSQL devel documentation: Write Ahead Log configuration (effective WAL level)](https://www.postgresql.org/docs/devel/runtime-config-wal.html) — tier-1 — postgresql.org
- [PostgreSQL documentation: Streaming Replication Protocol](https://www.postgresql.org/docs/current/protocol-replication.html) — tier-1 — postgresql.org
- [PostgreSQL documentation: Logical Streaming Replication Protocol (pgoutput parameters)](https://www.postgresql.org/docs/current/protocol-logical-replication.html) — tier-1 — postgresql.org
- [PostgreSQL documentation: Logical Replication Message Formats](https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html) — tier-1 — postgresql.org
- [PostgreSQL documentation: Replication Progress Tracking](https://www.postgresql.org/docs/current/replication-origins.html) — tier-1 — postgresql.org
- [PostgreSQL documentation: Synchronous Replication Support for Logical Decoding](https://www.postgresql.org/docs/current/logicaldecoding-synchronous.html) — tier-1 — postgresql.org
- [pg_recvlogical documentation](https://www.postgresql.org/docs/current/app-pgrecvlogical.html) — tier-1 — postgresql.org
- [PostgreSQL source: src/backend/commands/async.c (master)](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/commands/async.c) — tier-1 — raw.githubusercontent.com (postgres/postgres repository)
- [async.c on REL_19_STABLE](https://raw.githubusercontent.com/postgres/postgres/REL_19_STABLE/src/backend/commands/async.c) — tier-1 — raw.githubusercontent.com/postgres/postgres (official PostgreSQL source mirror)
- [PostgreSQL source: src/backend/commands/async.c (REL_18_STABLE)](https://raw.githubusercontent.com/postgres/postgres/REL_18_STABLE/src/backend/commands/async.c) — tier-1 — github.com/postgres/postgres
- [xact.c (master)](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/access/transam/xact.c) — tier-1 — github.com/postgres/postgres (official source mirror)
- [xact.c on REL_19_STABLE](https://raw.githubusercontent.com/postgres/postgres/REL_19_STABLE/src/backend/access/transam/xact.c) — tier-1 — raw.githubusercontent.com/postgres/postgres (official PostgreSQL source mirror)
- [PostgreSQL source: src/backend/libpq/be-secure.c (secure_write)](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/libpq/be-secure.c) — tier-1 — github.com/postgres/postgres
- [PostgreSQL source: src/backend/tcop/postgres.c (ProcessClientWriteInterrupt)](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/tcop/postgres.c) — tier-1 — github.com/postgres/postgres
- [PostgreSQL source: src/backend/libpq/pqcomm.c (SO_KEEPALIVE, send buffer)](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/libpq/pqcomm.c) — tier-1 — github.com/postgres/postgres
- [libpq fe-misc.c (pqSendSome, pqFlush)](https://raw.githubusercontent.com/postgres/postgres/master/src/interfaces/libpq/fe-misc.c) — tier-1 — github.com/postgres/postgres
- [PostgreSQL source: src/backend/replication/logical/decode.c](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/replication/logical/decode.c) — tier-1 — github.com/postgres/postgres
- [PostgreSQL source: src/backend/replication/logical/snapbuild.c](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/replication/logical/snapbuild.c) — tier-1 — github.com/postgres/postgres
- [PostgreSQL source: src/backend/replication/logical/logical.c](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/replication/logical/logical.c) — tier-1 — github.com/postgres/postgres
- [PostgreSQL source: src/backend/replication/logical/worker.c](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/replication/logical/worker.c) — tier-1 — github.com/postgres/postgres
- [pg_recvlogical.c source](https://raw.githubusercontent.com/postgres/postgres/master/src/bin/pg_basebackup/pg_recvlogical.c) — tier-1 — github.com (postgres/postgres)
- [Optimize LISTEN/NOTIFY via shared channel map and direct advancement (commit 282b1cd)](https://github.com/postgres/postgres/commit/282b1cde9dedf456ecf02eb27caf086023a7bb71) — tier-1 — github.com/postgres/postgres (official source mirror)
- [Commit 282b1cd patch text](https://github.com/postgres/postgres/commit/282b1cde9dedf456ecf02eb27caf086023a7bb71.patch) — tier-1 — github.com/postgres/postgres (official PostgreSQL source mirror)
- [pgsql: Optimize LISTEN/NOTIFY via shared channel map and direct advance](https://www.postgresql.org/message-id/E1vgSld-000fFk-2J%40gemulon.postgresql.org) — tier-1 — postgresql.org (pgsql-committers)
- [Commit 1b46990: Escalate ERRORs during async notify processing to FATAL](https://github.com/postgres/postgres/commit/1b46990) — tier-1 — github.com/postgres/postgres
- [Commit 8eeb4a0: Fix bug where we truncated CLOG that was still needed by LISTEN/NOTIFY](https://github.com/postgres/postgres/commit/8eeb4a0) — tier-1 — github.com/postgres/postgres
- [pgsql: Fix bug where we truncated CLOG that was still needed by LISTEN/NOTIFY](https://www.postgrespro.ru/list/thread-id/2778207) — tier-2 — postgrespro.ru (pgsql-committers archive mirror)
- [Commit history of src/backend/commands/async.c](https://github.com/postgres/postgres/commits/master/src/backend/commands/async.c) — tier-1 — github.com/postgres/postgres (official PostgreSQL source mirror)
- [async.c history](https://git.postgresql.org/gitweb/?p=postgresql.git;a=history;f=src/backend/commands/async.c;hb=HEAD) — tier-1 — git.postgresql.org
- [Optimize LISTEN/NOTIFY (original post, Joel Jacobson, 12 July 2025)](https://www.postgresql.org/message-id/6899c044-4a82-49be-8117-e6f669765f7e@app.fastmail.com) — tier-1 — postgresql.org (pgsql-hackers archive)
- [Optimize LISTEN/NOTIFY (full thread)](https://www.postgresql.org/message-id/flat/6899c044-4a82-49be-8117-e6f669765f7e%40app.fastmail.com) — tier-1 — postgresql.org (pgsql-hackers archive)
- [Proposal: Out-of-Order NOTIFY via GUC to Improve LISTEN/NOTIFY Throughput (Rishu Bagga, 2025-07-18)](https://www.postgresql.org/message-id/CAK80%3DjiWjMAfjwJymPoG3%3DFNE%3DhHygn5dXaoax1BWQT1rJrCCw%40mail.gmail.com) — tier-1 — postgresql.org (pgsql-hackers archive)
- [Proposal: Out-of-Order NOTIFY via GUC to Improve LISTEN/NOTIFY Throughput (full thread)](https://www.postgresql.org/message-id/flat/CAK80%3DjiWjMAfjwJymPoG3%3DFNE%3DhHygn5dXaoax1BWQT1rJrCCw%40mail.gmail.com) — tier-1 — postgresql.org (pgsql-hackers archive)
- [Re: Proposal: Out-of-Order NOTIFY (Tom Lane, WAL-transport proposal, 18 July 2025)](https://www.postgresql.org/message-id/1878165.1752858390%40sss.pgh.pa.us) — tier-1 — postgresql.org (pgsql-hackers archive)
- [Re: Proposal: Out-of-Order NOTIFY via GUC (Joel Jacobson, 2025-07-30)](https://www.postgresql.org/message-id/7c8c61c2-4dbf-4961-9e0c-a5ec77d8f846%40app.fastmail.com) — tier-1 — postgresql.org (pgsql-hackers archive)
- [Re: Proposal: Out-of-Order NOTIFY via GUC (Rishu Bagga, 2025-09-04)](https://www.postgresql.org/message-id/CAK80%3DjipUfGC%2BUQSzeA4oCP9daRtHZGm2SQZWLxC9NWmVTDtRQ%40mail.gmail.com) — tier-1 — postgresql.org (pgsql-hackers archive)
- [[PATCH] Write Notifications Through WAL (Rishu Bagga, 2025-09-25)](https://www.postgresql.org/message-id/CAK80%3DjjvgWJL5EdzH_Bxc37gn53PJaB5sDXB3VPfJuN7G1xL5Q%40mail.gmail.com) — tier-1 — postgresql.org (pgsql-hackers archive)
- [Re: [PATCH] Write Notifications Through WAL (thread archive including Arseniy Mukhin's benchmarks)](https://postgrespro.ru/list/id/CAE7r3MLwRNyvmMd2kaefeNYTRjGD8wOxLzyGn8vexaeTJdASaw@mail.gmail.com) — tier-2 — postgrespro.ru (Postgres Professional mailing-list archive)
- [Commitfest: Write Notifications Through WAL](https://commitfest.postgresql.org/patch/6205) — tier-1 — commitfest.postgresql.org
- [[PATCH] Write Notifications Through WAL (thread view)](https://hackorum.dev/topics/52339) — tier-3 — hackorum.dev (mailing-list aggregator)
- [Proposal: Out-of-Order NOTIFY via GUC to Improve LISTEN/NOTIFY Throughput (thread view)](https://hackorum.dev/topics/51945) — tier-3 — hackorum.dev (mailing-list aggregator)
- [Re: locks held during commit with synchronous replication (Tom Lane)](https://postgresql.org/message-id/2456.1382381184%40sss.pgh.pa.us) — tier-2 — postgresql.org (pgsql-general)
- [Thread: locks held during commit with synchronous replication](https://www.postgresql.org/message-id/flat/2456.1382381184%40sss.pgh.pa.us) — tier-2 — postgresql.org (pgsql-general)
- [Issue with COMMITs stuck on "AccessExclusiveLock on object 0 of class 1262 of database 0"](https://www.postgresql.org/message-id/CADWG95t0j9zF0uwdcMH81KMnDsiTAVHxmBvgYqrRJcD-iLwQhw%40mail.gmail.com) — tier-2 — postgresql.org (pgsql-general)
- [Re: one more question about LISTEN / NOTIFY (Craig Ringer)](https://www.postgresql.org/message-id/4C5E4419.7030009%40postnewspapers.com.au) — tier-2 — postgresql.org (pgsql-jdbc)
- [pg-bench-listen-notify](https://github.com/joelonsql/pg-bench-listen-notify) — tier-3 — github.com/joelonsql (personal repository)
- [Psycopg 3 Documentation: Asynchronous operations and notifications](https://www.psycopg.org/psycopg3/docs/advanced/async.html) — tier-1 — psycopg.org
- [Connection classes — notifies(), connect(), broken, fileno()](https://www.psycopg.org/psycopg3/docs/api/connections.html) — tier-1 — psycopg.org
- [Psycopg 3 release notes (3.2, 3.2.4, 3.2.10)](https://www.psycopg.org/psycopg3/docs/news.html) — tier-1 — psycopg.org
- [Psycopg 3 documentation: Transactions management](https://www.psycopg.org/psycopg3/docs/basic/transactions.html) — tier-1 — psycopg.org
- [psycopg_pool release notes](https://www.psycopg.org/psycopg3/docs/news_pool.html) — tier-1 — psycopg.org
- [psycopg 3 docs: Installation](https://www.psycopg.org/psycopg3/docs/basic/install.html) — tier-1 — psycopg.org
- [Daniele Varrazzo: improving Connection.notifies() in Psycopg 3.2](https://www.postgresql.org/message-id/CA%2Bmi_8Z6pSgVOpWgretCKpop8MzEWnJ-R-A3c6KQdodYY4D0YQ%40mail.gmail.com) — tier-1 — postgresql.org (psycopg mailing list)
- [psycopg connection.py source (notifies and wait)](https://raw.githubusercontent.com/psycopg/psycopg/master/psycopg/psycopg/connection.py) — tier-1 — github.com/psycopg
- [psycopg generators.py source (notifies generator)](https://raw.githubusercontent.com/psycopg/psycopg/master/psycopg/psycopg/generators.py) — tier-1 — github.com/psycopg
- [psycopg connection_async.py (master)](https://raw.githubusercontent.com/psycopg/psycopg/master/psycopg/psycopg/connection_async.py) — tier-1 — github.com/psycopg/psycopg
- [psycopg connection_async.py (3.2.0 tag)](https://raw.githubusercontent.com/psycopg/psycopg/3.2.0/psycopg/psycopg/connection_async.py) — tier-1 — github.com/psycopg/psycopg
- [psycopg _connection_base.py (master)](https://raw.githubusercontent.com/psycopg/psycopg/master/psycopg/psycopg/_connection_base.py) — tier-1 — github.com/psycopg/psycopg
- [Issue #1371: Async task cancellation is never delivered when the server stops responding on an established connection](https://github.com/psycopg/psycopg/issues/1371) — tier-1 — github.com/psycopg/psycopg
- [PR #1411: Time out the cancel operation to avoid hanging forever with unresponsive server](https://github.com/psycopg/psycopg/pull/1411) — tier-1 — github.com/psycopg/psycopg
- [PR #1412: Don't swallow CancelError in connection check](https://github.com/psycopg/psycopg/pull/1412) — tier-1 — github.com/psycopg/psycopg
- [Issue #543: AsyncConnection leaves queries running when interrupted or when an async task is cancelled](https://github.com/psycopg/psycopg/issues/543) — tier-1 — github.com/psycopg/psycopg
- [Issue #608: Async connection close method doesn't terminate the ongoing transaction](https://github.com/psycopg/psycopg/issues/608) — tier-1 — github.com/psycopg/psycopg
- [Issue #779: Add timeout (with a sensible default) to cancel_safe](https://github.com/psycopg/psycopg/issues/779) — tier-1 — github.com/psycopg/psycopg
- [Issue #1345: AsyncConnectionPool task cancellation still lost in getconn()](https://github.com/psycopg/psycopg/issues/1345) — tier-1 — github.com/psycopg/psycopg
- [psycopg release history](https://pypi.org/project/psycopg/#history) — tier-1 — pypi.org
- [Python docs: Coroutines and Tasks (asyncio.timeout, Task.cancel, to_thread)](https://docs.python.org/3/library/asyncio-task.html) — tier-1 — docs.python.org
- [psycopg issue #71: Replication support](https://github.com/psycopg/psycopg/issues/71) — tier-1 — github.com (psycopg/psycopg project tracker)
- [psycopg issue #71 comments (GitHub API)](https://api.github.com/repos/psycopg/psycopg/issues/71/comments?per_page=100) — tier-1 — api.github.com (psycopg/psycopg)
- [psycopg PR #1219: Don't fail on commands returning COPY_BOTH results](https://api.github.com/repos/psycopg/psycopg/pulls/1219) — tier-1 — api.github.com (psycopg/psycopg)
- [psycopg PR #1311: POC Implement Replication Connection Support](https://api.github.com/repos/psycopg/psycopg/pulls/1311) — tier-1 — api.github.com (psycopg/psycopg)
- [psycopg 3 pq abc.py (PGconn protocol)](https://raw.githubusercontent.com/psycopg/psycopg/master/psycopg/psycopg/pq/abc.py) — tier-1 — github.com (psycopg/psycopg)
- [psycopg2 — Replication support objects (extras)](https://www.psycopg.org/docs/extras.html) — tier-1 — psycopg.org
- [psycopg2 release notes](https://www.psycopg.org/docs/news.html) — tier-1 — psycopg.org
- [psycopg2 pqpath.c (replication feedback logic)](https://raw.githubusercontent.com/psycopg/psycopg2/master/psycopg/pqpath.c) — tier-1 — github.com (psycopg/psycopg2)
- [PgBouncer Features: SQL feature map for pooling modes](https://www.pgbouncer.org/features.html) — tier-1 — pgbouncer.org
- [sqlx-postgres PgListener documentation](https://docs.rs/sqlx-postgres/0.8.5/sqlx_postgres/struct.PgListener.html) — tier-2 — docs.rs
- [pg-listen source (index.ts)](https://raw.githubusercontent.com/andywer/pg-listen/master/src/index.ts) — tier-1 — github.com/andywer (project repository)
- [tcp(7) — Linux manual page](https://man7.org/linux/man-pages/man7/tcp.7.html) — tier-1 — man7.org (Linux man-pages project)
- [Dead connection handling in PostgreSQL — Amazon RDS](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Appendix.PostgreSQL.CommonDBATasks.DeadConnectionHandling.html) — tier-1 — docs.aws.amazon.com
- [Amazon RDS for PostgreSQL: Performing logical replication](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/PostgreSQL.Concepts.General.FeatureSupport.LogicalReplication.html) — tier-1 — docs.aws.amazon.com
- [When TCP sockets refuse to die](https://blog.cloudflare.com/when-tcp-sockets-refuse-to-die/) — tier-2 — blog.cloudflare.com
- [Oban Troubleshooting guide](https://oban.hexdocs.pm/troubleshooting.html) — tier-1 — oban.hexdocs.pm (official Oban docs)
- [Oban.Notifier documentation](https://oban.hexdocs.pm/Oban.Notifier.html) — tier-1 — oban.hexdocs.pm (official Oban docs)
- [Oban v2.17 changelog](https://oban.hexdocs.pm/2.17.8/changelog.html) — tier-1 — oban.hexdocs.pm (official Oban docs)
- [Oban documentation (dispatch_cooldown, stager interval, insert_trigger)](https://oban.hexdocs.pm/Oban.html) — tier-1 — oban.hexdocs.pm (official documentation)
- [Oban.Notifiers.Postgres](https://oban.hexdocs.pm/Oban.Notifiers.Postgres.html) — tier-1 — oban.hexdocs.pm (official documentation)
- [Oban.Peer — Oban documentation](https://oban.hexdocs.pm/Oban.Peer.html) — tier-1 — oban.hexdocs.pm (official Hex docs)
- [Oban Stager source (stager.ex)](https://raw.githubusercontent.com/oban-bg/oban/main/lib/oban/stager.ex) — tier-1 — github.com/oban-bg/oban (primary source)
- [Oban Postgres notifier source (notifiers/postgres.ex)](https://raw.githubusercontent.com/oban-bg/oban/main/lib/oban/notifiers/postgres.ex) — tier-1 — github.com/oban-bg/oban (primary source)
- [Oban basic engine source (engines/basic.ex)](https://raw.githubusercontent.com/oban-bg/oban/main/lib/oban/engines/basic.ex) — tier-1 — github.com/oban-bg/oban (primary source)
- [Oban engine source (engine.ex)](https://raw.githubusercontent.com/oban-bg/oban/main/lib/oban/engine.ex) — tier-1 — github.com/oban-bg/oban (primary source)
- [Oban queue producer source (queues/producer.ex)](https://raw.githubusercontent.com/oban-bg/oban/main/lib/oban/queues/producer.ex) — tier-1 — github.com/oban-bg/oban (primary source)
- [Oban Sonar source (sonar.ex)](https://raw.githubusercontent.com/oban-bg/oban/main/lib/oban/sonar.ex) — tier-1 — github.com/oban-bg/oban (primary source)
- [Oban oban.ex (dispatch_cooldown and stager options)](https://raw.githubusercontent.com/oban-bg/oban/main/lib/oban.ex) — tier-1 — github.com/oban-bg/oban (project source)
- [oban lib/oban/peers/database.ex](https://raw.githubusercontent.com/oban-bg/oban/main/lib/oban/peers/database.ex) — tier-1 — raw.githubusercontent.com (oban-bg/oban primary source)
- [oban lib/oban/peer.ex](https://raw.githubusercontent.com/oban-bg/oban/main/lib/oban/peer.ex) — tier-1 — raw.githubusercontent.com (oban-bg/oban primary source)
- [oban lib/oban/migrations/postgres/v11.ex](https://raw.githubusercontent.com/oban-bg/oban/main/lib/oban/migrations/postgres/v11.ex) — tier-1 — raw.githubusercontent.com (oban-bg/oban primary source)
- [River client source (client.go)](https://raw.githubusercontent.com/riverqueue/river/master/client.go) — tier-1 — github.com/riverqueue/river (primary source)
- [River producer source (producer.go)](https://raw.githubusercontent.com/riverqueue/river/master/producer.go) — tier-1 — github.com/riverqueue/river (primary source)
- [River notifier source (internal/notifier/notifier.go)](https://raw.githubusercontent.com/riverqueue/river/master/internal/notifier/notifier.go) — tier-1 — github.com/riverqueue/river (primary source)
- [River notify limiter source (internal/notifylimiter/limiter.go)](https://raw.githubusercontent.com/riverqueue/river/master/internal/notifylimiter/limiter.go) — tier-1 — github.com/riverqueue/river (primary source)
- [River DebouncedChan](https://raw.githubusercontent.com/riverqueue/river/master/internal/util/chanutil/debounced_chan.go) — tier-1 — github.com/riverqueue/river (primary source)
- [River job scheduler source (internal/maintenance/job_scheduler.go)](https://raw.githubusercontent.com/riverqueue/river/master/internal/maintenance/job_scheduler.go) — tier-1 — github.com/riverqueue/river (primary source)
- [river internal/leadership/elector.go](https://raw.githubusercontent.com/riverqueue/river/master/internal/leadership/elector.go) — tier-1 — raw.githubusercontent.com (riverqueue/river primary source)
- [river riverdriver/riverpgxv5/internal/dbsqlc/river_leader.sql](https://raw.githubusercontent.com/riverqueue/river/master/riverdriver/riverpgxv5/internal/dbsqlc/river_leader.sql) — tier-1 — raw.githubusercontent.com (riverqueue/river primary source)
- [river internal/maintenance/queue_maintainer_leader.go](https://raw.githubusercontent.com/riverqueue/river/master/internal/maintenance/queue_maintainer_leader.go) — tier-1 — raw.githubusercontent.com (riverqueue/river primary source)
- [River docs: PgBouncer](https://riverqueue.com/docs/pgbouncer) — tier-1 — riverqueue.com (official River docs)
- [River — Leader election](https://riverqueue.com/docs/leader-election) — tier-1 — riverqueue.com (official River docs)
- [Graphile Worker FAQ](https://worker.graphile.org/docs/faq) — tier-1 — worker.graphile.org (official Graphile Worker docs)
- [Graphile Worker configuration](https://worker.graphile.org/docs/config) — tier-1 — worker.graphile.org (official Graphile Worker docs)
- [Graphile Worker pool source (src/main.ts)](https://raw.githubusercontent.com/graphile/worker/main/src/main.ts) — tier-1 — github.com/graphile/worker (primary source)
- [Graphile Worker worker source (src/worker.ts)](https://raw.githubusercontent.com/graphile/worker/main/src/worker.ts) — tier-1 — github.com/graphile/worker (primary source)
- [Graphile Worker local queue source (src/localQueue.ts)](https://raw.githubusercontent.com/graphile/worker/main/src/localQueue.ts) — tier-1 — github.com/graphile/worker (primary source)
- [Graphile Worker migration 000011.sql](https://raw.githubusercontent.com/graphile/worker/main/sql/000011.sql) — tier-1 — github.com/graphile/worker (primary source)
- [Graphile Worker migration 000018.sql](https://raw.githubusercontent.com/graphile/worker/main/sql/000018.sql) — tier-1 — github.com/graphile/worker (primary source)
- [Graphile Worker migration 000020.sql](https://raw.githubusercontent.com/graphile/worker/main/sql/000020.sql) — tier-1 — github.com/graphile/worker (primary source)
- [pg-boss README](https://raw.githubusercontent.com/timgit/pg-boss/master/README.md) — tier-1 — github.com/timgit/pg-boss (primary source)
- [pg-boss docs: Workers (docs/api/workers.md)](https://raw.githubusercontent.com/timgit/pg-boss/master/docs/api/workers.md) — tier-1 — github.com/timgit/pg-boss (official docs source)
- [pg-boss docs: Constructor (docs/api/constructor.md)](https://raw.githubusercontent.com/timgit/pg-boss/master/docs/api/constructor.md) — tier-1 — github.com/timgit/pg-boss (official docs source)
- [pg-boss notifier source (src/notifier.ts)](https://raw.githubusercontent.com/timgit/pg-boss/master/src/notifier.ts) — tier-1 — github.com/timgit/pg-boss (primary source)
- [pg-boss manager source (src/manager.ts)](https://raw.githubusercontent.com/timgit/pg-boss/master/src/manager.ts) — tier-1 — github.com/timgit/pg-boss (primary source)
- [pg-boss worker source (src/worker.ts)](https://raw.githubusercontent.com/timgit/pg-boss/master/src/worker.ts) — tier-1 — github.com/timgit/pg-boss (primary source)
- [PGMQ extension SQL (enable_notify_insert, notify_queue_listeners)](https://raw.githubusercontent.com/pgmq/pgmq/main/pgmq-extension/sql/pgmq.sql) — tier-1 — github.com/pgmq/pgmq (project source)
- [Marten: Async Daemon](https://martendb.io/events/projections/async-daemon.html) — tier-1 — martendb.io (official documentation)
- [JasperFx DaemonSettings.cs (polling and stale-sequence settings)](https://raw.githubusercontent.com/JasperFx/jasperfx/main/src/JasperFx.Events/Daemon/DaemonSettings.cs) — tier-1 — github.com/JasperFx/jasperfx (primary source)
- [Elixir EventStore: Subscriptions](https://eventstore.hexdocs.pm/subscriptions.html) — tier-1 — eventstore.hexdocs.pm (official documentation)
- [Elixir EventStore module docs (checkpoint_threshold, checkpoint_after, max_size)](https://eventstore.hexdocs.pm/EventStore.html) — tier-1 — eventstore.hexdocs.pm (official documentation)
- [Elixir EventStore notifications publisher.ex](https://raw.githubusercontent.com/commanded/eventstore/master/lib/event_store/notifications/publisher.ex) — tier-1 — github.com/commanded/eventstore (primary source)
- [Elixir EventStore notifications listener.ex](https://raw.githubusercontent.com/commanded/eventstore/master/lib/event_store/notifications/listener.ex) — tier-1 — github.com/commanded/eventstore (primary source)
- [Debezium source: WalPositionLocator.java](https://raw.githubusercontent.com/debezium/debezium/main/debezium-connector-postgres/src/main/java/io/debezium/connector/postgresql/connection/WalPositionLocator.java) — tier-1 — github.com/debezium/debezium
- [Debezium PostgreSQL connector](https://debezium.io/documentation/reference/stable/connectors/postgresql.html) — tier-1 — debezium.io
- [Debezium Server](https://debezium.io/documentation/reference/stable/operations/debezium-server.html) — tier-1 — debezium.io
- [Debezium FAQ](https://debezium.io/documentation/faq/) — tier-1 — debezium.io
- [Sequin](https://github.com/sequinstream/sequin) — tier-1 — github.com (sequinstream/sequin)
- [pgstream](https://github.com/xataio/pgstream) — tier-1 — github.com (xataio/pgstream)
- [wal2json README](https://raw.githubusercontent.com/eulerto/wal2json/master/README.md) — tier-1 — github.com (eulerto/wal2json)
- [DBLog: A Watermark Based Change-Data-Capture Framework (Andreakis and Papapanagiotou)](https://arxiv.org/abs/2010.12597) — tier-2 — arxiv.org (preprint)
- [Postgres LISTEN/NOTIFY does not scale](https://www.recall.ai/blog/postgres-listen-notify-does-not-scale) — tier-3 — recall.ai (vendor blog)
- [Postgres LISTEN/NOTIFY Actually Scales](https://dbos.dev/blog/postgres-listen-notify-scalability) — tier-3 — dbos.dev (vendor blog)
- [Postgres LISTEN/NOTIFY actually scales (discussion)](https://news.ycombinator.com/item?id=49040296) — tier-3 — news.ycombinator.com (forum)
- [Comment by KraftyOne on the PostgreSQL 19 patch and the global lock](https://news.ycombinator.com/item?id=49041211) — tier-3 — news.ycombinator.com (forum)
- [Hacker News discussion of "Postgres LISTEN/NOTIFY does not scale"](https://hn.algolia.com/api/v1/items/44490510) — tier-3 — news.ycombinator.com (forum, via hn.algolia.com)
- [Mastering Postgres Replication Slots](https://www.morling.dev/blog/mastering-postgres-replication-slots/) — tier-3 — morling.dev (personal blog)
- [All the ways to capture changes in Postgres](https://blog.sequinstream.com/all-the-ways-to-capture-changes-in-postgres/) — tier-3 — blog.sequinstream.com (vendor blog)
- [Postgres sequences can commit out-of-order](https://blog.sequinstream.com/postgres-sequences-can-commit-out-of-order/) — tier-3 — blog.sequinstream.com
- [Push-based Outbox Pattern with Postgres Logical Replication](https://event-driven.io/en/push_based_outbox_pattern_with_postgres_logical_replication/) — tier-3 — event-driven.io (personal blog)
- [How Postgres sequences issues can impact your messaging guarantees](https://event-driven.io/en/ordering_in_postgres_outbox/) — tier-3 — event-driven.io (personal blog)
- [pypgoutput](https://github.com/dgea005/pypgoutput) — tier-3 — github.com (dgea005, personal prototype)
