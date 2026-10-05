---
type: "topic-research"
id: "01-what-postgresql-native-mechanisms-can-signal-new-work-to-con-web"
title: "What PostgreSQL-native mechanisms can signal new work to consumers — `LISTEN`/`NOTIFY`, `pg_notify()`, row- versus statement-level trigger-driven notification, and advisory-lock or other wake-up primitives — and how does each work internally, from the notifying transaction's commit to delivery on a listening connection?"
date: "2026-10-05T13:56:47+00:00"
author: "Toby Clemson"
producer: "research-topic"
status: "complete"
kind: "finding"
round: 1
depth: 3
question: "What PostgreSQL-native mechanisms can signal new work to consumers — `LISTEN`/`NOTIFY`, `pg_notify()`, row- versus statement-level trigger-driven notification, and advisory-lock or other wake-up primitives — and how does each work internally, from the notifying transaction's commit to delivery on a listening connection?"
source_profile: "web"
tags: ["research", "topic-research", "finding"]
last_updated: "2026-10-05T13:56:47+00:00"
last_updated_by: "Toby Clemson"
schema_version: 1
---

# What PostgreSQL-native mechanisms can signal new work to consumers — `LISTEN`/`NOTIFY`, `pg_notify()`, row- versus statement-level trigger-driven notification, and advisory-lock or other wake-up primitives — and how does each work internally, from the notifying transaction's commit to delivery on a listening connection?

## Question

Which mechanisms built into PostgreSQL can tell a consumer that new work
exists? The candidates are asynchronous notifications (`LISTEN`, `NOTIFY` and
`pg_notify()`), notifications fired from row-level or statement-level triggers,
advisory locks, and any other wake-up primitive. For each one, what happens
inside the server between the notifying transaction's commit and the moment a
listening client connection receives the signal? And what does that internal
behaviour mean for an event store that wants to replace fixed-interval polling
with push-based consumers built on psycopg 3?

## Findings

### Short answer

PostgreSQL has one primitive built for signalling between sessions:
**asynchronous notifications**. A session sends one with `NOTIFY` or the
function `pg_notify()`, and sessions that have run `LISTEN` on the channel
receive it. The other candidates in the question are either ways of using that
primitive or not signalling mechanisms at all.

- **Trigger-driven notification** is a trigger function that calls
  `pg_notify()`. The notification joins the writing transaction's pending list
  and goes through the same commit path as any other notification. Choosing
  `FOR EACH ROW` or `FOR EACH STATEMENT` changes how many queue entries a
  transaction produces and what they carry. It never changes when listeners
  receive them, and it never changes whether the transaction takes the
  commit-time lock described below.
- **Advisory locks** give a blocking wait that ends when the holder releases
  the lock. The wake-up carries no payload, an exclusive lock wakes one waiter
  at a time, and the waiting connection is busy running a statement, so it
  cannot receive notifications while it waits. Advisory locks are a coordination
  tool, for example to guarantee one active consumer per subscription. They are
  not a way to announce new work.
- **Logical decoding** is the only durable push stream. A replication slot
  reads committed changes out of the WAL in commit order, and the walsender
  pushes them to a client over the replication protocol.
  `pg_logical_emit_message()` can put a custom transactional message into that
  stream, which gives a durable equivalent of `NOTIFY`. The costs are WAL
  retention, a server restart to set `wal_level = logical`, failover
  configuration, and, for a Python library, a client gap: psycopg 3 does not
  implement the replication protocol.
- **`WAIT`**, new in PostgreSQL 19, lets a *standby* wait until a given LSN has
  been written, flushed or replayed. It exists for read-your-writes on replicas
  and is not a general way to signal new work.

Three properties of notifications shape every design that uses them:

1. **They are delivered at commit, in commit order, and only once the writes are
   visible.** A notification is delivered only if, and only after, its
   transaction commits. Notifications from different transactions arrive in
   commit order. By the time a client receives one, the notifying transaction's
   writes are visible to any snapshot taken afterwards, so the consumer can
   query for the new rows straight away.
2. **They are at-most-once and reach only current listeners.** PostgreSQL does
   not store notifications for sessions that were not listening. They do not
   survive a disconnect or a server restart, they are not available on hot
   standbys, and a transaction-pooled connection cannot receive them. A
   notification can therefore only be a wake-up hint. Correctness has to come
   from a durable consumer checkpoint and a read that cannot skip events, with a
   fallback poll behind both.
3. **They serialise commits across the whole cluster.** Every transaction with
   a pending notification takes one heavyweight lock before it commits. It
   holds that lock through its commit record, any WAL flush and any wait for a
   synchronous standby. Notifying from every append, directly or through any
   kind of trigger, therefore serialises the append path across every database
   on the instance. The remedies that work cut the number of notifying
   transactions. Changing trigger granularity does not.

### What PostgreSQL guarantees about notifications

- **Delivery at commit.** "If a `NOTIFY` is executed inside a transaction, the
  notify events are not delivered until and unless the transaction is
  committed." `LISTEN` also "takes effect at transaction commit", and a
  rolled-back `LISTEN` changes nothing. Running `LISTEN` again on a channel the
  session already listens to does nothing, so re-issuing it is safe.
  `pg_listening_channels()` returns the channels the current session listens
  to.
- **Ordering.** Notifications from one transaction arrive in the order they
  were sent. Notifications "from different transactions are delivered in the
  order in which the transactions committed."
- **Deduplication.** Within one transaction, repeated notifications on the same
  channel with an identical payload are delivered once. Different payloads are
  always delivered separately. Notifications "from different transactions will
  never get folded into one notification."
- **Payload and channel names.** In the default build the payload must be
  shorter than 8000 bytes. In the source, `NOTIFY_PAYLOAD_MAX_LENGTH` is
  `BLCKSZ - NAMEDATALEN - 128`, so that each message fits on one queue page.
  `NOTIFY` accepts only a string literal; `pg_notify(text, text)` is the
  documented form for channel names and payloads that are not constants.
  Channel names are identifiers: unquoted names are case-folded, and names are
  limited to 63 bytes. Rails Action Cable hashes longer names with SHA1 so that
  truncation cannot make two subscriptions share a channel.
- **What the client receives.** The channel name, the server PID of the
  notifying session, and the payload. A session that listens on a channel it
  also notifies gets its own notifications back, and can filter them by
  comparing the PID with its own.
- **Scope and restrictions.** Notifications are local to one database: each
  queued message carries the sender's database OID. A transaction that has
  executed `LISTEN` or `NOTIFY` cannot be prepared for two-phase commit.
  `LISTEN` and `NOTIFY` raise errors on a hot standby.
- **No durability or replay.** Listen registrations are "automatically cleared
  when the session ends". A listener only receives notifications that commit
  after its own `LISTEN` has committed. The queue directory `pg_notify/` is
  wiped at server start. The source of `async.c` explains: "Since notifications
  are not expected to survive database crashes, we can simply clean out the
  pg_notify data at any reboot, and there is no need for WAL support or
  fsync'ing." The result is at-most-once delivery, and only to sessions
  connected at the time.
- **The start-up rule.** A new listener "will receive all events committed
  after an instant during the transaction's commit step". The documented
  pattern is to "first execute (and commit!)" the `LISTEN`, then inspect the
  database state in a new transaction, then rely on notifications for later
  changes. The first few notifications may then repeat changes already seen,
  which the manual calls "usually harmless".

### From `NOTIFY` to the listening client

The design is described in the header of `src/backend/commands/async.c`. The
commit sequence is in `CommitTransaction()` in `xact.c`.

1. **Inside the notifying transaction.** `Async_Notify()` adds the
   notification to a list in the backend's own memory, which "will not be
   processed until transaction end". Duplicates are dropped as they arrive: by
   linear scan until the (sub)transaction has `MIN_HASHABLE_NOTIFIES` (16)
   entries, and by hash table after that. When a subtransaction commits, its
   entries are merged into the parent's list with duplicates removed. When it
   aborts, its entries are discarded.

2. **`PreCommit_Notify()`, before the commit is recorded.** If the transaction
   has no pending notifications and no pending `LISTEN` actions, the function
   returns straight away, so a transaction that does not notify never touches
   the lock. Otherwise it does the following:
   - It forces a transaction ID onto the transaction with
     `GetCurrentTransactionId()`.
   - It takes
     `LockSharedObject(DatabaseRelationId, InvalidOid, 0, AccessExclusiveLock)`.
     This is a heavyweight lock on "database 0". It shows up in lock logs as
     `AccessExclusiveLock on object 0 of class 1262 of database 0`, and in
     `pg_stat_activity` as wait event `Lock: object`. The source comment gives
     the reason: "Serialize writers by acquiring a special lock that we hold
     till after commit. This ensures that queue entries appear in commit order,
     and in particular that there are never uncommitted queue entries ahead of
     committed ones." The file header says that "all writers serialize on a
     cluster-wide heavyweight lock".
   - With the lock held, it appends its entries to the head of a single
     cluster-wide SLRU queue. Each entry holds the database OID, the
     transaction ID, the channel and the payload. The queue lives on disk under
     `pg_notify/`, with the pages in use cached in shared memory.
   - If the queue is full, it raises `too many notifications in the NOTIFY
     queue`. This happens before the commit is recorded, so the transaction can
     still fail, which is why the documentation says a `NOTIFY` transaction
     "will fail at commit".
   - If a transaction writes queue entries and then aborts, readers skip those
     entries because the transaction ID never committed.

3. **The commit runs with the lock held.**
   - `RecordTransactionCommit()` writes the commit record. It calls `XLogFlush`
     only when the transaction has already written WAL before its commit
     record and `synchronous_commit` is above `off`, or when a flush is forced
     for some other reason. Under the same "has written WAL" condition it waits
     for synchronous standbys in `SyncRepWaitForLSN`.
   - `ProcArrayEndTransaction()` then makes the transaction visible as
     committed.
   - A notifier waits for the lock inside `PreCommit_Notify()`, before it
     reaches `RecordTransactionCommit()`. Notifying transactions therefore
     cannot share a WAL flush through group commit, and `commit_delay` would
     lengthen the hold for any notifier that does flush.

4. **Signalling the listeners.** `AtCommit_Notify()` applies any pending
   `LISTEN` or `UNLISTEN` changes and calls `SignalBackends()`, which wakes
   listeners with `SendProcSignal(pid, PROCSIG_NOTIFY_INTERRUPT, …)`. Who gets
   woken depends on the version:
   - **PostgreSQL 18 and earlier.** The backend signals *every* listening
     backend in its own database that has not already caught up, whatever
     channel it listens on. It also signals listeners in other databases that
     are at least `QUEUE_CLEANUP_DELAY` pages behind, so that their queue
     pointers move forward and the global tail can advance.
   - **PostgreSQL 19, in beta (Beta 4 was released on 24 September 2026).**
     Commit `282b1cde` (authored by Joel Jacobson and reviewed and committed by
     Tom Lane, January 2026) adds a shared hash table that maps each (database
     OID, channel name) pair to the backends listening on it.
     - Only listeners on the affected channels are signalled, plus idle
       listeners that have fallen far behind.
     - A listener that has no interest in the new messages and is sitting at
       the old queue head has its pointer moved past them without being woken.
     - The commit reports "integer multiples" more throughput when many
       listeners each care about only a few messages, and "no improvement for
       the simplest case where every listener reads every message".
     - A May 2026 follow-up fixed a race in which a listener whose `LISTEN` had
       not yet committed could be moved past messages meant for it.
     - The commit does not touch the commit lock, and the PostgreSQL 19 release
       notes list no change to it.
   - Readings of the commit sequence disagree about whether this signalling
     happens inside the lock window. See "Open points and caveats".

5. **On the listening backend.** The signal handler sets the process latch.
   - If the backend is idle, meaning it is waiting for a client command and is
     not inside a transaction block, it handles the notifications straight
     away. Otherwise the handler only sets a flag, and the work happens "just
     before we next go idle".
   - `asyncQueueReadAllNotifications()` takes a snapshot and scans forward from
     the backend's queue position. For each entry it tests `XidInMVCCSnapshot`
     *before* `TransactionIdDidCommit`, "else we might return a message from a
     transaction that is not yet visible to snapshots".
   - When it reaches an entry from a transaction that is still running, it
     stops and backs up, so that the entry is read again next time.
   - It filters entries by database OID and by its own channel list. In
     PostgreSQL 18 this is a linear search; in 19 it is a per-backend hash
     table.
   - This ordering is what guarantees the "visible on wake-up" property above.

6. **On the wire.** Each notification is sent as a protocol
   `NotificationResponse` message.
   - "At present, NotificationResponse can only be sent outside a
     transaction", and it can arrive just before `ReadyForQuery`. The protocol
     documentation still tells clients to accept it at any point.
   - A listener that keeps a transaction open therefore receives nothing until
     that transaction ends.
   - In libpq, the client waits on `PQsocket()` with `select()`, then calls
     `PQconsumeInput()` and `PQnotifies()`. This uses no CPU while idle;
     `PQnotifies()` itself never reads from the socket.
   - How psycopg 3 surfaces these messages is covered in the psycopg sections
     below.

7. **Queue maintenance.**
   - **Tail and truncation.** The queue tail is the lowest position held by
     any listener. Notifying backends try to advance it, and truncate old
     segments, once every `QUEUE_CLEANUP_DELAY` pages.
   - **New listeners.** A new listener starts at the furthest position held by
     any other listener in its database, because everything before that is
     "surely committed".
   - **Stuck listeners.** A listener that sits in a long-running transaction
     stops cleanup entirely. The server logs warnings naming the blocking
     session once the queue is half full, and `NOTIFY` commits fail once it is
     full.
   - **Size.** The queue size is set by `max_notify_queue_pages`, which is
     documented from PostgreSQL 17. It defaults to 1,048,576 pages, which is
     8 GB with 8 kB pages, and it can only be set at server start.
   - **Monitoring.** `pg_notification_queue_usage()` reports how full the queue
     is. The LWLock wait events `NotifyQueue`, `NotifyQueueTail`, `NotifySLRU`
     and `NotifyBuffer` show contention inside the queue.
   - **Dead clients.** The backend of a client that has silently vanished stays
     registered as a listener until the server notices the client is gone. It
     can hold the queue back in the meantime, which is one reason server-side
     TCP keepalives matter (see "Detecting a dead listener").

### The commit lock: what it costs and how to keep it off the append path

**What sets the cost.** Two facts decide which mitigations work:

- **The unit of serialisation is the notifying transaction, not the
  notification.** A transaction that sends one `NOTIFY` or a thousand takes the
  lock once. A transaction that sends none never takes it.
- **Hold time is the work done inside the lock plus the commit's durability
  waits.** The in-lock work is the queue append, the commit-record insertion
  and possibly the signalling of listeners. The durability waits are any WAL
  flush and any wait for a synchronous standby.

**Evidence of the cost.**

- **A 2019 pgsql-general report.** COMMITs in one database blocked behind a
  single holder of the lock, while commits in other databases queued behind it
  too, with up to 500 waiters. In a similar 2019 case, Martijn van Oosterhout
  found that most of the time went to listening backends holding the queue
  lock.
- **Recall.ai (2025).** Recall.ai ran "tens of thousands of simultaneous
  writers", each issuing `NOTIFY` inside its write transaction. Hundreds of
  sessions queued on the lock, lock waits exceeded a second, and database CPU
  and I/O "actually plummeted" while throughput collapsed. That is the
  signature of a global mutex.
- **DBOS.** Because the lock is held through the WAL flush, DBOS notes that
  "stream writes need to commit sequentially". Their first implementation
  topped out at about 2.9K stream writes per second without CPU, memory or IOPS
  becoming the limit, which is roughly 0.35 ms per serialised commit.
- **pgsql-hackers benchmarks on unpatched master.**
  - Transactions that did nothing but `NOTIFY` reached about 154,000–190,000
    TPS with 8 clients on Apple M2 and M3 laptops.
  - With 1,000 clients and one listener, the same workload reached 7,679 TPS,
    against 95,430 TPS with the heavyweight lock commented out, a 12x gap.
  - `INSERT` plus `NOTIFY` at 100 clients reached about 30,000 TPS. The
    storage and durability settings were not stated.
  - Joel Jacobson found that the bottleneck moves between the heavyweight lock
    and the `kill()` wake-up syscalls, depending on the workload.
- **FOSDEM PGDay 2026.** A talk by Jimmy Angelakos is built around how "a
  simple trigger could bring our database to a crawl".

Because the lock admits one notifying commit at a time and no group commit
happens among them, the ceiling is roughly one divided by the hold time. The
figures below are derived arithmetic, not measurements:

| Storage | Flush latency | Approximate ceiling (notifying commits/s) |
|---|---|---|
| EBS gp3 | "single-digit millisecond" | about 110 to 1,000 |
| EBS io2 Block Express | average under 500 µs | about 2,000 |
| Either, with a synchronous standby | flush plus network round trip | lower |
| No flush inside the lock | none | tens to hundreds of thousands (the laptop benchmarks above) |

**`synchronous_commit` and the notify-only transaction.**
`RecordTransactionCommit()` decides whether the transaction has written WAL
before it inserts the commit record. The source comment allows an asynchronous
flush "if the current transaction has not performed any WAL-logged operation".
A transaction that only queues notifications has a transaction ID but has
written no WAL, because the notification queue is not WAL-logged. So it
already does two things, whatever `synchronous_commit` is set to:

- it commits asynchronously (`XLogSetAsyncXactLSN`) without waiting for an
  fsync;
- it skips `SyncRepWaitForLSN`.

For such a transaction, `SET LOCAL synchronous_commit = off` shortens the lock
hold by essentially nothing. The setting only matters when the notifying
transaction writes WAL. That can happen in three ways:

- **Deliberate bookkeeping**, such as updating a "last notified position" row
  or inserting a marker or outbox row.
- **Hint-bit full-page images.** Reading just-committed rows, for example with
  `SELECT max(position)`, sets hint bits on them. With data checksums on, or
  `wal_log_hints = on`, the first hint-bit change to a page after a checkpoint
  is written to WAL as a full-page image. `initdb` enables data checksums by
  default from PostgreSQL 18.
- **Heap pruning.** The `xact.c` comments note that pruning can write WAL
  during a transaction.

When that happens and `synchronous_commit` is `on` (the default), the notifier
holds the cluster-wide lock across a full local WAL flush. If synchronous
standbys are configured and `synchronous_commit` is above `local`, it also
holds the lock until the standby acknowledges.

Turning the setting off for that one transaction cuts the hold to
microseconds. By the arithmetic above, that raises the ceiling by one to three
orders of magnitude. It is safe for a notify-only transaction:

- The documentation says `off` carries "no risk of database inconsistency". The
  loss window is up to three times `wal_writer_delay`, which defaults to
  200 ms.
- Any bookkeeping row the notifier keeps may roll back after a crash, so
  sending a notification again must be harmless.
- It must not be applied to append transactions that carry event data, because
  that would give up the durability of those events.

Under many concurrent notifiers, though, it is queueing on the lock that
dominates, not fsync time.

**Strategies, ranked by how much writer serialisation they remove.**

1. **A dedicated notifier or detector process.** No writer transaction ever
   takes the lock. One session detects newly committed events and wakes
   everyone else.
   - *Polling variant.* The session polls the head of the log and issues one
     `NOTIFY` per batch of new positions.
   - *Logical-decoding variant.* The session reads committed changes from a
     replication slot instead (costs below).
   - *Without `NOTIFY` at all.* Marten (.NET) runs a single elected leader whose
     "high water mark" agent polls for the highest event sequence that is safe
     to process.
   - *Costs:* detection latency, a single point of failure that needs leader
     election, and gap-detection logic.
2. **Batched notifier transactions in each writer process (DBOS).**
   - Writers commit without `NOTIFY`. Notifications are buffered in memory and
     periodically flushed "in a single batch transaction", so "the global lock
     only needs to be taken when the buffer is flushed, not for each individual
     stream write".
   - Throughput went from about 2.9K to about 60K writes per second, roughly
     20x, at 15–100 ms latency. At the peak the database CPU was fully used, so
     the lock was no longer the limit.
   - Because the flush happens after the data commits, a consumer that wakes up
     always finds the data committed.
   - Buffered notifications are lost if the process crashes, which DBOS covers
     with an infrequent fallback poll.
3. **Throttled notification inside the write transaction.** `NOTIFY` stays in
   the write transaction but is skipped most of the time, and the transactions
   that skip it never take the lock.
   - *River (Go)* uses an in-memory limiter set to `FetchCooldown` (100 ms by
     default). It sends one `NotifyMany` call for the queues that pass.
   - *pgmq* keeps throttle state in an unlogged table. It calls `pg_notify`
     only when an `UPDATE` guarded by `throttle_interval_ms` (250 ms by
     default) changes a row.
   - *Cost: lost wake-ups.* Appends that commit inside the throttle window are
     never announced, so consumers need a fallback poll.
   - *Cost: a possible new hot spot (an inference not documented by pgmq).* A
     shared throttle row can swap waits on the `NOTIFY` lock for waits on a row
     lock. The row is held by the transaction that is notifying, which is also
     the transaction holding the `NOTIFY` lock. River's in-process limiter
     avoids that, but its cap grows with the number of writer processes.
4. **Statement-level or summary-row triggers.** These reduce queue volume and
   the queue-insertion work done inside the lock. They do not reduce the number
   of lock acquisitions, because every append transaction still carries a
   notification. Under concurrent single-batch appends, the usual event-store
   pattern, the gain is close to zero.

Strategies 1 to 3 all lose or delay some notifications, so consumers need a
fallback poll whichever one is chosen. For a psycopg 3 library, the pattern
these sources most directly support has four parts:

- The append transaction commits without `NOTIFY`.
- A post-commit hook marks the affected channel as dirty in an in-process
  coalescer.
- A background task on its own connection sends `pg_notify` for every dirty
  channel in one transaction every few tens of milliseconds, optionally
  committed with `synchronous_commit` off.
- Consumers treat notifications as hints and keep a slower safety poll.

**Upstream changes.**

- **Commit `282b1cde` (PostgreSQL 19).** Recall.ai's May 2026 update says this
  commit "eliminated the bottleneck". DBOS says the patch "addresses a narrower
  scenario" and leaves the core problem in place. One plausible way to
  reconcile them: Recall.ai had very many listeners on distinct channels, and
  their waits may have been made worse by woken listeners competing for
  `NotifyQueueLock`, as in van Oosterhout's 2019 case. For an event store whose
  consumers mostly listen on the same few channels, the commit does not remove
  the serialisation between writers.
- **Writing notifications through WAL.** Rishu Bagga's patch series, from
  September 2025, puts notification payloads in WAL and writes compact queue
  entries. Its stated aim is to remove the lock held from `PreCommit_Notify`
  to the end of the transaction.
  - Reported results: 60–70K TPS against about 30K on `pgbench -c 100`, and
    30–40x on a reviewer's insert-with-notify benchmark.
  - Tom Lane outlined a related redesign. Payloads would be written to WAL in
    parallel, and only a small fixed-size SLRU entry would be written under the
    global lock, so the lock would be held "much shorter than what happens
    now". A prototype of that design doubled `INSERT` plus `NOTIFY` throughput
    at 100 clients, from about 30,000 to about 64,000 TPS.
  - Tom Lane has also explained that the lock guarantees "no other would-be
    notifier can insert any messages in between ours", and other parts of the
    design depend on that guarantee.
  - Status: at its last recorded activity (v9, November 2025) the patch was
    marked "Needs review" in the PG19-4 commitfest. Current master still takes
    the "database 0" lock, holds it "till after commit", and describes the
    queue as having no WAL support.

### Row-level versus statement-level trigger notification

A trigger that calls `pg_notify()` adds an entry to the transaction's pending
list. The notification then goes through the commit path above. Trigger timing
therefore decides how many queue entries a transaction produces and what they
contain, never when listeners see them. Any such trigger also makes *every*
transaction that writes to the table take the commit-time lock. The PostgreSQL
documentation recommends putting `NOTIFY` in a trigger so that applications
cannot forget to send it.

- **`FOR EACH ROW`** runs "once for every row that the operation modifies".
  - Row-level `AFTER` triggers are queued and fire at the end of the statement,
    before any statement-level `AFTER` triggers.
  - A payload that differs per row, such as a row ID or position, queues one
    entry per row, and every listener receives each one.
  - A payload that is constant, or set per stream or category, collapses to one
    notification per distinct value per transaction, because of the
    deduplication rule. A row trigger with a constant payload therefore
    produces the same single queue entry as a statement trigger.
  - A `WHEN` condition on an `AFTER` row trigger means non-matching rows never
    queue a trigger event at all.
  - Commanded's Elixir EventStore puts its `FOR EACH ROW` trigger on the
    `streams` table, not the events table. Each append then sends one
    notification per stream row touched, with the payload
    `stream_uuid,stream_id,first_version,last_version`, however many events
    were written.
- **`FOR EACH STATEMENT`** runs once per statement "regardless of how many rows
  it modifies". It is the default when `CREATE TRIGGER` names neither level.
  PgQueuer, a Python queue, uses it: its trigger is
  `AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE` with no row clause.
  - It also fires for statements that change zero rows. An unguarded trigger
    therefore sends spurious notifications and makes no-op statements take the
    lock.
  - On an `AFTER` trigger on a plain table, `REFERENCING NEW TABLE AS …` (or
    `OLD TABLE`) exposes every changed row. One notification can then summarise
    the statement, for example with the highest new position or the distinct
    stream names, and can be skipped when the set is empty.
  - Restrictions: transition tables cannot be combined with an `UPDATE OF
    column` list or used on constraint triggers. Row-level triggers with
    transition relations cannot be defined on partitions or inheritance
    children. `TRUNCATE` triggers exist only at statement level.
- **Deferred constraint triggers** (`DEFERRABLE INITIALLY DEFERRED`, which must
  be `AFTER ROW`) fire at the end of the transaction rather than the end of the
  statement. They are still row-level, and the notification is still sent at
  commit. This is also the hook Cloudscale uses to assign positions at commit
  time (see "Reading without skipping events").

Choose trigger granularity to control queue volume and the payload. It cannot
fix commit serialisation.

### Advisory locks

**Mechanics.**

- `pg_advisory_lock()` and `pg_advisory_xact_lock()`, and their shared
  variants, obtain the lock "waiting if necessary". The `pg_try_…` variants
  never wait.
- A waiter sleeps in the heavyweight lock manager, with wait event
  `Lock: advisory`. `ProcSleep` "returns after the lock has been granted, or if
  a deadlock is detected", and the deadlock check runs after
  `deadlock_timeout`.
- On release, `ProcLockWakeup` scans the wait queue and wakes every waiter that
  is no longer blocked, by setting its latch through `ProcWakeup`.
- A transaction-level lock is released when its transaction ends. A
  session-level lock ignores rollback and is held until it is explicitly
  unlocked or the session ends. Session-level locks are re-entrant, so each
  acquisition needs a matching unlock.
- All of these locks share a lock table sized by `max_locks_per_transaction` ×
  `max_connections`, and `lock_timeout` applies to them.

**As a work signal**, an advisory lock has clear limits:

- The wake-up carries no payload. It only grants a lock, and an exclusive lock
  goes to one waiter at a time.
- The waiting connection is running a statement. Notifications are only
  delivered to idle sessions, so that connection cannot receive any while it
  waits.
- Each wake-up means acquiring a lock again.

The documented uses are mutual exclusion and coordination: application-defined
locks that "block each other in the expected way". For consumers, advisory
locks complement `LISTEN` rather than replace it, for example a
`pg_try_advisory_lock()` that guarantees a single active consumer per
subscription. That assessment is an inference from the documented behaviour;
the sources do not state it.

Advisory locks also appear as an *ordering fence*, not as a signal. Sequin has
each writer take transaction-scoped advisory locks keyed on the position it
expects. Readers take the lowest locked position from `pg_locks` as a ceiling
and read below it. However, the lock key is computed from the sequence's
`last_value` before the insert actually draws its number, so the two are not
paired atomically. That is an inference, and it should be tested under
concurrent writers before anyone relies on the scheme.

### Logical decoding: the durable alternative and what it costs

**What it provides.**

- Logical decoding turns WAL into a stream of changes in the order "they were
  made on the origin server". "Concurrent transactions are decoded in commit
  order", and rolled-back transactions "never get decoded".
- A slot "will emit each change just once in normal operation" and survives
  crashes. After a crash, though, a slot can rewind and send changes again,
  because "the current position of each slot is persisted only at checkpoint".
  In practice delivery is therefore at-least-once.
- Through the streaming replication protocol, the walsender pushes changes to
  the client. The SQL functions, such as `pg_logical_slot_get_changes()`, are a
  pull interface.
- `pg_logical_emit_message(transactional, prefix, content [, flush])` writes a
  custom message into WAL, either as part of the current transaction or
  straight away. `pgoutput` delivers these messages when its `messages` option
  is on.

**As a notifier that replaces writer-side `NOTIFY`.** One slot (using
`pgoutput`, `test_decoding` or `wal2json`) decodes either the inserted event
rows, through a publication, or transactional messages that writers emit. A
single notifier session then fans out wake-ups. `NOTIFY` is still taken, but
only by that one session, so writers never queue on it.

**Latency costs.**

- **Decoding waits for flushed WAL.** On a primary, the walsender only decodes
  up to the flush pointer. With `synchronous_commit = on`, that adds nothing.
  If writers commit asynchronously, though, their WAL is flushed by the WAL
  writer within at most three times `wal_writer_delay`. By inference, an
  asynchronous append may then take up to about 600 ms by default to reach the
  notifier.
- **Changes are released only at commit, one transaction after another.**
  - Output-plugin callbacks "are only invoked when the transaction actually
    commits".
  - Decoded changes beyond `logical_decoding_work_mem` (64 MB per replication
    connection by default) spill to local disk. `pg_stat_replication_slots`
    reports `spill_txns`, `spill_count` and `spill_bytes`.
  - Streaming of in-progress transactions (`pgoutput` protocol version 2 and
    later, PostgreSQL 14 and later) still applies changes in commit order.
  - The slot is one commit-ordered stream, so a large transaction that commits
    just before a small append holds back that append's wake-up.
  - Supabase notes that its Postgres Changes feature processes changes "on a
    single thread to preserve their order", so larger instances do not raise
    its throughput much.
- **Failover-safe slots add a standby round trip.** With
  `synchronized_standby_slots` (PostgreSQL 17 and later), decoded changes go
  out "only after the specified replication slots confirm receiving WAL". The
  SQL slot functions block in the same way on failover slots.
- **The fan-out is an extra hop.** The notifier still has to wake consumers,
  for example with its own `NOTIFY`.
- **Lag is hard to see.** For logical decoding, `pg_stat_replication` shows
  `NULL` lag unless the plugin emits tracking messages, so a notifier has to
  measure its own latency.
- No source gives end-to-end measurements from commit to wake-up. These are
  structural costs, not benchmarked ones.

**Retention costs.**

- **The slot pins WAL and catalog rows.** Slots "will prevent removal of
  required resources even when there is no connection using them". That
  includes both WAL and catalog rows (through `catalog_xmin`), and "in extreme
  cases this could cause the database to shut down to prevent transaction ID
  wraparound". AWS warns that an unread slot can fill instance storage and
  stop vacuum from removing obsolete tuples.
- **Retention moves only when the client confirms.** `confirmed_flush_lsn` and
  `restart_lsn` advance only from the client's feedback. A notifier needs
  nothing more from the WAL once it has sent a wake-up, so it can confirm
  straight away.
- **Long transactions pin WAL anyway.** By inference from the snapshot
  builder's comments, a write transaction left open holds `restart_lsn` back
  to roughly its own start, however far the notifier has read.
- **Retention can be capped.** `max_slot_wal_keep_size` (default `-1`,
  unlimited) moves a slot that falls too far behind to `lost`.
  `idle_replication_slot_timeout` (PostgreSQL 18) invalidates slots that stay
  inactive too long, at the next checkpoint. For a wake-up notifier over an
  authoritative events table, losing the slot only loses wake-ups that a
  catch-up poll recovers, so a tight cap is a reasonable trade.
- **Turning it on costs a restart.** `wal_level = logical` "can only be set at
  server start". On RDS and Aurora it needs `rds.logical_replication` and a
  reboot. The extra WAL is mostly a concern for `REPLICA IDENTITY FULL` tables
  with many updates and deletes, so an insert-only log adds little (an
  inference). The development documentation for the release after 18 describes
  an `effective_wal_level` that rises to `logical` automatically when the first
  logical slot is created; PostgreSQL 18 does not have this.
- **Each slot costs a walsender.** `max_replication_slots` and
  `max_wal_senders` both default to 10, and each walsender has its own decoding
  buffer. The pattern is therefore one slot per database, owned by one
  notifier.

**Crash and failover costs.**

- After a crash, a slot may resend changes, so wake-ups repeat.
- **Without failover slots** (before PostgreSQL 17, or when they are not
  configured), the slot disappears when a standby is promoted. The new slot
  becomes consistent only "once all those previously running transactions have
  committed or aborted", and commits that land in between produce no wake-up.
  The notifier should therefore send an unconditional resync wake-up after
  reconnecting.
- **PostgreSQL 17 failover slots** need a lot of configuration:
  - the slot is created with `failover = true`;
  - the standby sets `sync_replication_slots`, uses a physical slot through
    `primary_slot_name`, turns on `hot_standby_feedback` (which "may result in
    undesirable table bloat"), and puts a valid `dbname` in
    `primary_conninfo`.

  Synchronisation is periodic and asynchronous, and only slots already marked
  as synced can be used after failover. With `synchronized_standby_slots` set,
  "logical replication will not proceed" if those standby slots are missing or
  invalidated.
- **Patroni** instead copies permanent logical slots to standbys and advances
  them every `loop_wait` seconds. It warns that some messages may arrive twice
  after a failover, and that the slots must only be used on the leader.
- **Decoding on a standby** (PostgreSQL 16 and later) moves the load off the
  primary. It adds replication lag and conflict-driven invalidation. Aurora
  PostgreSQL does not support logical decoding from read replicas.

**The psycopg 3 gap.** psycopg 3 does not implement the streaming replication
protocol. Issue #71, "Add support for streaming replication as in psycopg2",
has been open since August 2021. A psycopg 3 event store would need one of
three workarounds:

- a separate notifier built on psycopg2's `LogicalReplicationConnection`
  (`start_replication`, `consume_stream`, `send_feedback`);
- an external tool such as `pg_recvlogical`;
- the SQL interface, which brings polling back, now against the slot.

**Net assessment.** For an event log whose table is the source of truth, a
logical-decoding notifier does take commit serialisation off the append path,
and it delivers commit-ordered wake-ups tagged with LSNs. It is safe only as a
disposable wake-up source:

- one slot per database;
- immediate confirmation after each wake-up;
- capped retention;
- a resync wake-up after any slot loss or failover;
- consumers that tolerate both duplicate and missing wake-ups through their own
  position-based catch-up reads.

### Comparison of mechanisms

| Mechanism | Payload | Survives disconnect or restart | Cost on the writer | Wake-up target | Ordering | psycopg 3 |
|---|---|---|---|---|---|---|
| `LISTEN` and `NOTIFY` (`pg_notify()`) | text under 8000 bytes | No (at-most-once) | Cluster-wide lock held through the commit record, WAL flush and any synchronous-standby wait | PostgreSQL 18 and earlier: every listener in the database. PostgreSQL 19: listeners on the channel | Commit order | `notifies()` generator, or handlers |
| Row-level trigger plus `pg_notify()` | Per row, deduplicated within a transaction | No | The lock on every writing transaction, plus one queue entry per distinct payload | As above | Commit order | As above |
| Statement-level trigger plus `pg_notify()` | Per statement; transition tables allow a summary | No | The lock on every writing transaction; fires even when no rows change | As above | Commit order | As above |
| Batched or dedicated notifier (built on `NOTIFY`) | Coalesced hint | No | None on the append path; one locked commit per flush | As above | Commit order of the flushes | Ordinary connection |
| Advisory lock wait | None | Not applicable | Holding and releasing a lock | Waiters whose request can now be granted | Lock-queue order | Ordinary SQL (a blocking statement) |
| Logical decoding (a slot, plus `pg_logical_emit_message()` if wanted) | Full changes or custom messages | Yes (the slot persists; it may resend) | WAL volume; retained WAL and catalog rows; a restart to enable | The slot's consumer | Commit order, with LSNs | Not supported (needs psycopg2, `pg_recvlogical` or SQL polling) |

### Reading without skipping events

Notifications arrive in commit order. Event positions, by contrast, usually
come from a sequence and are assigned when the row is inserted. A consumer that
wakes up and reads "everything after my last position" can therefore move past
a position whose transaction has not committed yet, and never come back for it.

**Why the obvious approach skips events.**

- **Sequence values are not tied to commit.** A value taken by `nextval` is
  never given back, so aborts, crashes and `INSERT ... ON CONFLICT` all leave
  gaps. Sequences "cannot be used to obtain 'gapless' sequences". With a
  `CACHE` setting above one, values are not even handed out in order across
  sessions. In every case, transactions can commit in a different order from
  the one in which they took their numbers.
- **A gap cannot be read as a signal.** It might be a transaction that is still
  running and will fill it, or a rollback that never will. Marten notes that
  most holes fill within milliseconds but some "never fill". Precision
  Nutrition found about 0.6% of its IDs missing in normal operation.
- **A worked example.** Transaction A takes position 4 and transaction B takes
  position 5, and B commits first. The consumer wakes on B's notification, reads
  `position > 3`, gets 5 and checkpoints at 5. When A commits, the consumer
  wakes again, reads `position > 5`, and never sees event 4. Precision Nutrition,
  Cloudscale, Oskar Dudycz and Axon users all describe exactly this failure.

So a notification is a hint, never a checkpoint. A position carried in the
payload is not a safe checkpoint, and neither is the highest position the
consumer has seen. Correctness has to come from one of two places. Either the
read query defines a safe frontier, or the write path hands out positions in
commit order. The sources describe five families of solution.

**1. Read only behind the transaction horizon.** Each event records
`transaction_id xid8 NOT NULL DEFAULT pg_current_xact_id()`. The consumer keeps
a checkpoint of (last transaction ID, last position) and reads
`WHERE (transaction_id, position) > (:last_tx, :last_pos) AND transaction_id <
pg_snapshot_xmin(pg_current_snapshot()) ORDER BY transaction_id, position LIMIT
n`. Emmett, eugene-khyst's reference implementation, Dudycz and Sequin all
describe this design.

- **Why it is safe.** A snapshot's `xmin` is the "lowest transaction ID that
  was still active". Every ID below it is "either committed and visible, or
  rolled back and dead", so no row below the bound can appear later.
- **Use `pg_current_xact_id()` and store `xid8`.** The function returns the
  top-level transaction ID even inside a subtransaction. Do not rely on the
  system `xmin` column, which is a 32-bit `xid` that wraps around. `xid8` "does
  not wrap around during the life of an installation". It has a B-tree
  operator class, whereas `xid` has only a hash class. `xid8` was added in
  PostgreSQL 13, and the old `txid_*` functions remain only for backward
  compatibility.
- **Query cost.** With a composite B-tree on `(transaction_id, position)`, the
  read is a single index range scan, roughly O(log N + batch) at any table
  size. Use The Index, Luke reports that the row-value form `(a, b) > (x, y)` is
  used as one index access predicate, which keeps keyset paging flat. If
  consumers subscribe by category or stream, an index that leads with that
  column keeps each subscription a single range scan (an inference from
  standard B-tree behaviour).
- **Cost: any long write transaction stalls every consumer.** Only
  transactions that hold a real transaction ID count, because a transaction
  gets one "when [it] first writes to the database". `GetSnapshotData` does
  not filter by database, so a write transaction left open in *any* database
  on the cluster holds the frontier back. Sequin saw unrelated long
  transactions stop cursor progress "for two minutes or more" and rejected the
  approach. While the frontier is held, each read stays cheap and returns
  nothing. When the writer finishes, the bound jumps and the backlog drains in
  `LIMIT`-sized pages.
- **Limiting the stall.** `transaction_timeout` (PostgreSQL 17) and
  `idle_in_transaction_session_timeout` bound it. Prepared transactions are not
  subject to `transaction_timeout`, so two-phase commit remains a risk (an
  inference).
- **Run each read on its own snapshot.** Reads should run as READ COMMITTED
  autocommit statements. `pg_current_snapshot()` returns the backend's active
  snapshot, so repeated reads inside one REPEATABLE READ transaction would keep
  reusing a frozen bound (an inference from the function's implementation).
- **Interaction with `NOTIFY` (an inference).** If the transaction holding the
  frontier back rolls back, or is unrelated and never notifies, nothing arrives
  when the frontier moves. A consumer using this method therefore needs a timed
  re-check whenever it knows committed work is waiting behind the frontier.
- **Cost: a single stream's events can come out of version order.** Ordering by
  `(transaction_id, position)` follows the order in which transactions *first
  wrote*. The documentation warns that this "might be different from the order
  in which the transactions started", and it is not commit order either. Under
  READ COMMITTED, each statement sees "data committed before the query began".
  So a transaction that got its ID early can build on a stream head committed
  later by a transaction with a higher ID:

  | Step | T1 | T2 |
  |---|---|---|
  | 1 | Writes anything, or calls `pg_current_xact_id()`. Gets transaction ID 100. | |
  | 2 | | Appends stream S version 5 (transaction ID 101, position 41) and commits. |
  | 3 | Reads S's head in a new statement, sees v5, and appends S v6 at position 42. | |
  | 4 | Commits. | |

  The feed then delivers v6 at (100, 42) before v5 at (101, 41). Nothing is
  skipped, but projections that assume each stream arrives in version order
  break. Emmett's append function opens exactly this window: it calls
  `pg_current_xact_id()` before reading the stream head and sets no isolation
  level.
  - *Exposed paths:* any transaction that gets its ID before it reads the
    stream head it builds on. That covers multi-stream appends, appends made
    after other writes in the same transaction, and append functions that call
    `pg_current_xact_id()` first. Blind `SET version = version + n` updates are
    also exposed, because PostgreSQL's heap write paths take the transaction ID
    before waiting on a concurrent modifier.
  - *Safe path:* a single-stream append whose first write is an exact
    expected-version check of the head.
  - *Fixes:* run appends at REPEATABLE READ or SERIALIZABLE. The snapshot is
    taken no later than the transaction ID, so the bad interleaving becomes a
    `could not serialize access due to concurrent update` error or a unique-key
    violation, and the retry gets a newer ID. The other fix is to guarantee
    that the head check is the first write and to forbid multi-stream or mixed
    business-and-event transactions. As a defence, a consumer that needs
    per-stream order can check version continuity and fail loudly on a gap.

**2. Read the difference between two snapshots (PgQ).** PgQ, Skype's queue,
uses the previous snapshot as the checkpoint. Each batch takes the events whose
writing transaction is visible in the current snapshot but not the previous
one:
`NOT txid_visible_in_snapshot(ev_txid, sn1) AND txid_visible_in_snapshot(ev_txid, sn2)`.

- **How batches are cut.** A ticker stores snapshots in `pgq.tick`. It ticks
  after 500 events or a 3-second lag by default, and otherwise every minute.
  Each transaction appears in exactly one batch, the one in which it first
  becomes visible, and events are ordered by ID within the batch.
- **Query shape.** The naive range `ev_txid >= xmin1` would rescan everything
  written since the oldest open transaction on every batch. The source itself
  says "the simple solution has a problem with long transactions". So
  `pgq.batch_event_sql` does two things:
  - it range-scans only `[xmax1 .. xmax2]`;
  - it collects the transactions that were in progress at `sn1` and have
    finished by `sn2`. IDs within 100 of the range start lower the range start;
    the rest are fetched through a `UNION ALL` branch with an exact `IN (...)`
    list.
- **Visibility checks are cheap.** `is_visible_fxid` returns straight away
  outside `[xmin, xmax)`. Inside that window it searches the in-progress list,
  linearly or by binary search once the list has more than 30 entries.
- **Under long transactions, delivery continues at the same cost.** The long
  transaction's rows stay invisible until it commits and are never rescanned.
  It then lands in the next batch. PgQ's own table rotation (three tables,
  rotated every two hours and emptied with `TRUNCATE`) is skipped while old
  transactions linger. A permanent event log cannot rotate at all, so its
  transaction-ID index covers the whole log, although each query still touches
  only the batch's range.
- **Costs.**
  - The checkpoint is a snapshot (`xmin:xmax:xip_list`) rather than two
    scalars. Its size is bounded by procarray limits.
  - A batch cannot be cut with a `LIMIT`, so paging needs an inner cursor such
    as (sn1, sn2, last position).
  - The rows and the snapshot must be read under the same snapshot.
  - Delivery order is the order in which transactions became visible, and only
    the stored snapshot sequence can reproduce it.
- **Fit with `NOTIFY` (an inference).** A committed transaction's events can be
  read straight away, whatever older transaction is still running. A
  transaction that was in flight lands in the first batch after it commits,
  which is exactly when its own notification arrives. Rollbacks add nothing.
  The consumer therefore needs no timer for held-back work, only the general
  fallback poll. Per-stream order is also kept: version *n* commits before
  *n+1*, so it lands in the same batch or an earlier one, with a lower
  position.

**3. Read in position order and handle gaps explicitly.**

- **Marten's high-water mark.** The mark moves up one position at a time and
  "stops under any hole" while a transaction that could fill it is still
  running. It then skips dead gaps and records each skip in
  `mt_high_water_skips`. The opt-in setting
  `SkipStaleGapsDespiteLiveTransactionsAfter` defaults to null. If it is
  enabled, the documentation warns that "an append that commits inside the
  skipped range will never be projected". The approach keeps order and is safe
  by default, but processing stalls behind slow appenders. The documentation
  does not say how the liveness check is implemented.
- **Axon's gap-aware tracking tokens.** These keep reading upwards and recheck
  the gaps they have recorded. The defaults are `gapTimeout` 60,000 ms,
  `maxGapOffset` 10,000 and `gapCleaningThreshold` 250. The approach never
  stalls, but an event that fills a gap is delivered after higher positions.
  An event that takes longer than the timeout to commit, or falls further back
  than the maximum offset, is silently dropped. An Axon maintainer calls the
  approach costly.
- **Sequin's advisory-lock ceiling**, described under "Advisory locks".
- Gaps left by rollbacks never produce a notification, so each of these schemes
  needs a timed re-check of open gaps.

**4. Hand out positions in commit order on the write path.** If no writer can
draw a position until the previous writer has committed, then position order
is commit order. `position > checkpoint` is then safe, and a notification for
position *n* means every lower position is final.

- **Commanded EventStore** assigns global positions with
  `UPDATE streams SET stream_version = stream_version + $2 WHERE stream_id = 0
  RETURNING …`. The row lock is held until commit, so positions are gapless and
  in commit order. The cost is that "writes now funnel through updates … which
  must happen serially".
- **Counter tables** work the same way but serialise whole transactions.
  Dudycz calls this the "nuke option". One forum benchmark, which is
  anecdotal, saw throughput fall from 1,600 to 120 requests per second.
- **Assigning positions at commit time** shrinks the serialised section.
  Cloudscale uses a deferred constraint trigger that runs `LOCK TABLE … IN
  EXCLUSIVE MODE` and then `nextval`. Sequin's variant also assigns the
  position late in the transaction. With this design, any position carried in
  a notification payload has to be sent from the deferred trigger, after the
  position is assigned.
- **Interaction with the `NOTIFY` lock (an untested inference).** An append
  path that already sends `NOTIFY` on every commit is already serial at commit
  time, so a commit-time position lock adds only the work done while holding
  it. The reverse also holds. If `NOTIFY` is moved off the append path to remove
  that serialisation, commit-ordered positions bring a serialised commit
  section back.

**5. Read a stream that is already in commit order (logical decoding).**
Decoding delivers transactions in commit order, so it removes the ordering
problem. It does so with the infrastructure costs described above, and at
least once rather than exactly once. Precision Nutrition considered ordering by
commit timestamps and rejected it, because of indexing limits at tens of
millions of rows and wraparound concerns.

| Approach | Consumer checkpoint | Held back by | Timed re-check needed for held work | Write-side cost | Per-stream order |
|---|---|---|---|---|---|
| Transaction horizon | (transaction ID, position) | any long write transaction, cluster-wide | yes | none | can invert unless appends run at REPEATABLE READ or check the head first |
| Snapshot difference | previous snapshot, plus an inner cursor | nothing | no | none | kept |
| Marten-style gaps | position | appenders still running | yes | none | kept |
| Axon-style gaps | position plus gap set | nothing (but can drop events) | yes | none | kept, apart from late gap-fills |
| Commit-ordered positions | position | nothing | no | serialised commit section | kept |

**How a `NOTIFY`-driven consumer should read.** These rules hold whichever
read model is chosen:

- Treat each notification as a hint, and combine several into one wake-up.
- Never take the checkpoint from the payload.
- After waking, repeat the safe read until it returns nothing.
- Keep a fallback poll running.

Which read model to choose depends on the append path:

- If appends can be serialised at commit, commit-ordered positions give the
  simplest consumer.
- If appends must stay concurrent, the snapshot difference fits `NOTIFY`
  wake-ups best (an inference).
- The transaction horizon is simpler, but it needs a timer and appends that
  keep per-stream order, and unrelated long transactions can stall it.
- Gap tracking with timeouts trades correctness for liveness. Avoid it wherever
  a skipped event is unacceptable.

### Receiving notifications in a psycopg 3 consumer

#### How psycopg 3 surfaces notifications

- psycopg recommends autocommit "if you wish to receive or send notifications
  in a timely manner". It also makes each `LISTEN` take effect immediately.
- **The `notifies()` generator** (on `Connection` or `AsyncConnection`) is the
  way to receive notifications on a dedicated listener.
  - The `timeout` and `stop_after` parameters were added in 3.2.
    `stop_after` can be exceeded when several notifications "arrive in the
    same packet".
  - The generator holds the connection's non-reentrant lock for its whole
    lifetime, across each `yield`. That is a `threading.Lock` for the sync
    class and an `asyncio.Lock` for the async class. While it runs, no other
    code can use the connection.
- **`add_notify_handler()` callbacks** only fire "during a connection
  operation, such as a query". A dedicated listener therefore cannot rely on
  them firing in the background.
- **Version history matters.**
  - Before 3.2.4, notifications received between `LISTEN` and the start of
    the generator "were lost".
  - From 3.2.4 they are captured and yielded once the generator starts
    (issue #962). That change introduced a memory leak if the generator is not
    used regularly (issue #1091).
  - 3.2.10 fixed the leak. If a handler is registered, notifications are no
    longer captured while the generator is not running, and mixing the two
    styles raises a `RuntimeWarning`.
  - Also from 3.2.4, psycopg raises the specific server error, such as
    `AdminShutdown` on server shutdown, instead of a generic
    `OperationalError`, so reconnect handlers must catch the parent class.

  A library should require 3.2.10 or later and use the generator alone on its
  listener.
- **Detecting a broken connection.** When the connection breaks, psycopg raises
  `OperationalError`, and `Connection.broken` becomes `True`, which separates an
  interrupted connection from a cleanly closed one.

#### Keep `LISTEN` off pooled connections

Run one dedicated listener per process per database, outside any
`psycopg_pool` pool:

- **The pool does not reset session state.** When a connection comes back,
  psycopg_pool commits or rolls back any open transaction, calls an optional
  `reset` callback, discards the connection if it is broken or past
  `max_lifetime`, and otherwise returns it to the pool. Nothing in that
  sequence issues `UNLISTEN`, so a `LISTEN` run on a pooled connection stays
  with every later borrower. PostgreSQL's own scrubbing command, `DISCARD ALL`,
  includes `UNLISTEN *`, which shows that listen registrations are exactly the
  kind of state a pool has to clear explicitly.
- **Unread notifications pile up without limit.** Every psycopg connection
  installs a libpq notify callback. With no handler registered, it appends each
  notification to a per-connection `_notifies_backlog` deque that has no size
  limit. The psycopg maintainer gave "calling LISTEN on a connection and then …
  not reading the notifications" as the way to get the issue #1091 leak. The
  reporter saw about 200 MB per hour at more than 10,000 notifications per
  minute.
- **A leaked listener can block the server queue.** A pooled connection that is
  listening and then borrowed for a long transaction stops queue cleanup for
  every writer.
- **A listener monopolises its connection anyway.** Because `notifies()` holds
  the lock, a listener borrowed from the pool would reduce the pool's capacity.
  It would also escape `max_lifetime` and `drain()`, which only act when a
  connection is returned.

**Building the listener.** Open it with the pool's connection class,
`connect(conninfo, **kwargs, autocommit=True)`, and apply the pool's
`configure` callback. Procrastinate does exactly this. From psycopg_pool 3.3,
`conninfo` and `kwargs` may be callables that are evaluated at every attempt,
for example to rotate credentials. Procrastinate handles this by calling
private pool methods (`_resolve_conninfo()`, `_resolve_kwargs()`). A library is
more robust if it owns the connection-parameter source and passes the same
value or callable to both the pool and the listener.

**Defensive guards on the pool.**

- Expose `LISTEN` only through the listener API.
- An optional `reset` callback can run `UNLISTEN *`, followed by a commit when
  the pool's connections are not in autocommit, because `UNLISTEN` takes
  effect at commit. `DISCARD ALL` cannot run inside a transaction block. To
  drop notifications psycopg has already buffered, call
  `list(conn.notifies(timeout=0))`. Either costs at least one extra round trip
  on every return.
- In tests or debug builds, assert that `pg_listening_channels()` returns no
  rows.
- psycopg_pool 3.3's `drain()` replaces every connection in a pool that is
  already contaminated.

#### Connection poolers

- **Transaction and statement pooling cannot deliver notifications.**
  - The PgBouncer feature map marks `LISTEN` as **Never** supported in
    transaction pooling, while `NOTIFY` is **Yes**.
  - In PgBouncer's `handle_server_work()`, a `NotificationResponse` goes to
    whichever client is currently linked to that server connection. If none is
    linked, the packet is dropped with the warning
    `got packet '%c' from server when not linked`. So notifications are either
    discarded or handed to unrelated clients.
  - Stale listening backends pile up, because `server_reset_query`
    (`DISCARD ALL`) runs only in session pooling. Turning on
    `server_reset_query_always` would simply run `UNLISTEN *` after every
    transaction.
  - The failure is silent: `LISTEN` succeeds, `NOTIFY` commits, and the
    consumer never wakes. Oban documents that transaction pooling "disables
    all of PostgreSQL's `LISTEN` and `NOTIFY` activity". PgBouncer issue #655,
    a request for transaction-mode `LISTEN` support, has been open since
    November 2021.
  - Managed transaction-mode poolers say the same. Neon and Google Cloud SQL
    list `LISTEN` as unsupported on pooled or transaction-mode connections.
    Supabase's Supavisor supports it only in session mode (port 5432) or on
    direct connections. Azure Flexible Server's built-in PgBouncer 1.25.2
    defaults to transaction mode. PgCat describes the same transaction-mode
    behaviour.
- **Session pooling works, but adds new ways to drop.**
  - In session mode, PgBouncer assigns a server connection "for the whole
    duration [the client] stays connected". It forwards asynchronous
    notifications to an idle client, and since 1.7 it does not change
    connection state when one arrives. AWS RDS Proxy pins a session that is
    "Listening on a notification channel".
  - The additional ways to lose the connection, and with it every listen
    registration, are these:
    - `DISCARD ALL` runs when a client disconnects, so a reconnecting client
      may land on another backend with no registration;
    - operator commands `KILL` and `SHUTDOWN`, and `PAUSE`, `RECONNECT` and
      `SHUTDOWN WAIT_FOR_SERVERS`, which wait for session clients to leave;
    - rolling restarts with `so_reuseport`;
    - managed restarts (Azure restarts its pooler on scaling, high-availability
      failover or server restart);
    - pool capacity, since each listener permanently takes one server slot and
      a reconnecting listener can hit `query_wait_timeout`.
- **Load-balancing poolers must send listeners to the primary.** Pgpool-II bug
  116, fixed in 2015, made notifications unreliable because queries went to a
  standby.
- **PgDog multiplexes `LISTEN` in transaction mode** when
  `pub_sub_channel_size` is set. It registers each channel once and fans
  notifications out in-process. Its delivery is documented as "at most once",
  so it is still a hint, not a gap-free channel.
- **Recommended topology.** Put the listener on a session-mode pooler alias or
  a direct connection. Writers can stay on the transaction pool, because
  `NOTIFY` works there. River, Oban and Brandur's notifier pattern all keep one
  listener per process away from transaction pooling.

#### The listener loop and in-process fan-out

The reference implementations agree on one design:

- **River (Go)** gives each client one notifier on a pooled connection that it
  takes over permanently (`Hijack()`) and closes outright rather than releasing
  it.
  - Subscriptions live in a map from topic to subscribers. `LISTEN` runs only
    for a topic's first subscriber and `UNLISTEN` only for its last.
  - `Listen` blocks until the server-side `LISTEN` has finished. To apply
    subscription changes, the notifier interrupts its wait and drains every
    queued operation first.
  - On reconnect it re-`LISTEN`s every topic, backs off exponentially, and
    pings after 5 seconds without a notification.
  - Delivery goes through a 1,000-slot buffer that drops notifications when
    full. One goroutine runs the subscriber callbacks in turn, so callbacks
    are kept tiny. Each one checks the payload and triggers a fetch limited by
    `FetchCooldown`, with `FetchPollInterval` (1 s by default) as "a fallback".
- **Oban (Elixir)** keeps a `Registry` of listeners separate from the
  connection. Registration "is durable across notifier restarts", and
  `handle_connect/1` re-`LISTEN`s every registered channel. Each notification is
  decoded once and sent to each listener's mailbox, so a slow listener cannot
  block the others. Oban's Sonar process pings its own channel, starting at
  5 s and backing off to 60 s. It reports `:isolated`, `:solitary` or
  `:clustered`. When the status is `:isolated`, the Stager switches to local
  1-second polling.
- **Postgrex.Notifications** reference-counts channels and re-`LISTEN`s all of
  them in one round trip on reconnect. It warns that notifications sent during
  a disconnection "are not queued and cannot be recovered".
- **GoodJob (Ruby)** removes its connection from the pool, waits one second at
  a time, sends `SELECT 1` every 10 s, and reconnects after 5 s.
- **Rails Action Cable** queues subscribe and unsubscribe requests and applies
  them between one-second waits. It has no reconnect logic, so a dropped
  connection silently ends delivery.
- **Procrastinate**, the psycopg 3 precedent, uses a standalone autocommit
  connection and quotes channels with `sql.Identifier`. It restarts its loop on
  `OperationalError`. Its callback only sets an `asyncio.Event`, which the
  fetch loop waits on alongside a polling timer (5 s by default).

For psycopg 3, these translate into the following shape:

1. **One owner per listener connection.** Because `notifies()` holds the
   connection lock, a single asyncio task or thread loops over
   `notifies(timeout=slice)`. Between slices it applies queued `LISTEN` and
   `UNLISTEN` commands, composed with
   `sql.SQL("LISTEN {}").format(sql.Identifier(channel))` because `LISTEN`
   takes no bind parameters, and then runs a liveness probe. Since 3.2.4, with
   no handler registered, notifications that arrive between slices are
   buffered and yielded on the next call. Procrastinate's older approach wraps
   each `__anext__()` in `asyncio.wait_for`. No psycopg documentation page
   describes what happens when a `notifies()` iteration is cancelled, so the
   native `timeout` is the documented route.
2. **A subscription registry kept outside the connection.** Reference-count
   channels, and drop subscribers when they close. `subscribe()` returns only
   once its `LISTEN` has run. The subscriber then reads from its checkpoint:
   subscribe first, then read current state, then rely on notifications.
3. **Cheap, coalescing fan-out.** Decode each payload once and keep it a small
   hint. Wake each matching subscriber with a non-blocking signal such as
   `asyncio.Event.set()`, which merges repeated wake-ups while the consumer is
   busy. Never run a consumer's read on the listener task.
   - If the listener runs on its own thread, wake asyncio consumers with
     `loop.call_soon_threadsafe(event.set)`, and use `threading.Event` for
     synchronous consumers.
   - If payloads must be delivered, use bounded queues that drop when full and
     re-read after a drop.
4. **Careful channel naming.** Quote channel names, namespace them with a
   schema or prefix, and hash any name over 63 bytes.
5. **Health and fallback.** Ping when idle. Optionally send a periodic
   `NOTIFY` to yourself on a private channel, as Oban's Sonar does, to prove
   that delivery works end to end. While the listener is unhealthy, poll on a
   short interval, and keep a slower background poll running at all times.

#### Start-up, wait and reconnect sequence

Every established PostgreSQL-backed queue examined uses `LISTEN` to speed up a
poll, never to replace it:

- Procrastinate "polls the database regardless".
- River's poll is "a back up in case something with listen/notify didn't
  work".
- Oban falls back to polling when its notifier is isolated.
- Graphile Worker polls every 2 s by default, and deliberately ignores a
  notification when no worker is idle.

A sequence that never misses events combines the documented `LISTEN` rule with
these patterns:

1. **Connect a dedicated, non-pooled listener in autocommit mode.**
   - Set libpq TCP liveness parameters (next section).
   - Set `target_session_attrs=read-write` (or `primary`) with a multi-host
     connection string, so that libpq only accepts a server "not in hot standby
     mode". `connect_timeout` applies to each host separately.
2. **Issue `LISTEN` for every channel before reading anything.** In autocommit
   mode it commits immediately. Reading first and then running `LISTEN` leaves
   a window in which events produce no notification for this session.
3. **Catch up from the durable checkpoint.** Read in batches until a batch
   comes back short or empty, using a pooled connection configured with
   `check=ConnectionPool.check_connection`, because the pool "doesn't actively
   check the state of the connections". After a reconnect, also wake every
   subscriber once, because anything notified during the outage is gone.
4. **Wait for a notification or the poll interval, whichever comes first, then
   catch up again.** `notifies(timeout=…, stop_after=…)` provides both in one
   call. One catch-up pass covers any number of pending notifications. If
   receiving and processing run in separate tasks, clear the wake-up flag
   *before* the read, so that a notification arriving during a long read is
   not lost (an inference from the race).
5. **Probe liveness whenever the wait times out**, for example with `SELECT 1`.
   Treat any `psycopg.OperationalError` subclass as a dead connection.
6. **On failure, back off with jitter while continuing to poll, then return to
   step 1.** Graphile Worker computes
   `jitter * min(MAX_DELAY, 50 * e^attempts)`, which is about 136 ms, 370 ms,
   1 s, 2.7 s and so on, and resets the counter once `LISTEN` succeeds. While
   the listener is down, consumers keep catching up at the poll interval over
   the pool. Never go back to waiting without first redoing steps 2 and 3, in
   that order.

The listener connection must never open a transaction. If it did, it would
delay its own deliveries and block queue cleanup for every writer. A consumer
that falls behind loses nothing, because a backlog of notifications collapses
into one catch-up pass. Graphile Worker relies on exactly this when it drops
notifications while all its workers are busy. The catch-up read itself must
use one of the safe read models above.

#### Detecting a dead listener

A listener parked in `notifies()` just waits for its socket. If the server host
vanishes or a partition drops packets, nothing arrives and nothing fails. RFC
9293 says keep-alives "MUST default to off", with an interval of at least two
hours. Three mechanisms bound the wait, and each covers a different state:

- **TCP keepalives cover an idle connection.** libpq enables `SO_KEEPALIVE`
  by default, but leaves the timers at the OS defaults. Linux uses 7,200 s
  idle, a 75 s interval and 9 probes, about 2 h 11 min in total; macOS uses
  two hours, 75 s and 8 probes. Neither default detects anything useful.
- **`TCP_USER_TIMEOUT` covers a connection with unacknowledged data**, such as
  a probe query sent into a dead link. On Linux, keepalives stop while data is
  being retransmitted, and `tcp_retries2` (default 15) gives a lower bound of
  924.6 s. tcp(7) says 13 to 30 minutes. With keepalives on, Linux also uses
  `TCP_USER_TIMEOUT` to "override keepalive to determine when to close a
  connection due to keepalive failure". Cloudflare's guidance is to set it to
  about `TCP_KEEPIDLE + TCP_KEEPINTVL × TCP_KEEPCNT`. RFC 5482 states the
  opposite constraint, but its concern is a different goal.
- **An application probe covers what TCP cannot see.** Examples are a pooler
  that is alive while its server leg is dead, or a platform where a socket
  option does nothing.

**Platform behaviour of the libpq parameters.** On Linux, `keepalives_idle`,
`keepalives_interval`, `keepalives_count` and `tcp_user_timeout` map to
`TCP_KEEPIDLE`, `TCP_KEEPINTVL`, `TCP_KEEPCNT` and `TCP_USER_TIMEOUT`. On
macOS, the idle setting maps to `TCP_KEEPALIVE`. xnu defines no
`TCP_USER_TIMEOUT`, so libpq accepts `tcp_user_timeout` and silently ignores
it. All four parameters are ignored on Unix-domain sockets.

**A direct-connection profile** that targets about 25 s detection for an idle
listener:

```text
keepalives=1 keepalives_idle=10 keepalives_interval=5 keepalives_count=3
tcp_user_timeout=25000 connect_timeout=5
```

The detection times below are derived from the timer semantics. They are not
measured end to end with psycopg.

| Scenario | Linux | macOS |
|---|---|---|
| Idle listener, link dies silently | dropped about 25 s after the last segment received | dropped about 25 s after the last segment received (idle time plus count × interval) |
| Probe query sent into a dead link | `tcp_user_timeout` drops it about 25 s after the send; about 50 s worst case from link loss | no user timeout; about 8.5 min of retransmissions by estimate, unless the keepalive timer still fires (unverified) |
| Failover by IP takeover, or server restart | fast: the next keepalive or probe gets an RST | fast |

Notes on the profile:

- AWS's Aurora fast-failover guidance is more aggressive: idle 1, interval 1,
  count 5. A process holds a single listener connection, so frequent probes
  cost almost nothing. The real constraint is false positives, so keep the
  timers well above the worst round-trip-time spikes you expect.
- With a multi-host connection string, a reconnect attempt can take the number
  of hosts × `connect_timeout`.
- On macOS, xnu has a per-socket `TCP_RXT_CONNDROPTIME` (`0x80`), which libpq
  never sets. Setting it from Python through a `socket.fromfd` duplicate is an
  untested option.

**Through a pooler or middleboxes**, the listener has two TCP legs. The
client's keepalives are answered by the pooler's kernel, so they prove only
that the pooler host is reachable.

- **PgBouncer** applies one `tune_socket()` to both legs. Its `tcp_keepalive`,
  `tcp_keepidle`, `tcp_keepintvl`, `tcp_keepcnt` and `tcp_user_timeout`
  settings (the last "only supported on Linux") cover both. The timers are
  "not set" by default.
- **When the server leg dies**, `disconnect_server` closes the linked client,
  but only once PgBouncer notices.
- **PgBouncer's idle timers do not close a linked session listener.**
  `client_idle_timeout` skips linked clients. `server_idle_timeout` (600 s) and
  `server_lifetime` (3,600 s) apply only to unlinked servers.
  `server_check_query` runs only at hand-off, so there is no in-session health
  check.
- **Other PgBouncer settings worth using.** `idle_transaction_timeout` is a
  useful guard for the listener's user. `pool_mode` can be set per database
  entry or per user, so a dedicated listener alias can use `session` while
  everything else uses `transaction`.
- **PostgreSQL's `idle_session_timeout`** (PostgreSQL 14 and later) would
  close a quiet listener. Set it to 0 for the listener's role, or keep the
  probe interval below it.
- **AWS RDS Proxy**'s idle client timeout (1,800 s by default) counts only
  application requests. Notifications from the server do not reset it, so the
  listener must send a query more often than that.
- **Supavisor** arms its idle timer only after a query completes. It also
  sends its own heartbeat.
- **Google Cloud SQL** pooling does not say whether its client idle timeout
  exempts session clients.
- **Azure's built-in PgBouncer** exposes no keepalive settings and no
  per-database `pool_mode`. A listener therefore needs either the whole server
  in session mode or a direct connection on port 5432.
- **Neon** listeners must use the direct endpoint.
- **Middleboxes drop state on quiet flows.** An AWS NAT gateway drops a flow
  after 350 s idle and replies with RST, and AWS recommends keepalives below
  that. An AWS Network Load Balancer defaults to 350 s (configurable from 60 to
  6,000 s; keepalives reset it; TLS listeners are fixed at 350 s). Azure
  Standard Load Balancer defaults to 4 minutes (configurable from 4 to 100
  minutes).

An illustrative configuration for this case uses
`keepalives_idle=30 keepalives_interval=10 keepalives_count=3
tcp_user_timeout=60000` on the client, the same values in PgBouncer, and
similar server-side `tcp_keepalives_*` and `tcp_user_timeout` settings. It
detects failures in about a minute while staying under every idle limit above.

**The application probe.** The probe crosses both legs, resets request-based
idle timers and refreshes NAT and load-balancer state. Run it every 30 to 60 s
by leaving `notifies(timeout=…)` and running `SELECT 1` on the same connection.
psycopg's documentation notes that polling detects a disconnection "with an
average delay of half the polling time". Give the probe its own deadline,
shorter than `tcp_user_timeout`, for example with `asyncio.wait_for`. When the
deadline cancels a running query, psycopg's async connection attempts a
server-side cancel with a 5 s timeout and closes the connection if that also
times out. The effective deadline is therefore up to the probe deadline plus
5 s. Discard the connection after a failed probe. The synchronous API has no
client-side query deadline, and `statement_timeout` cannot fire across a dead
network, so a sync listener depends on `tcp_user_timeout` on Linux and needs a
watchdog on macOS.

**The server side.** PostgreSQL's `tcp_keepalives_*` and `tcp_user_timeout`
settings let the server reap backends whose listener has vanished. That
matters because such a backend keeps its registration, can stall writing into
a socket nobody reads, and can hold back the notification queue.
`client_connection_check_interval` does not help here, because it checks only
"while running queries".

### Open points and caveats

- **Where signalling sits relative to the lock.** One reading of
  `CommitTransaction()` puts heavyweight-lock release *before*
  `AtCommit_Notify()`, so `SignalBackends()` would run after the lock is free.
  Another lists `AtCommit_Notify()` and `SignalBackends()` before
  `ResourceOwnerRelease(... LOCKS ...)`, so signalling would add to the hold
  time. What both readings agree on, and what the async.c source states, is
  that the lock covers the queue append, the commit record, any WAL flush and
  any synchronous-standby wait. The measured shift of the bottleneck between
  the lock and the `kill()` syscalls is consistent with either reading. Settle
  the order against the current `xact.c` before relying on it.
- **How large a cost signalling is.** Joel Jacobson argued that wake-up
  syscalls dominated in benchmarks that combine `LISTEN` and `NOTIFY`, and that
  the lock would be the next bottleneck. He also measured a 12x gain from
  removing the lock with 1,000 clients and one listener. The two observations
  describe different workloads.
- **Recall.ai and DBOS disagree** on whether PostgreSQL 19's channel map fixes
  the problem. The reconciliation offered above is plausible but not verified.
- **Unverified mechanics.** Several points are reasoned from the source or the
  documentation rather than tested:
  - macOS keepalive behaviour while data is unacknowledged;
  - setting `TCP_RXT_CONNDROPTIME` from Python;
  - the atomicity of Sequin's advisory-lock fence;
  - what happens when a `notifies()` iteration is cancelled;
  - whether managed poolers' idle timers count server-originated
    notifications;
  - the PgBouncer timer exemptions, which were read from the current janitor
    source and not re-tested on older versions.
- **Derived numbers.** The throughput ceilings and detection times in this
  finding are derived from documented latencies and timer semantics, not
  measured end to end.

## Sources

- [PostgreSQL 18 Documentation: NOTIFY](https://www.postgresql.org/docs/current/sql-notify.html) — tier-1 — postgresql.org (official documentation)
- [PostgreSQL 18 Documentation: LISTEN](https://www.postgresql.org/docs/current/sql-listen.html) — tier-1 — postgresql.org (official documentation)
- [PostgreSQL 17 Documentation: NOTIFY](https://www.postgresql.org/docs/17/sql-notify.html) — tier-1 — postgresql.org (official documentation)
- [PostgreSQL 16 Documentation: NOTIFY](https://www.postgresql.org/docs/16/sql-notify.html) — tier-1 — postgresql.org (official documentation)
- [PostgreSQL 19 Release Notes (devel)](https://www.postgresql.org/docs/devel/release-19.html) — tier-1 — postgresql.org (official documentation)
- [PostgreSQL 18 Release Notes](https://www.postgresql.org/docs/18/release-18.html) — tier-1 — postgresql.org
- [PostgreSQL 17 Release Notes](https://www.postgresql.org/docs/17/release-17.html) — tier-1 — postgresql.org (official documentation)
- [PostgreSQL 13 Release Notes](https://www.postgresql.org/docs/13/release-13.html) — tier-1 — postgresql.org (official documentation)
- [PostgreSQL 13.0 Release Notes](https://www.postgresql.org/docs/release/13.0/) — tier-1 — postgresql.org
- [Resource Consumption (max_notify_queue_pages, logical_decoding_work_mem)](https://www.postgresql.org/docs/current/runtime-config-resource.html) — tier-1 — postgresql.org (official documentation)
- [Client Connection Defaults (lock_timeout, transaction_timeout, idle_session_timeout)](https://www.postgresql.org/docs/current/runtime-config-client.html) — tier-1 — postgresql.org (official documentation)
- [PostgreSQL 17: Client Connection Defaults](https://www.postgresql.org/docs/17/runtime-config-client.html) — tier-1 — postgresql.org
- [PostgreSQL 16: Client Connection Defaults](https://www.postgresql.org/docs/16/runtime-config-client.html) — tier-1 — postgresql.org
- [Connections and Authentication (server TCP settings)](https://www.postgresql.org/docs/current/runtime-config-connection.html) — tier-1 — postgresql.org
- [Write Ahead Log configuration (synchronous_commit, wal_log_hints, commit_delay, wal_writer_delay, wal_level)](https://www.postgresql.org/docs/current/runtime-config-wal.html) — tier-1 — postgresql.org
- [PostgreSQL devel docs: Write Ahead Log configuration (effective_wal_level)](https://www.postgresql.org/docs/devel/runtime-config-wal.html) — tier-1 — postgresql.org
- [Asynchronous Commit](https://www.postgresql.org/docs/current/wal-async-commit.html) — tier-1 — postgresql.org
- [Replication configuration (max_slot_wal_keep_size, idle_replication_slot_timeout, synchronized_standby_slots, sync_replication_slots)](https://www.postgresql.org/docs/current/runtime-config-replication.html) — tier-1 — postgresql.org
- [CREATE TRIGGER](https://www.postgresql.org/docs/current/sql-createtrigger.html) — tier-1 — postgresql.org (official documentation)
- [Overview of Trigger Behavior](https://www.postgresql.org/docs/current/trigger-definition.html) — tier-1 — postgresql.org (official documentation)
- [Explicit Locking (Advisory Locks)](https://www.postgresql.org/docs/current/explicit-locking.html) — tier-1 — postgresql.org (official documentation)
- [System Administration Functions (advisory lock functions, pg_logical_emit_message, pg_logical_slot_get_changes, pg_create_logical_replication_slot)](https://www.postgresql.org/docs/current/functions-admin.html) — tier-1 — postgresql.org (official documentation)
- [System Administration Functions (replication functions, pg_logical_emit_message)](https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-REPLICATION) — tier-1 — postgresql.org (official documentation)
- [System Information Functions and Operators (transaction ID and snapshot functions, pg_listening_channels, pg_notification_queue_usage)](https://www.postgresql.org/docs/current/functions-info.html) — tier-1 — postgresql.org
- [The Cumulative Statistics System (wait events, pg_stat_replication_slots, pg_stat_replication lag)](https://www.postgresql.org/docs/current/monitoring-stats.html) — tier-1 — postgresql.org (official documentation)
- [Message Flow (Asynchronous Operations)](https://www.postgresql.org/docs/current/protocol-flow.html) — tier-1 — postgresql.org (official documentation)
- [libpq Asynchronous Notification](https://www.postgresql.org/docs/current/libpq-notify.html) — tier-1 — postgresql.org (official documentation)
- [libpq Connection Strings: Parameter Key Words](https://www.postgresql.org/docs/current/libpq-connect.html) — tier-1 — postgresql.org
- [Hot Standby](https://www.postgresql.org/docs/current/hot-standby.html) — tier-1 — postgresql.org
- [DISCARD](https://www.postgresql.org/docs/current/sql-discard.html) — tier-1 — postgresql.org
- [Sequence Manipulation Functions](https://www.postgresql.org/docs/current/functions-sequence.html) — tier-1 — postgresql.org
- [CREATE SEQUENCE](https://www.postgresql.org/docs/current/sql-createsequence.html) — tier-1 — postgresql.org
- [Transactions and Identifiers](https://www.postgresql.org/docs/current/transaction-id.html) — tier-1 — postgresql.org
- [Object Identifier Types](https://www.postgresql.org/docs/current/datatype-oid.html) — tier-1 — postgresql.org
- [Transaction Isolation](https://www.postgresql.org/docs/current/transaction-iso.html) — tier-1 — postgresql.org
- [Function Volatility Categories](https://www.postgresql.org/docs/current/xfunc-volatility.html) — tier-1 — postgresql.org
- [Logical Decoding Concepts](https://www.postgresql.org/docs/current/logicaldecoding-explanation.html) — tier-1 — postgresql.org (official documentation)
- [Logical Decoding Output Plugins](https://www.postgresql.org/docs/current/logicaldecoding-output-plugin.html) — tier-1 — postgresql.org
- [Streaming of Large Transactions for Logical Decoding](https://www.postgresql.org/docs/current/logicaldecoding-streaming.html) — tier-1 — postgresql.org
- [Logical Replication Failover](https://www.postgresql.org/docs/current/logical-replication-failover.html) — tier-1 — postgresql.org
- [pg_replication_slots view](https://www.postgresql.org/docs/current/view-pg-replication-slots.html) — tier-1 — postgresql.org
- [Streaming Replication Protocol](https://www.postgresql.org/docs/current/protocol-replication.html) — tier-1 — postgresql.org
- [Logical Streaming Replication Protocol (pgoutput options)](https://www.postgresql.org/docs/current/protocol-logical-replication.html) — tier-1 — postgresql.org
- [Hint Bits](https://wiki.postgresql.org/wiki/Hint_Bits) — tier-2 — wiki.postgresql.org
- [async.c (master)](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/commands/async.c) — tier-1 — github.com/postgres/postgres (official source mirror)
- [async.c (REL_18_STABLE)](https://raw.githubusercontent.com/postgres/postgres/REL_18_STABLE/src/backend/commands/async.c) — tier-1 — github.com/postgres/postgres (official source mirror)
- [proc.c (REL_18_STABLE)](https://raw.githubusercontent.com/postgres/postgres/REL_18_STABLE/src/backend/storage/lmgr/proc.c) — tier-1 — github.com/postgres/postgres (official source mirror)
- [PostgreSQL source: src/backend/access/transam/xact.c](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/access/transam/xact.c) — tier-1 — raw.githubusercontent.com (postgres/postgres repository)
- [PostgreSQL source: src/backend/access/transam/xlog.c (master)](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/access/transam/xlog.c) — tier-1 — github.com/postgres/postgres
- [PostgreSQL source: src/include/replication/syncrep.h (master)](https://raw.githubusercontent.com/postgres/postgres/master/src/include/replication/syncrep.h) — tier-1 — github.com/postgres/postgres
- [heapam.c, PostgreSQL source](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/access/heap/heapam.c) — tier-1 — github.com/postgres/postgres
- [xid8funcs.c](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/utils/adt/xid8funcs.c) — tier-1 — github.com/postgres/postgres (PostgreSQL source)
- [procarray.c (GetSnapshotData)](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/storage/ipc/procarray.c) — tier-1 — github.com/postgres/postgres (PostgreSQL source)
- [pg_opclass.dat (master)](https://raw.githubusercontent.com/postgres/postgres/master/src/include/catalog/pg_opclass.dat) — tier-1 — github.com/postgres/postgres (PostgreSQL source)
- [pg_opclass.dat (REL_13_STABLE)](https://raw.githubusercontent.com/postgres/postgres/REL_13_STABLE/src/include/catalog/pg_opclass.dat) — tier-1 — github.com/postgres/postgres (PostgreSQL source)
- [PostgreSQL source: walsender.c](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/replication/walsender.c) — tier-1 — github.com (postgres source repository)
- [PostgreSQL source: snapbuild.c](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/replication/logical/snapbuild.c) — tier-1 — github.com (postgres source repository)
- [libpq fe-connect.c (setKeepalives*, setTCPUserTimeout)](https://raw.githubusercontent.com/postgres/postgres/master/src/interfaces/libpq/fe-connect.c) — tier-1 — github.com (postgres/postgres official mirror)
- [Backend pqcomm.c (PG_TCP_KEEPALIVE_IDLE mapping, pq_settcpusertimeout)](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/libpq/pqcomm.c) — tier-1 — github.com (postgres/postgres official mirror)
- [Commit 282b1cde: Optimize LISTEN/NOTIFY via shared channel map and direct advancement](https://github.com/postgres/postgres/commit/282b1cde9dedf456ecf02eb27caf086023a7bb71) — tier-1 — github.com (postgres/postgres)
- [Commit history for async.c](https://github.com/postgres/postgres/commits/master/src/backend/commands/async.c) — tier-1 — github.com (postgres/postgres)
- [pgsql: Optimize LISTEN/NOTIFY via shared channel map and direct advancement](https://www.postgresql.org/message-id/E1vgSld-000fFk-2J%40gemulon.postgresql.org) — tier-1 — postgresql.org (pgsql-committers archive)
- [pgsql: Fix NOTIFY wakeups for pre-commit LISTEN entries](https://postgrespro.com/list/id/E1wSH2y-001Mpi-1v@gemulon.postgresql.org) — tier-2 — postgrespro.com (pgsql-committers mirror)
- [Re: Proposal: Out-of-Order NOTIFY via GUC to Improve LISTEN/NOTIFY Throughput](https://www.postgresql.org/message-id/eb110ca0-52ba-433a-aa39-7ebb5453d4ef%40app.fastmail.com) — tier-2 — postgresql.org (pgsql-hackers archive)
- [Proposal: Out-of-Order NOTIFY via GUC to Improve LISTEN/NOTIFY Throughput (full thread)](https://www.postgresql.org/message-id/flat/CAK80=jipUfGC+UQSzeA4oCP9daRtHZGm2SQZWLxC9NWmVTDtRQ@mail.gmail.com) — tier-1 — postgresql.org (pgsql-hackers archive)
- [Rishu Bagga, Re: Out-of-Order NOTIFY proposal (WAL-based patch benchmark)](https://www.postgresql.org/message-id/CAK80=jipUfGC+UQSzeA4oCP9daRtHZGm2SQZWLxC9NWmVTDtRQ@mail.gmail.com) — tier-1 — postgresql.org (pgsql-hackers archive)
- [Rishu Bagga, Re: Out-of-Order NOTIFY proposal (lock held until durable)](https://www.postgresql.org/message-id/CAK80=jjphTvd0CNscRRWNXeRAACmKvcM7TDF_4JxmrMo7qzU6Q@mail.gmail.com) — tier-1 — postgresql.org (pgsql-hackers archive)
- [Tom Lane, Re: Out-of-Order NOTIFY proposal (ordering and NotifyQueueLock)](https://www.postgresql.org/message-id/1758148.1752802118@sss.pgh.pa.us) — tier-1 — postgresql.org (pgsql-hackers archive)
- [Tom Lane, Re: Out-of-Order NOTIFY proposal (WAL-transport redesign)](https://www.postgresql.org/message-id/1878165.1752858390@sss.pgh.pa.us) — tier-1 — postgresql.org (pgsql-hackers archive)
- [Joel Jacobson, Re: Out-of-Order NOTIFY proposal (1,000-client benchmark)](https://www.postgresql.org/message-id/7c8c61c2-4dbf-4961-9e0c-a5ec77d8f846@app.fastmail.com) — tier-1 — postgresql.org (pgsql-hackers archive)
- [[PATCH] Write Notifications Through WAL (Rishu Bagga)](https://www.postgresql.org/message-id/CAK80%3DjjvgWJL5EdzH_Bxc37gn53PJaB5sDXB3VPfJuN7G1xL5Q%40mail.gmail.com) — tier-1 — postgresql.org (pgsql-hackers mailing list)
- [[PATCH] Write Notifications Through WAL thread view](https://hackorum.dev/topics/52339) — tier-3 — hackorum.dev (mailing-list aggregator)
- [Re: Optimize LISTEN/NOTIFY (Tom Lane, 14 October 2025)](https://www.postgresql.org/message-id/1009807.1760476747%40sss.pgh.pa.us) — tier-1 — postgresql.org (pgsql-hackers mailing list)
- [Re: Optimize LISTEN/NOTIFY (Tom Lane, 23 September 2025)](https://www.postgresql.org/message-id/3095599.1758644879%40sss.pgh.pa.us) — tier-1 — postgresql.org (pgsql-hackers mailing list)
- [Re: unexpected lock waits (Martijn van Oosterhout, 2019)](https://postgresql.org/message-id/20190609202244.GA28125%40svana.org) — tier-1 — postgresql.org (PostgreSQL mailing list archive)
- [Issue with COMMITs stuck on "AccessExclusiveLock on object 0 of class 1262 of database 0"](https://www.postgresql.org/message-id/CADWG95t0j9zF0uwdcMH81KMnDsiTAVHxmBvgYqrRJcD-iLwQhw%40mail.gmail.com) — tier-3 — postgresql.org (mailing-list user report)
- [Aln Kapa, "Do not understand why this happens" (NOTIFY lock-wait log)](https://www.postgresql.org/message-id/CAJqqVEU2dvMO%2Bq%3DBjpkCnEopT-xiWjY2Pn3XmSN8D%3D6HVnDh3g%40mail.gmail.com) — tier-3 — postgresql.org (pgsql-general user list)
- [LISTEN carefully: how NOTIFY can trip up your database (LT version)](https://www.postgresql.eu/events/fosdem2026/sessions/session/7408-listen-carefully-how-notify-can-trip-up-your-database-lt-version/) — tier-2 — postgresql.eu (FOSDEM PGDay 2026 programme)
- [Postgres LISTEN/NOTIFY does not scale](https://www.recall.ai/blog/postgres-listen-notify-does-not-scale) — tier-3 — recall.ai (company engineering blog)
- [Postgres LISTEN/NOTIFY Actually Scales](https://dbos.dev/blog/postgres-listen-notify-scalability) — tier-3 — dbos.dev (company engineering blog)
- [Amazon EBS General Purpose SSD volumes](https://docs.aws.amazon.com/ebs/latest/userguide/general-purpose.html) — tier-1 — docs.aws.amazon.com
- [Amazon EBS volume types](https://docs.aws.amazon.com/ebs/latest/userguide/ebs-volume-types.html) — tier-1 — docs.aws.amazon.com
- [pgq.batch_event_sql.sql (PgQ source)](https://raw.githubusercontent.com/pgq/pgq/master/functions/pgq.batch_event_sql.sql) — tier-1 — github.com/pgq/pgq (PgQ source)
- [pgq structure/tables.sql](https://raw.githubusercontent.com/pgq/pgq/master/structure/tables.sql) — tier-1 — github.com/pgq/pgq (PgQ source)
- [pgq.create_queue.sql](https://raw.githubusercontent.com/pgq/pgq/master/functions/pgq.create_queue.sql) — tier-1 — github.com/pgq/pgq (PgQ source)
- [pgq.ticker.sql](https://raw.githubusercontent.com/pgq/pgq/master/functions/pgq.ticker.sql) — tier-1 — github.com/pgq/pgq (PgQ source)
- [pgq.maint_rotate_tables.sql](https://raw.githubusercontent.com/pgq/pgq/master/functions/pgq.maint_rotate_tables.sql) — tier-1 — github.com/pgq/pgq (PgQ source)
- [JdbcEventStorageEngine.java (Axon Framework source)](https://raw.githubusercontent.com/AxonFramework/AxonFramework/axon-4.10.x/eventsourcing/src/main/java/org/axonframework/eventsourcing/eventstore/jdbc/JdbcEventStorageEngine.java) — tier-1 — raw.githubusercontent.com (AxonFramework/AxonFramework)
- [insert_events.sql.eex (Commanded EventStore source)](https://raw.githubusercontent.com/commanded/eventstore/master/lib/event_store/sql/statements/insert_events.sql.eex) — tier-1 — raw.githubusercontent.com (commanded/eventstore)
- [Commanded EventStore schema initialisation (init.ex)](https://raw.githubusercontent.com/commanded/eventstore/master/lib/event_store/sql/init.ex) — tier-1 — raw.githubusercontent.com (commanded/eventstore repository)
- [Async Daemon (Marten documentation)](https://martendb.io/events/projections/async-daemon.html) — tier-1 — martendb.io
- [Emmett PostgreSQL append function, appendToStream.ts](https://raw.githubusercontent.com/event-driven-io/emmett/main/src/packages/emmett-postgresql/src/eventStore/schema/appendToStream.ts) — tier-1 — github.com/event-driven-io/emmett
- [Emmett PostgreSQL consumer read query, readMessagesBatch.ts](https://raw.githubusercontent.com/event-driven-io/emmett/main/src/packages/emmett-postgresql/src/eventStore/schema/readMessagesBatch.ts) — tier-1 — github.com/event-driven-io/emmett
- [Emmett PostgreSQL schema, tables.ts](https://raw.githubusercontent.com/event-driven-io/emmett/main/src/packages/emmett-postgresql/src/eventStore/schema/tables.ts) — tier-1 — github.com/event-driven-io/emmett
- [eugene-khyst/postgresql-event-sourcing README](https://raw.githubusercontent.com/eugene-khyst/postgresql-event-sourcing/main/README.md) — tier-3 — github.com/eugene-khyst
- [eugene-khyst/postgresql-event-sourcing V1__eventsourcing_tables.sql](https://raw.githubusercontent.com/eugene-khyst/postgresql-event-sourcing/main/event-sourcing-app/src/main/resources/db/migration/V1__eventsourcing_tables.sql) — tier-3 — github.com/eugene-khyst
- [How Postgres sequences issues can impact your messaging guarantees](https://event-driven.io/en/ordering_in_postgres_outbox/) — tier-2 — event-driven.io
- [Postgres sequences can commit out-of-order](https://blog.sequinstream.com/postgres-sequences-can-commit-out-of-order/) — tier-3 — blog.sequinstream.com
- [Generating truly sequential IDs in PostgreSQL](https://www.cloudscale.ch/en/engineering-blog/2025/10/09/generating-truly-sequential-ids-in-postgresql) — tier-3 — cloudscale.ch
- [Fun with MVCC](https://tech.precisionnutrition.com/postgres/databases/2020/03/06/fun-with-mvcc) — tier-3 — tech.precisionnutrition.com
- [BIGSERIAL and commit order of globalIndex](https://discuss.axoniq.io/t/bigserial-and-commit-order-of-globalindex/2368) — tier-3 — discuss.axoniq.io
- [Event Storage in Postgres (comments)](https://dev.to/kspeakman/event-storage-in-postgres-4dk2/comments) — tier-3 — dev.to
- [Use The Index, Luke: Paging Through Results (seek method, row values)](https://use-the-index-luke.com/sql/partial-results/fetch-next-page) — tier-2 — use-the-index-luke.com
- [Psycopg 3: Asynchronous operations (notifications, detecting disconnections)](https://www.psycopg.org/psycopg3/docs/advanced/async.html) — tier-1 — psycopg.org (official documentation)
- [Psycopg 3: Connection classes API (notifies, add_notify_handler, broken)](https://www.psycopg.org/psycopg3/docs/api/connections.html) — tier-1 — psycopg.org (official documentation)
- [Psycopg release notes (3.2, 3.2.4, 3.2.10)](https://www.psycopg.org/psycopg3/docs/news.html) — tier-1 — psycopg.org
- [Connection pools — checking and reconnecting, life cycle](https://www.psycopg.org/psycopg3/docs/advanced/pool.html) — tier-1 — psycopg.org
- [Psycopg 3 — psycopg_pool API](https://www.psycopg.org/psycopg3/docs/api/pool.html) — tier-1 — psycopg.org
- [psycopg_pool release notes](https://www.psycopg.org/psycopg3/docs/news_pool.html) — tier-1 — psycopg.org
- [Psycopg 3 — sql module (Identifier composition)](https://www.psycopg.org/psycopg3/docs/api/sql.html) — tier-1 — psycopg.org
- [psycopg connection.py source](https://raw.githubusercontent.com/psycopg/psycopg/master/psycopg/psycopg/connection.py) — tier-1 — raw.githubusercontent.com (psycopg/psycopg repository)
- [psycopg connection_async.py source](https://raw.githubusercontent.com/psycopg/psycopg/master/psycopg/psycopg/connection_async.py) — tier-1 — raw.githubusercontent.com (psycopg/psycopg repository)
- [psycopg _connection_base.py source](https://raw.githubusercontent.com/psycopg/psycopg/master/psycopg/psycopg/_connection_base.py) — tier-1 — raw.githubusercontent.com (psycopg/psycopg repository)
- [psycopg _acompat.py source](https://raw.githubusercontent.com/psycopg/psycopg/master/psycopg/psycopg/_acompat.py) — tier-1 — raw.githubusercontent.com (psycopg/psycopg repository)
- [psycopg issue #962 — notifications lost while notifies() is not running](https://github.com/psycopg/psycopg/issues/962) — tier-2 — github.com (psycopg issue tracker)
- [psycopg issue #1091 — notification backlog leak](https://github.com/psycopg/psycopg/issues/1091) — tier-2 — github.com (psycopg issue tracker)
- [Replication support · Issue #71 · psycopg/psycopg](https://github.com/psycopg/psycopg/issues/71) — tier-1 — github.com/psycopg (project issue tracker)
- [psycopg2 docs: extras (LogicalReplicationConnection, ReplicationCursor)](https://www.psycopg.org/docs/extras.html) — tier-1 — psycopg.org
- [Procrastinate API reference — worker options](https://procrastinate.readthedocs.io/en/stable/reference.html) — tier-1 — procrastinate.readthedocs.io
- [procrastinate/psycopg_connector.py](https://raw.githubusercontent.com/procrastinate-org/procrastinate/main/procrastinate/psycopg_connector.py) — tier-1 — raw.githubusercontent.com (procrastinate-org/procrastinate)
- [procrastinate/worker.py](https://raw.githubusercontent.com/procrastinate-org/procrastinate/main/procrastinate/worker.py) — tier-1 — raw.githubusercontent.com (procrastinate-org/procrastinate)
- [Procrastinate manager.py source](https://raw.githubusercontent.com/procrastinate-org/procrastinate/main/procrastinate/manager.py) — tier-1 — raw.githubusercontent.com (procrastinate-org/procrastinate repository)
- [Procrastinate utils.py source](https://raw.githubusercontent.com/procrastinate-org/procrastinate/main/procrastinate/utils.py) — tier-1 — raw.githubusercontent.com (procrastinate-org/procrastinate repository)
- [River internal/notifier/notifier.go](https://raw.githubusercontent.com/riverqueue/river/master/internal/notifier/notifier.go) — tier-1 — raw.githubusercontent.com (riverqueue/river)
- [River producer.go](https://raw.githubusercontent.com/riverqueue/river/master/producer.go) — tier-1 — raw.githubusercontent.com (riverqueue/river)
- [River client.go](https://raw.githubusercontent.com/riverqueue/river/master/client.go) — tier-1 — raw.githubusercontent.com (riverqueue/river repository)
- [River notifylimiter package](https://raw.githubusercontent.com/riverqueue/river/master/internal/notifylimiter/limiter.go) — tier-1 — raw.githubusercontent.com (riverqueue/river repository)
- [River pgx v5 driver Listener (river_pgx_v5_driver.go)](https://raw.githubusercontent.com/riverqueue/river/master/riverdriver/riverpgxv5/river_pgx_v5_driver.go) — tier-1 — raw.githubusercontent.com (riverqueue/river)
- [River: Using PgBouncer](https://riverqueue.com/docs/pgbouncer) — tier-1 — riverqueue.com
- [Oban lib/oban/stager.ex](https://raw.githubusercontent.com/oban-bg/oban/main/lib/oban/stager.ex) — tier-1 — raw.githubusercontent.com (oban-bg/oban)
- [Oban lib/oban/notifiers/postgres.ex](https://raw.githubusercontent.com/oban-bg/oban/main/lib/oban/notifiers/postgres.ex) — tier-1 — raw.githubusercontent.com (oban-bg/oban)
- [Oban.Notifier (notifier.ex)](https://raw.githubusercontent.com/oban-bg/oban/main/lib/oban/notifier.ex) — tier-1 — raw.githubusercontent.com (oban-bg/oban)
- [Oban notifier listeners registry (listeners.ex)](https://raw.githubusercontent.com/oban-bg/oban/main/lib/oban/notifiers/listeners.ex) — tier-1 — raw.githubusercontent.com (oban-bg/oban)
- [Oban Sonar (sonar.ex)](https://raw.githubusercontent.com/oban-bg/oban/main/lib/oban/sonar.ex) — tier-1 — raw.githubusercontent.com (oban-bg/oban)
- [Oban.Notifiers.Postgres](https://oban.hexdocs.pm/Oban.Notifiers.Postgres.html) — tier-1 — oban.hexdocs.pm (official Oban documentation)
- [Oban 2.14 Troubleshooting — PgBouncer](https://oban.hexdocs.pm/2.14.0/troubleshooting.html) — tier-1 — oban.hexdocs.pm
- [Postgrex.Notifications (notifications.ex)](https://raw.githubusercontent.com/elixir-ecto/postgrex/master/lib/postgrex/notifications.ex) — tier-1 — raw.githubusercontent.com (elixir-ecto/postgrex)
- [Postgrex.Notifications documentation](https://postgrex.hexdocs.pm/Postgrex.Notifications.html) — tier-1 — postgrex.hexdocs.pm
- [Graphile Worker FAQ](https://worker.graphile.org/docs/faq) — tier-1 — worker.graphile.org
- [Graphile Worker src/main.ts](https://raw.githubusercontent.com/graphile/worker/main/src/main.ts) — tier-1 — raw.githubusercontent.com (graphile/worker)
- [Action Cable PostgreSQL subscription adapter (postgresql.rb)](https://raw.githubusercontent.com/rails/rails/main/actioncable/lib/action_cable/subscription_adapter/postgresql.rb) — tier-1 — raw.githubusercontent.com (rails/rails)
- [GoodJob Notifier (notifier.rb)](https://raw.githubusercontent.com/bensheldon/good_job/main/lib/good_job/notifier.rb) — tier-1 — raw.githubusercontent.com (bensheldon/good_job)
- [GoodJob README](https://raw.githubusercontent.com/bensheldon/good_job/main/README.md) — tier-1 — raw.githubusercontent.com (bensheldon/good_job)
- [pgmq extension SQL](https://raw.githubusercontent.com/pgmq/pgmq/main/pgmq-extension/sql/pgmq.sql) — tier-1 — raw.githubusercontent.com (pgmq/pgmq repository)
- [PgQueuer schema DDL rendering](https://raw.githubusercontent.com/janbjorge/pgqueuer/main/pgqueuer/adapters/persistence/schema_ddl.py) — tier-1 — raw.githubusercontent.com (janbjorge/pgqueuer repository)
- [The notifier pattern for applications that use Postgres](https://brandur.org/notifier) — tier-3 — brandur.org (personal blog)
- [PgBouncer Features — SQL feature map for pooling modes](https://www.pgbouncer.org/features.html) — tier-1 — pgbouncer.org
- [PgBouncer Configuration](https://www.pgbouncer.org/config.html) — tier-1 — pgbouncer.org
- [PgBouncer config.md (source of the configuration docs)](https://raw.githubusercontent.com/pgbouncer/pgbouncer/master/doc/config.md) — tier-1 — raw.githubusercontent.com (pgbouncer/pgbouncer repository)
- [PgBouncer Usage — admin console commands](https://www.pgbouncer.org/usage.html) — tier-1 — pgbouncer.org
- [PgBouncer Changelog](https://www.pgbouncer.org/changelog.html) — tier-1 — pgbouncer.org
- [PgBouncer source, src/server.c](https://raw.githubusercontent.com/pgbouncer/pgbouncer/master/src/server.c) — tier-1 — github.com (pgbouncer/pgbouncer repository)
- [PgBouncer src/janitor.c](https://raw.githubusercontent.com/pgbouncer/pgbouncer/master/src/janitor.c) — tier-1 — raw.githubusercontent.com (pgbouncer/pgbouncer repository)
- [PgBouncer src/sbuf.c](https://raw.githubusercontent.com/pgbouncer/pgbouncer/master/src/sbuf.c) — tier-1 — raw.githubusercontent.com (pgbouncer/pgbouncer repository)
- [PgBouncer src/util.c](https://raw.githubusercontent.com/pgbouncer/pgbouncer/master/src/util.c) — tier-1 — raw.githubusercontent.com (pgbouncer/pgbouncer repository)
- [PgBouncer src/objects.c](https://raw.githubusercontent.com/pgbouncer/pgbouncer/master/src/objects.c) — tier-1 — raw.githubusercontent.com (pgbouncer/pgbouncer repository)
- [Feature: Listen/Notify Support with Transaction Pooling, issue #655](https://github.com/pgbouncer/pgbouncer/issues/655) — tier-3 — github.com (pgbouncer issue tracker)
- [PgBouncer LISTEN/NOTIFY in transaction mode](https://www.netdata.cloud/guides/pgbouncer/pgbouncer-listen-notify-transaction-mode/) — tier-3 — netdata.cloud (vendor guide)
- [PgDog: Pub/sub (LISTEN/NOTIFY in transaction mode)](https://docs.pgdog.dev/features/connection-pooler/pub_sub/) — tier-1 — docs.pgdog.dev
- [PgCat README — pool modes](https://github.com/postgresml/pgcat) — tier-1 — github.com (postgresml/pgcat repository)
- [Pgpool-II bug 116: LISTEN notifications not reliably delivered](https://www.pgpool.net/mantisbt/116.html) — tier-3 — pgpool.net (bug tracker)
- [Avoiding pinning an RDS Proxy](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/rds-proxy-pinning.html) — tier-1 — docs.aws.amazon.com
- [Creating a proxy for Amazon RDS](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/rds-proxy-creating.html) — tier-1 — docs.aws.amazon.com
- [CreateDBProxy API reference](https://docs.aws.amazon.com/AmazonRDS/latest/APIReference/API_CreateDBProxy.html) — tier-1 — docs.aws.amazon.com
- [Neon: Connection pooling](https://neon.com/docs/connect/connection-pooling) — tier-1 — neon.com
- [Supabase: Connect to your database](https://supabase.com/docs/guides/database/connecting-to-postgres) — tier-1 — supabase.com
- [Supavisor tenant configuration](https://raw.githubusercontent.com/supabase/supavisor/main/docs/configuration/tenants.md) — tier-1 — raw.githubusercontent.com (supabase/supavisor repository)
- [Supavisor client_handler.ex](https://raw.githubusercontent.com/supabase/supavisor/main/lib/supavisor/client_handler.ex) — tier-1 — raw.githubusercontent.com (supabase/supavisor repository)
- [PgBouncer in Azure Database for PostgreSQL Flexible Server](https://learn.microsoft.com/en-us/azure/postgresql/flexible-server/concepts-pgbouncer) — tier-1 — learn.microsoft.com
- [Cloud SQL for PostgreSQL: Managed Connection Pooling](https://docs.cloud.google.com/sql/docs/postgres/managed-connection-pooling) — tier-1 — docs.cloud.google.com
- [Network Load Balancers: connection idle timeout](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/network-load-balancers.html) — tier-1 — docs.aws.amazon.com
- [Troubleshoot NAT gateways](https://docs.aws.amazon.com/vpc/latest/userguide/nat-gateway-troubleshooting.html) — tier-1 — docs.aws.amazon.com
- [Configure Azure Load Balancer TCP reset and idle timeout](https://learn.microsoft.com/en-us/azure/load-balancer/load-balancer-tcp-idle-timeout) — tier-1 — learn.microsoft.com
- [Fast failover with Amazon Aurora PostgreSQL](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/AuroraPostgreSQL.BestPractices.FastFailover.html) — tier-1 — docs.aws.amazon.com
- [tcp(7) Linux manual page](https://man7.org/linux/man-pages/man7/tcp.7.html) — tier-1 — man7.org (Linux man-pages project)
- [IP Sysctl — Linux kernel documentation](https://docs.kernel.org/networking/ip-sysctl.html) — tier-1 — docs.kernel.org
- [RFC 9293: Transmission Control Protocol](https://www.rfc-editor.org/rfc/rfc9293) — tier-1 — rfc-editor.org (IETF)
- [RFC 5482: TCP User Timeout Option](https://www.rfc-editor.org/rfc/rfc5482) — tier-1 — rfc-editor.org (IETF)
- [xnu bsd/netinet/tcp.h](https://raw.githubusercontent.com/apple-oss-distributions/xnu/main/bsd/netinet/tcp.h) — tier-1 — github.com (apple-oss-distributions, Apple's official open-source release)
- [xnu bsd/netinet/tcp_timer.h](https://raw.githubusercontent.com/apple-oss-distributions/xnu/main/bsd/netinet/tcp_timer.h) — tier-1 — github.com (apple-oss-distributions)
- [xnu bsd/netinet/tcp_timer.c](https://raw.githubusercontent.com/apple-oss-distributions/xnu/main/bsd/netinet/tcp_timer.c) — tier-1 — github.com (apple-oss-distributions)
- [xnu bsd/netinet/tcp_usrreq.c](https://raw.githubusercontent.com/apple-oss-distributions/xnu/main/bsd/netinet/tcp_usrreq.c) — tier-1 — github.com (apple-oss-distributions)
- [When TCP sockets refuse to die](https://blog.cloudflare.com/when-tcp-sockets-refuse-to-die/) — tier-2 — blog.cloudflare.com
- [socket — Low-level networking interface](https://docs.python.org/3/library/socket.html) — tier-1 — docs.python.org
- [Python — asyncio synchronization primitives](https://docs.python.org/3/library/asyncio-sync.html) — tier-1 — docs.python.org
- [Python — asyncio event loop (call_soon_threadsafe)](https://docs.python.org/3/library/asyncio-eventloop.html) — tier-1 — docs.python.org
- [Patroni docs: Dynamic configuration (permanent replication slots)](https://patroni.readthedocs.io/en/latest/dynamic_configuration.html) — tier-1 — patroni.readthedocs.io
- [AWS docs: Overview of PostgreSQL logical replication with Aurora](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/AuroraPostgreSQL.Replication.Logical.html) — tier-1 — docs.aws.amazon.com
- [AWS docs: Setting up logical replication for Aurora PostgreSQL](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/AuroraPostgreSQL.Replication.Logical.Configure.html) — tier-1 — docs.aws.amazon.com
- [AWS docs: Performing logical replication for Amazon RDS for PostgreSQL](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/PostgreSQL.Concepts.General.FeatureSupport.LogicalReplication.html) — tier-1 — docs.aws.amazon.com
- [Supabase docs: Postgres Changes](https://supabase.com/docs/guides/realtime/postgres-changes) — tier-1 — supabase.com
