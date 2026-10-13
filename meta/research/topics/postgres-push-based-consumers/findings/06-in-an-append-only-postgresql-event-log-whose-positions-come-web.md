---
type: "topic-research"
id: "06-in-an-append-only-postgresql-event-log-whose-positions-come-web"
title: "In an append-only PostgreSQL event log whose positions come from a `BIGSERIAL` sequence, how can a consumer woken by a notification avoid skipping events that concurrent transactions commit out of position order — transaction-id or snapshot-based high-water marks (`pg_current_xact_id`, `pg_snapshot_xmin`), gap detection with timeouts, or serialised appends — and how does notification order relate to commit order?"
date: "2026-10-05T13:56:47+00:00"
author: "Toby Clemson"
producer: "research-topic"
status: "complete"
kind: "finding"
round: 1
depth: 3
question: "In an append-only PostgreSQL event log whose positions come from a `BIGSERIAL` sequence, how can a consumer woken by a notification avoid skipping events that concurrent transactions commit out of position order — transaction-id or snapshot-based high-water marks (`pg_current_xact_id`, `pg_snapshot_xmin`), gap detection with timeouts, or serialised appends — and how does notification order relate to commit order?"
source_profile: "web"
tags: ["research", "topic-research", "finding"]
last_updated: "2026-10-05T13:56:47+00:00"
last_updated_by: "Toby Clemson"
schema_version: 1
---

# In an append-only PostgreSQL event log whose positions come from a `BIGSERIAL` sequence, how can a consumer woken by a notification avoid skipping events that concurrent transactions commit out of position order — transaction-id or snapshot-based high-water marks (`pg_current_xact_id`, `pg_snapshot_xmin`), gap detection with timeouts, or serialised appends — and how does notification order relate to commit order?

## Question

An append-only PostgreSQL event table takes its positions from a `BIGSERIAL` sequence, so concurrent append transactions can commit in a different order from the positions they were given. How can a consumer that is woken by `LISTEN`/`NOTIFY` read the log without permanently skipping events? The candidate techniques are:

- a transaction-id or snapshot-based high-water mark (`pg_current_xact_id`, `pg_snapshot_xmin`);
- gap detection with timeouts;
- serialised appends.

The question also asks how the order in which notifications arrive relates to the order in which transactions commit.

## Findings

### Short answer

- **A notification is a doorbell, not a cursor.** PostgreSQL delivers a `NOTIFY` only after the sending transaction commits. It delivers messages from different transactions "in the order in which the transactions committed". It enforces this with a cluster-wide lock that each notifying transaction holds from pre-commit until after its commit record is flushed. `BIGSERIAL` positions are handed out at insert time, so under concurrency commit order and position order diverge. The notification for position 13 can arrive before the one for positions 11–12. Positions carried in payloads are therefore not monotonic. Whether events get skipped depends entirely on the query the consumer runs when it is woken.
- **Three read-side designs are correct. The most common shortcut, a wall-clock timeout on gaps, is not.**
  - **Transaction-id watermark.** Stamp each event with `pg_current_xact_id()`. Read only rows where `transaction_id < pg_snapshot_xmin(pg_current_snapshot())`, and resume from `(transaction_id, position)`. This cannot skip events by construction. The price is liveness: any transaction that is writing anywhere in the cluster holds delivery back.
  - **Gap detection.** This is correct only when, before skipping a hole, the consumer asks PostgreSQL whether a live transaction could still fill it, and gathers that evidence consistently. Gap detection that gives up after a wall-clock timeout silently loses late commits. Axon's defaults allow this. Marten shipped it until version 9.16.1 (July 2026).
  - **Serialised appends.** A transaction-level lock taken just before positions are allocated makes allocation order equal commit order. The consumer can then checkpoint a plain integer and skip any gap at once. The price is a throughput ceiling set by how long the lock is held, and that time includes the commit flush.
- **Logical decoding is a fourth route.** The server decodes committed transactions in commit order and never emits aborted ones, so no watermark is needed. It costs `wal_level = logical`, one slot per receiver and WAL retention. It also needs a streaming client, and psycopg 3 does not provide one.
- **Every design needs a timer as well as the notification.** Aborted appends never notify. A watermark can rise without a new notification arriving. Signals sent outside the append transaction can be lost.
- **Persisted transaction ids mean something only within one transaction-id counter lineage.**
  - `pg_upgrade` preserves the counter.
  - `pg_dump`/restore, logical replication, logical-mode blue/green deployments and AWS DMS do not.
  - Failover and point-in-time recovery can reissue both transaction ids and positions.

### Why sequence positions become visible out of order

A `BIGSERIAL` default calls `nextval` when the row is inserted, not when the transaction commits. Changes to sequence state are not transactional: other sessions see them at once, and a value is "not reclaimed for re-use if the calling transaction later aborts". Aborts, crashes and even `INSERT ... ON CONFLICT` therefore leave permanent gaps. The documentation states that sequences "cannot be used to obtain 'gapless' sequences". With a sequence cache larger than one, "you should only assume that the nextval values are all distinct, not that they are generated purely sequentially". The row itself becomes visible only when its transaction commits.

This is how events get lost:

1. Transaction T1 takes positions 11–12, and transaction T2 takes position 13.
2. T2 commits first.
3. A reader that runs `WHERE position > 10` between the two commits sees 13 but not 11–12.
4. If the reader then checkpoints at 13, it never sees 11–12.

Oskar Dudycz, Eugene Khyst's reference implementation, Sequin and Precision Nutrition all describe this failure. Precision Nutrition hit it in production, and also found about 0.6% of ids permanently missing. An "expect `last + 1`" check therefore cannot tell a transient gap (an append still in flight) from a permanent one (a rolled-back append). Every technique below either tells those two apart or prevents the transient case from arising.

### How notification order relates to commit order

The PostgreSQL 18 `NOTIFY` documentation guarantees four things:

- A notification is delivered only if and when the sending transaction commits.
- Notifications from one transaction arrive in the order they were sent. The exception is that identical notifications (same channel, same payload) within one transaction are folded into one. They are never folded across transactions.
- "Messages from different transactions are delivered in the order in which the transactions committed".
- A listening session that is inside a transaction receives nothing until that transaction ends.

**How it is enforced.** The enforcement lives in `src/backend/commands/async.c` and `xact.c`:

- **The lock.** If a transaction has pending notifications, `PreCommit_Notify` forces it to be assigned a transaction id. It then calls `LockSharedObject(DatabaseRelationId, InvalidOid, 0, AccessExclusiveLock)`, a heavyweight lock on "database 0" that is shared across the whole cluster. The source comment reads: "Serialize writers by acquiring a special lock that we hold till after commit. This ensures that queue entries appear in commit order, and in particular that there are never uncommitted queue entries ahead of committed ones, so an uncommitted transaction can't block delivery of deliverable notifications."
- **The commit sequence.** `CommitTransaction` in `xact.c` then runs these steps in order:
  1. `RecordTransactionCommit` flushes the commit record when `synchronous_commit` is above `off`. It also waits for synchronous standbys, during which the backend still "continue[s] to hold locks".
  2. `ProcArrayEndTransaction` runs.
  3. The locks are released.
  4. `AtCommit_Notify` signals the listeners.
- **The listener side.** Listening backends check `XidInMVCCSnapshot` before `TransactionIdDidCommit`, and they stop at any queue entry whose transaction is still in progress.

By the time a client receives a notification, then, the sending transaction has committed and its rows are visible to a new snapshot. A transaction with no pending notifications and no `LISTEN` actions returns from `PreCommit_Notify` at once and never touches the lock.

**Is the guarantee changing?** No released version up to and including PostgreSQL 19 changes it.

- **Commit 282b1cde.** This is Joel Jacobson's commit, reviewed by Tom Lane, from January 2026. The PostgreSQL 19 release notes describe it as "Improve NOTIFY to only wake up backends that are listening to specified notifications". It adds a shared channel map and "direct advancement".
  - It gives "integer multiples" more throughput when many listeners each care about only a few messages, and "no improvement for the simplest case where every listener reads every message".
  - A May 2026 follow-up commit (0f24332ae) fixed listeners missing messages after that change.
  - It leaves the commit-order lock alone. `PreCommit_Notify` on master still takes the lock it holds "till after commit".
- **The out-of-order setting.** A July 2025 proposal for an opt-in out-of-order delivery setting was rejected by Tom Lane. One reason was that async.c depends on "queue entries appear[ing] in commit order".
- **The WAL patch.** The "Write Notifications Through WAL" patch is uncommitted as of October 2026; its last version, v9, dates from November 2025. It would replace the heavyweight lock with a short hold on a lightweight queue lock. It still writes queue entries in commit order and keeps the listeners' snapshot-visibility check, so "it is impossible for a listener to come across a notification that is not yet committed".

**What this means for a consumer.**

1. **Notification order is commit order, not position order.** In the example above, T2's notification arrives before T1's. A consumer that checkpoints from payload positions skips events exactly as a naive poller does.
2. **Aborted appends send nothing.** A gap left by a rollback never triggers a wake-up, so any design that waits on gaps needs a timer.
3. **Notifications are not durable.** Only "the sessions currently listening on that notification channel are notified", and a payload carries no durable position. Payloads are limited to about 8000 bytes. The notification queue is capped at 8 GB; once it is full, "transactions calling `NOTIFY` will fail at commit". A listener stuck in a long transaction blocks queue cleanup.
4. **The read query must run on every wake-up, on every connect or reconnect, and on a timer.** The `LISTEN` documentation sets out the start-up order:
   1. Execute `LISTEN` and commit.
   2. Inspect the database state in a new transaction.
   3. Rely on notifications from then on.

   The first few notifications "might refer to updates already observed", which is harmless.
5. **The listening connection must stay outside a transaction.** Psycopg 3 recommends autocommit for timely notifications. It provides a `notifies()` generator with `timeout` and `stop_after` parameters.
6. **`LISTEN` and `NOTIFY` are rejected on a hot standby, and notifications are not replicated.** A consumer must listen on the primary even if it reads events from a replica. After a failover it must reconnect and re-read.

### Technique 1: a transaction-id high-water mark

**The query.** Each event records the 64-bit (`xid8`) id of the transaction that wrote it. The consumer reads only events from transactions older than the oldest one still running. Dudycz and Khyst both publish this pattern. Watermill's PostgreSQL adapter uses the same filter, with the primary key `(transaction_id, offset)`.

```sql
ALTER TABLE events ADD COLUMN transaction_id xid8 NOT NULL DEFAULT pg_current_xact_id();
CREATE INDEX ON events (transaction_id, position);

SELECT position, ...
FROM events
WHERE (transaction_id, position) > (%(last_transaction_id)s::xid8, %(last_position)s)
  AND transaction_id < pg_snapshot_xmin(pg_current_snapshot())
ORDER BY transaction_id, position
LIMIT %(batch_size)s;
```

**Why it cannot skip.** The argument rests on four documented facts:

- The documentation defines a snapshot's `xmin` as the "lowest transaction ID that was still active. All transaction IDs less than `xmin` are either committed and visible, or rolled back and dead."
- Transaction ids are assigned "sequentially to transactions from a global counter used by all databases within the PostgreSQL cluster".
- An id is assigned "when a transaction first writes to the database".
- `xid8` "does not wrap around during the life of an installation".

So any event that is not yet visible belongs to a transaction whose id is at least `xmin`, and any transaction that starts writing later gets a higher id. Once the cursor has passed `(x, p)` with `x < xmin`, no row can ever appear behind it. Gaps in `position` stop mattering, because the consumer waits on transactions rather than on position numbers.

**Ordering and the resume token.**

- **Order.** Events come out in the order their transactions first wrote. That is neither commit order nor position order, and the documentation warns that first-write order can differ from start order. Dudycz notes that this "may create problems if forgotten".
- **Resume token.** The resume token is the pair `(transaction_id, position)`. A single `BIGSERIAL` value is no longer enough to resume from.

**Computing it correctly.**

- **Don't assign the consumer an xid.** Compute the watermark in a transaction that has not written anything and has not called `pg_current_xact_id()`. Either of those assigns the consumer an xid. The snapshot leaves the caller's own xid out of its in-progress list but counts it in `xmin`, so the watermark is capped at the consumer's own xid. That causes a stall, not a skip. The documentation recommends `pg_current_xact_id_if_assigned()` when a transaction "might otherwise be read-only".
- **Stamp the top-level id.** Store the top-level transaction id, which is what `pg_current_xact_id()` returns. `pg_visible_in_snapshot` "will not give the correct answer for a subtransaction ID".

**What holds the watermark back.** `GetSnapshotData` in `procarray.c` loops over every process in the process array, with no database filter. It skips:

- processes that have no xid;
- processes flagged as running lazy `VACUUM` or logical decoding;
- xids at or above `xmax`.

For every other process, it lowers `xmin` to that process's xid. That gives the following.

- **Held back by:**
  - Any transaction that has written, in any database of the cluster. Khyst's README says "in the same database", but the source code shows that the whole cluster counts.
  - Long batch writes.
  - Sessions left idle in a transaction after writing.
  - Prepared two-phase transactions. These appear in the process array as dummy entries, `pg_xact_status` reports them as "in progress", and they survive crashes. They hold the watermark until `COMMIT PREPARED` or `ROLLBACK PREPARED`.
- **Not held back by:**
  - **Read-only transactions**, such as reports, dumps, or a psycopg session that only ran `SELECT`. They set `backend_xmin` and block VACUUM, but they have no `backend_xid`.
  - **Sessions shown as `idle in transaction (aborted)`.** `AbortTransaction()` has already removed their xid from the process array.
  - **Replication slots' `xmin`/`catalog_xmin` and `hot_standby_feedback`.** These act on VACUUM horizons in `ComputeXidHorizons`, not on snapshots.

Three practitioner sources describe the cost:

- **Khyst** says a long-running transaction can "effectively 'pause' all event handlers".
- **Sequin** rejects the approach because "if you have a transaction running against another table for two minutes, your cursor/query will be unable to read the last two minutes".
- **Dudycz** notes only that queries "may be a bit delayed if our transactions last long".

**Interaction with notifications.** This is an inference from the documented behaviour.

1. T2's notification can arrive while an older appender T1 is still open, or while an unrelated older write transaction T0 is still open.
2. The read then returns nothing for T2.
3. When T1 commits, it sends its own notification. T0 never will.
4. A consumer that simply sleeps until the next notification can therefore stall with committed events waiting.

The remedy is to re-check on a short timer whenever rows exist at or above the watermark, for example with `EXISTS (SELECT 1 FROM events WHERE transaction_id >= pg_snapshot_xmin(pg_current_snapshot()))`.

**Detecting and attributing a stall.** The defining symptom is a watermark that stops advancing while committed, visible rows pile up at or above it. On each wake-up, the consumer can cheaply:

- record the watermark and the time it last moved;
- measure the lag in transactions without assigning itself an xid, using `pg_snapshot_xmax(s) - pg_snapshot_xmin(s)` (after casting to a numeric type) or `age(pg_snapshot_xmin(s)::xid)`.

To attribute a stall, match the watermark xid against the only two kinds of holder that matter: sessions with a non-null `backend_xid`, and prepared transactions.

```sql
WITH w AS (SELECT pg_snapshot_xmin(pg_current_snapshot())::xid AS xmin)
SELECT 'session' AS holder, a.pid, a.datname, a.usename, a.backend_type,
       a.state, now() - a.xact_start AS open_for, age(a.backend_xid) AS xid_age
FROM pg_stat_activity a JOIN w ON a.backend_xid = w.xmin
UNION ALL
SELECT 'prepared', NULL, p.database, p.owner, NULL,
       'prepared', now() - p.prepared, age(p.transaction)
FROM pg_prepared_xacts p JOIN w ON p.transaction = w.xmin;
```

How to read the result:

- `idle in transaction` means a client is holding a write transaction open.
- `active` means a long statement is running inside a writing transaction.
- A row that appears only in `pg_prepared_xacts` is an orphaned two-phase commit. Prepared transactions have no backend, so they never appear in `pg_stat_activity`.
- No match usually means the holder ended between the two reads. Check again before raising an alert.

`pg_locks` gives a second view. Every transaction with an xid holds an exclusive lock on that xid, and `pid` is null for prepared transactions. A `RowExclusiveLock` on the event table shows whether the holder is an event-store writer. That distinction is for diagnosis only. An xid is fixed at a transaction's first write anywhere, so a transaction that wrote elsewhere first can later insert an event carrying its old, low xid. Skipping past an "unrelated" holder can therefore lose events.

Privileges and cost limit what a consumer can see:

- **Ordinary roles.** In other users' rows, `state`, `query` and `xact_start` are null for ordinary roles, but `backend_xid` is still filled in. `pg_read_all_stats` gives the full picture.
- **`pg_prepared_xacts`.** Reading it briefly locks shared transaction-manager state. Run attribution queries only after a stall has been detected, not on every notification.

**Bounding the stall with server settings.**

| Holder | Setting that bounds it | Notes |
|---|---|---|
| Session idle inside an open transaction | `idle_in_transaction_session_timeout` | Default `0` (off) |
| One long statement | `statement_timeout` | Per statement, default `0`; does not bound many short statements |
| Whole transaction, active or idle | `transaction_timeout` (PostgreSQL 17+) | Default `0`; "Prepared transactions are not subject to this timeout" |
| Prepared transaction | None; `max_prepared_transactions = 0` disables the feature | `0` is the default; changing it needs a restart |
| Session whose client has died | TCP keepalive settings, or `idle_in_transaction_session_timeout` | Keepalives are ignored on Unix-domain sockets |
| Replication slot or standby feedback | Not applicable | These hold back VACUUM only |

Some limits apply to these bounds:

- **Every writing role must be covered.** The watermark is cluster-wide, so a bound holds only if it covers every role that writes to any database on the cluster, including other applications and administrators.
- **Per-role settings can be overridden.** `ALTER ROLE ... SET` and `ALTER DATABASE ... SET` apply only to new sessions, and a session can override them with `SET`.
- **Worst-case delay.**
  - With every writing role covered, it is the largest effective `transaction_timeout` among those roles.
  - Without that coverage, a looser combination of `statement_timeout` and `idle_in_transaction_session_timeout` applies.
  - It is unbounded wherever a writing role has no timeout, or wherever prepared transactions are enabled.

A useful operating rule is to alert when the watermark has stalled for longer than the configured `transaction_timeout`. When that happens, it points to a role the timeouts do not cover, or to an orphaned prepared transaction.

**Narrowing the watermark to fewer transactions.** Any safe narrowing must follow one rule: a consumer may move past an ordering key only if it is still waiting on every transaction that could commit a row at or below that key. Transaction ids fail this rule when the "still in flight" test is "touches the event table", because a transaction can hold an old xid before it first touches the table. Mapping `pg_locks` holders on the event table to their xids is therefore unsafe. So is Sequin's own tag-matching `pg_stat_activity` filter, which Sequin calls brittle: it requires `state = 'active'` and so excludes appenders that are idle in a transaction after inserting. Two safe xid-based narrowings exist:

- **Filter to the event store's own database.** A backend is connected to exactly one database, so a transaction in another database cannot insert into this event table. The consumer can drop in-progress xids that belong to other databases:

  ```sql
  -- statement 1 (READ COMMITTED; this transaction has not written anything)
  SELECT pg_current_snapshot() AS snap,
         array(SELECT backend_xid FROM pg_stat_activity
               WHERE datname = current_database() AND backend_xid IS NOT NULL) AS local_xids;
  -- statement 2
  SELECT coalesce(
    (SELECT x FROM pg_snapshot_xip($snap) AS x
      WHERE x::xid = ANY($local_xids)
         OR x::xid IN (SELECT transaction FROM pg_prepared_xacts
                       WHERE database = current_database())
      ORDER BY x LIMIT 1),
    pg_snapshot_xmax($snap)) AS watermark;
  -- statement 3: read events WHERE transaction_id < watermark, ordered as before
  ```

  This is derived from documented behaviour; no consulted source publishes it.
  - **Isolation level.** It must run under READ COMMITTED. Under REPEATABLE READ, a transaction that ended between the snapshot and the view read would be both invisible and excluded, which skips its events.
  - **Fresh statistics.** It needs fresh statistics for each poll, from a new transaction or `pg_stat_clear_snapshot()`, because `pg_stat_activity` is cached for the rest of the transaction.
  - **Prepared transactions.** These must come from `pg_prepared_xacts`, read after `pg_stat_activity`.
  - **Limited benefit.** Long writers in the same database still hold the watermark back.
- **Snapshot-pair batches instead of a scalar watermark (PgQ).** In PgQ:
  - each event stores `ev_txid`;
  - a ticker records `txid_current_snapshot()` in `pgq.tick` rows;
  - a batch is the set of events visible in the current tick's snapshot but not in the previous one.

  `pgq.batch_event_sql` avoids the "problem with long transactions (xmin1 very low)". It scans only the range between consecutive ticks' `xmax` values and fetches the xids that "were committed between snapshots" by exact lookup. A long or unrelated transaction then delays nothing but its own events, and nothing is skipped. The costs follow from that design:
  - The checkpoint becomes a tick (a snapshot) rather than a position.
  - Delivery follows commit-visibility order, so a slow appender's low positions arrive after higher ones.
  - A ticker process is needed, tuned by `queue_ticker_max_count`, `queue_ticker_max_lag` and `queue_ticker_idle_period`.

  Using a snapshot's full in-progress list (`pg_snapshot_xip`, `pg_visible_in_snapshot`) to deliver committed transactions above `xmin` early amounts to the same bookkeeping. The checkpoint must then carry the set of transaction ids that were still open.

Position-based narrowings, which wait only for event-table appenders, are covered under gap detection below.

**Reading from a replica.** On a hot standby, the snapshot's `xmin` and `xmax` track the primary's running transactions as of the replayed WAL, through KnownAssignedXids. The in-progress list, however, is always empty: `GetSnapshotData`'s recovery branch puts every known xid in `subxip`, and "just leave[s] the xip array empty". The server's own `XidInMVCCSnapshot` handles this. `pg_current_snapshot()` does not, because it copies only `xcnt` and `xip`. So on a standby:

- `pg_snapshot_xip()` returns no rows.
- `pg_visible_in_snapshot(x, snap)` returns true for every `x` below `xmax`, including transactions still running on the primary.

This conclusion comes from reading the source and is not documented. An `xmin`-only watermark is still safe on a replica, because "once the commit record for a transaction is replayed on the standby, the changes made by that transaction will be visible to any new snapshots taken on the standby". Several constraints apply:

- **Compute and read on the same node.** Compute the watermark and read the events on the same replica, ideally in one statement or one REPEATABLE READ transaction. A watermark computed on the primary and applied to a lagging replica can skip events.
- **Use only `xmin`.** Never use `xip` membership or `pg_visible_in_snapshot` on a standby.
- **Keep transaction-id calls off the replica.** `pg_current_xact_id()` fails on a replica, so appends must stay on the primary.
- **Expect extra stalls.** The replica's watermark trails the primary by the replay lag. A primary transaction that dies without writing an abort record stays in KnownAssignedXids until the next running-xacts record. Those records are logged every 15 s when there is activity, and at checkpoints.
- **Expect different visibility order.** Visibility order can differ between nodes. Andrey Borodin's "Long Fork" reproducer shows the primary and a standby disagreeing on which of two transactions is visible. An `xmin` watermark does not depend on commit order, so it stays safe on each node, but the order seen through a replica can differ from the primary's commit order.
- **Close the gap after a notification.** A notification received from the primary may precede replay on the replica. There are three ways to close that gap:
  1. Use `synchronous_commit = remote_apply` on appending sessions. The documentation warns of "much larger commit delays".
  2. Poll `pg_last_wal_replay_lsn()` on the replica until it reaches the primary's `pg_current_wal_insert_lsn()` read after the notification.
  3. On PostgreSQL 19, use `WAIT FOR LSN ... ` in `standby_replay` mode. It must run before the reading transaction takes its snapshot.

### Technique 2: gap detection

**Gap tracking with timeouts.**

- **Axon.** Axon's `GapAwareTrackingToken` keeps the global sequence number plus "a set of possible gaps", described as "sequence numbers of events that may have been inserted but have not been committed". It re-checks those gaps on each batch. The JDBC storage engine defaults are `gapTimeout` 60,000 ms, `maxGapOffset` 10,000 and `gapCleaningThreshold` 250. Once a gap expires or falls more than the offset behind, a late commit into it is never delivered.
- **Marten.** Marten's async daemon keeps a "high water mark", described as the "furthest known event sequence that the daemon 'knows' that all events with that sequence or lower can be safely processed in order". It advances only through contiguous positions and long relied on a 3-second wall-clock threshold. Discussion #4953 (opened 14 July 2026) showed the daemon silently moving past committed events under concurrent appends. Four causes were identified:
  - gap checks made of several SQL statements each took a separate snapshot under READ COMMITTED;
  - catch-up and rebuild paths skipped gaps without checking whether they were stale;
  - when idle, the mark could jump to reserved positions on the wall-clock threshold alone;
  - the threshold "couldn't distinguish slow appends from rolled-back transactions".

**Asking PostgreSQL which appenders are still live.** Marten 9.16.1 (18 July 2026) made gap detection a single statement. It now skips a gap only when no live transaction could still hold the missing positions, judged from three pieces of evidence:

- a granted `RowExclusiveLock` on `mt_events` in `pg_locks`;
- `pg_snapshot_xip(pg_current_snapshot())`;
- `pg_stat_activity.xact_start`.

Otherwise "the daemon holds: a slow append gets waited out". Users who want a cap can opt in to `SkipStaleGapsDespiteLiveTransactionsAfter`. Marten recommends `idle_in_transaction_session_timeout` as a backstop, and it now logs every skip and records it in `mt_high_water_skips`.

The lesson is that timeouts alone trade lost events for lower latency. Gap detection becomes safe only when it asks PostgreSQL which transactions are still running, the same information the transaction-id watermark uses. In exchange, the consumer keeps a single integer position. Two conditions apply:

- All of the evidence must come from one consistent snapshot.
- The bound must be established no later than the snapshot that reads the rows.

**What makes a position bound safe.** Positions can satisfy the narrowing rule that xids fail, provided three conditions hold:

- **Lock before allocation.** `INSERT` acquires the target table's `RowExclusiveLock` during parse analysis (`setTargetTable`). A default `nextval()` is evaluated later, during execution, and the lock is "held until the end of the transaction". Allocating positions in a separate `nextval()` statement before the insert breaks this ordering.
- **`CACHE 1`.** Values are sequential only with sequence `CACHE 1`.
- **Gap tolerance.** Bound-based schemes read up to a bound rather than demanding a contiguous run, so permanent gaps from aborts do no harm.

**An appender-registered position bound (Sequin).**

- **Append side.** Anthony Accomazzo's Sequin design has each appending transaction take a marker advisory lock `(sequence oid, 0)`. It also takes an advisory lock keyed on the sequence's current `last_value`, which is a lower bound on any position it will receive. Both are taken before the insert.
- **Read side.** Readers stop below the lowest key held by sessions that hold both locks.

This narrows the wait to exactly the registered appenders, so unrelated transactions never delay reads. Measured against the PostgreSQL documentation, the published code has races, each of which can be fixed:

1. **Exclusive try-locks drop registrations.** `perform pg_try_advisory_xact_lock(next_val)` silently fails when two appenders read the same `last_value`. This sequence then loses an event:
   1. T1 locks 10 and gets 11.
   2. T2 fails to register and gets 12.
   3. T3 registers 12 and gets 13.
   4. The reader reads up to 12 and moves to 11.
   5. T3 commits, and the reader moves to 13.
   6. T2 commits 12, which is never read.

   Fix: use `pg_advisory_xact_lock_shared` for the position key.
2. **The bound is computed after the read's snapshot.** An appender that is in flight at the snapshot and commits before `pg_locks` is read is neither visible nor counted. Fix: compute the bound in an earlier statement.
3. **An empty registry means no bound.** Fix: read `last_value` first, then scan `pg_locks`, and use `min(lowest registered key, last_value)` as the bound.
4. **Positions above 2^32 are misread.** Rebuild the key as `(classid::bigint << 32) | objid::bigint`, filtered on `objsubid = 1`.
5. **Prepared transactions are missed.** A `pid` join misses them, because `pid` is null for them.
6. **Sequence setup.** The sequence needs `CACHE 1`, and a null `last_value` must be primed or coalesced.
7. **Cost of `pg_locks`.** The view is "not a consistent snapshot", and reading it very frequently "can impact database performance".

Every writer must follow this protocol, so it belongs in the store's single append path.

**A barrier lock.** A consumer can briefly take `LOCK TABLE events IN SHARE MODE`. `SHARE` conflicts with the `ROW EXCLUSIVE` lock that `INSERT` holds until commit. Once the lock is granted, every earlier inserter has ended and every later one waits. With `CACHE 1`, `max(position)` read under the lock is therefore a gap-free bound that needs nothing from writers. The cost is that appenders stall while the consumer waits for and holds the lock; `lock_timeout` can cap that wait. The approach suits occasional fences, not every poll, and no consulted store uses it.

**With notifications.** A gap left by an abort never triggers a wake-up, so expiring or re-checking gaps always needs a timer.

### Technique 3: serialised appends

**Mechanism.** The append transaction takes an exclusive lock before it allocates positions and holds it until the transaction ends. Three kinds of lock work:

- **`pg_advisory_xact_lock`.** Transaction-level advisory locks "are automatically released at the end of the transaction".
- **An `UPDATE` on a single counter row.** Row-level locks "are released at transaction end".
- **`LOCK TABLE ... IN EXCLUSIVE MODE`.** This conflicts with the `ROW EXCLUSIVE` lock that `INSERT` takes, while reads continue under `ACCESS SHARE`.

The next appender cannot allocate until the previous transaction has finished, so allocation order equals commit order. Whenever a reader sees position *p*, every lower position is either visible or permanently gone. The consumer can checkpoint the highest position it sees and skip gaps immediately.

**Prior art.**

- **Commanded EventStore (Elixir).** Every append runs `UPDATE streams SET stream_version = stream_version + $2 WHERE stream_id = 0 RETURNING stream_version - $2`. The row lock serialises all appends, and global positions are derived from the returned value rather than from a sequence. The counter update rolls back on abort, so positions have no gaps. A trigger on `streams` calls `pg_notify` with the payload `stream_uuid,stream_id,first_version,last_version`.
- **Message DB.** `write_message` first takes `pg_advisory_xact_lock(hash_64(category))`, then inserts into a table whose `global_position` is `bigserial`. Order is therefore guaranteed only within a category: category consumers are safe, but readers across categories are not.
- **Python `eventsourcing`.** It runs `LOCK TABLE ... IN EXCLUSIVE MODE` in every insert transaction so that "the insert order [is] the same as the commit order", and "so that readers don't pass over gaps that are filled in later". The lock timeout defaults to `0`, meaning wait indefinitely. Version 9.5 (psycopg 3) also issues a payload-less `NOTIFY` in the same transaction.
- **Precision Nutrition.** Their fix was a counter table (`UPDATE stream_ids SET id = id + n RETURNING ...`) feeding a separate `events_stream` table. They warn of deadlock risk with pessimistic locking. They also warn that doing this in a trigger makes concurrent writers wait for each other's transactions to complete.

**With notifications.** This is an inference. If each append issues `NOTIFY` in the same transaction, serialised appends make notification order, commit order and position order identical. Payload positions then arrive in increasing order, and with a gap-free counter row a missed notification shows up as a jump.

### What serialisation costs, with and without `NOTIFY`

**Both designs already serialise at commit.** The `NOTIFY` lock is held across the commit record's WAL flush and any synchronous-standby wait. An event-store lock taken just before allocation is released at the same point. One inference follows: concurrent notifying transactions cannot share a WAL flush through group commit, because a second one is blocked at pre-commit until the first has flushed. Each design therefore has a serialised window:

- **Unserialised appends that issue `NOTIFY`:** the notification-queue write, plus the commit flush, plus any synchronous-standby round trip.
- **Serialised appends:** all of the above, plus the allocation and `INSERT` work done after the lock is granted (indexes, triggers), plus any client round trip between taking the lock and sending `COMMIT`.
- **Unserialised appends without `NOTIFY` in the transaction:** no serialised window. Group commit can share flushes.

Throughput is capped at roughly one divided by the window, in transactions per second, multiplied by the batch size for events per second. So a global append lock adds serialisation only for the part of the append transaction outside the commit path, which `NOTIFY` already serialises. The design that scales with concurrency is unserialised appends with no `NOTIFY` inside the append transaction.

**Measured evidence.** No published benchmark compares these designs directly. The available figures follow.

Commanded EventStore uses both a counter row and in-transaction `NOTIFY`. In its README benchmark, N concurrent writers each append 100 events, against "a local PostgreSQL database"; no hardware or version is given.

| Concurrent writers | Events per operation | Time per operation | Derived events/s |
|---|---|---|---|
| 1 | 100 | 20.3 ms | ~4,900 |
| 10 | 1,000 | 127.4 ms | ~7,800 |
| 20 | 2,000 | 376.8 ms | ~5,300 |
| 50 | 5,000 | 582.4 ms | ~8,600 |

Going from 1 to 50 writers less than doubles throughput, while the time for a round of appends grows from about 20 ms to about 580 ms. That is the pattern a serialised section produces, although the benchmark does not isolate the lock from other limits.

PostgreSQL hackers' benchmarks of `INSERT` plus `NOTIFY` (pgbench, 100 clients) show what the lock costs. Arseniy Mukhin's independent run, with no listeners and 10-byte payloads, gave these TPS figures:

| Workload | Master | WAL-notify patch v6 | v7 |
|---|---|---|---|
| `INSERT` only | 30,981.6 | 34,940.9 | 30,933.0 |
| `INSERT` + `NOTIFY` | 817.9 | 33,761.7 | 33,870.5 |
| `NOTIFY` only | 87,750.5 | 36,141.2 | 34,261.9 |

Rishu Bagga's runs of the same patch gave 63,445–66,372 TPS against 29,191–30,908 TPS on master for `INSERT` then `NOTIFY`. The gap between the two master results (about 30k TPS against 818 TPS) suggests, by inference, that the cost depends on how slow WAL flushes are on the machine. Fast-flush hardware hides most of it, and flush-bound storage exposes all of it.

In the out-of-order thread, Jacobson measured 7,679 TPS with the lock against 95,430 TPS without it, using 1,000 notifying connections and one listener. As noted above, the WAL-notify patch is not committed. Whether its later versions release the queue lock before the commit flush has not been checked against the patch code; the v5 message states the aim of "eliminating the need for a global lock to be held from PreCommit_Notify to the end of transaction".

Production reports point the same way:

- **Recall.ai (July 2025).** Their system ran "tens of thousands of simultaneous writers" that each issued `NOTIFY`. Commits queued on "AccessExclusiveLock on object 0 of class 1262 of database 0", and one process acquired the lock after 1015.921 ms with more than 300 waiting. Throughput collapsed while database CPU and I/O "plummeted". They moved signalling into the application layer.
- **Sequin.** Counter-row serialisation gave "Write throughput was much lower than we needed, and the lock durations between concurrent writes was very high (seconds)", even with the counter update moved to the end of the transaction.
- **Dudycz.** He found that a singleton counter made "all the processing that's calling this function sequential" and judged "the performance hit … too big to accept".
- **Laurenz Albe (Cybertec).** He says a counter row "will serialize all transactions that use that 'sequence'. That will deteriorate your data modification performance considerably". The pgbench documentation calls the same situation "mostly ... measuring update contention".

**Levers that move the ceiling.**

- **Lock placement.** Take the lock as late as possible, and send the lock, the insert and `COMMIT` in one round trip. A server-side function does this, and so does psycopg 3 pipeline mode, which sends statements "without having to read the result of the previously sent query".
- **Allocate inside the append.** Marten's "Quick" mode, which allocates within the append, "appears to lead to a 40-50% time reduction" and is "substantially less likely to lead to gaps". It is the default from Marten 9.
- **Batch size.** More events per transaction spread each serialised window over more events.
- **Asynchronous commit.** `synchronous_commit = off`, which can be set per transaction, removes the flush from the window. The risk is losing acknowledged appends from up to three times `wal_writer_delay`: "data loss, not data corruption". By inference, consumers may already have acted on events that disappear after a crash.
- **Synchronous replication** puts a standby round trip inside every lock-holding window.
- **Consumer-side trade.** With serialised appends, a woken consumer reads everything after its last position without waiting. With unserialised appends, it waits behind the slowest in-flight append. Serialisation trades append throughput for zero gap-wait at the consumer.

### Taking the wake-up signal out of the append transaction

Only transactions that carry a `NOTIFY` pay for the commit-order lock. So the main lever is how many committing transactions carry one, not how many notifications are sent. Systems that face the same wake-up problem use four patterns.

1. **`NOTIFY` inside every append.** Commanded EventStore, Disintegrate (Rust), Procrastinate (Python) and `eventsourcing` 9.5 all do this. Payloads that differ are never folded, so every append takes the lock. In `eventsourcing`, appends are already serialised by the table lock, so the `NOTIFY` lock adds little (inference). Its subscription waits for notifications in 0.1 s slices and then reads up to 500 rows, so bursts collapse into one read.
2. **`NOTIFY` in the transaction, but rate-limited.**
   - **River (Go).** River removed its insert trigger in 0.5.0 (May 2024). A per-client limiter now piggy-backs a notification onto an insert only when that queue's last notification is older than `FetchCooldown` (default 100 ms). `FetchPollInterval` (default 1 s) acts as "a fallback".
   - **pgmq.** `enable_notify_insert(queue, throttle_interval_ms DEFAULT 250)` installs a deferrable constraint trigger. The trigger checks an `UNLOGGED` throttle table and calls `PG_NOTIFY` only when the interval has passed.

   These gates cap lock acquisitions per window. There are inferred caveats. A suppressed transaction that commits after the consumer has read waits for the next signal or poll. River's cap applies per process. pgmq's gate is itself a write to a shared row.
3. **Signal after commit, outside the append transaction.**
   - **River.** River wakes local producers only after a non-transactional insert has committed. The source states the rule: "Should only ever be invoked *outside* a transaction ... the producer wouldn't yet be able to access the new jobs".
   - **Oban.** Oban dropped its per-row trigger in migration V12 and sends the signal from the application after the insert returns. A leader-only Stager also publishes one batched notification every 1 s (limit 5,000 jobs). Oban's scaling guide recommends `insert_trigger: false`, "with the tradeoff that notifications from within a database transaction may be sent even if the transaction is rolled back".
   - **Akka Persistence R2DBC.** Akka publishes written events over cluster pub-sub, but keeps polling because "delivery of published messages are not guaranteed". Its defaults are a 3 s refresh interval, 100 ms behind current time, and backtracking over 2 minutes. Publishing turns off automatically when throughput exceeds 400, measured as an average over a 10 s interval.
   - **Recall.ai** moved signalling into the application layer.
4. **Coalescing on the consumer side.**
   - **Graphile Worker** staggers a `refetchDelay` across workers.
   - **River's `FetchCooldown`** fetches "*at most* this often".
   - **Brandur Leach's notifier pattern** has one `LISTEN` connection per process fan out to subscribers through non-blocking buffered channels. It drops a signal when a buffer is full, which is tolerable because the signal only means "go and read".

| Source | What was measured | Result |
|---|---|---|
| River | Job processing throughput, debounced in-transaction design (M2 MacBook Air, 2,000 goroutines) | About 46,000 jobs/s |
| Graphile Worker | Creation-to-start latency | 4.28 ms average batched; 4.47 ms unbatched |
| Graphile Worker | Throughput (i9-14900K, 4 processes × 24 concurrency) | 15,600 jobs/s unbatched; about 183,000 executed per second batched |
| Recall.ai | Commit lock wait with `NOTIFY` in write transactions | 1015.921 ms, with more than 300 waiters |

No published benchmark compares an event store using a debounced post-commit notifier with in-transaction `NOTIFY`. The following points are inferred from `PreCommit_Notify` and the systems above.

- **Append throughput.** A decoupled notifier removes the `NOTIFY` lock from the append path. Its own small transactions take the lock at most once per window per process. Appends still serialise on whatever ordering mechanism the store uses.
- **Wake-up latency by gate type.**
  - A gate that fires on the first append of a quiet window adds almost nothing for that append. Later appends in the window depend on the next signal or the poll.
  - A gate that fires at the end of the window caps the added latency at the window length plus delivery.
  - Firing at both ends gives a fast first wake-up and still covers the last commit.
- **Correctness rules.**
  - Send the signal only after the commit has returned.
  - The consumer still needs its own watermark or gap logic.
  - Keep a fallback poll, because a signal sent outside the transaction is lost if the process dies between commit and notify, or if the listening connection drops. Every such system above keeps one: River 1 s, Oban 1 s, Akka 3 s plus backtracking.

For psycopg 3 this fits naturally (inference). Use a dedicated autocommit connection that runs a debounced `pg_notify` after each append's commit returns. On the consumer side, use one `LISTEN` connection per process that turns every signal into "read after my checkpoint", with a poll as the fallback.

### Logical decoding instead of a watermark

**What it guarantees.**

- **Commit order.** "Concurrent transactions are decoded in commit order", and "Transactions that were rolled back explicitly or implicitly never get decoded".
- **Whole transactions.** `pgoutput` sends transactions one at a time, framed by Begin and Commit messages. Each Begin carries the transaction's final LSN, commit timestamp and xid. Large transactions are transmitted when their commit is decoded. Optional streaming of in-progress transactions defaults to `off`. It should stay off for an event store, because streamed blocks can end in a Stream Abort.
- **No skipped events and no watermark.** The consumer needs no `pg_snapshot_xmin` comparison, gap timeout or serialised append. By inference, delivery is also not held back by unrelated long-running transactions, because each transaction is sent when its own commit is decoded.
- **Stream order is commit order.** A consumer can see position 11 before position 10, so after switching to a slot, position must not be used as a skip watermark.

**Checkpointing by LSN.**

- **Which LSN to store.** Store the Commit message's end LSN. On restart, decoding skips a transaction when its commit record starts before the start point (`SnapBuildXactNeedsSkip` returns `ptr < start_decoding_at`). Restarting at the end LSN therefore skips the last processed transaction, while restarting at the commit LSN would resend it. This is a reading of the source code, and PostgreSQL's own apply worker records `end_lsn` in the same way.
- **Where streaming resumes.** `START_REPLICATION` begins at the requested LSN or the slot's `confirmed_flush_lsn`, "whichever is greater". The consumer should compare that with its own checkpoint before starting. If the slot is ahead, the slot has passed data the consumer never made durable; treat that as a gap and re-bootstrap (inference).
- **Duplicates.** A slot's position is persisted only at checkpoint, so after a server crash "recent changes" can "be sent again". Delivery is at-least-once, so skip repeats by LSN.
- **Feedback.** Report a flushed position only once it is durably checkpointed, because the server may discard everything before it.
- **Idle advancing.** Since PostgreSQL 15, `pgoutput` no longer sends empty transactions. A slot on a quiet table therefore receives mostly keepalives. Advance and persist the LSN from keepalives, as the apply worker does.
- **Atomic checkpoints.** A consumer writing to the same database can update a checkpoint row in the same transaction. Alternatively it can use replication origins (`pg_replication_origin_xact_setup`), which persist progress "in a crash safe manner".

**Handing off from a table read to the slot.** Creating a slot over the replication protocol with `CREATE_REPLICATION_SLOT ... LOGICAL pgoutput (SNAPSHOT 'export')` returns `consistent_point` and a `snapshot_name`. That snapshot "will show exactly the state of the database after which all changes will be included in the change stream". The handoff runs like this:

1. Leave the replication connection idle.
2. On an ordinary connection (psycopg 3 works here), run `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY; SET TRANSACTION SNAPSHOT '<name>'`.
3. Read the unprocessed rows.
4. Durably record `consistent_point`.
5. Run `START_REPLICATION` from that point.

Each transaction then arrives exactly once, either in the table read or in the stream. `SNAPSHOT 'use'` does the same on a single connection. PostgreSQL's table-sync worker and pgcopydb's `clone --follow` both use this pattern.

Some cautions apply:

- **Slot creation waits.** Creating the slot waits for every transaction that is already running.
- **Crash during catch-up.** A crash during catch-up loses the exported snapshot. Recreating the slot is safe only if catch-up processing is idempotent (inference).
- **SQL-function slots.** `pg_create_logical_replication_slot` exports no snapshot. A consumer limited to SQL functions can only overlap the two phases and deduplicate by event identity, and even that is not formally gap-free (inference).
- **Debezium.** Debezium's LSN-offset model gives at-least-once delivery, not this exact boundary.

**Cost of the two client routes.**

- **SQL functions on an ordinary connection.** This route covers `pg_logical_slot_peek_changes` and `pg_replication_slot_advance`, and it is the only route psycopg 3 supports natively. Each call:
  - builds a fresh decoding context and invalidates catalog caches;
  - re-reads WAL from the slot's `restart_lsn`, not from the confirmed position, including WAL for other databases;
  - stops at the flush pointer without waiting;
  - materialises the whole result before returning. `upto_nchanges` is checked only after each commit.

  Peek does not confirm, so looping on peek alone re-decodes everything since the last advance. Advance re-reads the same range in fast-forward mode. `restart_lsn` moves only at running-xacts records (about every 15 s under activity, and at checkpoints), and long transactions pin it. Only one session can read a slot at a time; a second fails with "active for PID". Under asynchronous commit, a `NOTIFY`-woken call may find nothing yet (inference). Supabase's walrus polls with the consuming `get` variant in production.
- **Streaming over a replication connection.** The walsender keeps one decoding context per session and decodes each WAL record once. Since PostgreSQL 16 it is woken by a condition-variable broadcast when WAL is flushed, so it pushes each transaction as soon as its commit is flushed. Synchronous replication is supported only on this interface.

**Operational cost.**

- **WAL level.** `wal_level = logical` needs a restart on PostgreSQL 18. PostgreSQL 19 raises the effective level automatically when the first logical slot is created.
- **Amazon RDS.** RDS needs the static `rds.logical_replication` parameter, a reboot and the `rds_replication` role. AWS warns that an unread slot can "quickly fill up your DB instance's storage".
- **Capacity.** `max_replication_slots` and `max_wal_senders` both default to 10.
- **Retention.** Slots retain WAL and catalog rows even with no connection; "In extreme cases this could cause the database to shut down to prevent transaction ID wraparound". `max_slot_wal_keep_size` and `idle_replication_slot_timeout` cap retention by invalidating the slot, and an invalidated slot cannot resume.
- **Failover.** A slot survives failover only with `failover = true`, `sync_replication_slots`, a physical slot, `hot_standby_feedback` and `synchronized_standby_slots`.
- **Output plugins.** PostgreSQL 18.6 (CVE-2026-6471) restricts output plugins to an `output_plugin_libraries` allowlist. By default it holds only `pgoutput` and `test_decoding`.
- **Poolers.** PgBouncer has supported replication connections since 1.23.0.

**Python client support.**

- **psycopg2** has a full streaming API: `LogicalReplicationConnection`, `start_replication` and `send_feedback`. It is "not expected to receive new features".
- **psycopg 3** has none. Issue #71 has been open since August 2021. PR #1219 (November 2025) only stops `START_REPLICATION` from failing, and the maintainer said "No further replication support is planned for release 3.3".
- **asyncpg** has none either; issue #91 has been open since 2017.
- **pypgoutput** "is a prototype and not production tested".

A practical split is psycopg2, or a separate decoder process, for slot creation and streaming, with psycopg 3 for the snapshot-import catch-up and checkpoint writes. An append transaction can also emit `pg_logical_emit_message(transactional => true, ...)`, a commit-ordered, durable signal that only a slot can read.

By inference, logical decoding is the stronger primitive for a live tail. It cannot replay history, it needs one slot per independent receiver, and its slots can be invalidated, so the event table must remain the source of truth for catch-up. It fits best as an optional accelerator: a single dispatcher tails one slot and fans events out in-process.

### Keeping checkpoints valid when the database moves or rewinds

**What an `xid8` promises.** `xid8` packs a 32-bit epoch above the 32-bit xid, and is promised not to wrap only "during the life of an installation". A stored `transaction_id` is ordinary column data that any copy carries along unchanged. Whether it stays meaningful depends on whether the destination continues the source's counter. The `xid8funcs.c` header notes that "xid8s frequently persist beyond the global xmin horizon, or may even be shipped to other machines".

**How each kind of move treats the counter and the sequence.**

- **`pg_upgrade`: the counter continues.** `copy_xact_xlog_xid` runs `pg_resetwal -x` and `-e` to carry the old next-xid and epoch, and has done so since at least 9.3. Stored `xid8` values, checkpoints and sequence state all remain valid. Logical slots migrate only from version 17.0 or later.
- **`pg_dump`/restore: positions survive, the xid space does not.** Sequence data is dumped by default, but the target keeps its own transaction counter. Three cases follow:
  1. **Target counter below every stored value (the usual case for a new cluster).** New events sort before history. A `transaction_id > last_processed_transaction_id` checkpoint skips every new event. `transaction_id < pg_snapshot_xmin(...)` is false for every migrated row, so an undrained consumer stalls. `pg_xact_status` raises "transaction ID … is in the future".
  2. **Target counter between the stored minimum and maximum.** Old and new rows interleave silently. This is the worst case, because nothing fails loudly.
  3. **Target counter above every stored value.** Ordering survives by luck, but stored ids alias unrelated transactions in the target.
- **Logical replication: the same three cases, plus traps.**
  - A published `transaction_id` arrives as the publisher's value, while apply transactions draw xids from the subscriber's counter.
  - "Sequence data is not replicated". Sequences must be set at cutover, or refreshed with `ALTER SUBSCRIPTION ... REFRESH SEQUENCES` on PostgreSQL 19, after appends stop.
  - Never run the `xmin`-predicate read against a live subscriber (inference from the documented semantics):
    1. Publisher transactions 100 and 101 overlap, and 101 commits first.
    2. On a subscriber whose counter is higher, 101's row passes `101 < xmin`, and the consumer advances past it.
    3. Row 100 then arrives and is skipped for good.
  - Re-stamping rows on the subscriber, by omitting the column from the publication's column list, collapses all pre-existing rows into the initial sync's single xid.
- **Managed services.**
  - **RDS Blue/Green, physical mode** (minor or no version change): green replays blue's WAL, so the xid space and sequences carry over. A sequence may resume up to one cached block (typically 32) ahead (inference).
  - **RDS Blue/Green, logical mode** (major-version upgrades), **and all Aurora PostgreSQL blue/green deployments**: "NEXTVAL operations on sequence objects aren't synchronized". RDS raises green's sequences during switchover, reported by events RDS-EVENT-0307, 0308 and 0310. AWS documents nothing about the transaction-id counter, which by inference is never reconciled.
  - **AWS DMS** tells operators to update `NEXTVAL` "after you stop the replication". It applies changes in its own transactions (default `MinTransactionSize` 1000, `CommitTimeout` 1 s), so target xids have no relation to source xids.
  - **Google Cloud DMS** warns that sequence state "might vary".
  - **What the switchover does.** The RDS switchover stops writes in both environments, drops connections, enforces `default_transaction_read_only`, and ends `LISTEN` sessions (`AdminShutdown`). DMS checkpoints "can't resume" across it.
- **Failover and point-in-time recovery: history is cut back.**
  - Asynchronous replication can lose committed transactions, and Patroni bounds that loss by `maximum_lag_on_failover`.
  - Synchronous replication protects acknowledged commits only. A cancelled synchronous wait leaves a transaction that "has already committed locally, but might not have been replicated", so a push-woken consumer can see an event that later disappears.
  - Point-in-time recovery starts a new timeline.
  - After the cut, lost transactions' xids are reissued, and LSNs past the switch point are reused. Sequence positions are reissued or skipped depending on whether the 32-value pre-log record replicated (inference from `sequence.c`).
  - The system identifier does not change.
  - External checkpoints can then point at phantom events, skip new events that reuse positions, or treat reused xids as already consumed. Kafka hit the same problem: a consumer "will just miss whatever data had been written between the truncation point and its fetch offset". Checkpoints stored in the same database, and updated in the same transaction as the projection, rewind together with the log and stay consistent.

**Detecting a foreign or rewound history.**

- **Foreign xid space.** On a cluster that produced its own data, every committed `transaction_id` is below the snapshot's `xmax`. `SELECT max(transaction_id) >= pg_snapshot_xmax(pg_current_snapshot()) FROM events` flags dump cases 1 and 2 and the subscriber trap. The store should refuse to append or deliver until this is remediated.
- **Bounds check on reconnect.** Compare the checkpoint with the table maximum, and call `pg_xact_status(stored_xid8)`. A checkpoint beyond the current end proves a rewind. The check stops working once new appends pass the old values.
- **Identity check.** Store the last consumed event's unique id with the position, and confirm that it is still at that position on resume. This survives position reuse.
- **Timeline epoch**, the analogue of Kafka's leader epoch (KIP-320):
  1. Store the timeline ID with each checkpoint. On PostgreSQL 16 and later, `pg_split_walfile_name(pg_walfile_name(pg_current_wal_lsn()))` returns it.
  2. Store an upper-bound LSN, `pg_current_wal_insert_lsn()` read after each read.
  3. When the timeline changes, compare that LSN with the switch point in the timeline history.

  Also store the system identifier from `pg_control_system()`. A changed identifier means a different cluster, not a rewind.

**Remedies.**

- **Fence the counter (self-hosted only).** With the server stopped, run `pg_resetwal -e E`, where `E = floor(max(transaction_id)::text::numeric / 4294967296) + 1`. Every new `xid8` then exceeds all migrated values. This is the documented use for replication systems "such as Slony-I and Skytools". It needs data-directory access, which hosted services deny.
- **An era column (works anywhere).**
  - Stamp each event `(era, transaction_id, position)`, and order and checkpoint on the triple.
  - Read with `era < current_era OR transaction_id < pg_snapshot_xmin(pg_current_snapshot())`.
  - Increment the era as the last step of every logical move.

  This is a design derived from the documented mechanics, not a documented product.
- **Position-only checkpoints.** These survive any move that carries the sequence: `pg_upgrade`, `pg_dump`, or `setval`/`REFRESH SEQUENCES` after logical moves. Marten's high-water mark, which asks whether "any transaction [is] still running that could have reserved it?", is the reference design.
- **Cutover procedure.**
  1. Stop appends on the source.
  2. Let the dump finish or replication drain.
  3. Move the sequence to at least `max(position) + 1`.
  4. Fence the counter or bump the era.
  5. Run the detection check.
  6. Record a fence position.
  7. Reopen appends and consumers.
  8. Recreate any logical slot after the fence.
- **Against rewinds.**
  - **Synchronous commit** prevents losing acknowledged appends.
  - **Gate delivery on the standby.** Release a batch only once a standby's `flush_lsn` passes the batch's LSN bound. This mirrors `synchronized_standby_slots` for logical slots, at the cost of about one replication lag of extra latency.
  - **On detecting a rewind,** stop with an explicit error (as Kafka's `LogTruncationException` does), or reset to the last verified position and rebuild. Debezium re-snapshots after failover.

### Choosing an approach for a notification-driven consumer

| Approach | Never skips? | Resume token | Delivery order | Concurrent appends | Main cost or risk | Wake-ups beyond the notification |
|---|---|---|---|---|---|---|
| `xid8` watermark (`pg_snapshot_xmin`) | Yes, by construction | `(xid8, position)` | First-write (xid) order | Unrestricted | Any writing transaction in the cluster stalls delivery; xids are invalid across logical moves without a fence or era | Timer while rows sit above the watermark |
| `xid8` watermark filtered to own database | Yes, under READ COMMITTED with fresh statistics | `(xid8, position)` | First-write order | Unrestricted | Long writers in the same database still stall it | As above |
| Snapshot-pair batches (PgQ) | Yes | Tick (snapshot) | Commit-visibility order | Unrestricted | Needs a ticker; positions arrive out of order | Ticker cadence |
| Gap detection, timeout only | No: commits after the timeout are lost | Integer plus gap set | Position order | Unrestricted | Silent loss (Axon defaults, Marten before 9.16.1) | Timer to expire gaps |
| Gap detection that checks live transactions | Yes, if the evidence is consistent and precedes the read | Integer | Position order | Unrestricted | Complexity; stalls behind live appenders | Timer |
| Appender-registered position bound (fixed Sequin scheme) | Yes | Integer | Position order | Unrestricted, but every appender must register | `pg_locks` reads; protocol discipline | Timer |
| Serialised appends | Yes | Integer (gap-free with a counter row) | Position equals commit order | One at a time, or per category | Throughput ceiling of one over the lock-hold window | None with in-transaction `NOTIFY`; a poll if the signal is decoupled |
| Logical decoding slot | Yes | Commit end LSN | Commit order | Unrestricted | `wal_level = logical`; slot retention and invalidation; no psycopg 3 streaming | None when streaming; a signal or timer for the SQL functions |

**Rules that hold whatever the approach.**

1. **Treat the notification as a doorbell.** Run the read query on every wake-up, on every connect or reconnect (`LISTEN`, commit, then read), and on a fallback timer. Never checkpoint from payload positions.
2. **Listen on the primary.** Use a dedicated autocommit connection, outside any transaction.
3. **Establish the bound before reading.** Gather whatever justifies advancing (the snapshot `xmin`, live-transaction evidence, or a registered bound) consistently, and no later than the snapshot that reads the rows. Compute it and read on the same node, and never use `xip`-based tests on a standby.
4. **Keep consumers from holding the watermark back.** Consumers must not write, or call `pg_current_xact_id()`, before computing an `xid8` watermark.
5. **Bound and monitor stalls.** Set `transaction_timeout` for every role that writes anywhere on the cluster, and keep `max_prepared_transactions = 0` unless a transaction manager is in use. Alert when the watermark stalls longer than the timeout.
6. **Tag external checkpoints.** If checkpoints live outside the database, store an era or timeline, the last event's identity and the system identifier with each one, and check them on resume.

**How the evidence points.** This is a synthesis of the above, not a documented recommendation.

- **Modest append concurrency.** Serialised appends give the simplest correct consumer: an integer checkpoint, no gap handling, and checkpoints that survive any move that carries the sequence or counter. Take the lock last, send it with the insert and `COMMIT` in one round trip, and keep append transactions short. `NOTIFY` inside the append adds little extra serialisation, because commits are already serialised; this is what `eventsourcing` does.
- **Concurrent append throughput matters.** Keep appends unserialised and take `NOTIFY` out of the append transaction: post-commit, debounced, with a poll fallback. The consumer then reads with one of two designs:
  - **The `xid8` watermark.** This needs stall monitoring and timeouts, optionally a database filter, and an era column or counter fence if checkpoints persist xids.
  - **Live-transaction-aware gap detection over positions.** This keeps a position-only checkpoint that survives moves.
- **Logical decoding** can be added as an accelerator for a single dispatcher where the server configuration and a streaming client are available. It does not replace table reads for catch-up and recovery.

## Sources

- [PostgreSQL 18 documentation: NOTIFY](https://www.postgresql.org/docs/current/sql-notify.html) — tier-1 — postgresql.org
- [PostgreSQL 18 documentation: LISTEN](https://www.postgresql.org/docs/current/sql-listen.html) — tier-1 — postgresql.org
- [PostgreSQL 18 documentation: System Information Functions (transaction ID and snapshot functions)](https://www.postgresql.org/docs/current/functions-info.html) — tier-1 — postgresql.org
- [PostgreSQL 18 documentation: Sequence Manipulation Functions](https://www.postgresql.org/docs/current/functions-sequence.html) — tier-1 — postgresql.org
- [PostgreSQL 18 documentation: Transactions and Identifiers](https://www.postgresql.org/docs/current/transaction-id.html) — tier-1 — postgresql.org
- [PostgreSQL 18 documentation: Explicit Locking](https://www.postgresql.org/docs/current/explicit-locking.html) — tier-1 — postgresql.org
- [PostgreSQL: CREATE SEQUENCE](https://www.postgresql.org/docs/current/sql-createsequence.html) — tier-1 — postgresql.org
- [PostgreSQL: pg_sequences](https://www.postgresql.org/docs/current/view-pg-sequences.html) — tier-1 — postgresql.org
- [PostgreSQL: Transaction Isolation](https://www.postgresql.org/docs/current/transaction-iso.html) — tier-1 — postgresql.org
- [PostgreSQL docs: The Cumulative Statistics System (pg_stat_activity)](https://www.postgresql.org/docs/current/monitoring-stats.html) — tier-1 — postgresql.org
- [PostgreSQL docs: pg_prepared_xacts](https://www.postgresql.org/docs/current/view-pg-prepared-xacts.html) — tier-1 — postgresql.org
- [PostgreSQL docs: PREPARE TRANSACTION](https://www.postgresql.org/docs/current/sql-prepare-transaction.html) — tier-1 — postgresql.org
- [PostgreSQL docs: pg_locks](https://www.postgresql.org/docs/current/view-pg-locks.html) — tier-1 — postgresql.org
- [PostgreSQL docs: Client Connection Defaults (statement, transaction and idle timeouts)](https://www.postgresql.org/docs/current/runtime-config-client.html) — tier-1 — postgresql.org
- [PostgreSQL docs: Resource Consumption (max_prepared_transactions)](https://www.postgresql.org/docs/current/runtime-config-resource.html) — tier-1 — postgresql.org
- [PostgreSQL docs: Connections and Authentication (client_connection_check_interval, TCP keepalives)](https://www.postgresql.org/docs/current/runtime-config-connection.html) — tier-1 — postgresql.org
- [PostgreSQL docs: Setting Parameters](https://www.postgresql.org/docs/current/config-setting.html) — tier-1 — postgresql.org
- [PostgreSQL 17 Release Notes](https://www.postgresql.org/docs/17/release-17.html) — tier-1 — postgresql.org
- [PostgreSQL 18 documentation: WAL Configuration](https://www.postgresql.org/docs/current/wal-configuration.html) — tier-1 — postgresql.org
- [PostgreSQL 18 documentation: Asynchronous Commit](https://www.postgresql.org/docs/current/wal-async-commit.html) — tier-1 — postgresql.org
- [PostgreSQL 18 documentation: pgbench](https://www.postgresql.org/docs/current/pgbench.html) — tier-1 — postgresql.org
- [PostgreSQL 18 docs: Write Ahead Log configuration](https://www.postgresql.org/docs/current/runtime-config-wal.html) — tier-1 — postgresql.org
- [PostgreSQL 19 docs: Write Ahead Log configuration](https://www.postgresql.org/docs/19/runtime-config-wal.html) — tier-1 — postgresql.org
- [PostgreSQL 19 Release Notes](https://www.postgresql.org/docs/19/release-19.html) — tier-1 — postgresql.org (official documentation)
- [PostgreSQL 18.6 release notes](https://www.postgresql.org/docs/release/18.6/) — tier-1 — postgresql.org
- [PostgreSQL 15.0 release notes](https://www.postgresql.org/docs/release/15.0/) — tier-1 — postgresql.org
- [PostgreSQL docs: Logical Decoding Concepts](https://www.postgresql.org/docs/current/logicaldecoding-explanation.html) — tier-1 — postgresql.org
- [PostgreSQL docs: Logical Decoding Output Plugins](https://www.postgresql.org/docs/current/logicaldecoding-output-plugin.html) — tier-1 — postgresql.org
- [PostgreSQL docs: Streaming of Large Transactions for Logical Decoding](https://www.postgresql.org/docs/current/logicaldecoding-streaming.html) — tier-1 — postgresql.org
- [PostgreSQL docs: Logical Streaming Replication Protocol](https://www.postgresql.org/docs/current/protocol-logical-replication.html) — tier-1 — postgresql.org
- [PostgreSQL docs: Logical Replication Message Formats](https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html) — tier-1 — postgresql.org
- [PostgreSQL docs: Streaming Replication Protocol](https://www.postgresql.org/docs/current/protocol-replication.html) — tier-1 — postgresql.org
- [PostgreSQL docs: Streaming Replication Protocol Interface (logical decoding)](https://www.postgresql.org/docs/current/logicaldecoding-walsender.html) — tier-1 — postgresql.org
- [PostgreSQL docs: Logical Decoding SQL Interface](https://www.postgresql.org/docs/current/logicaldecoding-sql.html) — tier-1 — postgresql.org
- [PostgreSQL docs: Logical Decoding Examples](https://www.postgresql.org/docs/current/logicaldecoding-example.html) — tier-1 — postgresql.org
- [PostgreSQL docs: System Administration Functions](https://www.postgresql.org/docs/current/functions-admin.html) — tier-1 — postgresql.org
- [PostgreSQL docs: Replication configuration](https://www.postgresql.org/docs/current/runtime-config-replication.html) — tier-1 — postgresql.org
- [PostgreSQL docs: pg_replication_slots](https://www.postgresql.org/docs/current/view-pg-replication-slots.html) — tier-1 — postgresql.org
- [PostgreSQL docs: SET TRANSACTION](https://www.postgresql.org/docs/current/sql-set-transaction.html) — tier-1 — postgresql.org
- [PostgreSQL docs: Replication Progress Tracking](https://www.postgresql.org/docs/current/replication-origins.html) — tier-1 — postgresql.org
- [PostgreSQL docs: pg_recvlogical](https://www.postgresql.org/docs/current/app-pgrecvlogical.html) — tier-1 — postgresql.org
- [pg_resetwal](https://www.postgresql.org/docs/current/app-pgresetwal.html) — tier-1 — postgresql.org
- [pg_upgrade](https://www.postgresql.org/docs/current/pgupgrade.html) — tier-1 — postgresql.org
- [pg_dump](https://www.postgresql.org/docs/current/app-pgdump.html) — tier-1 — postgresql.org
- [Logical Replication: Restrictions](https://www.postgresql.org/docs/current/logical-replication-restrictions.html) — tier-1 — postgresql.org
- [Logical Replication: Subscription](https://www.postgresql.org/docs/current/logical-replication-subscription.html) — tier-1 — postgresql.org
- [Logical Replication: Architecture](https://www.postgresql.org/docs/current/logical-replication-architecture.html) — tier-1 — postgresql.org
- [Logical Replication: Column Lists](https://www.postgresql.org/docs/current/logical-replication-col-lists.html) — tier-1 — postgresql.org
- [Logical Replication: Upgrade](https://www.postgresql.org/docs/current/logical-replication-upgrade.html) — tier-1 — postgresql.org
- [Logical Replication: Replicating Sequences (devel)](https://www.postgresql.org/docs/devel/logical-replication-sequences.html) — tier-1 — postgresql.org
- [pg_createsubscriber](https://www.postgresql.org/docs/current/app-pgcreatesubscriber.html) — tier-1 — postgresql.org
- [PostgreSQL docs: Log-Shipping Standby Servers (asynchronous and synchronous replication)](https://www.postgresql.org/docs/current/warm-standby.html) — tier-1 — postgresql.org
- [PostgreSQL docs: Failover](https://www.postgresql.org/docs/current/warm-standby-failover.html) — tier-1 — postgresql.org
- [PostgreSQL docs: Continuous Archiving and Point-in-Time Recovery (Timelines)](https://www.postgresql.org/docs/current/continuous-archiving.html) — tier-1 — postgresql.org
- [PostgreSQL docs: pg_rewind](https://www.postgresql.org/docs/current/app-pgrewind.html) — tier-1 — postgresql.org
- [PostgreSQL docs: Logical Replication Failover](https://www.postgresql.org/docs/current/logical-replication-failover.html) — tier-1 — postgresql.org
- [PostgreSQL Documentation: Hot Standby](https://www.postgresql.org/docs/current/hot-standby.html) — tier-1 — postgresql.org
- [PostgreSQL 19 beta Documentation: WAIT FOR](https://www.postgresql.org/docs/devel/sql-wait.html) — tier-1 — postgresql.org
- [PostgreSQL source: src/backend/commands/async.c (master)](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/commands/async.c) — tier-1 — github.com/postgres/postgres (official source mirror)
- [PostgreSQL source: src/backend/commands/async.c (REL_17_STABLE)](https://raw.githubusercontent.com/postgres/postgres/REL_17_STABLE/src/backend/commands/async.c) — tier-1 — github.com/postgres/postgres
- [async.c on REL_18_STABLE](https://raw.githubusercontent.com/postgres/postgres/REL_18_STABLE/src/backend/commands/async.c) — tier-1 — github.com (postgres/postgres official source mirror)
- [PostgreSQL source: src/backend/access/transam/xact.c (master)](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/access/transam/xact.c) — tier-1 — github.com/postgres (PostgreSQL source)
- [PostgreSQL source: src/backend/storage/ipc/procarray.c](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/storage/ipc/procarray.c) — tier-1 — github.com/postgres/postgres (official source mirror)
- [PostgreSQL source: xid8funcs.c](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/utils/adt/xid8funcs.c) — tier-1 — github.com/postgres/postgres (PostgreSQL source)
- [PostgreSQL source: transam.h](https://raw.githubusercontent.com/postgres/postgres/master/src/include/access/transam.h) — tier-1 — github.com/postgres/postgres (PostgreSQL source)
- [PostgreSQL source: pgstatfuncs.c (pg_stat_get_activity privilege handling)](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/utils/adt/pgstatfuncs.c) — tier-1 — github.com/postgres/postgres (raw.githubusercontent.com)
- [PostgreSQL source: parse_clause.c (setTargetTable)](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/parser/parse_clause.c) — tier-1 — github.com/postgres/postgres
- [PostgreSQL source: snapmgr.c (XidInMVCCSnapshot)](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/utils/time/snapmgr.c) — tier-1 — github.com (postgres/postgres official mirror)
- [PostgreSQL source: standby.c (LogStandbySnapshot)](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/storage/ipc/standby.c) — tier-1 — github.com (postgres/postgres official mirror)
- [PostgreSQL source: access/transam README (Transaction Emulation during Recovery)](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/access/transam/README) — tier-1 — github.com (postgres/postgres official mirror)
- [PostgreSQL source: bgwriter.c](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/postmaster/bgwriter.c) — tier-1 — PostgreSQL source (github.com/postgres/postgres mirror)
- [PostgreSQL source: sequence.c](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/commands/sequence.c) — tier-1 — raw.githubusercontent.com (postgres/postgres repository)
- [PostgreSQL source: varsup.c](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/access/transam/varsup.c) — tier-1 — raw.githubusercontent.com (postgres/postgres repository)
- [PostgreSQL source: syncrep.c](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/replication/syncrep.c) — tier-1 — raw.githubusercontent.com (postgres/postgres repository)
- [PostgreSQL source: xlogfuncs.c](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/access/transam/xlogfuncs.c) — tier-1 — raw.githubusercontent.com (postgres/postgres repository)
- [pg_upgrade.c (master)](https://raw.githubusercontent.com/postgres/postgres/master/src/bin/pg_upgrade/pg_upgrade.c) — tier-1 — github.com/postgres/postgres (PostgreSQL source)
- [pg_upgrade.c (REL9_3_STABLE)](https://raw.githubusercontent.com/postgres/postgres/REL9_3_STABLE/contrib/pg_upgrade/pg_upgrade.c) — tier-1 — github.com/postgres/postgres (PostgreSQL source)
- [pg_upgrade.c (REL9_6_STABLE)](https://raw.githubusercontent.com/postgres/postgres/REL9_6_STABLE/src/bin/pg_upgrade/pg_upgrade.c) — tier-1 — github.com/postgres/postgres (PostgreSQL source)
- [PostgreSQL source: snapbuild.c](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/replication/logical/snapbuild.c) — tier-1 — github.com/postgres/postgres (official source mirror)
- [PostgreSQL source: walsender.c](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/replication/walsender.c) — tier-1 — github.com/postgres/postgres (official source mirror)
- [PostgreSQL source: decode.c](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/replication/logical/decode.c) — tier-1 — github.com/postgres/postgres (official source mirror)
- [PostgreSQL source: tablesync.c](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/replication/logical/tablesync.c) — tier-1 — github.com/postgres/postgres (official source mirror)
- [PostgreSQL source: worker.c (master)](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/replication/logical/worker.c) — tier-1 — github.com/postgres/postgres (official source mirror)
- [PostgreSQL source: worker.c (REL_12_STABLE)](https://raw.githubusercontent.com/postgres/postgres/REL_12_STABLE/src/backend/replication/logical/worker.c) — tier-1 — github.com/postgres/postgres (official source mirror)
- [PostgreSQL source: logicalfuncs.c](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/replication/logical/logicalfuncs.c) — tier-1 — PostgreSQL source (github.com/postgres/postgres mirror)
- [PostgreSQL source: slotfuncs.c](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/replication/slotfuncs.c) — tier-1 — PostgreSQL source (github.com/postgres/postgres mirror)
- [PostgreSQL source: logical.c](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/replication/logical/logical.c) — tier-1 — PostgreSQL source (github.com/postgres/postgres mirror)
- [PostgreSQL source: slot.c](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/replication/slot.c) — tier-1 — PostgreSQL source (github.com/postgres/postgres mirror)
- [PostgreSQL source: walsender.h](https://raw.githubusercontent.com/postgres/postgres/master/src/include/replication/walsender.h) — tier-1 — PostgreSQL source (github.com/postgres/postgres mirror)
- [PostgreSQL source: walsender_private.h](https://raw.githubusercontent.com/postgres/postgres/master/src/include/replication/walsender_private.h) — tier-1 — PostgreSQL source (github.com/postgres/postgres mirror)
- [PostgreSQL commit: Optimize walsender wake up logic using condition variables](https://github.com/postgres/postgres/commit/bc971f4025c) — tier-1 — github.com/postgres/postgres
- [PostgreSQL commit 282b1cde: Optimize LISTEN/NOTIFY via shared channel map and direct advancement](https://github.com/postgres/postgres/commit/282b1cde9dedf456ecf02eb27caf086023a7bb71) — tier-1 — github.com/postgres/postgres (official source mirror)
- [PostgreSQL commit history for async.c, including 282b1cde9 and 0f24332ae](https://api.github.com/repos/postgres/postgres/commits?path=src/backend/commands/async.c&per_page=12) — tier-1 — api.github.com (postgres/postgres repository)
- [Proposal: Out-of-Order NOTIFY via GUC to Improve LISTEN/NOTIFY Throughput (thread)](https://www.postgresql.org/message-id/flat/CAK80%3DjiWjMAfjwJymPoG3%3DFNE%3DhHygn5dXaoax1BWQT1rJrCCw%40mail.gmail.com) — tier-1 — postgresql.org (pgsql-hackers archive)
- [Proposal: Out-of-Order NOTIFY via GUC (Rishu Bagga, opening message, 2025-07-18)](https://www.postgresql.org/message-id/CAK80%3DjiWjMAfjwJymPoG3%3DFNE%3DhHygn5dXaoax1BWQT1rJrCCw%40mail.gmail.com) — tier-1 — postgresql.org (pgsql-hackers archive)
- [Re: Proposal: Out-of-Order NOTIFY via GUC to Improve LISTEN/NOTIFY Throughput](https://www.postgresql.org/message-id/CAE7r3MJ%2BMscuq-O0EQot5pqZrW5kycv-z-9kkfJbCcXhfi3Cag%40mail.gmail.com) — tier-1 — postgresql.org (pgsql-hackers archive)
- [Re: Proposal: Out-of-Order NOTIFY via GUC (Tom Lane, WAL-transport design, 2025-07-18)](https://www.postgresql.org/message-id/1878165.1752858390%40sss.pgh.pa.us) — tier-1 — postgresql.org (pgsql-hackers archive)
- [\[PATCH\] Write Notifications Through WAL](https://www.postgresql.org/message-id/CAK80%3DjjvgWJL5EdzH_Bxc37gn53PJaB5sDXB3VPfJuN7G1xL5Q%40mail.gmail.com) — tier-1 — postgresql.org (pgsql-hackers archive)
- [\[PATCH\] Write Notifications Through WAL (thread)](https://www.postgresql.org/message-id/flat/CAK80=jgom1QLy=R3JaHpEqJMBwYKxOdjiDX4qywJ9G8-pgWySw@mail.gmail.com) — tier-1 — postgresql.org (pgsql-hackers archive)
- [Re: \[PATCH\] Write Notifications Through WAL (Arseniy Mukhin review, 2025-10-08)](https://www.postgresql.org/message-id/CAE7r3MKV7oewo6EGGLPMKQvv74ujPurjjDBU=K8KH8CYJStXaw@mail.gmail.com) — tier-1 — postgresql.org (pgsql-hackers archive)
- [Re: \[PATCH\] Write Notifications Through WAL (Arseniy Mukhin benchmarks, 2025-10-17)](https://www.postgresql.org/message-id/CAE7r3MLwRNyvmMd2kaefeNYTRjGD8wOxLzyGn8vexaeTJdASaw@mail.gmail.com) — tier-1 — postgresql.org (pgsql-hackers archive)
- [Re: \[PATCH\] Write Notifications Through WAL (Rishu Bagga, v9, 2025-11-25)](https://www.postgresql.org/message-id/CAK80=jgom1QLy=R3JaHpEqJMBwYKxOdjiDX4qywJ9G8-pgWySw@mail.gmail.com) — tier-1 — postgresql.org (pgsql-hackers archive)
- [Re: Optimize LISTEN/NOTIFY (Joel Jacobson, 2026-01-15)](https://www.postgresql.org/message-id/18521cfd-830e-4e8f-bcb0-eacee535480a@app.fastmail.com) — tier-1 — postgresql.org (pgsql-hackers archive)
- [Re: docs: LISTEN/NOTIFY performance considerations (Bruce Momjian, 2026-07-29)](https://www.postgresql.org/message-id/amop9PA1KhlY0S0z@momjian.us) — tier-1 — postgresql.org (pgsql-hackers archive)
- [Commit Sequence Numbers and Visibility (Jeff Davis, 2026-06-02)](https://www.postgresql.org/message-id/ea6ecdc74dbce67849526668a461bc4760241439.camel@j-davis.com) — tier-2 — pgsql-hackers mailing list, postgresql.org
- [Re: Commit Sequence Numbers and Visibility (Andrey Borodin, 2026-08-27)](https://www.postgresql.org/message-id/D82D6A49-851C-487A-BB41-022C801A39C9@yandex-team.ru) — tier-2 — pgsql-hackers mailing list, postgresql.org
- [Re: Commit Sequence Numbers and Visibility (Jeff Davis, 2026-09-01)](https://www.postgresql.org/message-id/799ffd05a291bb642e21d0acb83296f493a69da8.camel@j-davis.com) — tier-2 — pgsql-hackers mailing list, postgresql.org
- [Hot Standby](https://wiki.postgresql.org/wiki/Hot_Standby) — tier-2 — wiki.postgresql.org
- [Marten documentation: Async Daemon (High Water Mark)](https://martendb.io/events/projections/async-daemon) — tier-1 — martendb.io
- [Marten documentation: Appending Events](https://martendb.io/events/appending.html) — tier-1 — martendb.io
- [Async daemon can advance projection progress past committed events during concurrent appends (Discussion #4953)](https://github.com/JasperFx/marten/discussions/4953) — tier-2 — github.com/JasperFx/marten (project discussion forum)
- [Axon Framework API: JdbcEventStorageEngine.Builder (4.13)](https://apidocs.axoniq.io/4.13/org/axonframework/eventsourcing/eventstore/jdbc/JdbcEventStorageEngine.Builder.html) — tier-1 — apidocs.axoniq.io
- [Axon Framework API: GapAwareTrackingToken (4.13)](https://apidocs.axoniq.io/4.13/org/axonframework/eventhandling/GapAwareTrackingToken.html) — tier-1 — apidocs.axoniq.io
- [Message DB source: acquire-lock.sql](https://raw.githubusercontent.com/message-db/message-db/master/database/functions/acquire-lock.sql) — tier-1 — github.com/message-db/message-db (official project source)
- [Message DB source: write-message.sql](https://raw.githubusercontent.com/message-db/message-db/master/database/functions/write-message.sql) — tier-1 — github.com/message-db/message-db (official project source)
- [Message DB source: messages.sql](https://raw.githubusercontent.com/message-db/message-db/master/database/tables/messages.sql) — tier-1 — github.com/message-db/message-db (official project source)
- [Commanded EventStore source: insert_events.sql.eex](https://raw.githubusercontent.com/commanded/eventstore/master/lib/event_store/sql/statements/insert_events.sql.eex) — tier-1 — github.com/commanded/eventstore (official project source)
- [Commanded EventStore source: init.ex (notify_events trigger)](https://raw.githubusercontent.com/commanded/eventstore/master/lib/event_store/sql/init.ex) — tier-1 — github.com/commanded/eventstore (official project source)
- [Commanded EventStore README (benchmarks)](https://raw.githubusercontent.com/commanded/eventstore/master/README.md) — tier-1 — github.com/commanded
- [Commanded EventStore append benchmark source](https://raw.githubusercontent.com/commanded/eventstore/master/bench/storage/append_events_bench.exs) — tier-1 — github.com/commanded
- [Commanded EventStore Subscriptions guide](https://raw.githubusercontent.com/commanded/eventstore/master/guides/Subscriptions.md) — tier-1 — github.com/commanded
- [Commanded EventStore notifications listener](https://raw.githubusercontent.com/commanded/eventstore/master/lib/event_store/notifications/listener.ex) — tier-1 — github.com/commanded/eventstore
- [eventsourcing library PostgreSQL recorder source](https://raw.githubusercontent.com/pyeventsourcing/eventsourcing/main/eventsourcing/postgres.py) — tier-1 — github.com/pyeventsourcing
- [eventsourcing 9.5 postgres.py](https://raw.githubusercontent.com/pyeventsourcing/eventsourcing/9.5/eventsourcing/postgres.py) — tier-1 — github.com/pyeventsourcing/eventsourcing
- [eventsourcing 9.5 persistence.py](https://raw.githubusercontent.com/pyeventsourcing/eventsourcing/9.5/eventsourcing/persistence.py) — tier-1 — github.com/pyeventsourcing/eventsourcing
- [eventsourcing documentation: Persistence](https://eventsourcing.readthedocs.io/en/stable/topics/persistence.html) — tier-1 — eventsourcing.readthedocs.io
- [Disintegrate notify_event_listener function](https://raw.githubusercontent.com/disintegrate-es/disintegrate/main/disintegrate-postgres/src/listener/sql/fn_notify_event_listener.sql) — tier-1 — github.com/disintegrate-es/disintegrate
- [Disintegrate event insert trigger](https://raw.githubusercontent.com/disintegrate-es/disintegrate/main/disintegrate-postgres/src/listener/sql/trigger_notify_event_listener.sql) — tier-1 — github.com/disintegrate-es/disintegrate
- [Disintegrate Postgres event listener](https://raw.githubusercontent.com/disintegrate-es/disintegrate/main/disintegrate-postgres/src/listener.rs) — tier-1 — github.com/disintegrate-es/disintegrate
- [Procrastinate schema.sql](https://raw.githubusercontent.com/procrastinate-org/procrastinate/main/procrastinate/sql/schema.sql) — tier-1 — github.com/procrastinate-org/procrastinate
- [Akka Persistence R2DBC: Query plugin](https://doc.akka.io/libraries/akka-persistence-r2dbc/current/query.html) — tier-1 — doc.akka.io
- [Akka Persistence R2DBC reference.conf](https://raw.githubusercontent.com/akka/akka-persistence-r2dbc/main/core/src/main/resources/reference.conf) — tier-1 — github.com/akka/akka-persistence-r2dbc
- [PgQ source: pgq.batch_event_sql](https://raw.githubusercontent.com/pgq/pgq/master/functions/pgq.batch_event_sql.sql) — tier-1 — github.com/pgq/pgq
- [PgQ source: structure/tables.sql (pgq.tick, event template)](https://raw.githubusercontent.com/pgq/pgq/master/structure/tables.sql) — tier-1 — github.com/pgq/pgq
- [PgQ source: pgq.ticker](https://raw.githubusercontent.com/pgq/pgq/master/functions/pgq.ticker.sql) — tier-1 — github.com/pgq/pgq
- [Watermill SQL: PostgreSQL schema adapter](https://raw.githubusercontent.com/ThreeDotsLabs/watermill-sql/master/pkg/sql/schema_adapter_postgresql.go) — tier-1 — github.com/ThreeDotsLabs/watermill-sql
- [River CHANGELOG](https://raw.githubusercontent.com/riverqueue/river/master/CHANGELOG.md) — tier-1 — github.com/riverqueue/river
- [River internal/notifylimiter/limiter.go](https://raw.githubusercontent.com/riverqueue/river/master/internal/notifylimiter/limiter.go) — tier-1 — github.com/riverqueue/river
- [River client.go](https://raw.githubusercontent.com/riverqueue/river/master/client.go) — tier-1 — github.com/riverqueue/river
- [River benchmarks documentation](https://riverqueue.com/docs/benchmarks) — tier-1 — riverqueue.com
- [pgmq extension SQL (enable_notify_insert, notify_queue_listeners)](https://raw.githubusercontent.com/pgmq/pgmq/main/pgmq-extension/sql/pgmq.sql) — tier-1 — github.com/pgmq/pgmq
- [Oban documentation: Scaling](https://oban.hexdocs.pm/scaling.html) — tier-1 — oban.hexdocs.pm (official Oban docs)
- [Oban lib/oban/notifier.ex](https://raw.githubusercontent.com/oban-bg/oban/main/lib/oban/notifier.ex) — tier-1 — github.com/oban-bg/oban
- [Oban lib/oban/notifiers/postgres.ex](https://raw.githubusercontent.com/oban-bg/oban/main/lib/oban/notifiers/postgres.ex) — tier-1 — github.com/oban-bg/oban
- [Oban lib/oban/engine.ex](https://raw.githubusercontent.com/oban-bg/oban/main/lib/oban/engine.ex) — tier-1 — github.com/oban-bg/oban
- [Oban lib/oban/stager.ex](https://raw.githubusercontent.com/oban-bg/oban/main/lib/oban/stager.ex) — tier-1 — github.com/oban-bg/oban
- [Oban Postgres migration V01](https://raw.githubusercontent.com/oban-bg/oban/main/lib/oban/migrations/postgres/v01.ex) — tier-1 — github.com/oban-bg/oban
- [Oban Postgres migration V12](https://raw.githubusercontent.com/oban-bg/oban/main/lib/oban/migrations/postgres/v12.ex) — tier-1 — github.com/oban-bg/oban
- [Graphile Worker: Performance](https://worker.graphile.org/docs/performance) — tier-1 — worker.graphile.org
- [Graphile Worker: Configuration](https://worker.graphile.org/docs/config) — tier-1 — worker.graphile.org
- [Graphile Worker src/index.ts (localQueue and refetchDelay options)](https://raw.githubusercontent.com/graphile/worker/main/src/index.ts) — tier-1 — github.com/graphile/worker
- [Graphile Worker src/interfaces.ts (pollInterval option)](https://raw.githubusercontent.com/graphile/worker/main/src/interfaces.ts) — tier-1 — github.com/graphile/worker
- [The Notifier Pattern for Applications That Use Postgres](https://brandur.org/notifier) — tier-2 — brandur.org (practitioner writing by River's co-author)
- [Psycopg 3 documentation: Asynchronous operations and notifications](https://www.psycopg.org/psycopg3/docs/advanced/async.html) — tier-1 — psycopg.org
- [Psycopg 3 documentation: Pipeline mode support](https://www.psycopg.org/psycopg3/docs/advanced/pipeline.html) — tier-1 — psycopg.org
- [Psycopg 3 docs: Transactions management](https://www.psycopg.org/psycopg3/docs/basic/transactions.html) — tier-1 — psycopg.org
- [psycopg 3 docs: Cursor types (simple query protocol)](https://www.psycopg.org/psycopg3/docs/advanced/cursors.html) — tier-1 — psycopg.org
- [Psycopg 3 release notes](https://www.psycopg.org/psycopg3/docs/news.html) — tier-1 — psycopg.org
- [psycopg issue #71: Replication support](https://github.com/psycopg/psycopg/issues/71) — tier-1 — github.com (psycopg project tracker)
- [psycopg PR #1219: Don't fail on commands returning COPY_BOTH results](https://github.com/psycopg/psycopg/pull/1219) — tier-1 — github.com (psycopg project repository)
- [psycopg2 docs: Replication support objects (extras)](https://www.psycopg.org/docs/extras.html) — tier-1 — psycopg.org
- [psycopg2 source: lib/extras.py](https://raw.githubusercontent.com/psycopg/psycopg2/master/lib/extras.py) — tier-1 — github.com/psycopg/psycopg2
- [psycopg2 on PyPI](https://pypi.org/pypi/psycopg2/json) — tier-1 — pypi.org
- [asyncpg issue #91: Add support for logical replication protocol](https://github.com/MagicStack/asyncpg/issues/91) — tier-1 — github.com (asyncpg project tracker)
- [pypgoutput README](https://github.com/dgea005/pypgoutput) — tier-1 — github.com (pypgoutput project repository)
- [wal2json README](https://github.com/eulerto/wal2json) — tier-1 — github.com (wal2json project repository)
- [supabase walrus README](https://github.com/supabase/walrus) — tier-1 — github.com/supabase (project repository)
- [pgcopydb docs: pgcopydb follow](https://pgcopydb.readthedocs.io/en/latest/ref/pgcopydb_follow.html) — tier-1 — pgcopydb.readthedocs.io (official pgcopydb documentation)
- [Debezium connector for PostgreSQL](https://debezium.io/documentation/reference/stable/connectors/postgresql.html) — tier-1 — debezium.io
- [PgBouncer changelog](https://www.pgbouncer.org/changelog.html) — tier-1 — pgbouncer.org
- [PgBouncer configuration reference](https://www.pgbouncer.org/config.html) — tier-1 — pgbouncer.org
- [Patroni docs: Replication modes](https://patroni.readthedocs.io/en/latest/replication_modes.html) — tier-1 — patroni.readthedocs.io
- [KIP-320: Allow fetchers to detect and handle log truncation](https://cwiki.apache.org/confluence/display/KAFKA/KIP-320%3A+Allow+fetchers+to+detect+and+handle+log+truncation) — tier-1 — cwiki.apache.org (Apache Kafka)
- [Amazon RDS for PostgreSQL: Performing logical replication](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/PostgreSQL.Concepts.General.FeatureSupport.LogicalReplication.html) — tier-1 — docs.aws.amazon.com
- [Limitations and considerations for Amazon RDS blue/green deployments](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/blue-green-deployments-considerations.html) — tier-1 — docs.aws.amazon.com (Amazon RDS User Guide)
- [Switching a blue/green deployment in Amazon RDS](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/blue-green-deployments-switching.html) — tier-1 — docs.aws.amazon.com (Amazon RDS User Guide)
- [PostgreSQL replication methods for blue/green deployments](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/blue-green-deployments-replication-type.html) — tier-1 — docs.aws.amazon.com (Amazon RDS User Guide)
- [Creating a blue/green deployment in Amazon RDS](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/blue-green-deployments-creating.html) — tier-1 — docs.aws.amazon.com (Amazon RDS User Guide)
- [Overview of Amazon RDS Blue/Green Deployments](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/blue-green-deployments-overview.html) — tier-1 — docs.aws.amazon.com (Amazon RDS User Guide)
- [Amazon RDS event categories and event messages](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_Events.Messages.html) — tier-1 — docs.aws.amazon.com (Amazon RDS User Guide)
- [BlueGreenDeployment](https://docs.aws.amazon.com/AmazonRDS/latest/APIReference/API_BlueGreenDeployment.html) — tier-1 — docs.aws.amazon.com (Amazon RDS API Reference)
- [Limitations and considerations for Amazon Aurora blue/green deployments](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/blue-green-deployments-considerations.html) — tier-1 — docs.aws.amazon.com (Amazon Aurora User Guide)
- [Using a PostgreSQL database as an AWS DMS source](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Source.PostgreSQL.html) — tier-1 — docs.aws.amazon.com (AWS DMS User Guide)
- [Using a PostgreSQL database as a target for AWS Database Migration Service](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Target.PostgreSQL.html) — tier-1 — docs.aws.amazon.com (AWS DMS User Guide)
- [Target metadata task settings](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Tasks.CustomizingTasks.TaskSettings.TargetMetadata.html) — tier-1 — docs.aws.amazon.com (AWS DMS User Guide)
- [Change processing tuning settings](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Tasks.CustomizingTasks.TaskSettings.ChangeProcessingTuning.html) — tier-1 — docs.aws.amazon.com (AWS DMS User Guide)
- [Creating tasks for ongoing replication using AWS DMS](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Task.CDC.html) — tier-1 — docs.aws.amazon.com (AWS DMS User Guide)
- [Best practices for AWS Database Migration Service](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_BestPractices.html) — tier-1 — docs.aws.amazon.com (AWS DMS User Guide)
- [Migrating data from PostgreSQL databases with homogeneous data migrations in AWS DMS](https://docs.aws.amazon.com/dms/latest/userguide/dm-migrating-data-postgresql.html) — tier-1 — docs.aws.amazon.com (AWS DMS User Guide)
- [Database Migration Service for PostgreSQL known limitations](https://docs.cloud.google.com/database-migration/docs/postgres/known-limitations) — tier-1 — docs.cloud.google.com (Google Cloud documentation)
- [Gaps in sequences in PostgreSQL (Laurenz Albe)](https://www.cybertec-postgresql.com/en/gaps-in-sequences-postgresql/) — tier-2 — cybertec-postgresql.com
- [Laurenz Albe: Four reasons why VACUUM won't remove dead rows from a table](https://www.cybertec-postgresql.com/en/reasons-why-vacuum-wont-remove-dead-rows/) — tier-2 — cybertec-postgresql.com
- [How Postgres sequences issues can impact your messaging guarantees (Oskar Dudycz)](https://event-driven.io/en/ordering_in_postgres_outbox/) — tier-2 — event-driven.io
- [Push-based Outbox Pattern with Postgres Logical Replication (Oskar Dudycz, 2022)](https://event-driven.io/en/push_based_outbox_pattern_with_postgres_logical_replication/) — tier-3 — event-driven.io (personal blog)
- [postgresql-event-sourcing README (Eugene Khyst)](https://github.com/eugene-khyst/postgresql-event-sourcing) — tier-3 — github.com (personal repository)
- [Postgres sequences can commit out-of-order (Anthony Accomazzo)](https://blog.sequinstream.com/postgres-sequences-can-commit-out-of-order/) — tier-3 — blog.sequinstream.com
- [Postgres LISTEN/NOTIFY does not scale](https://www.recall.ai/blog/postgres-listen-notify-does-not-scale) — tier-3 — recall.ai
- [Fun with Multiversion Concurrency Control (Part 1)](https://tech.precisionnutrition.com/postgres/databases/2020/03/06/fun-with-mvcc) — tier-3 — tech.precisionnutrition.com
- [Fun with Multiversion Concurrency Control - Part 2](https://tech.precisionnutrition.com/postgres/databases/2020/05/15/fun-with-mvcc-2) — tier-3 — tech.precisionnutrition.com
- [Postgres Replication Slots: Confirmed Flush LSN vs. Restart LSN](https://www.morling.dev/blog/postgres-replication-slots-confirmed-flush-lsn-vs-restart-lsn/) — tier-3 — morling.dev (personal blog)
