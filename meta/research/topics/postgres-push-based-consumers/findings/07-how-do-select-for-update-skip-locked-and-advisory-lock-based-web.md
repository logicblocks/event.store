---
type: "topic-research"
id: "07-how-do-select-for-update-skip-locked-and-advisory-lock-based-web"
title: "How do `SELECT ... FOR UPDATE SKIP LOCKED` and advisory-lock-based claiming distribute work among competing consumers, how do they combine with notification-driven wake-ups, and how do they preserve per-stream or per-partition ordering?"
date: "2026-10-05T13:56:47+00:00"
author: "Toby Clemson"
producer: "research-topic"
status: "complete"
kind: "finding"
round: 1
depth: 3
question: "How do `SELECT ... FOR UPDATE SKIP LOCKED` and advisory-lock-based claiming distribute work among competing consumers, how do they combine with notification-driven wake-ups, and how do they preserve per-stream or per-partition ordering?"
source_profile: "web"
tags: ["research", "topic-research", "finding"]
last_updated: "2026-10-05T13:56:47+00:00"
last_updated_by: "Toby Clemson"
schema_version: 1
---

# How do `SELECT ... FOR UPDATE SKIP LOCKED` and advisory-lock-based claiming distribute work among competing consumers, how do they combine with notification-driven wake-ups, and how do they preserve per-stream or per-partition ordering?

## Question

Several consumer processes compete for work in a PostgreSQL-backed queue or event store. This finding covers three things:

- how `SELECT ... FOR UPDATE SKIP LOCKED` and advisory locks decide which consumer gets which item;
- how that claiming fits behind `LISTEN`/`NOTIFY` wake-ups, so consumers react to new work without polling on a fixed interval;
- what keeps events for the same stream or partition in order.

The question also takes in the issues that decide whether such a design is correct in practice:

- transactions that commit out of position order;
- what happens when a claim holder fails;
- the cost that `NOTIFY` adds to every append;
- how stream partitions are shared among the members of a consumer group.

## Findings

### In brief

`FOR UPDATE SKIP LOCKED` and advisory locks are ways to claim work. Neither one orders work. Both let several consumers run the same ordered claim query at the same time, each get a different item, and never wait for one another:

- SKIP LOCKED passes over rows that another transaction has locked.
- With advisory locks, a consumer tries an integer key that the application chooses, which lives in shared memory rather than on a tuple, and moves on if the try fails.

Both are built so that a second consumer can take the next item while the first is still working. That is exactly why neither keeps per-key order on its own.

Five conclusions run through the rest of this finding:

1. **Ordering needs a second mechanism layered on claiming.**
   - Job queues that guarantee per-key order only let a key's head item be claimed while an earlier item for that key is in flight.
   - Event stores more often claim a whole ordering unit (a subscription, stream or virtual partition) and have the owner read that unit's events in order. The claim then *is* the ordering guarantee.
2. **Notifications are hints in front of a query that stays authoritative.**
   - `NOTIFY` is delivered only after commit, and in commit order. A wake-up therefore means "run the claim or catch-up query again now".
   - Every surveyed system that uses `LISTEN` also keeps a timed fallback read.
   - None of them treats a payload as a cursor.
3. **Commit order is not position order.**
   - Positions drawn from a sequence are assigned before commit. A wake-up can therefore reveal a later position while an earlier one is still in flight.
   - A consumer has to stop at a safe high-water mark, or it will skip events for good. The ways to establish one are serialising writers, gating on the transaction-ID horizon, tracking snapshots, or waiting on gaps.
4. **How long the claim lives decides what happens on failure.**
   - Holding a row lock or advisory lock for the whole handler makes the database the failure detector and the commit the fence. The cost is a pinned connection and an open transaction.
   - Claim-and-mark with a lease frees the connection, but recovery becomes time-based. Checkpoint writes must then be fenced with an epoch, so that a former owner that stalled cannot overwrite the new owner's progress.
5. **Notifying costs the writer.**
   - Every notifying transaction takes a cluster-wide lock and holds it across the WAL flush of its commit. Notifying commits are therefore serialised.
   - PostgreSQL 19 cuts down needless listener wake-ups but leaves that lock in place.

### Distributing work among competing consumers

#### How SKIP LOCKED hands out rows

**What the docs say.** The PostgreSQL documentation defines the behaviour: with `SKIP LOCKED`, "any selected rows that cannot be immediately locked are skipped". It warns that this "provides an inconsistent view of the data, so this is not suitable for general purpose work, but can be used to avoid lock contention with multiple consumers accessing a queue-like table". Some details matter in practice:

- **Only row locks are skipped.** The table-level `ROW SHARE` lock is still taken in the ordinary way.
- **`LIMIT` stops the locking.** With a top-level `LIMIT`, "locking stops once enough rows have been returned to satisfy the limit". Rows skipped by `OFFSET` are still locked.
- **Ordering can slip, but not here.** At `READ COMMITTED`, an `ORDER BY` query with a locking clause can return rows out of order, because sorting happens before locking. This only happens when the query has to wait for a lock, and SKIP LOCKED never waits.

Michael Paquier's write-up of the 9.5 feature shows the basic effect. One session holds a lock on row 1. A second session's `ORDER BY id FOR UPDATE SKIP LOCKED` then returns rows 2, 3 and 4.

**Production queues.** They all use the same shape:

- **River.** The `JobGetAvailable` CTE selects `state = 'available'` rows for one queue with `ORDER BY priority ASC, scheduled_at ASC, id ASC LIMIT @max_to_lock FOR UPDATE SKIP LOCKED`. An `UPDATE` then sets `state = 'running'`, increments `attempt`, and stamps `attempted_at` and `attempted_by`.
- **Procrastinate.** The fetch function uses `FOR UPDATE OF jobs SKIP LOCKED`.
- **Solid Queue.** It polls its `ready_executions` table with `FOR UPDATE SKIP LOCKED` "to avoid blocking and waiting on locks when polling jobs".

#### How advisory locks hand out work

**The two forms.**

- **Session-level** (`pg_advisory_lock`). These locks "do not honor transaction semantics":
  - they survive a rollback;
  - they stack, so each acquisition needs its own unlock;
  - they are released when the session ends.
- **Transaction-level** (`pg_advisory_xact_lock`). These are released when the transaction ends, and there is no explicit unlock.

**Keys and variants.**

- Non-blocking `pg_try_*` variants return a boolean instead of waiting.
- A key is either one `bigint` or two `integer`s, and the two key spaces do not overlap.
- Keys share one namespace across the database, so unrelated users of advisory locks can collide.

**Memory.** Advisory locks draw on the same shared-memory pool as ordinary locks, sized by `max_locks_per_transaction` and `max_connections`. If the pool runs out, the server cannot grant any lock at all.

**Que, the canonical advisory-lock queue.** Its README says that "workers don't block each other when trying to lock jobs, as often occurs with 'SELECT FOR UPDATE'-style locking". Because the locks live in memory, taking one costs no disk write. If a process dies, its jobs "immediately become available for any other worker to pick up". Que walks jobs in `(priority, run_at, job_id)` order with a recursive CTE and calls `pg_try_advisory_lock()` on one candidate at a time. A non-recursive `SELECT` "would have the undesirable side-effect of locking multiple jobs at once".

**The `LIMIT` trap.** That side-effect is a documented hazard. The PostgreSQL docs mark `SELECT pg_advisory_lock(id) FROM foo WHERE id > 12345 LIMIT 100` as "danger!". The `LIMIT` is not guaranteed to apply before the lock function runs, so the query can take locks the application never expected and never releases. The fix is to apply the `LIMIT` in a subquery first. GoodJob does this with a `MATERIALIZED` CTE that selects up to 1,000 candidate ids in `priority DESC, created_at ASC` order, then calls `pg_try_advisory_lock(...)` in the outer query with `LIMIT 1`.

**Benchmark.** depesz compared the two primitives directly. Eight workers drained 10,000 rows in about 15.6 s with SKIP LOCKED and about 15.8 s with a `pg_try_advisory_xact_lock(123, hashtext(...))` predicate. His conclusion was that "the biggest benefit is that we can leave advisory locks for other purposes".

#### Event stores usually claim the ordering unit, not the event

Events in an append-only log are never updated, so there is no per-event row to lock and mark. Event stores instead claim one owner for a coarser unit:

- **Marten (`HotCold` mode)** runs leader election "individually for each projection on each tenant database", so that "each projection is running on exactly one running process". By default it uses a session-scoped `pg_try_advisory_lock`. The `UseAdvisoryLockTransaction` option switches it to `pg_try_advisory_xact_lock`.
- **Commanded's Elixir EventStore** lets "only one instance of a named subscription to a stream" connect.
  - It enforces this with a session advisory lock keyed by two 32-bit values: one derived from the `subscriptions` table's `oid`, the other identifying the subscription.
  - If the lock is lost, the subscription state machine disconnects, resets its event tracking and purges in-flight events.
  - The lock owner receives a `lock_released` message so it can "attempt to reacquire the lost lock".
- **Row locks give the same pattern.**
  - The `postgresql-event-sourcing` reference implementation locks the row of the subscription it is working on and uses `SKIP LOCKED`, so other instances move on to "the 'next' subscription".
  - Watermill's PostgreSQL adapter keeps one offsets row per consumer group and reads it with `SELECT ... FOR UPDATE` without SKIP LOCKED. A second member of the group therefore waits on that row.
  - pg_eventstore locks all of a manager's subscriptions as one `SubscriptionsSet`, and offers `force_lock: true` to take over a set that is already locked.
- **Graphile Worker** uses the same idea for its named queues. It locks the queue row rather than the job (described under ordering below).

The common lesson is to put the claim on a row or key that stands for the ordering unit (subscription, stream or partition), and have the holder read that unit's events in order.

### How long a claim lasts, and what happens when its holder fails

#### Three claim lifetimes

Production PostgreSQL queues fall into three families:

1. **Lock held by the handler's transaction.** The consumer runs `SELECT ... FOR UPDATE SKIP LOCKED` (or `pg_advisory_xact_lock`), runs the handler, and commits the handler's writes and the acknowledgement or checkpoint in that same transaction. Row locks are released "at transaction end or during savepoint rollback". Transaction-level advisory locks "are automatically released at the end of the transaction".
2. **Session-level advisory lock held on a connection.** Que, GoodJob's default `:advisory` strategy and Marten's leader election all work this way. A session lock "is held until explicitly released or the session ends". It survives rollback: "a lock acquired during a transaction that is later rolled back will still be held following the rollback".
3. **Claim-and-mark with a lease, visibility timeout or heartbeat.** The claim is an ordinary committed row update, and no lock is held while the handler runs. Examples:
   - pgmq's visibility timeout;
   - Graphile Worker's `locked_at` and `locked_by` columns;
   - River's `running` state, backed by a rescuer;
   - Solid Queue's claimed executions, with process heartbeats;
   - Procrastinate's `doing` status, with worker heartbeats;
   - GoodJob's `:skiplocked` strategy.

#### Failure recovery

**Lock held by the transaction: the database detects the failure and the commit fences the work.**

- **Clean failure.** If the worker process dies, its socket closes, the server rolls back, and the work can be claimed again at once.
- **Fencing comes for free.** The handler's database writes commit together with the acknowledgement, so a worker that has lost its connection cannot commit late. Que recommends this: "It's best to destroy the job in the same transaction as any other changes you make". River offers it as `JobCompleteTx`: "A successful commit guarantees that the job will never rerun." Without it, River says there is "a small chance of failure between when a job's work function is invoked and when a River client sets it as complete."

**The weak case: a host that vanishes without closing the socket** (power loss or a network partition).

- PostgreSQL detects a dead connection "only at the next interaction with the socket".
- `client_connection_check_interval` only polls "while running queries". It does not help a backend that sits idle in a transaction while the client runs the handler.
- With Linux's default keepalives (2 hours idle, then 9 probes 75 s apart), a lock can stay orphaned for more than two hours.
- Two server timeouts can end such a session:
  - `idle_in_transaction_session_timeout` exists "to ensure that idle sessions do not hold locks for an unreasonable amount of time".
  - `transaction_timeout`, added in PostgreSQL 17, terminates "any session that spans longer than the specified amount of time in a transaction".

  Both must be longer than the slowest legitimate handler. Recovery after a partition therefore takes roughly as long as the longest allowed handler.

**Two further costs of all-or-nothing rollback.**

- An attempt counter written inside the failed transaction rolls back with everything else. A "poison" event that crashes the handler can then be retried forever, unless attempts are recorded in a separate transaction.
- Effects outside the database are not undone, so delivery to external systems is still at-least-once.

**Session-level advisory lock: released at once on process death, but loss is silent.**

- Que and GoodJob both rely on the immediate release.
- The lock belongs to the session, so the handler can commit in several short transactions. But the commit no longer fences anything.
- Marten documents the main hazard. When "the connection holding the lock is lost — a database restart or a fail-over", the client keeps believing it holds the lock: "Without monitoring, `HasLock` keeps reporting true after the lock is gone, … two nodes can end up running the same projection." Marten's `UseMonitoredAdvisoryLock` is on by default for this reason.
- Session locks need matched unlocks ("for each completed lock request there must be a corresponding unlock request"). An unlock "is effective even if the calling transaction fails later". Bookkeeping mistakes therefore leak locks or release them too early.

**Claim-and-mark with leases: recovery is time-based, and duplicates are the price.**

- **pgmq.** "If the message is NOT deleted or archived within the visibility timeout, it will become visible again and can be read by another consumer." Its guarantee is "exactly once delivery of a message within a visibility timeout", and nothing beyond that window.
- **River.** The rescuer comment describes the trade-off: rescue "can result in repeat or duplicate execution of a job that is not actually stuck but is still working… Setting a value too low will result in more duplicate executions, whereas too high of a value will result in jobs being stuck for longer than necessary before they are retried". River defaults to 1 hour and requires the rescue threshold to be longer than the job timeout.
- **Graphile Worker.** A crashed worker's jobs "will remain locked for 4 hours before they can be re-attempted". The manual `force_unlock_workers` comes with the warning "Do NOT pass any alive worker ids to this method or Bad Things may happen."
- **AWS SQS** documents the same tension. Its remedy is to start with a short timeout and "implement a heartbeat mechanism to periodically extend the visibility timeout".

**Heartbeats.** They decouple how fast a failure is detected from how long a handler may run:

- **Procrastinate** workers send a heartbeat "every 10 seconds by default". Jobs in `doing` whose worker has not sent one for 30 seconds are treated as stalled, and a periodic task can retry them.
- **Solid Queue** sends heartbeats every 60 seconds and treats a process as dead after 5 minutes. It marks that process's claimed jobs as failed (`ProcessPrunedError`) instead of retrying them, because the job itself may have killed the process.

So what to do after expiry (retry, discard, or route to a dead-letter path) is a separate design choice.

#### Connection usage

| Claim lifetime | Connections held | Pooler compatibility | Other cost |
|---|---|---|---|
| Lock held by handler transaction | One connection, inside an open transaction, for each in-flight handler. The connection count caps concurrency | Works behind PgBouncer transaction mode, but pins a server connection for the whole handler | Holds back vacuum: "an open transaction prevents vacuuming away recently-dead tuples" |
| Session advisory lock | One dedicated, persistent session per lock holder. No open transaction ("keeps no transaction open while it is the leader", Marten) | PgBouncer lists session advisory locks and `LISTEN` as "Never" supported in transaction pooling | Silent loss must be monitored |
| Claim-and-mark | A connection only for the short claim, heartbeat and completion statements | Compatible with transaction pooling. GoodJob's `:skiplocked` mode: "No advisory locks are held. Compatible with PgBouncer in transaction mode." | Each heartbeat is a write that leaves a dead tuple, but no long transaction holds back vacuum |

psycopg's documentation also warns that sessions left idle in a transaction hold locks and bloat tables. The vacuum cost of long transactions is measurable. In Brandur's benchmark of the Que queue, one long-lived transaction left nearly 100,000 dead rows that could not be removed. Lock acquisition slowed from under 10 ms to over 100 ms, and the backlog grew to about 60,000 jobs within an hour, because index scans had to step over dead tuples to find a job they could lock. High-churn queue and checkpoint tables are the most exposed.

Connection budgets in practice:

- **GoodJob** budgets "1 connection per execution pool thread" plus "2 additional connections … for utility functionality (e.g. LISTEN/NOTIFY, cron, etc.)". It is "not compatible with PgBouncer in _transaction_ mode" unless it runs `:skiplocked` with LISTEN/NOTIFY turned off.
- **Solid Queue** sizes its worker threads at "the queue database's connection pool size minus 2, as each worker uses connections for polling and heartbeat".
- **River** needs "a minimum of two connections" that bypass transaction pooling, because LISTEN/NOTIFY carries both new jobs and leadership changes.
- **Que** offers a separate locking connection URL for applications whose normal connections "can't use advisory locks".
- **Marten** offers transaction-scoped leader locks as "the better fit behind PgBouncer in transaction pooling mode". The catch is that "the leader keeps a transaction open for as long as it holds leadership". Choosing between the two lock forms trades pooler compatibility against transaction age.

#### Fencing a stale owner with an epoch

**Why expiry is not enough.** A lease lets another consumer take over, but it does not stop the old owner from writing. Kleppmann's analysis: "if the GC pause lasts longer than the lease expiry period, and the client doesn't realise that it has expired, it may go ahead and make some unsafe change". He cites HBase GC pauses of several minutes and a GitHub incident in which packets were delayed for about 90 seconds.

**The fix: a fencing token.** This is "a number that increases … every time a client acquires the lock". The storage must "take an active role in checking tokens, and rejecting any writes on which the token has gone backwards". A client checking its own lease is not enough, because the pause can land between the check and the write.

**Chubby.** Google's Chubby provides the same thing as a **sequencer** (lock name, mode and generation number), which the recipient "is expected to test". Servers that cannot check sequencers fall back on **lock-delay**: after a holder fails, nobody may reclaim the lock for up to a minute. The paper calls lock-delay "imperfect".

**How mature consumer-group systems fence their progress commits:**

- **Kinesis Client Library (KCL).**
  - Each shard's lease item stores `leaseOwner`, `checkpoint` and `leaseCounter`. The counter "increases if the lease ownership is transferred to another worker".
  - Take, renew and update are all DynamoDB conditional writes that expect the counter value the worker last saw. Each acquisition also gets a UUID concurrency token.
  - On a mismatch, the checkpointer throws `ShutdownException` ("Can't update checkpoint - instance doesn't hold the lease for this shard").
- **Kafka.**
  - Under KIP-848, offset commits are checked against the member epoch and rejected with `STALE_MEMBER_EPOCH` or `FENCED_MEMBER_EPOCH`.
  - KIP-447 covers the exact GC-pause zombie case. If a field does not match, "the client will be fenced immediately", which the Java client surfaces as `CommitFailedException`.
  - KIP-98 fences zombie producers by bumping the producer epoch.
  - Together, these let Kafka commit the checkpoint and the handler's output in one fenced transaction.
- **Azure Event Hubs** guarantees that only one instance succeeds in claiming a partition. Its abstract `update_checkpoint` takes no owner or etag, though, so checkpoint writes are not fenced. It relies on at-least-once delivery and idempotent downstream systems instead.
- **Marten**, on PostgreSQL, pairs advisory-lock leadership with a compare-and-set on progress: `update … set last_seq_id = ? … where name = ? and last_seq_id = ?`. If no rows match, it throws `ProgressionProgressOutOfOrderException` instead of silently overwriting.
- **AWS SQS is the counter-example.** It fails silently: "If you use an old `ReceiptHandle`, the request will succeed, but the message might not be deleted." A fenced write should report that the lease was lost.

**A PostgreSQL shape.** This is a synthesis of KCL and Marten. The lease and the checkpoint live in one row, guarded by an epoch that only ever increases:

```sql
CREATE TABLE consumer_partitions (
  consumer_group   text        NOT NULL,
  partition        int         NOT NULL,
  owner_id         text,
  epoch            bigint      NOT NULL DEFAULT 0,
  lease_expires_at timestamptz,
  position         bigint      NOT NULL DEFAULT 0,
  PRIMARY KEY (consumer_group, partition)
);

-- claim, or take over an expired lease
UPDATE consumer_partitions
   SET owner_id = %(me)s,
       epoch = epoch + 1,
       lease_expires_at = clock_timestamp() + %(ttl)s
 WHERE consumer_group = %(group)s AND partition = %(partition)s
   AND (owner_id IS NULL OR lease_expires_at < clock_timestamp())
RETURNING epoch, position;

-- renew; zero rows means the lease was lost
UPDATE consumer_partitions
   SET lease_expires_at = clock_timestamp() + %(ttl)s
 WHERE consumer_group = %(group)s AND partition = %(partition)s
   AND epoch = %(epoch)s;

-- checkpoint, in the same transaction as the handler's writes
UPDATE consumer_partitions
   SET position = %(new_position)s
 WHERE consumer_group = %(group)s AND partition = %(partition)s
   AND epoch = %(epoch)s
   AND position = %(expected_previous_position)s;
```

In psycopg 3, check `cursor.rowcount`. If it is zero, raise a lease-lost error inside the `conn.transaction()` block, so the whole unit of work rolls back.

**Why the guarded `UPDATE` cannot race.** Under READ COMMITTED, an `UPDATE` that finds its target row already changed by a concurrent transaction waits for that transaction to finish. If the other transaction commits, "the `WHERE` clause is re-evaluated to see if the updated version of the row still matches". The two possible orderings both come out correct:

- **The takeover commits first.** The stale owner's checkpoint no longer matches the epoch, so it updates zero rows and its transaction rolls back, taking the handler's database writes with it.
- **The stale owner's checkpoint locks the row first.** The claimant waits. When it re-evaluates, its `RETURNING position` gives it the progress the stale owner just committed, so nothing is lost or replayed from an older checkpoint.

Under REPEATABLE READ or SERIALIZABLE, the same conflict raises "could not serialize access due to concurrent update". Treat that as a lost lease, not a fatal error.

**Rules that follow from these sources:**

1. **Fence and effects in one transaction.** Put the fence and the handler's database effects (read-model rows, outbox rows) in one transaction. This is the same design as Kafka's transactional offset commits.
2. **Write the check as a lock, not a read.** A plain `SELECT epoch` followed by an `UPDATE` is a check-then-act race. Use the conditional `UPDATE` itself, or a locking read, and decide where in the transaction it goes:
   - **At the end.** The checkpoint `UPDATE` is the last statement. The row lock is held only briefly, but work done after the lease was lost is wasted, and two owners' handler transactions can briefly overlap.
   - **At the start.** Begin with `SELECT 1 … AND epoch = %(epoch)s FOR SHARE` (or `FOR NO KEY UPDATE`). This blocks takeover for the life of the transaction, which `idle_in_transaction_session_timeout` or `transaction_timeout` must then bound.
   - **Never use `FOR KEY SHARE`.** It conflicts only with `FOR UPDATE`, so it would not block the claimant's ordinary `UPDATE`.
3. **Fence on an epoch, not an owner id.** A worker that loses a lease and later wins it back would otherwise have late writes from its earlier tenure accepted. Use a `bigint` epoch that is never reset. Monotonicity matters once the token is passed to stores that remember only the highest value they have seen.
4. **Use the database clock, via `clock_timestamp()`.** `now()` and `transaction_timestamp()` "do not change during the transaction", so inside a long handler they compare the lease against a stale instant. Expiry only decides *when* a takeover may happen. The epoch is what provides safety.
5. **Optionally require forward progress.** An expected-previous-position guard (Marten's), or a `position < new_position` guard, stops any path from moving the checkpoint backwards.

**Advisory-lock ownership still needs an epoch.** A checkpoint written on the same session that holds the advisory lock is fenced implicitly: if that session ends, the lock is released and the connection can no longer commit. The fence disappears in two cases:

- The checkpoint goes through a different, pooled connection.
- The session is reset while the client still believes it holds the lock. For example, `DISCARD ALL`, a common pool-reset command, runs `SELECT pg_advisory_unlock_all()`.

The robust pattern, which is what Marten does, is to bump an epoch row when the lock is acquired and fence checkpoint writes on that epoch.

**Job completion.** This is a weaker case. Graphile Worker deletes a completed job (when it has no named queue) by job id alone, with no `locked_by` check. Deleting a finished job is terminal and close to idempotent. An unfenced checkpoint, by contrast, is a mutable high-water mark that can move backwards. Per-message completions should be guarded with the claim token, for example `WHERE id = $1 AND claim_token = $2`.

**Effects outside the database.** The fence protects only stores that check it. The sources give four mitigations:

- a transactional outbox written in the fenced transaction (the relay "might publish a message more than once", so receivers must be idempotent);
- forwarding the epoch to downstream systems that can reject superseded epochs;
- idempotency keyed on event identity or position;
- a grace delay after expiry before the new owner starts. Like an owner checking its own lease before each effect, this narrows the window but cannot close it.

**Lease timing.** Cosmos DB's change feed defaults (60 s expiry, 13 s renewal) are a reasonable reference ratio. With them, a crashed owner's partitions move after roughly one expiry period plus one renewal interval.

#### Detecting that the lock-holding connection is gone

**Two separate clocks.** The server ends lock ownership, so a new consumer can take over only once the backend decides its client has gone. The old owner learns it has lost the lock only when its own side of the socket fails. These two detection clocks run independently.

**Silent failures are slow under Linux defaults.** Clean process death closes the socket promptly. Host crashes, partitions and NAT drops do not:

| State | Mechanism that applies | Default time to detect |
|---|---|---|
| Idle socket | Keepalive probes, which start after `tcp_keepalive_time` | 7,200 s, then "approximately an additional 11 minutes" |
| Data in flight | `tcp_retries2` | 13 to 30 minutes (Cloudflare measured a lower bound of 924.6 s) |

| Mechanism | Side | Catches | Blind spot / false-positive risk |
|---|---|---|---|
| libpq `keepalives`, `keepalives_idle`, `keepalives_interval`, `keepalives_count` | client | A silent peer on an idle or waiting socket (LISTEN, lock holder, query wait) | No probes while data is unacknowledged. A proxy that answers probes hides server loss. Low false-positive risk, because the peer's kernel answers |
| libpq `tcp_user_timeout` (PG 12+) | client | Data stuck unacknowledged, or held by a zero window | Does nothing when no data is in flight. If shorter than the keepalive budget, it cuts probing short and can kill a live peer that has stopped reading |
| Server `tcp_keepalives_*`, `tcp_user_timeout` | server | The same, from the backend side. This is what actually ends the session and frees its locks | Same as the client equivalents |
| `client_connection_check_interval` (PG 14+) | server | A dead client while a query runs (for example, blocked in `pg_advisory_lock()`) | Only sees what the kernel reports, so it still needs keepalives for partitions |
| `idle_in_transaction_session_timeout` | server | A client that is alive but stuck, which kernel probes never catch | High false-positive risk for handler transactions, which look idle between statements |
| `transaction_timeout` (PG 17+) | server | Any transaction over a hard cap | High if set below the longest legitimate handler |
| `idle_session_timeout` (PG 14+) | server | An idle session outside a transaction | Would kill a healthy session that holds an advisory lock but is idle |

**Keep the timers consistent.** On Linux, `TCP_USER_TIMEOUT` "will override keepalive to determine when to close a connection due to keepalive failure". Cloudflare found that "with TCP_USER_TIMEOUT set, the TCP_KEEPCNT is totally ignored". It recommends setting `TCP_USER_TIMEOUT = TCP_KEEPIDLE + TCP_KEEPINTVL * TCP_KEEPCNT`, so that idle and in-flight sockets share one detection budget. None of these TCP settings apply to Unix-domain sockets.

**Applying this in psycopg 3:**

- **Client settings.** Extra keyword arguments to `connect()` are passed to libpq, for example `keepalives=1, keepalives_idle=10, keepalives_interval=5, keepalives_count=3, tcp_user_timeout=25000`.
- **Server settings per session.** The server-side `tcp_keepalives_*`, `tcp_user_timeout` and `client_connection_check_interval` parameters have user context. A consumer can set them for its own session with `options="-c ..."` or with `ALTER ROLE`, without changing the whole cluster.
- **How a loss shows up.** It appears as `psycopg.OperationalError` on the next operation, and the connection's `broken` flag becomes true. A broken connection is never silently reconnected. So a successful round trip on a connection that has never been reconnected is evidence that the same backend, and any session lock it holds, still exists. Since psycopg 3.2.4, a server-initiated disconnect raises the specific error, such as `IdleInTransactionSessionTimeout`.
- **Detecting loss without polling.** Wait for the socket to become readable (`selectors` on `conn.fileno()`, or `loop.add_reader()` in asyncio). Run `SELECT 1` only when that happens. The psycopg docs advise against a loop of `SELECT 1` queries.
- **The LISTEN loop as heartbeat.** `notifies(timeout=…)` (psycopg 3.2+) wakes periodically, so a cheap round trip on each wake-up doubles as a heartbeat.
- **Where heartbeats go.** psycopg serialises the queries on one connection. A heartbeat on the handler's own connection would queue behind the handler's statements. Put heartbeats on the dedicated lock-holding or LISTEN connection instead.
- **Cancelling stuck queries in asyncio.** Bounding a query with `asyncio.wait_for` used to fail against a server that had stopped responding: the cancellation "is swallowed forever" (issue #1371). This was fixed in psycopg 3.3.6, and the fix requires libpq 17 or newer. On older stacks, the client's `tcp_user_timeout` is what bounds such a hang.

**Middleboxes defeat transport-level detection.** When a proxy keeps the client side alive after losing its server side, "keepalive probes are answered by the middlebox" and "tcp_user_timeout never fires". Behind a proxy, only an application-level round trip with a deadline proves the server session is alive. PgBouncer in session mode runs `server_reset_query` (by default `DISCARD ALL`, which includes `pg_advisory_unlock_all()` and `UNLISTEN *`) when the client disconnects. A session lock held through PgBouncer is therefore released only once PgBouncer itself notices the client has gone.

**Suggested balance (synthesis).**

- **Detect on the client before the server.** Give the client a shorter budget than the server, so the old owner usually learns of the loss before a new owner can take over. For example: client keepalives idle/interval/count of 10/5/3 with `tcp_user_timeout=25000` (about 25 s), and server, per session, 15/5/5 with `tcp_user_timeout=40000` (about 40 s).
- **Pick a budget against the takeover delay you can accept.** AWS's Aurora fast-failover guidance uses 1/1/5, about 5 s. CYBERTEC suggests 60/5/3. Absorb a lossy network with more probes, not longer intervals.
- **Treat application timeouts as backstops.** Size `idle_in_transaction_session_timeout` and `transaction_timeout` from handler durations. They are not liveness probes.
- **Turn on `client_connection_check_interval`** for consumer sessions.
- **Fencing is still required.** During a partition, the two clocks cannot coordinate.

### Combining claiming with notification-driven wake-ups

#### Why NOTIFY suits a claim loop

`NOTIFY` is tied to commit. Its documented properties make it a good trigger for re-running a claim query:

- **After commit only.** Notifications are "not delivered until and unless the transaction is committed".
- **Delayed for busy listeners.** A listener that is inside a transaction receives them only after that transaction ends.
- **Duplicates collapse.** Identical channel-and-payload pairs within one transaction are delivered once.
- **Commit order.** Notifications from different transactions are delivered "in the order in which the transactions committed".

By the time a wake-up arrives, the producer's rows are committed, and a fresh query at `READ COMMITTED` can see them. Payloads are limited to 8,000 bytes by default, so they are best treated as hints, not data.

**The startup race.** The `LISTEN` docs describe a race when a listener starts and give the rule for avoiding it: "first execute (and commit!) that command, then in a new transaction inspect the database state as needed by the application logic, then rely on notifications to find out about subsequent changes". The first few notifications may repeat state that has already been seen, which "is usually harmless".

**psycopg specifics.** Psycopg 3 advises autocommit on the listening connection "if you wish to receive or send notifications in a timely manner". `Connection.notifies()` accepts `timeout` and `stop_after`, so one wait serves both the notification wake-up and the fallback timer. The alternative, `add_notify_handler`, runs handlers only "during the normal query processing". The docs say to use only one of the two methods.

**The resulting loop:**

1. `LISTEN` and commit.
2. Run the claim or catch-up query until it returns nothing.
3. Wait for a notification or a timeout.
4. Go back to step 2.

#### Every system keeps a fallback poll

| System | Fallback behaviour |
|---|---|
| Procrastinate | Each worker polls "every now and then, just in case". The `fetch_job_polling_interval` timer starts once a fetch returns nothing |
| River | `FetchPollInterval` (default 1 s, plus 0–10 % jitter) catches what LISTEN/NOTIFY misses. `FetchCooldown` (default 100 ms) limits fetch frequency, which absorbs bursts. `PollOnly` drops LISTEN for environments such as transaction-mode PgBouncer |
| GoodJob | Falls back to `poll_interval` (10 s in production) when LISTEN/NOTIFY is disabled or unavailable |
| Que | Jobs that are ready immediately are broadcast via LISTEN/NOTIFY, "so polling is unnecessary for them". Polling, every 5 s by default, covers scheduled and retried jobs |
| Oban | The Stager runs every second. If pubsub fails, it switches to a "local" mode in which every node polls |
| Graphile Worker | `pollInterval` of 2,000 ms, re-armed after every fetch |
| Solid Queue | Does not use LISTEN at all. Workers poll every 0.1 s |
| `postgresql-event-sourcing` | A trigger issues `NOTIFY`. The JDBC driver still polls for notifications with a timeout |

#### Broadcast versus targeted wake-ups

Most systems broadcast:

- Procrastinate's insert trigger calls `pg_notify` on a per-queue channel and an any-queue channel, with a `{"type": "job_inserted", "job_id": ...}` payload.
- Oban publishes on an `insert` channel, which queue producers "use this as a signal to dispatch new jobs".
- Commanded's EventStore sends one notification per append with a range payload (stream uuid, stream id, first and last version).

A broadcast wakes every listener, but SKIP LOCKED or a try-advisory-lock makes the race safe. Losing consumers get an empty claim, not a duplicate, so the cost is wasted queries, which River's cooldown limits.

Que targets a single listener instead:

- Its `que_job_notify()` trigger picks one locker from an unlogged `que_lockers` table. It chooses among lockers that are listening and serve the job's queue, "weighted by their number of workers" via `NEW.id % count`.
- It then sends `pg_notify('que_listener_' || locker_pid, ...)`.

The notification itself therefore hands out the work.

#### What notifying costs the writer

**The lock.** PostgreSQL's `async.c` serialises notifying transactions in `PreCommit_Notify`: "Serialize writers by acquiring a special lock that we hold till after commit. This ensures that queue entries appear in commit order". The call is `LockSharedObject(DatabaseRelationId, InvalidOid, 0, AccessExclusiveLock)`, a lock on "database 0". The file header confirms that "all writers serialize on a cluster-wide heavyweight lock". Notifying commits in *different databases* of one cluster therefore queue behind each other. Transactions that send no notifications never take this lock.

**How long it is held.** `CommitTransaction()` sets the window:

1. `PreCommit_Notify()` takes the lock and writes the queue entries.
2. `RecordTransactionCommit()` runs. For a transaction that wrote WAL under synchronous commit, this means `XLogFlush()` and then `SyncRepWaitForLSN()`.
3. Locks are released.
4. Only then does `AtCommit_Notify()` signal listeners.

So the lock covers the WAL flush, and the synchronous-replication wait where one applies. Notifying transactions that write data cannot share a group commit with each other (inference from the code).

**Measured cost.**

| Benchmark (`pgbench -c 100 -j 10`, master) | TPS |
|---|---|
| INSERT only (Mukhin) | 30,982 |
| INSERT plus NOTIFY (Mukhin) | 818, about 38x lower |
| NOTIFY only (Mukhin, no WAL flush) | 87,751 |
| INSERT plus NOTIFY (Bagga, other hardware) | about 29,000–31,000 |

The large difference between the two hardware setups fits a serialised section whose length is set by WAL flush latency (inference). Bagga notes that the cost "becomes especially expensive in environments where compute and storage are separated". In production, Recall.ai saw hundreds of sessions waiting on `AccessExclusiveLock on object 0 of class 1262 of database 0`, with load spiking while CPU and I/O dropped. A 2025 comment on the PostgreSQL docs list also notes that the documentation does not mention this global lock.

**What PostgreSQL 19 changes.** Commit `282b1cd` was written by Joel Jacobson and committed by Tom Lane on 2026-01-15. The PostgreSQL 19 release notes, still undated as of 2026-09-14, list it as making NOTIFY "only wake up backends that are listening to specified notifications". It adds three things:

- **A shared channel map**, so a notifier signals only the listeners on its channels.
- **Direct advancement**, which moves an uninterested idle listener's queue pointer without waking it.
- **A `wakeupPending` flag**, so a listener that has already been signalled is not signalled again.

Measured gains:

- **Many uninterested listeners.** Jacobson's ping-pong benchmark stayed flat at about 9,000 TPS with up to 1,000 extra listeners on other channels. Master fell to 238 TPS.
- **1,000 channels with one listener each.** Lane measured 19,966 messages/s against 1,565 on pre-patch HEAD, which he called "not in the same league".
- **Every listener reads every message.** The commit message says: "There is no improvement for the simplest case where every listener reads every message."

All these benchmarks used NOTIFY-only transactions, which skip the flush. A later fix, `0f24332` (2026-05-27), closed a window in which a listener whose `LISTEN` had not yet committed could miss messages. PostgreSQL 19 deployments should run a build that includes it.

**What PostgreSQL 19 does not change.** Master still takes the same lock in `PreCommit_Notify`, and the new direct-advancement logic depends on it. All the signalling work happens after the lock is released, so the serialised section is not shortened. Recall.ai's updated post says the commit "has eliminated the bottleneck"; the source code contradicts this.

**Relief still under discussion.**

- Tom Lane proposed carrying payloads through WAL, so that the lock covers only a small queue entry. Rishu Bagga's "Write Notifications Through WAL" patch raised INSERT plus NOTIFY from 818 to about 33,800 TPS, while NOTIFY-only throughput fell to about 34,000–36,000 TPS. It was still under review at v9 (2025-11-25) and is not in the PostgreSQL 19 notes.
- A GUC that would have skipped the lock entirely (allowing out-of-order notifications) was not adopted.

#### Limiting the cost

**What drives the cost.** The cost scales with how many transactions notify, not how many messages they send. A transaction that sends a hundred notifications takes the lock once. Batching inside a transaction shortens the hold, but only skipping or moving notifications reduces how often the lock is taken (inference from the documented ordering). PostgreSQL already collapses identical channel-and-payload pairs within a transaction. `async.c` switches to a hash table for that check once a transaction has 16 pending notifications.

**Strategies in use:**

| Strategy | Examples | Effect on the lock |
|---|---|---|
| One notification per statement, with an empty or constant payload | Older Graphile Worker (`after insert … for each statement`, `pg_notify('jobs:insert', '')`). PgQueuer's statement-level `AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE` trigger with a table-change payload | Fewer queue entries. A constant payload collapses repeats within a transaction |
| One notification per append, carrying a range | Commanded EventStore: an `AFTER INSERT OR UPDATE ON streams` trigger. An append updates two `streams` rows (the target and `$all`), so a 1-event and a 500-event append send the same few notifications. The publisher reads the range in one `read_stream_forward` | Lock held briefly. Consumers get a useful hint |
| Notify from application insert calls, not triggers | Graphile Worker 0.16.0 (one `{"r":<random>,"count":<n>}` per `add_jobs` batch, read by `workerPool.nudge(n)`). Oban 2.17.0 (V12 drops the per-row trigger; one payload per insert call, deduplicated by queue; `insert_trigger` can switch it off) | One notification per insert operation |
| Time-based throttling, so most commits send nothing | River 0.5.0: a client-side `notifylimiter`, per queue, with a `FetchCooldown` window, piggy-backed on the insert transaction. pgmq: `enable_notify_insert(queue, throttle_interval_ms DEFAULT 250)`, using a conditional `UPDATE` on an `UNLOGGED` throttle row | The only lever that reduces how many transactions take the lock |
| Drop NOTIFY from the write path | Recall.ai moved the signal to the application layer. Oban can use a non-PostgreSQL notifier | Removes the cost |
| Notify in a separate, data-free transaction after the append commits | Inference from the NOTIFY-only measurements | Short hold, because there is no flush. The wake-up is no longer atomic with the append, and a crash between the two loses it |

Procrastinate is the per-row contrast. It sends two distinct notifications per job, so PostgreSQL can never collapse them. PostgreSQL's transition tables (`REFERENCING NEW TABLE AS ...`) would let a statement-level trigger send one summary notification, but none of the systems surveyed use them for this.

pgmq's design has two consequences (inference):

- Its throttle row is a hotspot per queue. Concurrent inserters wait for the winner's commit before they skip notifying.
- It throttles on the leading edge, so inserts later in the window send nothing.

#### Throttled notifications need a consumer-armed trailing read

**Where the throttle sits matters.**

| System | Throttle location | Edge | Window W | Fallback poll P |
|---|---|---|---|---|
| River | Appender: in-process, per queue | Leading. Suppressed notifications are dropped | `FetchCooldown`, 100 ms | `FetchPollInterval`, 1 s plus jitter |
| River | Consumer: fetch debouncer | Leading, plus one trailing fetch per period | 100 ms | same |
| pgmq | Appender: in the database | Leading. Suppressed inserts are not notified | 250 ms | polling or `read_with_poll`, chosen by the consumer |
| Oban | Consumer: dispatch cooldown | Trailing only, coalesced | about 5 ms, jittered | Stager, 1 s |
| Graphile Worker | Consumer: refetch delay | Deferred trailing fetch, plus an early abort once enough notifications arrive | 100 ms × (0.5–1.5) | 2 s |

- **Appender-side throttle.** If the appender suppresses notifications, no consumer ever learns about the suppressed appends. Such events wait up to one poll interval P (about 1–2 s in the systems surveyed), not just the window W.
- **The usual trailing edge does not help.** A standard leading-plus-trailing throttle fires its trailing call "only if the throttled function is invoked more than once during the wait timeout" (lodash). River's debouncer follows the same rule. A consumer that sees only the leading notification never fires a trailing read for the appends that were suppressed. River's code calls the poll loop "a back up in case something with listen/notify didn't work, or the fetch limiter was limited so there's still jobs to pick up".
- **Consumer-side throttle.** If the consumer receives every notification and folds them into one read at the end of the window, as Oban does, delivery is guaranteed within about W.

**Two ways to get roughly W latency:**

1. **Consumer-armed trailing read (inference).** On every notification, read immediately. Also, unconditionally, schedule one more catch-up read at arrival + W + a small margin, and fold any further notifications into that pending read. The cost is at most one extra read per window.
2. **Move the throttle to the consumer.** Notify once per transaction, with a constant payload, and debounce reads on the consumer side. This gives up the appender-side saving that the throttle was meant to provide.

**Why the fallback poll is still needed.** It sets the worst case for three reasons:

- **Window timing.** The throttle window is stamped at insert time, while delivery happens at commit. A suppressed append that commits after the trailing read gets neither a notification nor a read.
- **Rolled-back leaders.** If the transaction that carried the leading notification rolls back, River's in-memory limiter still records the timestamp, so later appends in that window lose their wake-up (inference). pgmq's throttle-row update rolls back with its transaction, so it avoids this.
- **Outages.** Notifications sent while a LISTEN session is disconnected are lost.

**Practices for sizing and running the poll:**

- Jitter P across consumers. River adds up to 10 %.
- Re-arm the poll timer after every read, however it was triggered, as Graphile Worker does.
- Fetch again immediately after a full batch, as River and Graphile Worker do.
- Always read from the stored checkpoint up to a safe high-water mark. Never drive the read from notification payloads.

### Preserving per-stream and per-partition ordering

#### Why claiming alone breaks order

If two consecutive items for one key go to two consumers, the two run at the same time and can finish in either order. Two systems state this directly:

- Commanded's docs say that with several subscribers "the ordering of event processing is no longer guaranteed" unless events are partitioned.
- Solid Queue's per-key `limits_concurrency` semaphores cap how many jobs run at once, but its docs state that "queue order is not taken into account for unblocking jobs". A concurrency limit is not an ordering guarantee.

#### Patterns that restore per-key order among competing claimers

1. **Head-of-key exclusion predicate (Procrastinate).**
   - **The rule.** A candidate cannot be fetched while another job with the same `lock` is either:
     - `doing`, or
     - `todo` with higher priority, or with equal priority and a lower id.
   - **The query.** It orders by `priority DESC, id ASC` under `FOR UPDATE OF jobs SKIP LOCKED`.
   - **The race guard.** A partial unique index, `ON procrastinate_jobs (lock) WHERE status = 'doing'`, ensures at most one running job per lock even if two fetches race.
   - **The documented guarantee.** Jobs sharing a lock "will be started in order; a job will not start before the previous one is finished", whatever the outcome of the previous job.
   - **The documented stall.** If the oldest job for a lock sits in a queue nobody consumes, or is scheduled far in the future, every later job for that lock waits behind it.
2. **Lock the group row, not the item (Graphile Worker).**
   - **Queue rows.** Named queues have rows in `_private_job_queues`.
   - **The claim.** It restricts jobs to `job_queue_id in (select id from _private_job_queues where is_available = true for update skip locked)` and stamps `locked_by` and `locked_at` on the queue row.
   - **Batches.** A batch fetch keeps only the first job per queue with `select distinct on (job_queue_id, ...)`.
   - **Guarantee and warning.** Jobs in one named queue "run one at a time". High-cardinality queue names such as UUIDs "will create many dead queues that degrade performance".
3. **Head-of-group visibility (pgmq FIFO).**
   - **Groups.** Messages carry a group id in their `x-pgmq-group` header.
   - **Reading.** `read_grouped_head()` "skips groups whose head message is not visible (being processed by another worker)" and uses `FOR UPDATE SKIP LOCKED`.
   - **Fairness against throughput.** `read_grouped` fills a batch from the oldest eligible group, while `read_grouped_rr` interleaves groups round-robin.
   - **Recovery.** A failed group resumes once its visibility timeout expires.
4. **A per-key advisory lock.**
   - **What it gives.** A non-blocking, transaction-level try-lock keyed by stream gives mutual exclusion per stream. The two-integer form can carry a namespace as well as an object id.
   - **What it does not give.** The lock alone orders nothing. The holder must also process only the earliest unprocessed item for that key, usually by reading forward from a per-key checkpoint.
   - **Hash collisions (inference).** Mapping stream names to 32- or 64-bit keys can collide. A collision forces two unrelated streams to run serially, but does not break exclusion.
5. **One elected owner plus partitioned dispatch.**
   - **Commanded.** One advisory-lock owner reads the log in order and fans events out to up to `concurrency_limit` subscribers. Events go round-robin, unless a `partition_by` function routes each key to one subscriber. The result is that "events for each stream are processed serially (by a single subscriber) in the order they were appended". Back-pressure comes from acknowledgements: a subscriber "will not receive further published events until it has confirmed receipt of all received events".
   - **Marten** processes events "in order" through each projection, with one process per projection.
   - **River Pro sequences** order jobs within a sequence "by when they were inserted (`id ASC`)" and run separate sequences in parallel. A sequence halts on a cancelled or discarded job unless `ContinueOnCancelled` or `ContinueOnDiscarded` is set.

**The shared cost is head-of-line blocking.** In every pattern, a stuck, failed or far-scheduled head item stalls its key. Many fine-grained keys also add bookkeeping, as Graphile's high-cardinality warning shows.

For an append-only event log (synthesis), the closest fit is pattern 2, 4 or 5 applied to a row or key per stream, partition or subscription. The owner reads that unit forward from its own checkpoint, and pattern 1's per-item exclusion is never needed.

#### Ordering across concurrent appends: the safe high-water mark

Claiming the right unit is not enough if the owner's read can skip events. Two orderings can disagree:

- **Position order.** Global positions usually come from a sequence at insert time. `nextval` values are "not reclaimed for re-use if the calling transaction later aborts". Gaps can appear without an abort, for example with `INSERT ... ON CONFLICT`, and sequences "cannot be used to obtain 'gapless' sequences".
- **Notification order.** This follows commit order.

**How a naive consumer loses an event:**

1. Transaction A draws position 10. Transaction B draws position 11.
2. B commits first, and its notification wakes the consumer.
3. The consumer reads `position > checkpoint`, sees 11, and moves its checkpoint to 11.
4. A commits. Event 10 is now behind the checkpoint and is lost for good.

Oskar Dudycz puts it this way: "If we optimistically assume that the gaps were caused by rollbacks, we may lose messages". Commanded's issue #3 records the same race.

A safe high-water mark is the highest position P such that every event at or below P is either visible now or can never appear. The systems surveyed establish it in five ways.

**1. Serialise writers, so that position order equals commit order.**

- **Commanded EventStore.** Every append runs `UPDATE streams SET stream_version = stream_version + $2 WHERE stream_id = 0`. That row lock is held to commit, which serialises all appends globally. The maintainer describes the result as "single writer performance".
- **Message DB.** `write_message` first takes `pg_advisory_xact_lock(hash_64(category))`, then inserts into `messages`, whose `global_position` is a `bigserial`. So "all writes to streams in a given category are queued and processed in sequence". The guarantee covers category readers only, because nothing orders commits across categories.
- **The Python `eventsourcing` library** (psycopg 3) runs `LOCK TABLE … IN EXCLUSIVE MODE` before inserting, "so that readers don't pass over gaps that are filled in later".
  - Reads continue, but the lock conflicts with `VACUUM`, `ANALYZE` and `CREATE INDEX CONCURRENTLY`.
  - `POSTGRES_LOCK_TIMEOUT` defaults to 0, which means writers wait indefinitely.

A useful side effect of serialised writes (inference): notifications arrive in position order, so a position carried in a notification payload is itself a safe high-water mark.

**2. Transaction-ID visibility horizon.** Each event row stores `pg_current_xact_id()` (an `xid8`), and readers return only rows below the oldest running transaction. Emmett and `postgresql-event-sourcing` both use:

```sql
(transaction_id, position) > (:last_tx, :last_position)
AND transaction_id < pg_snapshot_xmin(pg_current_snapshot())
ORDER BY transaction_id, position
```

**Why it works.** `xmin` is the "lowest transaction ID that was still active. All transaction IDs less than `xmin` are either committed and visible, or rolled back and dead". `xid8` "does not wrap around during the life of an installation".

**Its costs:**

- **A composite checkpoint.** It has to be the pair `(transaction_id, position)`. Delivery "respects transaction start order, not append order".
- **Long transactions stall delivery.** Any long-running writing transaction stalls every consumer: "any long-running transaction in your system will set a ceiling".
- **The horizon is cluster-wide.** `GetSnapshotData` lowers `xmin` from every running backend without checking which database it belongs to. A writing transaction in *another database* on the same server can therefore hold back the horizon.
- **Wake-ups can arrive too early.** A notification can arrive before its event falls below the horizon.

**3. Snapshot-difference batches (PgQ).**

- **Ticks.** A ticker writes tick rows, each carrying a snapshot (`tick_snapshot`).
- **Batches.** A batch is every event whose transaction is "visible in the current snapshot but invisible in the previous snapshot". Scans stay small by range-scanning `[xmax1..xmax2]` and listing older in-progress transaction IDs explicitly.
- **Effect.** Each committed transaction lands in exactly one batch, and one long transaction never holds back later commits.
- **Price.** The checkpoint is a whole snapshot, and delivery follows visibility order rather than position order.
- **Modern equivalents.** `pg_visible_in_snapshot(xid8, pg_snapshot)` and `pg_snapshot_xip`.
- **PgQ is time-driven, not push-driven.** The `pgqd` daemon runs the ticker every `ticker_period` (default 1 s). Queues tick at most `queue_ticker_max_lag` (default 3 s) after an event, and the ticker functions issue no `NOTIFY`.

**4. Gap detection with waiting, liveness checks and timeouts.**

- **Marten's high-water mark** is "the furthest known event sequence that the daemon 'knows' that all events with that sequence or lower can be safely processed in order".
  - It reads the highest *committed* `seq_id`, "never mt_events_sequence.last_value", and walks forward to the first gap.
  - Once a gap has been stuck longer than `StaleSequenceThreshold` (default 3 s), a liveness probe checks:
    - `pg_locks` for row-exclusive locks on the events table;
    - open client transactions;
    - in-progress write transaction IDs.
  - It also applies an "allocation fence": a session that has been idle since before the gap's numbers were allocated cannot hold one.
  - Gaps it proves dead are skipped in one step and recorded in `mt_high_water_skips`.
  - `SkipStaleGapsDespiteLiveTransactionsAfter` defaults to null, so Marten "never knowingly skips past a live appender".
  - It looks for a new high-water mark every 250 ms when busy and every 1 s when quiet.
- **Axon's `GapAwareTrackingToken`** keeps the highest index plus the set of lower sequence numbers "that have not been seen yet", and delivers late events when they appear. Its defaults are `gapTimeout` 60 s, `maxGapOffset` 10,000 and `gapCleaningThreshold` 250. Delivery can be out of order, but nothing that commits within the timeout is skipped.
- **Prooph's `GapDetection`** exists because "projections skip events due to newer events becoming visible before old ones". It retries after 0, 5, 50 and 500 ms, then accepts the gap.

**5. Advisory-lock fencing of in-flight positions (Sequin).**

- **Writers register.** Each writer takes `pg_try_advisory_xact_lock_shared(1, 0)` to mark itself as an appender. It then takes an exclusive advisory lock keyed on `last_value + 1`, the lowest position it could draw.
- **Readers compute the floor.** Readers find the minimum such key in `pg_locks` and read below it.
- **Trade-off.** Only appenders hold the ceiling back, and Sequin calls the scheme "uncapped, parallel writes". It is complex, though, and depends on every writer following the protocol.

The `postgresql-event-sourcing` project also offers a reader-side lock. The reader briefly takes `LOCK … IN SHARE ROW EXCLUSIVE MODE`, which waits for in-flight inserts, then reads up to `pg_sequence_last_value`.

#### What serialising writers costs, compared with the horizon

**Mechanism (inference from the docs).** The serialising lock is held from acquisition until the commit's WAL flush. The append rate on one lock therefore cannot exceed 1 / hold time, and each extra queued writer adds roughly one hold time to latency. Writers serialised behind one lock never commit together, so they cannot share a group commit.

**Measured evidence** (Commanded EventStore, 2018-era, hardware not stated):

- **Issue #107.** One writer took 20,288.68 µs per 100-event append (about 4,900 events/s). Fifty concurrent writers took 582,350.50 µs for 50 such appends (about 8,600 events/s). That is about 1.75× the throughput for 50× the concurrency, which fits near-serial execution.
- **Issue #139.** Turning off the `$all` row update and its link rows cut append time by 38.53 % for one writer and 79.53 % for 10 writers. Those link inserts are also removed, so the comparison does not isolate the lock alone.

**Practitioners.**

- Sequin found a counter row gave "lock durations between concurrent writes … (seconds)". Moving the counter update to the end of the transaction helped.
- Dudycz rejected a single-row counter because the "performance hit was too big to accept".
- Marten's "Quick" append mode, which draws sequence numbers inside the INSERT, cut append time by 40–50 % and reduced "event skipping". The longer the gap between drawing a position and committing it, the more out-of-order gaps readers see.

**Bottom line:**

| Design | Append throughput and latency | Consumer cost |
|---|---|---|
| Global row or table lock | Capped at about 1 / hold time. Latency grows linearly with concurrent writers | One gapless or commit-ordered position. Events readable as soon as they commit |
| Per-category advisory lock | The same limit per category. A hot category is still serial | All-stream readers are not protected |
| Transaction-ID horizon | Not serialised (inferred; no head-to-head benchmark exists) | Visibility delayed by the longest concurrently running writing transaction in the cluster. Composite checkpoint |

#### Waking when a stall resolves without a notification

**When no signal comes.** A consumer gated by a horizon or a gap stalls until the blocker resolves. If the blocker is an appender that commits, its own notification wakes the consumer. Three cases produce no signal at all:

1. The blocking appender rolls back, and its notification is discarded with it.
2. The horizon is held by a transaction that never appends, possibly in another database.
3. A sequence gap is never filled. Marten notes that "most holes fill in within milliseconds", but the rest are permanent.

**Options, in layers (largely inference built from documented primitives):**

- **Wait inside the server on a per-transaction advisory key.**
  - Each appender takes `pg_advisory_xact_lock(pg_current_xact_id()::text::bigint)`.
  - A stalled consumer calls `pg_advisory_lock_shared(k)` and then `pg_advisory_unlock_shared(k)` for the blocking transaction ID.
  - That call blocks until the appender commits *or aborts*. This closes case 1 without polling.
  - If it returns immediately, the blocker is not a cooperating appender (case 2).
  - Bound the wait with `lock_timeout`, and run it on a connection other than the LISTEN connection.
  - A unique-index probe insert could wait on a known gap position. But it cannot tell "not yet" from "never", it needs insert rights, and it delays the real appender.
- **Classify the blocker once and arm a single deadline.**
  - `pg_stat_activity` exposes `backend_xid`, `xact_start`, `state` and `state_change`. Marten's `GapLivenessProbe` shows how to prove a gap dead.
  - If every writer role has `transaction_timeout = T`, a blocker must end by `xact_start + T`.
  - The consumer can wait in `notifies(timeout=...)` until that deadline, re-check once with `pg_xact_status`, and skip a gap that is proven dead.
  - The docs advise against setting these timeouts cluster-wide, so set them per role or per database.
  - An idle consumer stays purely notification-driven.
- **Stop gating on the horizon.**
  - Running PgQ-style snapshot-difference reads on every wake-up means every deliverable event comes from a committed appender that sent its own notification, so stalls never arise.
  - The cost is delivery in commit order rather than position order. Per-stream order still holds when appends use optimistic concurrency, because a writer of version n+1 must have seen version n committed (inference).
  - Logical decoding also delivers "in commit order" and never decodes rolled-back transactions. But it needs `wal_level=logical` and a slot that retains WAL, and psycopg 3 does not yet support streaming replication (issue #71, open since August 2021).

#### Assigning partitions among consumer-group members

When several processes share one logical subscription, per-key order holds only while each key has at most one owner. The surveyed systems use five assignment designs:

**1. Static hash-modulo (Message DB and Eventide).**

- `get_category_messages` adds `MOD(@hash_64(cardinal_id(stream_name)), $6) = $5` for a given group size and member index.
- The cardinal id drops compound suffixes, so related streams always go to the same member.
- Eventide promises that "the consumer processing the stream is always the same consumer". Members sharing an identifier overwrite each other's positions and "skip messages".
- Membership is pure configuration, which has these consequences (inferred from the design):
  - A crashed member's share stops until it is restarted.
  - Changing `group_size` remaps most streams, so the group needs a coordinated stop and either reprocessing or reset positions.

**2. An advisory lock per unit.**

- Marten (`HotCold`) and the Elixir EventStore use this, along with the row-lock equivalents described earlier.
- First come, first served is not load balancing. Marten warns that "it's still likely that all projections will end up running on the first process to start up".
- A lock holder never gives up a lock voluntarily, so work moves only on a crash or shutdown.

**3. Lease rows renewed by heartbeat.**

- **Axon's token store** keeps one row per processor and segment. Another node may "steal" a claim the owner has not updated within `claimTimeout` (10 s). It retries free segments every `tokenClaimInterval` (5,000 ms), and it warns that "an event may be handled twice (or more)".
- **prooph** claims with `UPDATE … WHERE name = ? AND (locked_until IS NULL OR locked_until < ?)`.
- **PgQ's cooperative consumer** takes over a sub-consumer's batch once it has been inactive longer than `i_dead_interval`. Its batches are not tied to keys, so it gives up per-stream order.

**4. Hash segments separate from the member count (Axon).**

- Segments use bitmasks. Splitting creates mask `(mask << 1) + 1`, and merging finds the partner segment with `segmentId ^ firstBit`.
- Each segment has its own token, so whole segments move with their tokens and no keys are rehashed.
- A tracking processor starts with 1 segment and a pooled processor with 16. Spreading segments across nodes needs `releaseSegment(...)` or Axon Server.

**5. Leader assignment (Wolverine).**

- A leader, elected by advisory lock on PostgreSQL, keeps node and assignment tables. It ensures "every single known asynchronous event projection and every event subscription is running on exactly one running node".
- It rebalances when nodes join "to purposely spread the running load across your entire cluster". An optional settle period helps during rolling deploys.
- It reassigns a departed node's work at once.

**Leader elections often use lease rows plus a NOTIFY for fast handover.**

- **River** keeps a `river_leader` row. The leader re-elects itself every 5 s, and the lease lasts that interval plus 10 s of padding. On shutdown it resigns with "a delete with NOTIFY", and followers bid after a short random interval.
- **Oban's database peer** keeps an `oban_peers` row with a 30 s default interval. A departing leader deletes its row and broadcasts a leader-down notification.

| Design | Member joins | Member leaves cleanly | Member crashes |
|---|---|---|---|
| Static hash-modulo | Every member restarted with the new size; streams remapped | Its share stops until it restarts | Same as leaving |
| Advisory lock per unit | No rebalancing; gets only free units | Releases its locks; others take them on their next attempt | Locks free when the server ends the session; speed depends on keepalives |
| Lease row per unit | Claims unclaimed units; balancing needs an explicit release | Releases its claims | Taken over after the claim timeout; duplicates possible |
| Hash segments | As lease rows; segments move with their tokens | As lease rows | As lease rows |
| Leader assignment | Leader rebalances | Work reassigned at once | Health check notices; work reassigned |

#### Choosing the partition count and migrating checkpoints

**Keep the partition count independent of the member count.** Then adding or removing members needs no checkpoint migration.

- **Message DB ties partitions to members.** Its partition count *is* the group size, and the function offers no migration.
- **Akka Projections fixes a fine-grained slice count and moves only the assignment.**
  - A slice "is deterministically defined based on the persistence id". It is stored as a `slice INT` column and indexed `(slice, entity_type, db_timestamp, seq_nr)`.
  - Instances take contiguous ranges ("if using 1024 slices and running 4 Projection instances the slice ranges would be 0-255, 256-511, 512-767, 768-1023").
  - Because offsets are stored per slice, "more Projection instances can be added when needed and still reuse the offsets for the previous slice distributions".
  - The one rule is that when the slice count changes, "the projections with the old slice distribution must be stopped before starting new projections".

**Sizing the count:**

- **Parallelism ceiling.** No more members can be busy than there are partitions. Kafka practice is to "over-partition a bit", sizing for throughput one or two years ahead.
- **Balance (arithmetic).** The busiest of N members carries ⌈P/N⌉ partitions. P=16 over N=5 is 25 % above the mean. P=256 is about 1.6 %, and P=1024 about 0.1 %. A single hot stream can never be split, so raising P beyond the number of active streams gains nothing.
- **Powers of two make later splits and merges local.** PostgreSQL's own hash partitioning requires each modulus to be "a factor of the next larger modulus". Axon's masks and Akka's 1024 slices work the same way.
- **Per-partition cost in PostgreSQL (inference).** Each partition adds a checkpoint row, lease renewals (WAL and dead tuples) and possibly a query per wake-up. Keep these flat by reading a member's slices with one range predicate and renewing all its leases in one statement.
- **Use a stable hash.** Python's `hash()` on `str` is seeded randomly and "not predictable between repeated invocations of Python". Use an explicitly specified hash, and store the slice (or full hash) on each event row.

**A practical default (synthesis).** Fix a power-of-two slice count M (Akka uses 1024) and assign slices to members as ranges. If checkpoints must be kept per partition, make P a power of two at least 4 to 8 times the expected peak member count.

**Migrating checkpoints when the count does change:**

- **Split: always safe if each child inherits the parent's checkpoint.** The children's key spaces are subsets of the parent's, so nothing is skipped or repeated. Axon hands both halves the same token. Only the current owner may split, and only with no batch in flight.
- **Merge: the hard case.**
  - Taking the larger checkpoint loses the lagging sibling's events. Never do this.
  - Taking the smaller checkpoint gives duplicates, which is acceptable only with idempotent handlers.
  - Axon's `MergedTrackingToken` advances both halves separately "until both halves represent the same position", delivering an event only to a half whose token actually advanced.
  - A simpler alternative is a drain-to-barrier merge: pause both siblings, run the lagging one forward to the leader's checkpoint, then write one merged row.
- **Arbitrary rehash** (for example 12 → 20 partitions, or a new hash function):
  - **A global barrier.** Drain every old partition to a common position, as Kinesis requires parent shards to be drained before children, and as Kafka's KIP-253 proposes.
  - **A generalised composite checkpoint (inference).**
  - **Reset and replay.**
- **Consistent hashing matters less here.** Schemes such as jump consistent hash reduce how many keys move when buckets are added, and jump hash requires buckets to be numbered sequentially. With a fixed slice count and an assignment table, though, reassignment already moves the minimum, so consistent hashing mainly matters if partitions map straight to members. Unlike Kinesis shards or Kafka partitions, virtual partitions over one PostgreSQL log hold no data and never close. Only checkpoints need reconciling.
- **Safety rules for every migration:**
  - Quiesce and fence the affected rows by bumping their epochs.
  - Version the topology (slice count, ranges, hash version) in a group configuration row with a generation number that members check.
  - Make each change atomic in one transaction.

### A consumer design for a psycopg 3, PostgreSQL-only event store (synthesis)

The surveyed designs point to the following shape. The combination is synthesis; each element is drawn from the sources above.

1. **Connections.**
   - Hold one dedicated, autocommit, session-mode connection per process for `LISTEN`. It must not go through a transaction pooler.
   - Tune client and server keepalives and `tcp_user_timeout` on it, and turn on `client_connection_check_interval`.
   - The same session can hold session advisory locks for partition ownership at no extra connection cost.
2. **Claiming.**
   - Hash stream ids into a fixed, power-of-two set of slices stored on each event row.
   - Claim ownership of slice ranges, never individual events, with a lease row that carries an epoch. This can be backed by a session advisory lock if that is preferred.
   - Add an explicit balancing step: either a fair-share rule from heartbeat rows, or an elected leader.
   - Use NOTIFY for fast handover on graceful leave, and lease or lock expiry for crashes.
3. **Waking.**
   - Commit the `LISTEN`, catch up, then wait in `notifies(timeout=...)`.
   - Treat every notification only as a cue to re-run the ordered catch-up read from the stored checkpoint.
   - Arm one trailing read after each notification if appenders throttle.
   - Keep a jittered fallback poll for lost notifications, late commits and reconnects. Shorten it to a computed deadline while a stall is outstanding.
4. **Reading.**
   - Read forward only up to a safe high-water mark. The choice is between serialising appends (simplest consumer, capped write throughput), a `(transaction_id, position)` horizon (parallel writes, stalls behind any long writing transaction in the cluster), and gap detection with liveness checks.
   - Per-stream order inside a slice follows from single ownership plus forward reading.
5. **Checkpointing.**
   - Commit the projection's writes and an epoch-fenced, forward-only checkpoint `UPDATE` in one transaction, using `clock_timestamp()` for lease expiry.
   - Treat a zero-row update or a serialisation failure as lost ownership.
   - Make handlers idempotent for any effect outside PostgreSQL.
6. **Choosing a claim lifetime.**
   - Short handlers with effects inside the database can hold the claim in the handler transaction. This gives exactly-once database effects, at the cost of a pinned connection and vacuum pressure. Set `idle_in_transaction_session_timeout` or `transaction_timeout` just above the handler budget.
   - Long handlers, or handlers with external effects, should use claim-and-mark with a heartbeat-renewed lease and the epoch fence.
7. **Writing side.**
   - Send at most one notification per append transaction, with a constant payload or a position range.
   - Throttle only if notifying commits contend on the cluster-wide lock, and only with consumer-armed trailing reads to cover suppressed windows.
   - On PostgreSQL 19, prefer fine-grained channels only if many uninterested listeners exist. The commit-time lock still serialises notifying appends.

## Sources

- [PostgreSQL Documentation: SELECT (The Locking Clause)](https://www.postgresql.org/docs/current/sql-select.html) — tier-1 — postgresql.org
- [PostgreSQL Documentation: Explicit Locking (Advisory Locks)](https://www.postgresql.org/docs/current/explicit-locking.html) — tier-1 — postgresql.org
- [PostgreSQL Documentation: System Administration Functions (Advisory Lock Functions)](https://www.postgresql.org/docs/current/functions-admin.html) — tier-1 — postgresql.org
- [PostgreSQL Documentation: NOTIFY](https://www.postgresql.org/docs/current/sql-notify.html) — tier-1 — postgresql.org
- [PostgreSQL Documentation: LISTEN](https://www.postgresql.org/docs/current/sql-listen.html) — tier-1 — postgresql.org
- [PostgreSQL documentation: CREATE TRIGGER](https://www.postgresql.org/docs/current/sql-createtrigger.html) — tier-1 — postgresql.org
- [PostgreSQL documentation: Sequence Manipulation Functions](https://www.postgresql.org/docs/current/functions-sequence.html) — tier-1 — postgresql.org
- [PostgreSQL documentation: System Information Functions (transaction ID and snapshot functions)](https://www.postgresql.org/docs/current/functions-info.html) — tier-1 — postgresql.org
- [PostgreSQL: Transactions and Identifiers](https://www.postgresql.org/docs/current/transaction-id.html) — tier-1 — postgresql.org (official documentation)
- [PostgreSQL: Transaction Isolation](https://www.postgresql.org/docs/current/transaction-iso.html) — tier-1 — postgresql.org
- [PostgreSQL documentation: pg_locks](https://www.postgresql.org/docs/current/view-pg-locks.html) — tier-1 — postgresql.org
- [PostgreSQL documentation: Index Uniqueness Checks](https://www.postgresql.org/docs/current/index-unique-checks.html) — tier-1 — postgresql.org
- [PostgreSQL documentation: The Cumulative Statistics System (pg_stat_activity)](https://www.postgresql.org/docs/current/monitoring-stats.html) — tier-1 — postgresql.org
- [PostgreSQL documentation: Logical Decoding Concepts](https://www.postgresql.org/docs/current/logicaldecoding-explanation.html) — tier-1 — postgresql.org
- [PostgreSQL documentation: Logical Decoding Output Plugins](https://www.postgresql.org/docs/current/logicaldecoding-output-plugin.html) — tier-1 — postgresql.org
- [PostgreSQL: WAL Configuration](https://www.postgresql.org/docs/current/wal-configuration.html) — tier-1 — postgresql.org (official documentation)
- [PostgreSQL: pgbench](https://www.postgresql.org/docs/current/pgbench.html) — tier-1 — postgresql.org (official documentation)
- [PostgreSQL: Date/Time Functions and Operators](https://www.postgresql.org/docs/current/functions-datetime.html) — tier-1 — postgresql.org
- [PostgreSQL: DISCARD](https://www.postgresql.org/docs/current/sql-discard.html) — tier-1 — postgresql.org
- [CREATE TABLE — hash partitioning (PostgreSQL documentation)](https://www.postgresql.org/docs/current/sql-createtable.html) — tier-1 — postgresql.org
- [PostgreSQL documentation: Client Connection Defaults (timeouts)](https://www.postgresql.org/docs/current/runtime-config-client.html) — tier-1 — postgresql.org
- [PostgreSQL documentation: Connections and Authentication settings](https://www.postgresql.org/docs/current/runtime-config-connection.html) — tier-1 — postgresql.org
- [PostgreSQL: libpq Connection Strings and Parameters](https://www.postgresql.org/docs/current/libpq-connect.html) — tier-1 — postgresql.org
- [PostgreSQL 12 Release Notes](https://www.postgresql.org/docs/release/12.0/) — tier-1 — postgresql.org
- [PostgreSQL 14 Release Notes](https://www.postgresql.org/docs/14/release-14.html) — tier-1 — postgresql.org
- [PostgreSQL 17 Release Notes](https://www.postgresql.org/docs/17/release-17.html) — tier-1 — postgresql.org
- [PostgreSQL 19 release notes](https://www.postgresql.org/docs/19/release-19.html) — tier-1 — postgresql.org
- [PostgreSQL source: src/backend/commands/async.c (master)](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/commands/async.c) — tier-1 — github.com (postgres/postgres)
- [async.c on REL_18_STABLE (pre-change SignalBackends)](https://raw.githubusercontent.com/postgres/postgres/REL_18_STABLE/src/backend/commands/async.c) — tier-1 — github.com/postgres/postgres (official source)
- [xact.c on master (CommitTransaction and RecordTransactionCommit ordering)](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/access/transam/xact.c) — tier-1 — github.com/postgres/postgres (official source)
- [PostgreSQL source: procarray.c (GetSnapshotData, ComputeXidHorizons)](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/storage/ipc/procarray.c) — tier-1 — github.com/postgres/postgres
- [PostgreSQL commit 282b1cd: Optimize LISTEN/NOTIFY via shared channel map and direct advancement](https://github.com/postgres/postgres/commit/282b1cde9dedf456ecf02eb27caf086023a7bb71) — tier-1 — github.com (postgres/postgres)
- [Commit 0f24332: Fix NOTIFY wakeups for pre-commit LISTEN entries](https://github.com/postgres/postgres/commit/0f24332aeb4f43409c2a7bec9fef1e3317689bc5) — tier-1 — github.com/postgres/postgres (official mirror)
- [pgsql-hackers thread: Optimize LISTEN/NOTIFY](https://www.postgresql.org/message-id/flat/6899c044-4a82-49be-8117-e6f669765f7e%40app.fastmail.com) — tier-1 — postgresql.org (pgsql-hackers archive)
- [Tom Lane, Re: Optimize LISTEN/NOTIFY (2026-01-15 18:55, v34 and v35 benchmarks)](https://www.postgresql.org/message-id/803584.1768503335%40sss.pgh.pa.us) — tier-1 — postgresql.org (pgsql-hackers archive)
- [Tom Lane, Re: Proposal: Out-of-Order NOTIFY via GUC (WAL-transport idea)](https://www.postgresql.org/message-id/1878165.1752858390%40sss.pgh.pa.us) — tier-1 — postgresql.org (pgsql-hackers archive)
- [pgsql-hackers thread: Proposal: Out-of-Order NOTIFY via GUC to Improve LISTEN/NOTIFY Throughput](https://www.postgresql.org/message-id/flat/1878165.1752858390%40sss.pgh.pa.us) — tier-1 — postgresql.org (pgsql-hackers archive)
- [pgsql-hackers thread: [PATCH] Write Notifications Through WAL](https://www.postgresql.org/message-id/flat/CAK80%3Djgom1QLy%3DR3JaHpEqJMBwYKxOdjiDX4qywJ9G8-pgWySw%40mail.gmail.com) — tier-1 — postgresql.org (pgsql-hackers archive)
- [The docs for NOTIFY do not mention the global lock that NOTIFY takes](https://www.postgresql.org/message-id/175222328116.3157497.2260121997954044239%40wrigleys.postgresql.org) — tier-3 — postgresql.org (pgsql-docs mailing-list archive, user doc comment)
- [pg_async_notify_test benchmark tool](https://github.com/joelonsql/pg_async_notify_test) — tier-3 — github.com (personal repository)
- [Psycopg 3: Asynchronous operations (Asynchronous notifications)](https://www.psycopg.org/psycopg3/docs/advanced/async.html) — tier-1 — psycopg.org
- [psycopg 3 documentation: Connection classes (notifies)](https://www.psycopg.org/psycopg3/docs/api/connections.html) — tier-1 — psycopg.org
- [Psycopg 3: Transactions management](https://www.psycopg.org/psycopg3/docs/basic/transactions.html) — tier-1 — psycopg.org
- [psycopg 3: Release notes](https://www.psycopg.org/psycopg3/docs/news.html) — tier-1 — psycopg.org
- [psycopg issue #1371: Async task cancellation is never delivered when the server stops responding on an established connection](https://github.com/psycopg/psycopg/issues/1371) — tier-1 — github.com/psycopg/psycopg (official project tracker)
- [psycopg issue #71: Replication support](https://github.com/psycopg/psycopg/issues/71) — tier-1 — github.com/psycopg/psycopg
- [PgBouncer Features](https://www.pgbouncer.org/features.html) — tier-1 — pgbouncer.org
- [PgBouncer: Configuration](https://www.pgbouncer.org/config.html) — tier-1 — pgbouncer.org
- [tcp(7) — Linux manual page](https://man7.org/linux/man-pages/man7/tcp.7.html) — tier-1 — man7.org (Linux man-pages project)
- [Linux kernel documentation: IP Sysctl](https://docs.kernel.org/networking/ip-sysctl.html) — tier-1 — docs.kernel.org
- [Fast failover with Amazon Aurora PostgreSQL](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/AuroraPostgreSQL.BestPractices.FastFailover.html) — tier-1 — docs.aws.amazon.com
- [When TCP sockets refuse to die](https://blog.cloudflare.com/when-tcp-sockets-refuse-to-die/) — tier-2 — blog.cloudflare.com
- [TCP keepalive for a better PostgreSQL experience](https://www.cybertec-postgresql.com/en/tcp-keepalive-for-a-better-postgresql-experience/) — tier-2 — cybertec-postgresql.com
- [postgresqlco.nf: tcp_keepalives_idle](https://postgresqlco.nf/doc/en/param/tcp_keepalives_idle/) — tier-2 — postgresqlco.nf
- [postgresqlco.nf: tcp_user_timeout](https://postgresqlco.nf/doc/en/param/tcp_user_timeout/) — tier-2 — postgresqlco.nf
- [postgresqlco.nf: client_connection_check_interval](https://postgresqlco.nf/doc/en/param/client_connection_check_interval/) — tier-2 — postgresqlco.nf
- [Procrastinate: Discussions](https://procrastinate.readthedocs.io/en/stable/discussions.html) — tier-1 — procrastinate.readthedocs.io
- [Procrastinate: Ensure jobs run sequentially and in order](https://procrastinate.readthedocs.io/en/stable/howto/advanced/locks.html) — tier-1 — procrastinate.readthedocs.io
- [Procrastinate: Retry stalled jobs](https://procrastinate.readthedocs.io/en/stable/howto/production/retry_stalled_jobs.html) — tier-1 — procrastinate.readthedocs.io
- [Procrastinate schema.sql (procrastinate_fetch_job_v2, notify trigger, lock index)](https://raw.githubusercontent.com/procrastinate-org/procrastinate/main/procrastinate/sql/schema.sql) — tier-1 — github.com (procrastinate-org/procrastinate)
- [Graphile Worker: Introduction](https://worker.graphile.org/docs) — tier-1 — worker.graphile.org
- [Graphile Worker: addJob (queueName)](https://worker.graphile.org/docs/library/add-job) — tier-1 — worker.graphile.org
- [Graphile Worker: Admin functions](https://worker.graphile.org/docs/admin-functions) — tier-1 — worker.graphile.org
- [Graphile Worker configuration](https://worker.graphile.org/docs/config) — tier-1 — worker.graphile.org (official Graphile Worker documentation)
- [Graphile Worker getJobs.ts (job and named-queue claiming SQL)](https://raw.githubusercontent.com/graphile/worker/main/src/sql/getJobs.ts) — tier-1 — github.com (graphile/worker)
- [completeJobs.ts (Graphile Worker)](https://raw.githubusercontent.com/graphile/worker/main/src/sql/completeJobs.ts) — tier-1 — github.com (graphile/worker)
- [Graphile Worker migration 000011.sql](https://raw.githubusercontent.com/graphile/worker/main/sql/000011.sql) — tier-1 — github.com (graphile/worker)
- [Graphile Worker CHANGELOG](https://raw.githubusercontent.com/graphile/worker/main/CHANGELOG.md) — tier-1 — github.com (graphile/worker)
- [Graphile Worker src/main.ts](https://raw.githubusercontent.com/graphile/worker/main/src/main.ts) — tier-1 — github.com (graphile/worker)
- [Graphile Worker `src/localQueue.ts`](https://raw.githubusercontent.com/graphile/worker/main/src/localQueue.ts) — tier-1 — raw.githubusercontent.com (graphile/worker primary source)
- [Graphile Worker `src/index.ts` (localQueue and refetchDelay option docs)](https://raw.githubusercontent.com/graphile/worker/main/src/index.ts) — tier-1 — raw.githubusercontent.com (graphile/worker primary source)
- [River client.go (FetchCooldown, FetchPollInterval, PollOnly)](https://raw.githubusercontent.com/riverqueue/river/master/client.go) — tier-1 — github.com (riverqueue/river)
- [River river_job.sql (JobGetAvailable)](https://raw.githubusercontent.com/riverqueue/river/master/riverdriver/riverpgxv5/internal/dbsqlc/river_job.sql) — tier-1 — github.com (riverqueue/river)
- [River CHANGELOG](https://raw.githubusercontent.com/riverqueue/river/master/CHANGELOG.md) — tier-1 — github.com (riverqueue/river)
- [River internal/notifylimiter/limiter.go](https://raw.githubusercontent.com/riverqueue/river/master/internal/notifylimiter/limiter.go) — tier-1 — github.com (riverqueue/river)
- [River `internal/util/chanutil/debounced_chan.go`](https://raw.githubusercontent.com/riverqueue/river/master/internal/util/chanutil/debounced_chan.go) — tier-1 — raw.githubusercontent.com (riverqueue/river primary source)
- [River `producer.go` (fetch debouncer, fetchPollLoop, jitter)](https://raw.githubusercontent.com/riverqueue/river/master/producer.go) — tier-1 — raw.githubusercontent.com (riverqueue/river primary source)
- [River leadership elector.go](https://github.com/riverqueue/river/blob/master/internal/leadership/elector.go) — tier-1 — github.com (riverqueue project source)
- [River: Using River with PgBouncer](https://riverqueue.com/docs/pgbouncer) — tier-1 — riverqueue.com
- [River: Transactional job completion](https://riverqueue.com/docs/transactional-job-completion) — tier-1 — riverqueue.com
- [River Pro: Sequences](https://riverqueue.com/docs/pro/sequences) — tier-1 — riverqueue.com
- [Que README](https://github.com/que-rb/que) — tier-1 — github.com (que-rb/que)
- [Que docs README (polling, LISTEN/NOTIFY, locking connection)](https://raw.githubusercontent.com/que-rb/que/master/docs/README.md) — tier-1 — github.com (que-rb/que)
- [Que migration 4 up.sql (que_job_notify, que_lockers)](https://raw.githubusercontent.com/que-rb/que/master/lib/que/migrations/4/up.sql) — tier-1 — github.com (que-rb/que)
- [Que module documentation 0.14.3 (recursive CTE job lock)](https://www.rubydoc.info/gems/que/0.14.3/Que) — tier-1 — rubydoc.info (Que gem API docs)
- [GoodJob README](https://github.com/bensheldon/good_job) — tier-1 — github.com (bensheldon/good_job)
- [Solid Queue README](https://github.com/rails/solid_queue) — tier-1 — github.com (rails/solid_queue)
- [pgmq FIFO queues (v1.12.0)](https://docs.pgedge.com/pgmq/v1-12-0/fifo-queues/) — tier-2 — docs.pgedge.com
- [pgmq README](https://raw.githubusercontent.com/pgmq/pgmq/main/pgmq-extension/README.md) — tier-1 — github.com (pgmq)
- [pgmq SQL functions reference (`enable_notify_insert`)](https://pgmq.github.io/pgmq/latest/api/sql/functions/) — tier-1 — pgmq.github.io (official pgmq documentation)
- [pgmq extension SQL (notify_queue_listeners, notify_insert_throttle)](https://raw.githubusercontent.com/pgmq/pgmq/main/pgmq-extension/sql/pgmq.sql) — tier-1 — github.com (pgmq/pgmq)
- [Oban.Notifier](https://oban.hexdocs.pm/Oban.Notifier.html) — tier-1 — hexdocs.pm (oban package docs)
- [Oban module documentation (`dispatch_cooldown`, `insert_trigger`, Stager)](https://oban.hexdocs.pm/Oban.html) — tier-1 — oban.hexdocs.pm (official Oban documentation)
- [Oban CHANGELOG at v2.17.0](https://raw.githubusercontent.com/oban-bg/oban/v2.17.0/CHANGELOG.md) — tier-1 — github.com (oban-bg/oban)
- [Oban migration V12 (drops oban_notify trigger)](https://raw.githubusercontent.com/oban-bg/oban/main/lib/oban/migrations/postgres/v12.ex) — tier-1 — github.com (oban-bg/oban)
- [Oban lib/oban/engine.ex](https://raw.githubusercontent.com/oban-bg/oban/main/lib/oban/engine.ex) — tier-1 — github.com (oban-bg/oban)
- [Oban lib/oban/stager.ex](https://raw.githubusercontent.com/oban-bg/oban/main/lib/oban/stager.ex) — tier-1 — github.com (oban-bg/oban)
- [Oban `lib/oban/queues/producer.ex`](https://raw.githubusercontent.com/oban-bg/oban/main/lib/oban/queues/producer.ex) — tier-1 — raw.githubusercontent.com (oban-bg/oban primary source)
- [Oban.Peer documentation](https://oban.hexdocs.pm/Oban.Peer.html) — tier-1 — hexdocs.pm
- [Oban peers database.ex](https://github.com/oban-bg/oban/blob/main/lib/oban/peers/database.ex) — tier-1 — github.com (oban-bg project source)
- [PgQueuer schema_ddl.py](https://raw.githubusercontent.com/janbjorge/pgqueuer/main/pgqueuer/adapters/persistence/schema_ddl.py) — tier-1 — github.com (janbjorge/pgqueuer)
- [PgQueuer domain/schema/notify.py](https://raw.githubusercontent.com/janbjorge/pgqueuer/main/pgqueuer/domain/schema/notify.py) — tier-1 — github.com (janbjorge/pgqueuer)
- [PgQ source: pgq.batch_event_sql.sql](https://raw.githubusercontent.com/pgq/pgq/master/functions/pgq.batch_event_sql.sql) — tier-1 — raw.githubusercontent.com (pgq/pgq repository)
- [PgQ source: pgq.ticker.sql](https://raw.githubusercontent.com/pgq/pgq/master/functions/pgq.ticker.sql) — tier-1 — raw.githubusercontent.com (pgq/pgq repository)
- [PgQ source: tables.sql](https://raw.githubusercontent.com/pgq/pgq/master/structure/tables.sql) — tier-1 — github.com/pgq/pgq
- [pgqd README](https://github.com/pgq/pgqd) — tier-1 — github.com/pgq/pgqd
- [pgq_coop.next_batch.sql](https://github.com/pgq/pgq-coop/blob/master/functions/pgq_coop.next_batch.sql) — tier-1 — github.com (pgq project source)
- [lodash `throttle.js` (leading and trailing semantics)](https://raw.githubusercontent.com/lodash/lodash/4.17.21-npm/throttle.js) — tier-1 — raw.githubusercontent.com (lodash/lodash primary source)
- [Commanded EventStore: Subscriptions](https://eventstore.hexdocs.pm/subscriptions.html) — tier-1 — hexdocs.pm (eventstore package docs)
- [Commanded EventStore advisory_locks.ex](https://raw.githubusercontent.com/commanded/eventstore/master/lib/event_store/advisory_locks.ex) — tier-1 — github.com (commanded/eventstore)
- [Commanded EventStore subscription_fsm.ex](https://raw.githubusercontent.com/commanded/eventstore/master/lib/event_store/subscriptions/subscription_fsm.ex) — tier-1 — github.com (commanded/eventstore)
- [EventStore storage lock.ex](https://github.com/commanded/eventstore/blob/master/lib/event_store/storage/lock.ex) — tier-1 — github.com (commanded project source)
- [Commanded EventStore insert_events.sql.eex](https://raw.githubusercontent.com/commanded/eventstore/master/lib/event_store/sql/statements/insert_events.sql.eex) — tier-1 — github.com (commanded/eventstore)
- [Commanded EventStore lib/event_store/sql/init.ex](https://raw.githubusercontent.com/commanded/eventstore/master/lib/event_store/sql/init.ex) — tier-1 — github.com (commanded/eventstore)
- [Commanded EventStore notifications/publisher.ex](https://raw.githubusercontent.com/commanded/eventstore/master/lib/event_store/notifications/publisher.ex) — tier-1 — github.com (commanded/eventstore)
- [Commanded EventStore append_events_bench.exs](https://raw.githubusercontent.com/commanded/eventstore/master/bench/storage/append_events_bench.exs) — tier-1 — github.com/commanded/eventstore (source code)
- [Commanded EventStore issue #3: Could read_all_events_forward miss events?](https://github.com/commanded/eventstore/issues/3) — tier-3 — github.com/commanded/eventstore issue tracker
- [Commanded EventStore issue #107: Benchmarks](https://github.com/commanded/eventstore/issues/107) — tier-3 — github.com/commanded/eventstore issue tracker
- [Commanded EventStore issue #139: Make global $all stream building a separate task](https://github.com/commanded/eventstore/issues/139) — tier-3 — github.com/commanded/eventstore issue tracker
- [Marten: Async Daemon](https://martendb.io/events/projections/async-daemon.html) — tier-1 — martendb.io
- [Marten: Appending Events](https://martendb.io/events/appending.html) — tier-1 — martendb.io (official documentation)
- [Marten source: HighWaterDetector.cs](https://raw.githubusercontent.com/JasperFx/marten/master/src/Marten/Events/Daemon/HighWater/HighWaterDetector.cs) — tier-1 — raw.githubusercontent.com (JasperFx/marten repository)
- [Marten source: GapLivenessProbe.cs](https://raw.githubusercontent.com/JasperFx/marten/master/src/Marten/Events/Daemon/HighWater/GapLivenessProbe.cs) — tier-1 — raw.githubusercontent.com (JasperFx/marten repository)
- [JasperFx.Events source: DaemonSettings.cs](https://raw.githubusercontent.com/JasperFx/jasperfx/main/src/JasperFx.Events/Daemon/DaemonSettings.cs) — tier-1 — raw.githubusercontent.com (JasperFx/jasperfx repository)
- [UpdateProjectionProgress.cs (Marten)](https://raw.githubusercontent.com/JasperFx/marten/master/src/Marten/Events/Daemon/Progress/UpdateProjectionProgress.cs) — tier-1 — github.com/JasperFx (primary source)
- [Wolverine distribution of Marten projections and subscriptions](https://wolverinefx.net/guide/durability/marten/distribution.html) — tier-1 — wolverinefx.net
- [Wolverine leadership election and troubleshooting](https://wolverinefx.net/guide/durability/leadership-and-troubleshooting.html) — tier-1 — wolverinefx.net
- [Message DB source: acquire-lock.sql](https://raw.githubusercontent.com/message-db/message-db/master/database/functions/acquire-lock.sql) — tier-1 — raw.githubusercontent.com (message-db/message-db repository)
- [Message DB source: write-message.sql](https://raw.githubusercontent.com/message-db/message-db/master/database/functions/write-message.sql) — tier-1 — raw.githubusercontent.com (message-db/message-db repository)
- [Message DB source: messages.sql](https://raw.githubusercontent.com/message-db/message-db/master/database/tables/messages.sql) — tier-1 — raw.githubusercontent.com (message-db/message-db repository)
- [message-db get-category-messages.sql](https://github.com/message-db/message-db/blob/master/database/functions/get-category-messages.sql) — tier-1 — github.com (message-db project source)
- [Message DB README](https://github.com/message-db/message-db) — tier-1 — github.com (message-db project)
- [Eventide Consumers user guide](https://eventide-project.org/user-guide/consumers.html) — tier-1 — eventide-project.org
- [Eventide Message DB server functions](https://eventide-project.org/user-guide/message-db/server-functions.html) — tier-1 — eventide-project.org
- [Emmett source: readMessagesBatch.ts](https://raw.githubusercontent.com/event-driven-io/emmett/main/src/packages/emmett-postgresql/src/eventStore/schema/readMessagesBatch.ts) — tier-1 — raw.githubusercontent.com (event-driven-io/emmett repository)
- [Emmett appendToStream.ts](https://raw.githubusercontent.com/event-driven-io/emmett/main/src/packages/emmett-postgresql/src/eventStore/schema/appendToStream.ts) — tier-1 — github.com/event-driven-io/emmett (source code)
- [postgresql-event-sourcing (README)](https://github.com/eugene-khyst/postgresql-event-sourcing) — tier-3 — github.com (eugene-khyst personal repository)
- [eventsourcing postgres.py](https://raw.githubusercontent.com/pyeventsourcing/eventsourcing/main/eventsourcing/postgres.py) — tier-1 — github.com/pyeventsourcing/eventsourcing (source code)
- [eventsourcing docs: Persistence](https://eventsourcing.readthedocs.io/en/stable/topics/persistence.html) — tier-1 — eventsourcing.readthedocs.io (official documentation)
- [Axon Framework API: GapAwareTrackingToken](https://apidocs.axoniq.io/4.10/org/axonframework/eventhandling/GapAwareTrackingToken.html) — tier-1 — apidocs.axoniq.io
- [Axon Framework API: JdbcEventStorageEngine.Builder](https://apidocs.axoniq.io/4.10/org/axonframework/eventsourcing/eventstore/jdbc/JdbcEventStorageEngine.Builder.html) — tier-1 — apidocs.axoniq.io
- [Axon Framework reference: streaming event processors](https://docs.axoniq.io/axon-framework-reference/4.10/events/event-processors/streaming/) — tier-1 — docs.axoniq.io
- [Axon Framework Segment.java](https://github.com/AxonFramework/AxonFramework/blob/axon-4.10.x/messaging/src/main/java/org/axonframework/eventhandling/Segment.java) — tier-1 — github.com (AxonFramework project source)
- [Segment (Axon Framework 4.10 API)](https://apidocs.axoniq.io/4.10/org/axonframework/eventhandling/Segment.html) — tier-1 — apidocs.axoniq.io
- [MergedTrackingToken (Axon Framework 4.10 API)](https://apidocs.axoniq.io/4.10/org/axonframework/eventhandling/MergedTrackingToken.html) — tier-1 — apidocs.axoniq.io
- [TrackerStatus.java (Axon Framework source, 4.10.x)](https://raw.githubusercontent.com/AxonFramework/AxonFramework/axon-4.10.x/messaging/src/main/java/org/axonframework/eventhandling/TrackerStatus.java) — tier-1 — github.com/AxonFramework
- [Prooph PDO event store source: GapDetection.php](https://raw.githubusercontent.com/prooph/pdo-event-store/master/src/Projection/GapDetection.php) — tier-1 — raw.githubusercontent.com (prooph/pdo-event-store repository)
- [prooph PdoEventStoreProjector.php](https://github.com/prooph/pdo-event-store/blob/master/src/Projection/PdoEventStoreProjector.php) — tier-1 — github.com (prooph project source)
- [Watermill SQL Pub/Sub documentation](https://watermill.io/pubsubs/sql/) — tier-1 — watermill.io
- [watermill-sql offsets_adapter_postgresql.go](https://github.com/ThreeDotsLabs/watermill-sql/blob/master/pkg/sql/offsets_adapter_postgresql.go) — tier-1 — github.com (ThreeDotsLabs project source)
- [pg_eventstore subscriptions documentation](https://github.com/yousty/pg_eventstore/blob/main/docs/subscriptions.md) — tier-1 — github.com (yousty pg_eventstore project docs)
- [Query plugin — eventsBySlices (Akka Persistence R2DBC)](https://doc.akka.io/libraries/akka-persistence-r2dbc/current/query.html) — tier-1 — doc.akka.io
- [Events from slices (Akka Projections)](https://doc.akka.io/libraries/akka-projection/current/eventsourced.html) — tier-1 — doc.akka.io
- [R2DBC offset store documentation source (Akka Projections)](https://raw.githubusercontent.com/akka/akka-projection/main/docs/src/main/paradox/r2dbc.md) — tier-1 — github.com/akka
- [Akka Projection R2DBC PostgreSQL DDL](https://raw.githubusercontent.com/akka/akka-projection/main/akka-projection-r2dbc/ddl-scripts/create_tables_postgres.sql) — tier-1 — github.com/akka
- [Akka Persistence R2DBC PostgreSQL DDL](https://raw.githubusercontent.com/akka/akka-persistence-r2dbc/main/ddl-scripts/create_tables_postgres.sql) — tier-1 — github.com/akka
- [How to do distributed locking](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html) — tier-2 — martin.kleppmann.com
- [The Chubby lock service for loosely-coupled distributed systems (OSDI 2006)](https://static.googleusercontent.com/media/research.google.com/en//archive/chubby-osdi06.pdf) — tier-1 — research.google.com (Google Research, OSDI)
- [DynamoDB metadata tables and load balancing in KCL](https://docs.aws.amazon.com/streams/latest/dev/kcl-dynamoDB.html) — tier-1 — docs.aws.amazon.com
- [DynamoDBLeaseRefresher.java (amazon-kinesis-client)](https://raw.githubusercontent.com/awslabs/amazon-kinesis-client/master/amazon-kinesis-client/src/main/java/software/amazon/kinesis/leases/dynamodb/DynamoDBLeaseRefresher.java) — tier-1 — github.com/awslabs (primary source)
- [DynamoDBLeaseRenewer.java (amazon-kinesis-client)](https://raw.githubusercontent.com/awslabs/amazon-kinesis-client/master/amazon-kinesis-client/src/main/java/software/amazon/kinesis/leases/dynamodb/DynamoDBLeaseRenewer.java) — tier-1 — github.com/awslabs (primary source)
- [DynamoDBCheckpointer.java (amazon-kinesis-client)](https://raw.githubusercontent.com/awslabs/amazon-kinesis-client/master/amazon-kinesis-client/src/main/java/software/amazon/kinesis/checkpoint/dynamodb/DynamoDBCheckpointer.java) — tier-1 — github.com/awslabs (primary source)
- [Complete the resharding action (Amazon Kinesis Data Streams)](https://docs.aws.amazon.com/streams/latest/dev/kinesis-using-sdk-java-after-resharding.html) — tier-1 — docs.aws.amazon.com
- [KIP-98: Exactly Once Delivery and Transactional Messaging](https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging) — tier-1 — cwiki.apache.org (Apache Kafka)
- [KIP-253: Support in-order message delivery with partition expansion](https://cwiki.apache.org/confluence/display/KAFKA/KIP-253%3A+Support+in-order+message+delivery+with+partition+expansion) — tier-1 — cwiki.apache.org (Apache Kafka wiki)
- [KIP-447: Producer scalability for exactly once semantics](https://cwiki.apache.org/confluence/display/KAFKA/KIP-447%3A+Producer+scalability+for+exactly+once+semantics) — tier-1 — cwiki.apache.org (Apache Kafka)
- [KIP-848: The Next Generation of the Consumer Rebalance Protocol](https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol) — tier-1 — cwiki.apache.org (Apache Kafka)
- [CommitFailedException (Kafka Javadoc)](https://kafka.apache.org/39/javadoc/org/apache/kafka/clients/consumer/CommitFailedException.html) — tier-1 — kafka.apache.org
- [Partition load balancing for event processing - Azure Event Hubs](https://learn.microsoft.com/en-us/azure/event-hubs/event-processor-balance-partition-load) — tier-1 — learn.microsoft.com
- [azure.eventhub.aio.CheckpointStore class](https://learn.microsoft.com/en-us/python/api/azure-eventhub/azure.eventhub.aio.checkpointstore) — tier-1 — learn.microsoft.com
- [Change Feed Processor (Azure Cosmos DB)](https://learn.microsoft.com/en-us/azure/cosmos-db/change-feed-processor) — tier-1 — learn.microsoft.com
- [Amazon SQS visibility timeout](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-visibility-timeout.html) — tier-1 — docs.aws.amazon.com
- [DeleteMessage (Amazon SQS API Reference)](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_DeleteMessage.html) — tier-1 — docs.aws.amazon.com
- [Pattern: Transactional outbox](https://microservices.io/patterns/data/transactional-outbox.html) — tier-2 — microservices.io
- [How to choose the number of topics/partitions in a Kafka cluster?](https://www.confluent.io/blog/how-choose-number-topics-partitions-kafka-cluster/) — tier-2 — confluent.io
- [A Fast, Minimal Memory, Consistent Hash Algorithm (jump consistent hash)](https://arxiv.org/abs/1406.2294) — tier-2 — arxiv.org
- [Command line and environment — PYTHONHASHSEED (Python documentation)](https://docs.python.org/3/using/cmdline.html) — tier-1 — docs.python.org
- [Picking task from queue – revisit](https://www.depesz.com/2016/05/04/picking-task-from-queue-revisit/) — tier-2 — depesz.com
- [Postgres 9.5 feature highlight: SKIP LOCKED for row-level locking](https://paquier.xyz/postgresql-2/postgres-9-5-feature-highlight-skip-locked-row-level/) — tier-2 — paquier.xyz
- [Postgres Job Queues & Failure By MVCC](https://brandur.org/postgres-queues) — tier-2 — brandur.org
- [How Postgres sequences issues can impact your messaging guarantees](https://event-driven.io/en/ordering_in_postgres_outbox/) — tier-2 — event-driven.io (Oskar Dudycz)
- [Postgres sequences can commit out-of-order](https://blog.sequinstream.com/postgres-sequences-can-commit-out-of-order/) — tier-3 — blog.sequinstream.com (Sequin vendor blog)
- [Postgres LISTEN/NOTIFY does not scale](https://www.recall.ai/blog/postgres-listen-notify-does-not-scale) — tier-3 — recall.ai (company engineering blog)
