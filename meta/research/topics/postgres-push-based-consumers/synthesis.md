---
type: "topic-research"
id: "synthesis"
title: "PostgreSQL Push-Based Consumer Delivery"
date: "2026-10-05T16:29:43+00:00"
author: "Toby Clemson"
producer: "research-topic"
status: "complete"
kind: "synthesis"
rounds_covered: 1
tags: ["research", "topic-research", "synthesis"]
last_updated: "2026-10-05T16:29:43+00:00"
last_updated_by: "Toby Clemson"
schema_version: 1
---

# PostgreSQL Push-Based Consumer Delivery

## Overview

**Push delivery is feasible on PostgreSQL alone, but only as a wake-up layer on
top of the polling read the library already does.** PostgreSQL has one
primitive built for signalling between sessions: asynchronous notifications
(`LISTEN`, `NOTIFY` and `pg_notify()`). Notifications are delivered at commit,
in commit order, and only once the sender's writes are visible. They are also
at-most-once, reach only sessions listening at that moment, survive neither a
disconnect nor a restart, never reach a standby, and cannot be received through
a transaction-mode pooler. A notification therefore cannot carry work or
advance a checkpoint. It can only say "the log may have moved; read it now".
Every mature PostgreSQL-backed queue and event store examined that uses
notifications is built this way. Every job queue among them (River, Oban,
Graphile Worker, GoodJob, pg-boss, pgmq, Procrastinate, Que) also keeps a
fallback poll. Commanded's Elixir EventStore recovers a miss only when a later
notification reveals the gap. The scholarly literature on push versus pull
(dissemination, interrupt-polling hybrids, reliable multicast, leases, failure
detectors, pull-based log brokers) reaches the same conclusion: push is a
latency optimisation, and pull is the correctness mechanism.

**On the two costs the brief names, push helps differently.**

- **The latency floor** (on average half the 200 ms poll interval) is what
  notifications remove. On an unloaded server, delivery takes well under a
  millisecond. A woken consumer's latency becomes delivery, plus any
  coalescing window, plus one read. A lost notification costs latency up to
  the fallback interval, never correctness.
- **The idle query load** (consumers × poll frequency) falls in two steps.
  Notifications let the fallback interval grow from 200 ms to seconds. Most of
  the remaining load disappears once the "is there anything new?" check runs
  once per process rather than once per consumer. That second step needs no
  notifications at all.

**There are three costs and one correctness hazard to design around.**

- **Writer cost.** Every transaction that issues `NOTIFY` takes one
  cluster-wide heavyweight lock just before its commit record. It holds the
  lock through its WAL flush and any synchronous-standby wait, so notifying
  commits run one at a time, across every database on the instance. In one
  benchmark, adding `NOTIFY` to a one-row insert cut throughput from about
  31,000 to about 818 transactions per second at 100 clients. A production
  outage at Recall.ai traced back to this lock. PostgreSQL 19 wakes fewer
  listeners but keeps the lock.
- **Failure coupling.** All databases share one notification queue (8 GB by
  default). The slowest listener's position pins it. Once the queue is full,
  every transaction that issued `NOTIFY` fails at commit, data and all.
- **Listener fragility.** A listener needs its own session-level connection on
  the primary. It must be kept alive through idle timers, half-open TCP,
  middleboxes, poolers and failovers, and it must re-`LISTEN` and catch up
  after every reconnect.
- **Ordering.** `BIGSERIAL` positions are drawn at insert time but become
  visible at commit, so concurrent appends can become visible out of position
  order. A consumer that wakes on a notification and checkpoints the highest
  position it saw can skip an earlier event for good. A wake-up makes this
  window more likely to be hit, not less. Correctness has to come from a safe
  read frontier.

**Recommended direction.** Keep the position-based scan of the events table as
the only data path, and add the following around it:

1. **A safe frontier.** Keep the library's existing per-log append lock as the
   requirement for log-wide push consumers. In the repository this is the
   default `LOG` serialisation guarantee: a transaction-level advisory lock
   taken before positions are drawn and held until commit. It makes
   `sequence_number` order equal commit order among the library's writers, so
   `sequence_number > checkpoint` is already gap-safe. A deployment that needs
   concurrent unserialised appends must instead adopt a transaction-ID
   (`xid8`) frontier or evidence-based gap detection.
2. **A post-commit, debounced notifier on the writer side.** Each process sends
   one `pg_notify` to one static, versioned, schema-scoped channel, from its
   own short autocommit transaction, after the append's commit has returned. A
   leading-and-trailing window of about 50 ms keeps notifying commits to at
   most two per process per window, and keeps the cluster-wide lock and the
   queue-full failure off the append path.
3. **One dedicated listener connection per process.** It runs in autocommit,
   outside any pool, on a direct or session-mode connection that reaches only
   the primary, with tuned keepalives and a heartbeat. It fans out in process
   to per-subscriber, level-triggered wake-up flags.
4. **A consumer loop of listen, catch up, drain, then wait with a timeout.**
   The timeout is a long, freshly jittered fallback poll, seconds rather than
   200 ms. The loop drops back to the current short interval whenever the
   listener is unhealthy, and offers an explicit poll-only mode for
   deployments that cannot hold a session connection.

The recommendation holds while append concurrency stays within what one
serialised commit at a time can sustain, and while consumers can reach the
primary through a session-capable path. Logical decoding is the only durable
push stream PostgreSQL offers. It stays an optional accelerator, not the
default, because it needs `wal_level = logical`, retained WAL, failover-slot
configuration and a replication client that psycopg 3 does not provide.

## Findings

### What PostgreSQL offers for signalling new work

**Asynchronous notifications.** These are the only purpose-built
cross-session signal. The path from `NOTIFY` to a listening client has six
steps:

1. **Inside the transaction.** `Async_Notify()` appends to a backend-local
   pending list. Duplicates with the same channel and payload are dropped,
   using a linear scan up to 16 entries and a hash table after that.
2. **Pre-commit.** `PreCommit_Notify()` forces a transaction ID. It takes
   `LockSharedObject(DatabaseRelationId, InvalidOid, 0, AccessExclusiveLock)`,
   a heavyweight lock on "database 0" that serialises notifying writers across
   the whole cluster. It then appends the entries to one SLRU-backed queue
   under `pg_notify/`.
3. **The commit, under the lock.** `RecordTransactionCommit()` writes, and
   where required flushes, the commit record. It also waits for synchronous
   standbys. The transaction then becomes visible, and the lock is released
   at transaction end.
4. **Signalling.** `SignalBackends()` sends `PROCSIG_NOTIFY_INTERRUPT`.
   - On PostgreSQL 18 and earlier it signals every listening backend in the
     database, whatever channel it listens on.
   - PostgreSQL 19 (commit `282b1cde`, Joel Jacobson, committed by Tom Lane on
     15 January 2026) adds a shared map from channel to listeners. It signals
     only interested backends and advances idle uninterested ones without
     waking them.
5. **The listener reads.** Each listening backend scans forward from its
   queue position. It tests snapshot visibility before commit status, so a
   delivered notification's writes are always visible to a snapshot taken
   afterwards. It stops at entries from transactions still in progress.
6. **On the wire.** Each notification becomes a `NotificationResponse`
   message, sent only while the session is outside a transaction. A listener
   that keeps a transaction open receives nothing until it ends.

**Triggers.** A trigger-driven notification is a trigger calling `pg_notify()`.
It joins the same pending list and the same commit path:

- **`FOR EACH ROW`** with a per-row payload queues one entry per row. With a
  constant payload, it folds to one entry per transaction.
- **`FOR EACH STATEMENT`** fires once per statement. It still fires for
  statements that change no rows. With `REFERENCING NEW TABLE` it can send one
  summary per statement.
- **What granularity cannot change.** It changes queue volume and payload,
  never delivery timing. Every transaction that writes the table still takes
  the commit lock.

**Advisory locks** are a coordination tool, not a signal. A waiter blocks
inside a statement, so it cannot receive notifications while it waits, and the
wake-up carries no payload. Their role here is to guarantee single ownership,
for example one active consumer per subscription, and to serialise appends.

**Logical decoding** is the only durable push stream. A replication slot
decodes committed transactions in commit order, never emits rolled-back ones,
and the walsender pushes them over the replication protocol.
`pg_logical_emit_message()` adds a transactional, durable equivalent of
`NOTIFY`. Its costs are covered under the alternatives below.

**Other primitives.**

- **`WAIT FOR LSN`** (PostgreSQL 19) lets a standby wait for replay. It is for
  read-your-writes on replicas, not for signalling work.
- **Server-side long-polling**, such as pgmq's `read_with_poll`, is a PL/pgSQL
  loop that re-runs the read and calls `pg_sleep`. It is still polling, and it
  holds a backend for the whole wait.

| Mechanism | Payload | Survives disconnect or restart | Cost on the writer | Ordering | psycopg 3 support |
|---|---|---|---|---|---|
| `LISTEN`/`NOTIFY` (`pg_notify()`) | Text under 8,000 bytes | No (at most once) | Cluster-wide lock through commit record, WAL flush and any synchronous-standby wait | Commit order | `notifies()` generator |
| Row or statement trigger + `pg_notify()` | Per row, folded within a transaction; or a statement summary | No | The same lock on every writing transaction | Commit order | As above |
| Batched or dedicated notifier (built on `NOTIFY`) | Coalesced hint | No | None on the append path; one locked commit per flush | Commit order of the flushes | Ordinary connection |
| Advisory lock wait | None | Not applicable | Holding and releasing a lock | Lock-queue order | A blocking statement |
| Logical decoding slot | Full changes or emitted messages | Yes; may resend after a crash | WAL volume, retained WAL and catalog rows, a restart to enable | Commit order, with LSNs | Not supported (psycopg2, `pg_recvlogical` or SQL polling only) |
| In-database long poll (`read_with_poll`) | Rows | Yes (it is a table read) | None | Read order | Ordinary SQL, holding a backend |

### What `LISTEN`/`NOTIFY` guarantees, and how it fails

#### Guarantees

- **Commit-time delivery.** A notification is delivered "until and unless the
  transaction is committed", and from different transactions "in the order in
  which the transactions committed". Within one transaction, order is kept
  and identical channel-and-payload pairs fold into one. Notifications from
  different transactions are never folded.
- **Visibility on wake-up.** By the time a client receives a notification,
  the sender's rows are visible to any new snapshot.
- **The start-up rule.** `LISTEN` takes effect at commit, and a session
  receives only notifications committed after that point. The documented
  race-free order is: execute and commit `LISTEN`, then inspect the database
  in a new transaction, then rely on notifications. The first few
  notifications may repeat what the read saw, which "is usually harmless".
  This order must be repeated after every reconnect.

#### What it does not guarantee

- **No durability.** Registrations end with the session. Nothing is kept for
  absent listeners. The queue is wiped at every restart ("notifications are
  not expected to survive database crashes"). Notifications never reach a
  standby, and hot standbys reject `LISTEN` and `NOTIFY`.
- **No delivery through transaction pooling.** A `LISTEN` issued through
  PgBouncer in transaction mode succeeds silently. The registration then sits
  on a backend the client no longer holds. Notifications go to nobody, or to
  whichever client next holds that backend.
- **No two-phase commit.** A transaction that ran `LISTEN` or `NOTIFY` cannot
  be prepared.

#### The commit lock

**What it serialises.** The unit of serialisation is the notifying
transaction, not the notification. A transaction that sends one notification
or a thousand takes the lock once, and one that sends none never takes it.
The lock covers the queue append, the commit record, any WAL flush and any
synchronous-standby wait. Notifying commits therefore cannot share a
group-commit flush.

**Measured costs.**

| Concurrency | Workload | With the lock | Comparison | Source |
|---|---|---|---|---|
| 100 clients | `INSERT` + `NOTIFY` in one transaction | 817.9 TPS | 30,981.6 TPS for `INSERT` alone (about 38×); 87,750.5 TPS for `NOTIFY` alone | Mukhin, pgsql-hackers, October 2025 |
| 100 clients | `INSERT` + `NOTIFY` | about 29,000–31,000 TPS | about 63,000–66,000 TPS with the WAL-transport prototype | Bagga, pgsql-hackers |
| 1,000 clients, 1 listener | `NOTIFY` | 7,679 TPS | 95,430 TPS with the lock removed (about 12×) | Jacobson, pgsql-hackers, July 2025 |
| Not published | Trigger `NOTIFY` on every write | about 2,900 writes/s | about 60,000 writes/s with batched notifications (about 20×) | DBOS |
| Tens of thousands of writers | `NOTIFY` in the write path | Outage: 300+ waiters, lock acquired after 1,015.9 ms, CPU and I/O falling | Notification moved to the application | Recall.ai |

**Why the measurements differ.** The two master-branch figures at 100 clients
(about 818 and about 30,000 TPS) differ by environment. They fit a serialised
section whose length is set by flush latency: the cost is hidden on fast-flush
hardware and fully exposed on flush-bound or remote storage. The ceiling is
roughly one notifying commit per (flush latency + in-lock work). By derived
arithmetic, that is about 110–1,000 per second on EBS gp3 and about 2,000 per
second on io2 Block Express, and lower again with a synchronous standby.

**Notify-only transactions are cheap.** A transaction whose only action is
`NOTIFY` writes no WAL before its commit record, so it takes the asynchronous
commit path whatever `synchronous_commit` says. It skips the flush and the
standby wait. This is a reading of `RecordTransactionCommit()`, consistent
with the 87,751 TPS notify-only figure. It is why moving `NOTIFY` out of the
append transaction is the main lever.

**Upstream status.**

- **PostgreSQL 19 (Beta 4 on 24 September 2026).** Commit `282b1cde` removes
  wake-ups of uninterested listeners. It reports "integer multiples" of
  throughput when many listeners each care about few messages, and "no
  improvement for the simplest case where every listener reads every message".
  The lock stays, and the direct-advance logic depends on it. A follow-up fix
  (`0f24332`, May 2026) closed a window in which a listener whose `LISTEN` had
  not yet committed could miss messages. A PostgreSQL 19 deployment should run
  a build that includes it.
- **Recall.ai and DBOS disagree.** Recall.ai's updated post credits the commit
  with eliminating its bottleneck. DBOS and the source code say the global
  lock remains. Both can be true if Recall.ai's waits included many
  uninterested listeners.
- **The "Write Notifications Through WAL" patch.** It would hold the lock only
  for a small fixed-size queue entry. It raised `INSERT` + `NOTIFY` from 818 to
  about 33,800 TPS, but made bare `NOTIFY` about 2.5× slower. It had
  correctness problems in review and is uncommitted, last at v9 in November
  2025. An out-of-order `NOTIFY` setting was rejected.
- **Planning assumption.** Treat the lock as a lasting constraint.

#### The shared queue and failure when it fills

**How it fills.** One queue serves the whole cluster. Its size is set by
`max_notify_queue_pages`, which defaults to 1,048,576 pages (8 GB) from
PostgreSQL 17 and needs a restart to change. The queue can be truncated only
up to the slowest listener's position. Two kinds of listener stop truncation
for everyone:

- a listener stuck inside a long transaction;
- a backend blocked writing to a client that has vanished. It sends
  notifications into the dead socket until the buffers fill, then waits in
  `ClientWrite`. That lasts until the kernel's retransmission timeout ends the
  connection, about 15–16 minutes on Linux defaults.

**Warnings and failure.** From half full, the server logs warnings naming a
blocking process, and `pg_notification_queue_usage()` reports the fill level.
Once the queue is full, transactions calling `NOTIFY` "will fail at commit".
If `NOTIFY` shares a transaction with an append, one stuck listener anywhere
in the cluster can stop every append.

**How likely a vanished listener is to fill it.** With small payloads, a
single vanished listener realistically cannot fill 8 GB before the kernel ends
its connection. That needs about 230,000–390,000 notifications per second
sustained. With payloads near 8,000 bytes on a busy system it can, at about
1,100 per second. The most dangerous case is a client that is alive but has
stopped reading, because the server keeps sending window probes and the queue
can be pinned indefinitely.

**An older bug.** Minor releases from before commit `8eeb4a0` (November 2025,
back-patched to 14–18) could truncate commit-status data that lagging
listeners still needed. Long-lived listeners should run a recent minor
release.

#### Payload and channel limits

- **Payload.** It must be shorter than 8,000 bytes
  (`BLCKSZ − NAMEDATALEN − 128`).
- **Channel names.** They are identifiers of at most 63 bytes, and unquoted
  names fold to lower case. `LISTEN` silently truncates a long name, while
  `pg_notify()` rejects it. `LISTEN` cannot take bind parameters, so it needs
  `psycopg.sql.Identifier`.
- **Design consequence.** A payload is no more durable than its notification.
  The usual design sends nothing, a routing key or a position hint, never the
  event body.

### Poolers, connection cost and listener health

#### How poolers and proxies treat a listener

`NOTIFY` is transactional, so it works through any pooler that keeps a
transaction on one backend. `LISTEN` is session state, so it works only where
one client keeps one backend for the whole subscription.

| Pooler or proxy | `LISTEN` on its multiplexed path | Route that supports a listener | Idle timer that closes a receive-only listener |
|---|---|---|---|
| PgBouncer | Transaction mode: "Never"; fails silently | Session mode, settable per database alias or user | No; `client_idle_timeout` skips linked clients |
| Amazon RDS Proxy | Pins the session; no multiplexing | Every connection; the pin cannot be configured away | Yes: `IdleClientTimeout` (1,800 s default) counts only client requests; hard 24-hour cap |
| Supavisor (Supabase) | Transaction mode (port 6543): unsupported | Session mode (port 5432) or a direct connection | Only if `client_idle_timeout` is set |
| PgCat | Not documented; unsafe by inference | Session mode | Not established |
| Cloud SQL Managed Connection Pooling | Transaction mode (the default): unsupported | Session mode | Off by default |
| Neon pooled endpoint | PgBouncer transaction mode: unsupported | Direct connection | Not applicable |
| Azure built-in PgBouncer | Defaults to transaction mode; no per-database `pool_mode` | Direct connection on 5432, or the whole server in session mode | — |
| PgDog | Emulated in transaction mode when `pub_sub_channel_size` is set; at most once | Session mode passes `LISTEN` through | Yes, if the global `client_idle_timeout` is set |

**PgDog's transaction-mode pub/sub.** It is the only pooler found that
emulates `LISTEN` in transaction mode, and its weaknesses rule it out as a
primary path:

- **Silent outages.** When its dedicated connection breaks, clients get no
  signal, and its documentation says it "will not attempt re-delivery".
- **Silent lag drops.** A slow subscriber loses messages, recorded only in a
  counter.
- **Early acknowledgement.** It acknowledges `LISTEN` before it takes effect,
  which breaks the listen-then-read rule.
- **Immaturity.** It is still at version 0.1.x, with a run of pub/sub fixes
  through September 2026.

Its maintainers advise storing messages in a table and polling as well.

**Recommended topology.** Writers stay on whatever pool they already use,
because `NOTIFY` works in transaction mode. The listener uses a direct
connection or a session-mode alias.

#### What a dedicated listener connection costs

**The budget.** Every working listener is a real backend counted against
`max_connections`, however many clients the pooler accepts. Each pooler adds
its own charge:

- PgBouncer counts it against `default_pool_size` (20 by default).
- RDS Proxy holds one proxy slot for it permanently.
- Any open listener stops Aurora Serverless v2 from auto-pausing.

**Per-connection cost.** It is a few MiB of memory per backend. CPU cost
mattered before PostgreSQL 14: Andres Freund measured 5,000 idle connections
cutting one active connection from 33,457 to 9,807 TPS. PostgreSQL 14's
snapshot work largely fixed that.

**The cheap pattern.** One session can listen on any number of channels. One
listener per process, fanned out in process, is the cheap pattern. River,
Oban, GoodJob, Commanded's EventStore, Procrastinate, oban-py and Brandur
Leach's "notifier" pattern all use it.

#### psycopg 3 mechanics that matter

- **Autocommit.** Use autocommit on the listener. Otherwise even a heartbeat
  `SELECT` opens a transaction, which delays delivery and blocks queue
  cleanup.
- **The `notifies()` generator.**
  - It holds the connection's non-re-entrant lock for its whole life. No other
    statement can run on that connection while it waits, so registration
    changes and heartbeats must run between bounded runs of
    `notifies(timeout=…, stop_after=…)` (available since 3.2).
  - The timeout is a total deadline computed once and checked only after a
    whole batch has been yielded.
- **Versions.**
  - Before 3.2.4, notifications arriving between generator runs were lost.
  - 3.2.4 buffers them, but leaked memory on connections that never iterated
    (about 200 MB an hour at more than 10,000 notifications a minute).
  - 3.2.10 fixed the leak and deprecated mixing `add_notify_handler()` with
    the generator.
  - Handlers fire only during other queries, so they cannot drive an idle
    listener.
  - **Minimum.** Require 3.2.10 or later and use the generator only.
- **Closing mid-batch loses the rest.** Closing the generator while it is
  suspended mid-batch silently drops the remaining notifications of that
  batch. This covers `close()`, a `break` that drops the last reference, an
  exception in the loop body, and an abandoned generator in a cancelled task.
  It comes from a reading of the source in every release up to 3.3.6. Let
  bounded runs end by themselves.
- **Cancellation.** Bounding a heartbeat with an asyncio timeout was
  unreliable before psycopg 3.3.6 combined with libpq 17. Cancellation could
  hang indefinitely against an unresponsive server (issue #1371). On older
  stacks, `tcp_user_timeout` is what bounds such a hang.
- **Disconnect errors.** Server-initiated disconnects raise specific
  `OperationalError` subclasses such as `AdminShutdown` and
  `IdleSessionTimeout` (since 3.2.4). One `except psycopg.OperationalError`
  covers them all.
- **Keep the listener out of `psycopg_pool`.** The pool never runs `UNLISTEN`
  on return, so a registration leaks to later borrowers. A permanently held
  connection also shrinks the pool and escapes `max_lifetime`. Open the
  listener from the same connection-parameter source as the pool, but as a
  standalone `connect(..., autocommit=True)`, as Procrastinate does.

#### Keeping the listener alive and detecting its death

**Idle timers count only client requests.** No server-pushed notification
resets any of these timers:

- PostgreSQL's `idle_session_timeout` (set it to 0 for the listener's
  session);
- RDS Proxy's `IdleClientTimeout`;
- Supavisor's and PgDog's `client_idle_timeout`.

A heartbeat query is required more often than half the shortest such timer on
the path, and below layer-4 idle limits:

- AWS NAT gateway: 350 s, fixed;
- AWS Network Load Balancer: 350 s by default;
- Azure Load Balancer: 4 minutes by default.

**Default TCP settings detect nothing useful.**

- libpq enables keepalives but uses OS timings. On Linux that is 7,200 s idle
  and 9 probes 75 s apart, about 2 h 11 min.
- Once data is in flight, `tcp_retries2` takes 13–30 minutes.
- Set `keepalives_idle`, `keepalives_interval`, `keepalives_count` and
  `tcp_user_timeout` explicitly. Cloudflare's rule is
  `TCP_USER_TIMEOUT ≈ idle + interval × count`. Examples range from 10/5/3 with
  25 s for a direct connection, through 30/10/3 with 60 s behind middleboxes,
  to AWS's aggressive 1/1/5.
- `tcp_user_timeout` is a no-op on macOS, and all four settings are ignored on
  Unix-domain sockets.
- Keepalives test only the nearest TCP hop. Behind a proxy, only an
  application round trip proves the server session is alive.

**Probes and what they catch.** A `SELECT 1` heartbeat cannot see every
failure.

| Failure | `SELECT 1` heartbeat | `NOTIFY` to itself | Canary `NOTIFY` from another connection |
|---|---|---|---|
| Silent TCP or middlebox loss (with `tcp_user_timeout`) | Detected | Detected | Detected |
| Listener stuck in a transaction | Missed | Detected | Detected |
| Queue full | Missed | Detected | Detected |
| `LISTEN` registration lost (transparent reconnect, `DISCARD ALL`, proxy re-attach) | Missed | Detected | Detected |
| Connected to a hot standby | Missed | Detected (rejected) | Detected |
| Proxy that answers queries but drops unprompted messages | Missed | Missed | Detected |
| Notifications already missed | Missed | Missed | Missed |

**Probes used in production.** Oban's Sonar pings a dedicated channel and
classifies the node as isolated, solitary or clustered. PgQueuer sends a
uniquely tagged health event and waits for it. pg-boss queries
`pg_listening_channels()`.

**A cheap identity heartbeat.**
`SELECT pg_backend_pid(), array(SELECT pg_listening_channels())` detects a
changed backend or a lost registration. Two published bugs show that the
probe itself needs testing:

- River's ping once used an already-cancelled context and never reached the
  server.
- pgmq's mixed-case queue names silently never notified until 1.13.1.

In both cases polling hid the fault.

#### Reconnecting and failover

**Three loss windows.** No reconnect logic makes a restart or failover
lossless for notifications. Three windows lose them:

1. notifications committed but unread when the server dies;
2. everything committed while no session is registered;
3. anything committed before the new `LISTEN` takes effect.

Surviving failover is therefore a property of the catch-up read. Managed
high-availability setups keep committed data, so a catch-up against the new
writer finds every event that committed before the failover.

**The reconnect loop.**

1. Connect with a `connect_timeout` (psycopg otherwise waits up to 130 s per
   host), keepalives, `tcp_user_timeout`, `autocommit=True` and a multi-host
   connection string with `target_session_attrs=read-write`.
2. Issue `LISTEN` before reading anything.
3. Catch up from the durable checkpoint.
4. Wake every in-process subscriber.
5. Wait in `notifies(timeout=…)`, and heartbeat between waits.
6. On any `OperationalError` or identity mismatch, discard the connection,
   back off with jitter (capped around 30 s), and repeat.

**Use `read-write`, not `primary`.** After an RDS blue/green switchover the
old primary stays up read-only but is not a hot standby. `primary` would
accept it, `LISTEN` would succeed, and the listener would wait forever.

**Documented outage windows.**

- Aurora: under 60 s, often under 30 s.
- RDS Multi-AZ cluster: under 35 s.
- RDS Multi-AZ instance: 60–120 s.
- Cloud SQL high availability: about 60 s.
- Azure zone-redundant high availability: 60–120 s.

**RDS Proxy.** It pins listeners and does not document whether a pinned
listener survives failover.

**The AWS Advanced Python Wrapper.** It does not cover a listener blocked in
`notifies()`. It carries over no `LISTEN` registrations, so the listener must
re-`LISTEN` on the same wrapper object after failover.

**A simpler alternative.** Brandur Leach's notifier treats an unhealthy
listener as fatal and lets the supervisor restart the process. That suits
deployments where restarts are fast and failovers rare.

### Hybrid push and poll: the design that has converged

#### Prior art

None of the systems examined uses a notification to carry work. They differ
in where the notification is sent and how the fallback works.

| System | Wake-up path | Who notifies | Fallback poll | Published lesson |
|---|---|---|---|---|
| River (Go) | `LISTEN`/`NOTIFY` | Client code, debounced per queue (`FetchCooldown` 100 ms), inside the insert transaction; trigger removed in 0.5.0 | `FetchPollInterval` 1 s, jittered; `PollOnly` mode | One `NOTIFY` per job was too much overhead at high throughput |
| Oban (Elixir) | Postgres or Erlang `PG` notifier | Application code since 2.17.0 (trigger removed) | Stager 1 s; local polling when Sonar reports isolation | Detect a broken notifier and fall back to polling |
| Graphile Worker (Node) | `LISTEN`/`NOTIFY` | The `add_job`/`add_jobs` SQL functions | 2 s, for future-dated jobs and retries | Drops events when no worker is idle; polling remains because nothing fires when time "ticks over" |
| GoodJob (Ruby) | `LISTEN`/`NOTIFY` | Application code | 10 s in production | Dedicated connection, keepalive, reconnect; poll-only for PgBouncer |
| pg-boss (Node) | Opt-in `useListenNotify` | Inside the insert transaction | 2 s; 30 s while the listener is healthy | Notification is "an opt-in optimization on top of polling, not a replacement for it" |
| pgmq | `read_with_poll`; opt-in throttled trigger | Deferred constraint trigger, 250 ms per queue | Client's choice | Mixed-case queues never notified until 1.13.1 |
| Solid Queue (Ruby) | Polling only | — | Workers 0.1 s, dispatchers 1 s | A tiny polled table with a covering index: about 1,300 polls/s at 110 µs average at 37signals |
| Message DB / Eventide | Polling only | — | Sleeps only after an empty batch | Category advisory lock on write; "global position may have gaps" |
| Marten async daemon (.NET) | Polling with a high-water mark | — | 250 ms busy, 1 s quiet | `NOTIFY` "not adequate by itself"; events skipped under concurrent appends until 9.16.1 |
| Commanded EventStore (Elixir) | `LISTEN`/`NOTIFY` | Trigger on the `streams` table, range payload | No timed poll; subscriptions catch up from storage when a later notification reveals a gap, or on overflow | One listener per node reads once and broadcasts |
| Procrastinate (Python, psycopg 3) | `LISTEN`/`NOTIFY` | Insert trigger | 5 s; "polls the database regardless" | A standalone autocommit connection outside the pool |

#### The loop

1. **Subscribe first.** Issue and commit `LISTEN`.
2. **Clear, then read.** Clear the pending-hint flag, then drain from the
   durable checkpoint in bounded batches until a batch comes back short.
3. **Wait.** Wait for a notification or a timeout, whichever comes first.
4. Go back to step 2.

Mesa monitors (a notify is a hint; the waiter re-checks in a loop) and
Chandy–Lamport snapshots explain the order. Reading before subscribing loses
any event committed in between. Subscribing first can only cause a duplicate,
which an idempotent read absorbs. Clearing the flag after the read would erase
exactly the hints the read could not see. The guarantee that every
notification is either delivered or already visible holds only under these
conditions:

- the `LISTEN` has committed, which in autocommit means the command has
  returned;
- the catch-up read's snapshot is taken after that commit;
- reads go to the same primary.

#### The wake-up signal should be level-triggered

A notification only marks a consumer runnable. Mogul and Ramakrishnan's
receive-livelock remedy shows the pattern: "interrupts only to initiate
polling", bounded quotas, and re-enabling only when no work is pending.

- **A pending flag.** Represent the wake-up as one pending flag per
  subscriber, such as an `asyncio.Event` or `threading.Event`, never a queue
  of per-event callbacks. Any number of notifications between two reads
  collapses into one read, and memory scales with subscribers, not with
  notification rate.
- **Bounded batches.** Bound each batch. Fetch again immediately while
  batches come back full. Go back to waiting only after a short batch.
- **A cooldown.** A consumer-side cooldown (River uses 100 ms, Oban 5 ms) caps
  the read rate under heavy append load.

#### Sizing the fallback poll

**It is a lease on silence.** Its interval is set by the staleness tolerable
when push fails, not by typical latency, so it can be far longer than the
interval it replaces. Clamp it between two bounds, after Bhide et al.'s
TTR_min and TTR_max:

- **The upper bound** comes from the acceptable staleness under failure.
- **The lower bound** comes from the fleet's query budget (N·c/T).

**Defaults in use.** River 1 s, Graphile Worker 2 s, Procrastinate 5 s,
GoodJob 10 s, pg-boss 30 s while healthy.

**Jitter freshly on every cycle.** Floyd and Jacobson show that independent
periodic processes can lock into step in an abrupt phase transition. A common
coupling is a timer reset by a notification every listener sees, or after a
database restart. Draw each interval from a wide range, such as
[0.5T, 1.5T]. Do not reset the timer on shared events, or reset it only with
fresh jitter.

**Adapt to evidence that push failed, not to the data rate.**

- Poll at once, and drop to the lower bound, on reconnect, on re-`LISTEN`, or
  when a poll finds events nothing announced.
- Grow back multiplicatively after consecutive polls that find nothing
  unannounced.
- If poll latency rises, raise the lower bound. Kaul et al. show that past a
  point, polling harder makes data staler.

**Keep the timers separate.** They do different jobs and have different
limits.

| Timer | Purpose | Limit |
|---|---|---|
| Heartbeat | Stop idle timers closing the listener; detect a silently dead path | Below every idle timeout on the path |
| Fallback poll | Recover missed hints and unannounced work | The worst-case latency for a missed hint |
| Gap or frontier re-check | Release a hold that will end without a notification | Short; only while held |
| Refetch | Drain a backlog | Immediately after a full batch |
| Reconnect | Avoid hammering a restarting server | Exponential with a cap |

**Measure missed notifications.** Count the safety-net reads that find
events. The fraction of events first seen by a timeout-driven read, rather
than a notification-driven one, is the production measure of missed
notifications and the input for tuning the interval.

#### Switching between waiting and continuous polling

Queueing-control theory (Heyman; Lu and Serfozo) says the optimal on/off
policy is hysteretic: switch up at a higher threshold than you switch down.

- **Switch to continuous batched polling** on a full batch, or when
  notifications arrived during the read.
- **Switch back** only after a dwell time with no work, never after one empty
  poll.
- **Keep the subscription live in both modes.** Never `UNLISTEN` on a mode
  change. Discard queued hints while polling.

No standard constants exist. The literature supplies structures, not settings.

#### Sending fewer notifications

**The lever.** Only transactions that carry a notification pay for the lock,
so the lever is how many committing transactions notify:

| Strategy | Examples | Effect on the lock |
|---|---|---|
| One notification per row or per append, in the transaction | Older River and Oban triggers; Commanded; Procrastinate | Every append takes the lock |
| Statement-level trigger or constant payload | Older Graphile Worker; PgQueuer | Less queue volume, but the same number of lock acquisitions |
| Throttled, inside the transaction | River's per-process limiter; pgmq's throttle row | Fewer acquisitions; suppressed appends go unannounced; pgmq's row becomes a hot spot |
| Post-commit, from a separate transaction | DBOS's batched flusher; River's local wake-ups; Recall.ai's application layer | No lock on the append path; notify-only commits are cheap; a crash between the two commits loses the wake-up |
| No `NOTIFY` at all | Solid Queue, Message DB, Marten | No cost; latency set by polling |

**Firing pattern.** Use leading and trailing edges. If no window is open,
send at once and open one. If a window is open, mark it pending and send once
when it closes.

- A pure trailing debounce adds the whole window to every wake-up, and never
  fires under continuous load.
- A pure leading throttle leaves a burst's last appends waiting for the
  fallback poll.
- If the appender throttles, consumers should arm one trailing catch-up read
  per notification, because the trailing edge of a standard throttle does not
  cover suppressed appends.

**Rules for a post-commit notifier.**

- Send only after the commit reply.
- Never pipeline the `pg_notify` in the same Sync group as the insert, which
  puts it back inside the append's transaction.
- Log notification errors rather than raising them, since a caller that sees
  an exception after a successful commit may retry and append twice.

#### Channel and payload design

**One static channel.** With one shared listener per process, a single static,
schema-scoped channel is the simplest layout:

- no `LISTEN`/`UNLISTEN` churn and no interrupted waits;
- one `LISTEN` on reconnect;
- no mapping from category names to identifiers;
- "all events" consumers come free.

**Per-category channels** gain a server-side advantage only on PostgreSQL 19,
and only when many listener connections each care about largely separate
categories. Before PostgreSQL 19, every listener is signalled anyway.

**Payloads.** Use an empty payload or a small self-describing one, such as
`{"v":1,"category":…,"position":…}`. Treat any position in it as a lower bound
on the high-water mark, never as a checkpoint.

**Versioning.** Version the channel name from the start, for example
`<schema>_events_v1`. Readers should be tolerant: an unknown version or an
unparseable payload means "wake up anyway". Mismatched channels after an
upgrade fail silently. River's channel rename was covered only by polling.

### The ordering problem: reading without skipping events

**Why positions commit out of order.**

- `BIGSERIAL` calls `nextval` at insert. Sequence values are never returned,
  so aborts, crashes and `INSERT … ON CONFLICT` leave gaps.
- Transactions can commit in a different order from the one in which they drew
  their numbers.
- Notifications arrive in commit order, the order that diverges from position
  order.

**How an event is lost.** T1 takes position 10 and T2 takes 11. T2 commits
first, and its notification wakes the consumer. The consumer reads
`position > checkpoint`, sees 11, and checkpoints 11. When T1 commits, event 10
is behind the checkpoint and is never read.

**Reports of the failure.** Precision Nutrition, Cloudscale, Oskar Dudycz,
Sequin, Axon users and Protean all describe it. Marten shipped it under
concurrent appends until 9.16.1 (July 2026). Emmett lost messages because its
cursor advanced on position alone. Precision Nutrition also found about 0.6% of
ids permanently missing.

**What does not help.** Running the consumer at `SERIALIZABLE` does not
help, because sequence draws sit outside transactional isolation. A
notification payload cannot settle a gap, because it says nothing about
uncommitted transactions.

**The safe frontier.** Every correct design reads only up to a safe frontier:
a point below which nothing more can become visible.

| Approach | Never skips? | Resume token | Delivery order | Concurrent appends | Main cost | Needs a timer beyond notifications? |
|---|---|---|---|---|---|---|
| Serialised appends (lock before allocation, held to commit) | Yes | Integer position | Position = commit order | One at a time per lock scope | Throughput ceiling ≈ 1 / (in-lock work + flush + standby wait) | No, with in-transaction `NOTIFY`; a fallback poll otherwise |
| `xid8` horizon (`transaction_id < pg_snapshot_xmin(pg_current_snapshot())`) | Yes | `(xid8, position)` | First-write order | Unrestricted | Any writing transaction anywhere in the cluster stalls delivery; xids invalid across logical moves | Yes, while rows sit above the horizon |
| Snapshot-difference batches (PgQ) | Yes | A snapshot (tick) plus an inner cursor | Commit-visibility order | Unrestricted | A ticker; positions arrive out of numeric order | Ticker cadence |
| Gap detection with a wall-clock timeout (Axon defaults; Marten before 9.16.1) | **No**: late commits are lost | Integer plus gap set | Position order | Unrestricted | Silent loss | Yes |
| Gap detection with live-transaction evidence (Marten 9.16.1) | Yes, if evidence comes from one snapshot gathered before the read | Integer | Position order | Unrestricted | Complexity; stalls behind live appenders | Yes |
| Appender-registered advisory-lock floor (Sequin, with published races fixed) | Yes | Integer | Position order | Unrestricted, but every writer must follow the protocol | `pg_locks` reads; protocol discipline | Yes |
| Logical decoding slot | Yes | Commit end LSN | Commit order | Unrestricted | `wal_level = logical`, retention, no psycopg 3 streaming | No, while streaming |

#### Serialised appends

**Why it works.** The append transaction takes an exclusive lock before it
draws positions and holds it to commit. Three kinds of lock work:

- `pg_advisory_xact_lock`;
- a counter-row `UPDATE`;
- `LOCK TABLE … IN EXCLUSIVE MODE`.

A commit becomes visible before its locks are released, so allocation order
equals commit order. Every gap below a visible position is permanent and can
be skipped at once. Message DB, Commanded, Python `eventsourcing` and
Precision Nutrition do this.

**Two conditions.**

- **Positions drawn under the lock.** Drawing them in a separate `nextval()`
  before the lock reopens the window.
- **Every writer takes the lock.** Advisory locks are voluntary, so a bulk
  import or hand-written `INSERT` that skips the lock brings the race back.

**Scope.** Per-category locks protect only category readers. Message DB offers
no read across the whole store for that reason.

**Cost.** The lock is held through the commit flush and any synchronous
standby round trip. Throughput through one lock is therefore at most about
1 / hold time, and serialised writers never share a group-commit flush.

| Commit path inside the lock window | Published latency | Ceiling per lock (calculated, not measured) |
|---|---|---|
| Enterprise NVMe with power-loss protection | 0.14 ms | about 7,000/s |
| AWS EBS io2 Block Express | under 500 µs | about 2,000/s |
| Consumer SSD | 0.49–9.3 ms | about 100–2,000/s |
| Synchronous replication across zones | local flush + up to ~2 ms + standby flush | about 300–500/s |

**Measured effect.** Commanded's benchmark (100-event batches) rose only from
about 4,900 to about 8,600 events per second going from 1 to 50 writers, while
per-operation time rose from about 20 ms to about 580 ms. Batching more events
per transaction raises events per second, not transactions per second.

**Ways to shrink the window.**

- Take the lock as late as possible.
- Send the lock, the insert and `COMMIT` in one round trip, with a server-side
  function or psycopg pipeline mode.
- Allocate within the insert. Marten's "Quick" append cut append time by
  40–50%.

**Interaction with `NOTIFY`.** A store that already serialises appends pays
most of `NOTIFY`'s serialisation cost already. In-transaction `NOTIFY` then
adds little throughput cost, and makes notification order, commit order and
position order identical. That is what `eventsourcing` 9.5 does. The design
that scales with concurrency is unserialised appends with no `NOTIFY` inside
the append transaction.

#### The transaction-ID horizon

**The scheme.** Stamp each row with
`transaction_id xid8 NOT NULL DEFAULT pg_current_xact_id()`, index
`(transaction_id, position)`, and read:

```sql
WHERE (transaction_id, position) > (:last_tx, :last_pos)
  AND transaction_id < pg_snapshot_xmin(pg_current_snapshot())
ORDER BY transaction_id, position
LIMIT :batch
```

Every transaction ID below a snapshot's `xmin` is "either committed and
visible, or rolled back and dead", so nothing can appear behind the cursor. It
needs no writer coordination, and gaps stop mattering. Emmett, Watermill,
eugene-khyst's reference implementation and Oskar Dudycz use it.

**Its costs.**

- **Cluster-wide stalls.** `GetSnapshotData` has no database filter. Any
  transaction holding an xid, in any database on the cluster, holds every
  consumer back:
  - a session idle in transaction after writing;
  - a migration;
  - `SELECT … FOR UPDATE`;
  - a prepared transaction.

  Read-only transactions do not hold it back. With stock settings there is no
  time bound. Sequin rejected the approach because a two-minute transaction on
  another table hid two minutes of events.
- **Bounding stalls.** Use `transaction_timeout` (PostgreSQL 17+) per role for
  every writing role on the cluster, `idle_in_transaction_session_timeout`,
  and `max_prepared_transactions = 0`. Any session can override them.
- **Ordering inversion within a stream.** Ordering follows each
  transaction's first write. A transaction that already holds an xid before it
  reads a stream head can deliver version n+1 before version n. Emmett's
  append function opens exactly this window. Running appends at
  `REPEATABLE READ` or higher, or making the head check the first write,
  closes it.
- **A timer is needed.** An unrelated blocker that finishes sends no
  notification.
- **The consumer's own xid.** The consumer must compute the horizon in a
  transaction that has not written anything, or its own xid caps the horizon.
- **Checkpoints across moves.** Stored xids mean nothing across `pg_dump` and
  restore, logical replication, logical-mode blue/green deployments or AWS
  DMS. The destination's counter does not continue the source's, which can
  skip all new events or interleave them silently. Mitigations are a
  `pg_resetwal -e` counter fence (self-hosted only), an era column, or
  position-only checkpoints.
- **Migrating an existing store.** Emmett's and Watermill's migrations show
  that moving live checkpoints to the pair format is a breaking change with an
  overlap window. It needs care, or a constant backfill xid for history.

#### Gap detection

**Timeouts alone lose events.** Detection that gives up on a gap after a
wall-clock timeout loses any append slower than the timeout. Axon defaults to
60 s or 10,000 positions. Marten used a 3-second threshold before 9.16.1, and
prooph retries after 0, 5, 50 and 500 ms.

**Evidence-based detection works.** Marten 9.16.1 skips a gap only when no
live transaction could fill it. It judges that in a single statement, using
`pg_locks`, the snapshot's in-progress list and `pg_stat_activity`, and records
every skip. Marten discussion #4953 found four ways in which the older design
skipped committed events. One was checks spread over several statements,
whose READ COMMITTED snapshots disagreed.

**Rules for any gap scheme.**

- Gather the evidence in one snapshot, no later than the read.
- Use sequence `CACHE 1`.
- Expect gaps left by rollbacks never to produce a wake-up, so a short
  re-check timer is needed while holding at a gap.

#### Logical decoding instead of a frontier

**What it removes.** Decoding delivers transactions in commit order and never
emits rolled-back ones, so the ordering problem disappears for the live tail.

**Checkpointing.** Store the Commit message's end LSN. A slot's position is
persisted only at checkpoint, so delivery is at least once. A slot created
over the replication protocol with an exported snapshot gives an exact
hand-off from a table read to the stream.

**Costs.**

- `wal_level = logical` needs a restart up to PostgreSQL 18. PostgreSQL 19
  raises the effective level automatically when the first logical slot is
  created.
- Slots retain WAL and catalog rows even with no connection, in extreme cases
  up to a wraparound shutdown.
- `max_replication_slots` and `max_wal_senders` both default to 10.
- Slots survive failover only from PostgreSQL 17, and only with
  failover-slot configuration.
- It needs the `REPLICATION` role attribute.

**The client gap.** psycopg 3 has no streaming replication API (issue #71,
open since August 2021, with no further support planned for 3.3), so
streaming needs psycopg2, `pg_recvlogical` or a sidecar.

**Where it fits.** As a single disposable wake-up source per database, with
immediate confirmation, capped retention and a resync wake-up after any slot
loss. The events table stays the source of truth for catch-up.

#### Notification order and the frontier together

These rules hold whichever frontier is chosen:

1. **Payloads are never checkpoints.** Never take a checkpoint from a payload,
   and never treat a payload position as proof that lower positions are final.
   The one exception is serialised appends with `NOTIFY` in the same
   transaction.
2. **Read on every trigger.** Run the safe read on every wake-up, on every
   connect and reconnect (after `LISTEN`), and on the fallback timer.
3. **Arm a short re-check while held.** Arm it whenever the read is held at a
   gap, at the horizon or at a writer's lock floor, because the event that
   releases the hold may never notify.
4. **Persist only the safe checkpoint.** That is the contiguous prefix, the
   horizon tuple or the LSN, never the highest position seen.

#### How this applies to this library

This is an observation of the repository at the time of writing, not drawn
from the research sources.

**What the repository does today.**

- The events table draws `sequence_number BIGSERIAL` at insert
  (`sql/create_events_table.sql`).
- Consumers checkpoint `last_sequence_number` from the highest event they
  processed, and resume with `sequence_number_after(...)`
  (`processing/consumers/state/`).
- The Postgres adapter's `save` path calls `obtain_write_locks` before
  inserting
  (`src/logicblocks/event/store/adapters/postgres/adapter.py`). That takes
  `pg_advisory_xact_lock` on a name chosen by the configured
  `EventSerialisationGuarantee`, and the default is `LOG`.

**What follows** (inference from the analysis above):

- **Under `LOG`,** every library append serialises on one per-table lock taken
  before `nextval` and held to commit. This is the "serialised appends"
  frontier. `sequence_number > checkpoint` is gap-safe for log-wide and
  category consumers, as long as nothing writes events outside the adapter.
  The append path already pays roughly one serialised commit per append. An
  in-transaction `NOTIFY` would add little throughput cost, but it would widen
  the serialisation domain to every notifying transaction on the cluster.
- **Under `CATEGORY`,** commits are ordered only within a category. Category
  consumers are safe. Log-wide consumers can skip events.
- **Under `STREAM`,** neither category nor log-wide consumers are protected.
- **The hazard exists today.** It is not created by push. Fixed-interval
  polling can already skip an event under `CATEGORY` or `STREAM` whenever a
  poll lands between two out-of-order commits. Notification-driven wake-ups
  arrive exactly when lower-numbered concurrent writes are most likely still
  in flight, so they make the window more likely to be hit.

### Competing consumers, claiming and per-stream order

**Claiming is not ordering.** `SELECT … FOR UPDATE SKIP LOCKED` and
`pg_try_advisory_lock` both let several consumers run the same ordered claim
query and each take a different item without waiting. That is exactly why
neither preserves per-key order on its own. depesz measured the two as
equivalent in throughput.

**The `LIMIT` trap.** Advisory-lock claims must apply `LIMIT` in a subquery
first. The PostgreSQL documentation marks `pg_advisory_lock(id) … LIMIT 100`
as "danger!".

**Event stores claim the ordering unit, not the event.** The holder of a
subscription, stream, partition or slice reads that unit forward from its own
checkpoint, so the claim *is* the ordering guarantee:

- Marten elects one process per projection with advisory locks.
- Commanded allows only one instance of a named subscription.
- Graphile Worker locks the queue row.
- pgmq FIFO reads only group heads.
- Procrastinate excludes a lock's later jobs while an earlier one runs.

Every pattern shares head-of-line blocking: a stuck head stalls its key.

**Claim lifetimes decide failure behaviour.**

| Claim lifetime | Failure detection | Connections | Pooler compatibility | Hazard |
|---|---|---|---|---|
| Lock held by the handler's transaction | The database: rollback on disconnect; the commit fences late writes | One open transaction per in-flight handler | Works, but pins a server connection | Vacuum held back; half-open sockets orphan locks for hours without keepalives |
| Session advisory lock | Released at once when the session ends | One persistent session per holder | Session-mode only | Loss is silent. Marten notes that "HasLock keeps reporting true after the lock is gone". `DISCARD ALL` unlocks everything |
| Claim-and-mark with a lease or heartbeat | Time-based (River's rescuer, pgmq's visibility timeout, Procrastinate's 10 s/30 s heartbeat) | Only for short statements | Transaction pooling works | Duplicates; checkpoint writes need fencing |

**Fence checkpoints with an epoch.** A lease expiry does not stop a paused
former owner from writing. Kleppmann's GC-pause case is the canonical one. The
fix, used by Chubby sequencers, KCL's `leaseCounter`, Kafka's member and
producer epochs and Marten's compare-and-set, is a token that rises with every
ownership change and is checked by the store.

- **A PostgreSQL shape.** One row holds the lease, a monotonic `epoch` and the
  position. Checkpoint with
  `UPDATE … WHERE epoch = :epoch AND position = :expected`, in the same
  transaction as the handler's database effects. Zero rows means the lease was
  lost and the transaction rolls back.
- **Under READ COMMITTED** the guarded `UPDATE` cannot race. Use
  `clock_timestamp()`, not `now()`, for lease expiry.
- **Advisory-lock ownership still needs an epoch** whenever checkpoints go
  through a different, pooled connection.

**Assigning partitions.** Keep the partition count independent of the member
count. A fixed power-of-two slice count stored on each event row, assigned to
members as ranges, means membership changes move only assignments, never
checkpoints. Akka uses 1,024 slices.

- **Splits** are safe if children inherit the parent's checkpoint.
- **Merges** must take the lower checkpoint or drain to a barrier, never the
  higher one.
- **Hashing** must be stable, which rules out Python's seeded `hash()` for
  `str`.

**Delivery guarantees follow from where the position is committed.**
Committing the position in the same transaction as in-database effects gives
effectively-once database effects. External effects stay at least once, so
they need idempotency keyed on event identity, or an outbox. This follows the
end-to-end argument.

**Fan-out with claiming.** A broadcast wakes every listener. `SKIP LOCKED` or
a try-lock makes the race safe, but wastes N−1 empty claims per event. Three
ways to reduce the waste:

- Graphile Worker drops events when no worker is idle.
- Que targets one listener per job.
- The scheduling literature favours late binding: hand the hint to an idle
  worker, which then claims.

### How the approaches scale

#### On the write side

**Notifying commits.** They are capped near 1 / (flush + in-lock work) across
the whole cluster, however many clients there are, and the relative loss grows
with concurrency and flush latency. Heavy notifying in one database slows
notifying commits in every other database on the instance.

**Batching notifications.** Notify-only transactions skip the flush, so a
post-commit notifier flushing every W ms costs about P_writers / W lock
acquisitions per second. With 20 writer processes and W = 50 ms that is 400
per second, well under the measured notify-only rate of about 87,750 TPS at
100 clients. This is derived arithmetic, not a measurement.

#### On the listener side

**Before PostgreSQL 19,** every `NOTIFY` signalled every listening backend in
the database.

| Idle listeners on unrelated channels | Round trips/s (master) | Patched (PostgreSQL 19 design) |
|---|---|---|
| 0 | 9,126 | about 9,000 |
| 10 | 6,233 | about 9,000 |
| 100 | 2,020 | about 9,000 |
| 1,000 | 238 | about 9,000 (a later M3 Max run: 202.5 against 25,720.6, about 127×) |

**What PostgreSQL 19 leaves.** It removes the cost for listeners on other
channels. Every listener on the same channel is still woken. A long channel
list per listener is also a cost: a review build fell from 3,296 to 1,274
notifications per second with 10,000 extra channels. Keeping the number of
listening connections small, one per process, helps on every version.

#### On the read side

**The read herd.** One commit wakes N subscriptions, and N identical reads
follow. `asyncio.Event.set()` wakes every waiter. The tail pages are cached, so
each read mostly pays fixed per-statement overhead. N simultaneous checkouts
also grow the pool to `max_size`.

| Approach | Reads per head change | Peak concurrent statements | Added latency |
|---|---|---|---|
| Unbounded individual reads | N | N | none, but contention at high N |
| Concurrency-limited (Marten defaults to 4 per database, "no measured throughput cost") | N | L | up to ⌈N/L⌉ reads |
| Staggered or cooldown | N, fewer under bursts | about N × read time ÷ W | about W/2 |
| Shared head reader per process (Commanded EventStore, Axon's cache, Marten composite projections) | 1 per process | 1 per process | in-memory dispatch only |

**What a shared reader costs.** It needs eviction of slow subscribers to
private catch-up, and a boundary hand-off that neither skips nor duplicates
events.

#### Polling

**Load and latency.** Idle load is N / T queries per second. Mean latency is
about T/2 plus query time, and the worst case is T. For example, 300
subscriptions at 1 s make about 300 queries per second while idle.

**Cheap polling is possible.** Solid Queue sustains about 1,300 polls per
second at 110 µs average by keeping the polled table tiny and covered by an
index. An append-only events table cannot be kept tiny. A head probe
(`max(sequence_number)` through a backward index scan) costs a few buffer hits
whether the log holds a million events or a billion.

**Split a budget by the square-root rule.** For a fixed poll budget across
streams queried separately, polling rate should follow the *square root* of
each stream's event rate. Allocating in proportion to the rate does no better
than uniform polling. A shared global-position query removes the per-stream
split entirely. With notifications active, the remaining poll budget is a
safety net, which calls for uniform, long intervals.

**Desynchronise pollers.** Independent random phases keep the average load at
N / T but cut expected concurrency to about N·d / T for a query of duration d.
Lock-step pollers put N queries on the server at once. Draw jitter freshly on
every cycle.

#### What the research says

Every field examined reaches the same conclusion:

- **Push** gives the best latency and wastes the least work at low load. It
  costs per-subscriber state and behaves worst under overload.
- **Pull** degrades gracefully, batches well and applies back-pressure
  naturally.
- **The best designs are hybrids** that adapt to load.

| Result | Source | PostgreSQL analogue |
|---|---|---|
| Interrupts only start polling; re-arm after empty polls | Mogul and Ramakrishnan; Linux NAPI | A notification starts a drain; hints are ignored while batches are full |
| Pull primarily, push what pull would miss (98–100% fidelity, against 70–80% for pull alone) | Bhide et al., Adaptive Push-Pull | Notifications plus a slow fallback poll |
| Choose push or pull per producer–consumer pair by relative rate | Silberstein et al., Feeding Frenzy | Notify for low-rate streams, poll high-rate ones; Akka turns publishing off above 400 events/s |
| Coalesce to spread fixed cost, knowing what is latency-sensitive | Tai et al., Calibrated Interrupts | Debounced, batched `NOTIFY`; consumer cooldowns |
| Pull with long polls and consumer-controlled batching | Kafka design; RFC 6202 | `notifies(timeout=…)` as a long poll; demand-gated fetch sizes |
| Independent pollers overload the source | Liu et al., RSS study | One shared head watcher per process |
| Waking every waiter wastes work linearly | Linux Scalability Project; Vattani et al. | One listener per process; single-flight reads |
| Leases make failures cost performance, not correctness | Gray and Cheriton | The fallback poll as a lease on silence |

**What the literature does not contain.** No peer-reviewed record measures
PostgreSQL `LISTEN`/`NOTIFY`, compares notification-driven with polling
consumers on a PostgreSQL table, or measures jitter on database polling. The
PostgreSQL-specific numbers above come from the project's source, its mailing
lists and practitioner benchmarks.

### Comparison of the candidate approaches

Latencies assume an unloaded server. "Fallback" means a missed notification.
N is consumers, P is processes and T is the poll interval.

| Approach | Baseline latency | Steady-state query load | Durability (at-least-once) | Ordering | Pooler compatibility | Connection cost | Operational complexity | Scaling to many consumers |
|---|---|---|---|---|---|---|---|---|
| **A. Fixed-interval polling per consumer** (today, T = 200 ms) | About T/2 (100 ms) mean, T worst | N / T, always, mostly empty | Yes: reads from the checkpoint | Needs a safe frontier | Any mode | Pooled, one checkout per poll | Lowest | Idle load linear in N; lock-step synchronisation risk |
| **B. Polling with a shared per-process head probe and bounded backoff** | About T/2 of the probe interval; up to the backoff cap after a quiet spell | About P / T probes, plus reads only for consumers behind the head | Yes | Needs a safe frontier | Any mode | Pooled | Low | Idle load linear in P, not N |
| **C. In-database long poll** (`read_with_poll` style) | About the inner poll interval / 2 | Re-queries inside the server for every waiter | Yes | Needs a safe frontier | Works, but pins a server connection per waiter | One backend per waiting consumer | Low | Poor: connections linear in N |
| **D. In-transaction `NOTIFY` hint + fallback poll + one listener per process** | Commit + sub-ms delivery + read; fallback ≤ T_fallback | Coalesced reads on change, plus P / T_fallback | Yes, from the table and poll; notifications at most once | Hints only. Under serialised appends, notification order = position order | Writers any mode; listener needs session or direct | One listener per process | Medium | Every notifying append takes the cluster-wide lock through its flush. A full queue fails appends. Before PostgreSQL 19, per-listener signalling cost |
| **E. Post-commit, debounced `NOTIFY` hint + fallback poll + one listener per process** (recommended) | Commit + one round trip + 0 to W window + read; fallback ≤ T_fallback | As D; notifying commits ≤ writer processes / W, flush-free | Yes; a crash between commit and notify costs ≤ T_fallback | Hints only | Notifier any mode (autocommit); listener needs session or direct | One listener (+ optionally one notifier connection) per process | Medium–high | Append path untouched by the lock and by queue health |
| **F. Logical-decoding notifier** (one slot, one dispatcher) + table reads | Commit + flush + decode, streamed | No polling for wake-ups; one walsender | Slot durable, at least once, may resend | Commit order with LSNs; removes the frontier problem for the live tail | Replication connection (PgBouncer ≥ 1.23) | One walsender per slot (10 by default); `REPLICATION` role | High: `wal_level = logical`, retention caps, failover slots, no psycopg 3 streaming | One slot fans out; single-threaded ordered decode |
| **G. Transaction-mode pooler pub/sub** (PgDog) | Similar to D | Similar to D | At most once, with silent gaps, early `LISTEN` acknowledgement and no client signal on outage | Hints only | Yes, transaction mode | No dedicated backend per process | Low to set up; high to trust (0.1.x) | Good fan-out; silent failure modes |

### Operational gotchas to design for

1. **Missed notifications are normal.** Disconnects, reconnects, restarts,
   failovers, and the window before `LISTEN` takes effect all lose them. Only
   listen-then-catch-up on every connect, plus the fallback poll, recovers
   them.
2. **Transaction-mode pooling breaks `LISTEN` silently.** `LISTEN` succeeds,
   `NOTIFY` commits, and nothing arrives. Single-client tests usually pass.
   Ship an explicit poll-only mode, as River (`PollOnly`), pg-boss and GoodJob
   do.
3. **Connections are exhausted per listener, not per client.** Each listener
   is a real backend. Behind RDS Proxy, each is a permanent pin. One per
   process, never one per consumer.
4. **The commit lock is cluster-wide.** It is held through flush and
   synchronous replication. A commit stuck on an unavailable synchronous
   standby holds it, and every notifying transaction in every database queues
   behind it, including ones with `synchronous_commit = off`. Diagnose with
   `log_lock_waits` and the `Lock: object` wait event on
   `object 0 of class 1262 of database 0`.
5. **A full notification queue fails `NOTIFY` commits.** One listener idle in
   a transaction, or a client that has stopped reading, anywhere in the
   cluster can pin the queue. Monitor `pg_notification_queue_usage()` and keep
   the listener in autocommit.
6. **Idle timers kill quiet listeners.** `idle_session_timeout`, RDS Proxy's
   `IdleClientTimeout` and pooler `client_idle_timeout` ignore server-pushed
   messages. RDS Proxy also enforces a 24-hour cap, so recycle with overlap:
   open the new connection and commit its `LISTEN` before closing the old one.
7. **Half-open TCP looks like a quiet system.** Without explicit keepalives
   and `tcp_user_timeout`, detection takes hours. Behind a proxy, only an
   application heartbeat proves the server side is alive.
8. **Standbys and failovers.** Standbys reject `LISTEN`. A promoted standby
   has no registrations. Use `target_session_attrs=read-write`, and always
   re-`LISTEN` and catch up.
9. **psycopg pitfalls.**
   - Notifications were lost between generator runs before 3.2.4.
   - The backlog leaked before 3.2.10.
   - Handlers and the generator must not be mixed.
   - Closing the generator mid-batch drops the rest of the batch.
   - Asyncio cancellation of a stuck query was unbounded before 3.3.6 with
     libpq 17.
   - A pooled connection that ran `LISTEN` keeps its registration.
10. **Wake-ups do not make reads safe.** Without a safe frontier, a
    notification-woken consumer skips events that commit out of position
    order. Timeout-based gap skipping loses slow appends.
11. **Some holds end without a notification.** Rollbacks, numbers burned by
    `ON CONFLICT`, and unrelated transactions holding the `xid8` horizon all
    release a hold silently. A short re-check timer is needed while held.
12. **Channel and payload drift fails silently.** Case folding, 63-byte
    truncation and version skew between writers and listeners all stop
    wake-ups without an error. Version channels and payloads, and make readers
    tolerant.
13. **Health checks can lie.** River's heartbeat never reached the server, and
    pgmq's mixed-case bug never notified. Polling hid both. Test the probe,
    and prefer an end-to-end canary.
14. **Stored transaction IDs do not survive logical moves.** If the `xid8`
    frontier is adopted, fence or version checkpoints across dump and restore,
    logical replication and blue/green deployments.

### Recommendation for this library

#### Direction

Adopt the hybrid in which a notification is only a coalesced wake-up hint over
the existing position-based scan, with a long fallback poll (approach E in the
comparison). The evidence supports it on every axis the brief names:

- It removes the latency floor in the normal path.
- It collapses idle load from N / 200 ms to about P / T_fallback.
- It keeps durability exactly where it is today: the stored checkpoint and the
  table.
- It needs nothing beyond PostgreSQL.
- It matches what every mature PostgreSQL-backed system converged on, and
  what the push-versus-pull literature predicts.

#### The concrete mechanism

**1. Keep the read path and its frontier.**

- The only data path stays "read from the stored checkpoint, in bounded
  batches, until a short batch".
- **Under the default `LOG` serialisation guarantee,** keep `sequence_number`
  as the checkpoint, with no gap handling. Positions are already commit-ordered
  among library writers. Gaps below a visible position are permanent and can
  be skipped at once, so no gap re-check timer is needed.
- **Require `LOG` for log-wide consumers.** Document `LOG` as the guarantee
  log-wide push consumers require, and `CATEGORY` as sufficient for
  category-scoped consumers. Document that writes outside the adapter break
  the guarantee.
- **If concurrent unserialised appends become necessary,** treat the frontier
  as a separate decision:
  - an `xid8` horizon, with role-scoped `transaction_timeout`,
    `max_prepared_transactions = 0`, a held-back-events metric, a re-check
    timer, `REPEATABLE READ` appends, and an era or fence for moves; or
  - evidence-based gap detection in Marten's style, which keeps an integer
    checkpoint.

**2. Writer side: a post-commit, debounced notifier.**

- After an append's commit returns, mark the store's channel dirty in an
  in-process coalescer.
- A notifier sends `SELECT pg_notify(%s, %s)` from its own short autocommit
  transaction, with leading and trailing edges and a window of about 50 ms.
  Use 5–10 ms for latency-sensitive single-process deployments, and 100–250 ms
  where many processes share the database.
- Use one static, lower-case, schema-scoped, versioned channel, for example
  `<events_table>_v1`.
- Send an empty payload or a small versioned one, such as `{"v":1}` with an
  optional category and highest `sequence_number` as hints.
- Never send it in the same pipeline Sync as the insert. Log failures, never
  raise them.
- **Why post-commit rather than in-transaction.**
  - Under `LOG`, an in-transaction `NOTIFY` would cost little extra
    throughput. But it would couple append availability to the cluster-wide
    notification queue.
  - It would widen serialisation to every notifying transaction on the
    cluster.
  - Under `CATEGORY` or `STREAM` it would re-serialise every append globally.
  - The post-commit form's only loss, a crash between commit and notify, is
    covered by the next append's notification or the fallback poll.
- **The acceptable simplification.** For a single-process deployment with
  modest append rates, sending `NOTIFY` in the append transaction under `LOG`
  works. It trades atomic wake-ups for the coupling described above.

**3. Consumer side: one listener per process.**

- Open a standalone `psycopg.connect(..., autocommit=True)` or the async
  equivalent, outside `psycopg_pool`, from the same parameter source as the
  pool.
- Route it to a direct connection or a session-mode pooler alias, with a
  multi-host connection string and `target_session_attrs=read-write`.
- Set `connect_timeout` (a few seconds) and explicit keepalives with
  `tcp_user_timeout`, below any middlebox idle limit, for example 30/10/3 with
  60 s. Run `SET idle_session_timeout = 0` on the session.
- Require psycopg 3.2.10 or later; 3.3.6 or later with libpq 17 or later is
  preferable for bounded cancellation.
- One task or thread owns the connection and loops on bounded
  `notifies(timeout=…)` runs that end on their own.
- Between runs it applies registration changes and runs the heartbeat:
  `SELECT pg_backend_pid(), array(SELECT pg_listening_channels())`, at most
  half the shortest idle timer on the path.

**4. In-process dispatch.**

- Keep a reference-counted registry of subscribers outside the connection, so
  it survives reconnects.
- On a notification, set each relevant subscriber's level-triggered flag
  (`asyncio.Event`, or `loop.call_soon_threadsafe(event.set)` from a listener
  thread). On an unknown or unparseable payload, set all of them.
- Never run a consumer's read on the listener task.
- Cap concurrent catch-up reads with a small shared limiter, kept smaller than
  the pool, about 4 as Marten found. Add a shared head reader only if many
  subscriptions in one process follow the same tail.

**5. The consumer loop.**

1. `LISTEN`, committed.
2. Catch up.
3. Clear the flag, then drain until a batch comes back short.
4. Wait for the flag or a fallback timeout drawn freshly from about
   [0.5 T_fallback, 1.5 T_fallback].
5. Go back to step 3.

- Start with T_fallback of a few seconds. The upper bound is the worst-case
  latency acceptable when a notification is lost.
- While the listener is unhealthy (disconnected, failed heartbeat or missing
  canary), drop to today's short interval until it recovers.
- On reconnect, re-`LISTEN` and wake every subscriber.
- Expose a poll-only mode that keeps today's behaviour, for transaction-mode
  poolers. Pair it with the shared head probe of approach B, so idle load
  scales with processes rather than consumers even there.

**6. Health and observability.**

- Every few seconds, send an end-to-end canary `NOTIFY` with a nonce from the
  append-side pool, and expect it on the listener.
- Record `pg_notification_queue_usage()`.
- Count fallback-poll reads that found events. That is the measure of missed
  notifications.
- Report listener state: healthy, degraded or poll-only.

**7. Optional extensions** (inference, not evaluated here):

- The singleton and distributed brokers' 10–30 s loops could use the same
  listener for fast handover: a resign or membership change sends a
  `NOTIFY`, as River and Oban do for leadership. The existing loop stays as
  the fallback.
- Where the distributed broker hands sources to subscribers, fence each
  subscriber's checkpoint writes with an epoch bumped on every acquisition.

#### Expected effect

These are derived estimates, not measurements.

- **Normal-path latency** is delivery plus the debounce window (zero for the
  first append after a quiet spell, at most W otherwise) plus one read:
  single-digit milliseconds on an unloaded server.
- **Degraded latency** is at most T_fallback (1 + jitter).
- **Idle load** is about P / T_fallback reads and heartbeats per process,
  rather than N / 0.2 s.
- **Under load,** reads per consumer are capped by the cooldown and by
  batching, so queries grow with batches, not events.

#### Conditions under which this holds

- **Append concurrency stays within one serialised commit at a time** under
  `LOG`. That is roughly 1 / (append work + flush + any synchronous-standby
  wait) per event store, which is the library's ceiling today, independent of
  push. If it must rise, the frontier decision above comes first.
- **Consumers can reach the primary over a session-capable path**: direct,
  session-mode PgBouncer, Supabase 5432, Cloud SQL session mode, or RDS Proxy
  with the pin accepted. Otherwise poll-only with the shared head probe is the
  right mode.
- **The cost of a lost notification is bounded by T_fallback.** If a
  workload cannot tolerate even that, it needs logical decoding, not tighter
  `NOTIFY` tuning.
- **PostgreSQL 19 does not change the advice for a single shared channel.** It
  helps only when many listeners use distinct channels. The commit lock
  remains on every released version.
- **Revisit if** the "Write Notifications Through WAL" work lands, which would
  make in-transaction `NOTIFY` cheap, or if psycopg 3 gains replication
  streaming, which would make a slot-based notifier practical from the same
  driver.

## Open Threads

**Measurements no source has published.**

- No benchmark compares a debounced post-commit notifier with in-transaction
  `NOTIFY` for an event-store append path. None measures `INSERT` + `NOTIFY`
  against insert-only on PostgreSQL 19 code, or with synchronous replication.
  A pgbench sweep on target hardware would settle the window size and the
  real lock cost:
  - with and without `pg_advisory_xact_lock`;
  - with and without in-transaction `NOTIFY`;
  - across `synchronous_commit` settings;
  - recording append TPS, commit-to-handler p50 and p99, and consumer reads
    per second.
- That notify-only transactions skip the WAL flush while holding the lock is
  read from `RecordTransactionCommit()` and consistent with one benchmark. No
  source measured lock hold time directly.

**Unresolved readings of PostgreSQL.**

- Readings of `xact.c` disagree about whether listener signalling
  (`AtCommit_Notify`) runs inside the lock window. The disagreement matters
  for how much PostgreSQL 19's targeted signalling shortens the serialised
  section.
- Recall.ai and DBOS disagree about whether PostgreSQL 19 removes the
  bottleneck. The proposed reconciliation (many uninterested listeners) is
  unverified.

**Untested client and proxy behaviour.**

- **psycopg:**
  - mid-batch generator close losing notifications is a source reading, and
    the sketched reproduction test has not been run;
  - which exception an idle listener sees when the server terminates it
    differs between the changelog and the code;
  - whether a socket closed from another thread wakes a blocked
    `notifies()` is undocumented.
- **AWS:**
  - how RDS Proxy relays `NotificationResponse`;
  - whether server-pushed traffic resets `IdleClientTimeout`;
  - whether a pinned listener survives writer failover.
- **PgDog:** its documentation and source disagree on automatic reconnection.
- **Supavisor:** the idle-timer behaviour and its unit are inferred from
  source.

**Untested frontier techniques.**

- the database-filtered `xid8` watermark;
- the fixed version of Sequin's advisory-lock floor;
- using the sequence's own `RowExclusiveLock` to prove a gap dead;
- the `LOCK TABLE … IN SHARE MODE` barrier;
- CORFU-style hole filling with `ON CONFLICT DO NOTHING`;
- the per-transaction advisory key that lets a stalled consumer wait for a
  blocker inside the server.

All are derived from documented behaviour and need tests under concurrent
writers.

**Policy constants the literature does not supply.** It gives structures for
hysteresis thresholds, safety-net bounds and heartbeat periods, not settings.
Calibrate them against measured per-query cost, notification delay and loss,
and real arrival traces.

**Questions specific to this library.**

- Confirm that no code path writes events outside the adapter's lock.
- Establish whether deployments use the `CATEGORY` or `STREAM` guarantees with
  log-wide consumers. If they do, the existing poller can already skip events,
  and a test with two concurrent out-of-order commits would demonstrate it.
- Measure the `LOG` guarantee's append ceiling on target infrastructure,
  including any synchronous standby.

**Logical decoding.** It was deliberately deprioritised in the brief. A
further round would establish:

- a production-grade Python streaming client, or a psycopg2 or sidecar split;
- the exported-snapshot hand-off from table reads;
- retention caps;
- failover-slot behaviour on the target managed service.

## Sources

- [PostgreSQL 18 Documentation: NOTIFY](https://www.postgresql.org/docs/current/sql-notify.html) — tier-1 — postgresql.org (official documentation)
- [PostgreSQL 18 Documentation: LISTEN](https://www.postgresql.org/docs/current/sql-listen.html) — tier-1 — postgresql.org (official documentation)
- [Message Flow (Asynchronous Operations)](https://www.postgresql.org/docs/current/protocol-flow.html) — tier-1 — postgresql.org (official documentation)
- [libpq Asynchronous Notification](https://www.postgresql.org/docs/current/libpq-notify.html) — tier-1 — postgresql.org (official documentation)
- [Resource Consumption (max_notify_queue_pages, logical_decoding_work_mem)](https://www.postgresql.org/docs/current/runtime-config-resource.html) — tier-1 — postgresql.org (official documentation)
- [Client Connection Defaults (lock_timeout, transaction_timeout, idle_session_timeout)](https://www.postgresql.org/docs/current/runtime-config-client.html) — tier-1 — postgresql.org (official documentation)
- [Connections and Authentication (server TCP settings)](https://www.postgresql.org/docs/current/runtime-config-connection.html) — tier-1 — postgresql.org
- [Write Ahead Log configuration (synchronous_commit, wal_log_hints, commit_delay, wal_writer_delay, wal_level)](https://www.postgresql.org/docs/current/runtime-config-wal.html) — tier-1 — postgresql.org
- [Asynchronous Commit](https://www.postgresql.org/docs/current/wal-async-commit.html) — tier-1 — postgresql.org
- [Replication configuration (max_slot_wal_keep_size, idle_replication_slot_timeout, synchronized_standby_slots, sync_replication_slots)](https://www.postgresql.org/docs/current/runtime-config-replication.html) — tier-1 — postgresql.org
- [libpq Connection Strings: Parameter Key Words](https://www.postgresql.org/docs/current/libpq-connect.html) — tier-1 — postgresql.org
- [Hot Standby](https://www.postgresql.org/docs/current/hot-standby.html) — tier-1 — postgresql.org
- [CREATE TRIGGER](https://www.postgresql.org/docs/current/sql-createtrigger.html) — tier-1 — postgresql.org (official documentation)
- [Explicit Locking (Advisory Locks)](https://www.postgresql.org/docs/current/explicit-locking.html) — tier-1 — postgresql.org (official documentation)
- [System Administration Functions (advisory lock functions, pg_logical_emit_message, pg_logical_slot_get_changes, pg_create_logical_replication_slot)](https://www.postgresql.org/docs/current/functions-admin.html) — tier-1 — postgresql.org (official documentation)
- [System Information Functions and Operators (transaction ID and snapshot functions, pg_listening_channels, pg_notification_queue_usage)](https://www.postgresql.org/docs/current/functions-info.html) — tier-1 — postgresql.org
- [Sequence Manipulation Functions](https://www.postgresql.org/docs/current/functions-sequence.html) — tier-1 — postgresql.org
- [Transactions and Identifiers](https://www.postgresql.org/docs/current/transaction-id.html) — tier-1 — postgresql.org
- [Transaction Isolation](https://www.postgresql.org/docs/current/transaction-iso.html) — tier-1 — postgresql.org
- [PostgreSQL: PREPARE TRANSACTION](https://www.postgresql.org/docs/current/sql-prepare-transaction.html) — tier-1 — postgresql.org
- [The Cumulative Statistics System (wait events, pg_stat_replication_slots, pg_stat_replication lag)](https://www.postgresql.org/docs/current/monitoring-stats.html) — tier-1 — postgresql.org (official documentation)
- [Logical Decoding Concepts](https://www.postgresql.org/docs/current/logicaldecoding-explanation.html) — tier-1 — postgresql.org (official documentation)
- [Streaming Replication Protocol](https://www.postgresql.org/docs/current/protocol-replication.html) — tier-1 — postgresql.org
- [PostgreSQL 19 release notes](https://www.postgresql.org/docs/19/release-19.html) — tier-1 — postgresql.org
- [PostgreSQL 19 Release Notes (devel)](https://www.postgresql.org/docs/devel/release-19.html) — tier-1 — postgresql.org (official documentation)
- [async.c (master)](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/commands/async.c) — tier-1 — github.com/postgres/postgres (official source mirror)
- [async.c (REL_18_STABLE)](https://raw.githubusercontent.com/postgres/postgres/REL_18_STABLE/src/backend/commands/async.c) — tier-1 — github.com/postgres/postgres (official source mirror)
- [PostgreSQL source: src/backend/access/transam/xact.c](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/access/transam/xact.c) — tier-1 — raw.githubusercontent.com (postgres/postgres repository)
- [procarray.c (GetSnapshotData)](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/storage/ipc/procarray.c) — tier-1 — github.com/postgres/postgres (PostgreSQL source)
- [Commit 282b1cde: Optimize LISTEN/NOTIFY via shared channel map and direct advancement](https://github.com/postgres/postgres/commit/282b1cde9dedf456ecf02eb27caf086023a7bb71) — tier-1 — github.com (postgres/postgres)
- [pgsql: Optimize LISTEN/NOTIFY via shared channel map and direct advancement](https://www.postgresql.org/message-id/E1vgSld-000fFk-2J%40gemulon.postgresql.org) — tier-1 — postgresql.org (pgsql-committers archive)
- [pgsql: Fix NOTIFY wakeups for pre-commit LISTEN entries](https://postgrespro.com/list/id/E1wSH2y-001Mpi-1v@gemulon.postgresql.org) — tier-2 — postgrespro.com (pgsql-committers mirror)
- [Commit 1b46990: Escalate ERRORs during async notify processing to FATAL](https://github.com/postgres/postgres/commit/1b46990) — tier-1 — github.com/postgres/postgres
- [Commit 8eeb4a0: Fix bug where we truncated CLOG that was still needed by LISTEN/NOTIFY](https://github.com/postgres/postgres/commit/8eeb4a0) — tier-1 — github.com/postgres/postgres
- [Optimize LISTEN/NOTIFY (Joel Jacobson, initial proposal and benchmarks)](https://www.postgresql.org/message-id/6899c044-4a82-49be-8117-e6f669765f7e%40app.fastmail.com) — tier-1 — postgresql.org (pgsql-hackers)
- [Re: Proposal: Out-of-Order NOTIFY via GUC (Joel Jacobson, 2025-07-30)](https://www.postgresql.org/message-id/7c8c61c2-4dbf-4961-9e0c-a5ec77d8f846%40app.fastmail.com) — tier-1 | tier-2 (recorded at different tiers by different findings) — postgresql.org (pgsql-hackers archive)
- [Re: [PATCH] Write Notifications Through WAL (Arseniy Mukhin, 2025-10-17 insert-with-notify benchmarks)](https://www.postgresql.org/message-id/CAE7r3MLwRNyvmMd2kaefeNYTRjGD8wOxLzyGn8vexaeTJdASaw%40mail.gmail.com) — tier-1 — postgresql.org (pgsql-hackers)
- [[PATCH] Write Notifications Through WAL (Rishu Bagga)](https://www.postgresql.org/message-id/CAK80%3DjjvgWJL5EdzH_Bxc37gn53PJaB5sDXB3VPfJuN7G1xL5Q%40mail.gmail.com) — tier-1 — postgresql.org (pgsql-hackers mailing list)
- [Tom Lane, Re: Out-of-Order NOTIFY proposal (ordering and NotifyQueueLock)](https://www.postgresql.org/message-id/1758148.1752802118@sss.pgh.pa.us) — tier-1 — postgresql.org (pgsql-hackers archive)
- [Tom Lane, Re: Out-of-Order NOTIFY proposal (WAL-transport redesign)](https://www.postgresql.org/message-id/1878165.1752858390@sss.pgh.pa.us) — tier-1 — postgresql.org (pgsql-hackers archive)
- [Re: docs: LISTEN/NOTIFY performance considerations (Nikolay Samokhvalov, 2026-07-21)](https://www.postgresql.org/message-id/CAM527d8oDVb2K+tTjpjLkd7g14HLDiwwnvv=rcn4BMEs=MatkQ@mail.gmail.com) — tier-1 — postgresql.org (pgsql-hackers archive)
- [Issue with COMMITs stuck on "AccessExclusiveLock on object 0 of class 1262 of database 0"](https://www.postgresql.org/message-id/CADWG95t0j9zF0uwdcMH81KMnDsiTAVHxmBvgYqrRJcD-iLwQhw%40mail.gmail.com) — tier-2 | tier-3 (recorded at different tiers by different findings) — postgresql.org (mailing-list user report)
- [Postgres LISTEN/NOTIFY does not scale](https://www.recall.ai/blog/postgres-listen-notify-does-not-scale) — tier-3 — recall.ai (company engineering blog)
- [Postgres LISTEN/NOTIFY Actually Scales](https://dbos.dev/blog/postgres-listen-notify-scalability) — tier-3 — dbos.dev (company engineering blog)
- [LISTEN carefully: how NOTIFY can trip up your database (LT version)](https://www.postgresql.eu/events/fosdem2026/sessions/session/7408-listen-carefully-how-notify-can-trip-up-your-database-lt-version/) — tier-2 — postgresql.eu (FOSDEM PGDay 2026 programme)
- [The notifier pattern for applications that use Postgres](https://brandur.org/notifier) — tier-2 | tier-3 (recorded at different tiers by different findings) — brandur.org (personal blog)
- [Analyzing the Limits of Connection Scalability in Postgres](https://www.citusdata.com/blog/2020/10/08/analyzing-connection-scalability/) — tier-2 — citusdata.com
- [Psycopg 3: Asynchronous operations (notifications, detecting disconnections)](https://www.psycopg.org/psycopg3/docs/advanced/async.html) — tier-1 — psycopg.org (official documentation)
- [Psycopg 3: Connection classes API (notifies, add_notify_handler, broken)](https://www.psycopg.org/psycopg3/docs/api/connections.html) — tier-1 — psycopg.org (official documentation)
- [Psycopg release notes (3.2, 3.2.4, 3.2.10)](https://www.psycopg.org/psycopg3/docs/news.html) — tier-1 — psycopg.org
- [Connection pools — checking and reconnecting, life cycle](https://www.psycopg.org/psycopg3/docs/advanced/pool.html) — tier-1 — psycopg.org
- [psycopg issue #962 — notifications lost while notifies() is not running](https://github.com/psycopg/psycopg/issues/962) — tier-1 | tier-2 (recorded at different tiers by different findings) — github.com (psycopg issue tracker)
- [psycopg issue #1091 — notification backlog leak](https://github.com/psycopg/psycopg/issues/1091) — tier-1 | tier-2 (recorded at different tiers by different findings) — github.com (psycopg issue tracker)
- [Issue #1371: Async task cancellation is never delivered when the server stops responding on an established connection](https://github.com/psycopg/psycopg/issues/1371) — tier-1 — github.com/psycopg/psycopg
- [AsyncConnection.notifies() generator blocks AsyncConnection.execute() (issue #340)](https://github.com/psycopg/psycopg/issues/340) — tier-1 | tier-2 (recorded at different tiers by different findings) — github.com (psycopg project issue tracker)
- [Replication support · Issue #71 · psycopg/psycopg](https://github.com/psycopg/psycopg/issues/71) — tier-1 — github.com/psycopg (project issue tracker)
- [psycopg2 docs: extras (LogicalReplicationConnection, ReplicationCursor)](https://www.psycopg.org/docs/extras.html) — tier-1 — psycopg.org
- [PgBouncer Features — SQL feature map for pooling modes](https://www.pgbouncer.org/features.html) — tier-1 — pgbouncer.org
- [PgBouncer Configuration](https://www.pgbouncer.org/config.html) — tier-1 — pgbouncer.org
- [Feature: Listen/Notify Support with Transaction Pooling, issue #655](https://github.com/pgbouncer/pgbouncer/issues/655) — tier-1 | tier-3 (recorded at different tiers by different findings) — github.com (pgbouncer issue tracker)
- [Avoiding pinning an RDS Proxy](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/rds-proxy-pinning.html) — tier-1 — docs.aws.amazon.com
- [Supabase: Connect to your database](https://supabase.com/docs/guides/database/connecting-to-postgres) — tier-1 — supabase.com
- [Cloud SQL for PostgreSQL: Managed Connection Pooling](https://docs.cloud.google.com/sql/docs/postgres/managed-connection-pooling) — tier-1 — docs.cloud.google.com
- [Neon: Connection pooling](https://neon.com/docs/connect/connection-pooling) — tier-1 — neon.com
- [PgBouncer in Azure Database for PostgreSQL Flexible Server](https://learn.microsoft.com/en-us/azure/postgresql/flexible-server/concepts-pgbouncer) — tier-1 — learn.microsoft.com
- [PgDog: Pub/sub (LISTEN/NOTIFY in transaction mode)](https://docs.pgdog.dev/features/connection-pooler/pub_sub/) — tier-1 — docs.pgdog.dev
- [Issue #1462: client_idle_timeout is global-only for long-idle LISTEN clients](https://github.com/pgdogdev/pgdog/issues/1462) — tier-1 — github.com/pgdogdev/pgdog
- [Fast failover with Amazon Aurora PostgreSQL](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/AuroraPostgreSQL.BestPractices.FastFailover.html) — tier-1 — docs.aws.amazon.com
- [Switching a blue/green deployment in Amazon RDS](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/blue-green-deployments-switching.html) — tier-1 — docs.aws.amazon.com (Amazon RDS User Guide)
- [Troubleshoot NAT gateways](https://docs.aws.amazon.com/vpc/latest/userguide/nat-gateway-troubleshooting.html) — tier-1 — docs.aws.amazon.com
- [Network Load Balancers: connection idle timeout](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/network-load-balancers.html) — tier-1 — docs.aws.amazon.com
- [Configure Azure Load Balancer TCP reset and idle timeout](https://learn.microsoft.com/en-us/azure/load-balancer/load-balancer-tcp-idle-timeout) — tier-1 — learn.microsoft.com
- [Scaling to Zero ACUs with automatic pause and resume for Aurora serverless](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/aurora-serverless-v2-auto-pause.html) — tier-1 — docs.aws.amazon.com
- [AWS Advanced Python Wrapper (README)](https://github.com/aws/aws-advanced-python-wrapper) — tier-1 — github.com/aws
- [tcp(7) Linux manual page](https://man7.org/linux/man-pages/man7/tcp.7.html) — tier-1 — man7.org (Linux man-pages project)
- [When TCP sockets refuse to die](https://blog.cloudflare.com/when-tcp-sockets-refuse-to-die/) — tier-2 — blog.cloudflare.com
- [River client.go](https://raw.githubusercontent.com/riverqueue/river/master/client.go) — tier-1 — raw.githubusercontent.com (riverqueue/river repository)
- [River internal/notifier/notifier.go](https://raw.githubusercontent.com/riverqueue/river/master/internal/notifier/notifier.go) — tier-1 — raw.githubusercontent.com (riverqueue/river)
- [River notifylimiter package](https://raw.githubusercontent.com/riverqueue/river/master/internal/notifylimiter/limiter.go) — tier-1 — raw.githubusercontent.com (riverqueue/river repository)
- [River CHANGELOG](https://raw.githubusercontent.com/riverqueue/river/master/CHANGELOG.md) — tier-1 — raw.githubusercontent.com (riverqueue/river source)
- [River: Using PgBouncer](https://riverqueue.com/docs/pgbouncer) — tier-1 — riverqueue.com
- [Oban lib/oban/notifiers/postgres.ex](https://raw.githubusercontent.com/oban-bg/oban/main/lib/oban/notifiers/postgres.ex) — tier-1 — raw.githubusercontent.com (oban-bg/oban)
- [Oban lib/oban/stager.ex](https://raw.githubusercontent.com/oban-bg/oban/main/lib/oban/stager.ex) — tier-1 — raw.githubusercontent.com (oban-bg/oban)
- [Oban Sonar (sonar.ex)](https://raw.githubusercontent.com/oban-bg/oban/main/lib/oban/sonar.ex) — tier-1 — raw.githubusercontent.com (oban-bg/oban)
- [Oban: Scaling Applications](https://oban.hexdocs.pm/2.24.1/scaling.html) — tier-1 — oban.hexdocs.pm
- [Graphile Worker FAQ](https://worker.graphile.org/docs/faq) — tier-1 — worker.graphile.org
- [Graphile Worker src/main.ts](https://raw.githubusercontent.com/graphile/worker/main/src/main.ts) — tier-1 — raw.githubusercontent.com (graphile/worker)
- [GoodJob README](https://raw.githubusercontent.com/bensheldon/good_job/main/README.md) — tier-1 — raw.githubusercontent.com (bensheldon/good_job)
- [pg-boss notifier source (src/notifier.ts)](https://raw.githubusercontent.com/timgit/pg-boss/master/src/notifier.ts) — tier-1 — github.com/timgit/pg-boss (primary source)
- [pg-boss Workers API](https://pgboss.io/api/workers) — tier-1 — pgboss.io
- [pgmq extension SQL](https://raw.githubusercontent.com/pgmq/pgmq/main/pgmq-extension/sql/pgmq.sql) — tier-1 — raw.githubusercontent.com (pgmq/pgmq repository)
- [Introducing Solid Queue](https://dev.37signals.com/introducing-solid-queue/) — tier-1 — dev.37signals.com
- [Message DB acquire-lock.sql](https://raw.githubusercontent.com/message-db/message-db/master/database/functions/acquire-lock.sql) — tier-1 — github.com/message-db/message-db (source)
- [Async Daemon (Marten documentation)](https://martendb.io/events/projections/async-daemon.html) — tier-1 — martendb.io
- [Async daemon can advance projection progress past committed events during concurrent appends #4953](https://github.com/JasperFx/marten/discussions/4953) — tier-2 — github.com (JasperFx/marten discussions)
- [Marten: Appending Events (QuickAppend and tombstone events)](https://martendb.io/events/appending.html) — tier-1 — martendb.io
- [Commanded EventStore README (benchmarks)](https://raw.githubusercontent.com/commanded/eventstore/master/README.md) — tier-1 — github.com/commanded/eventstore
- [Elixir EventStore notifications publisher.ex](https://raw.githubusercontent.com/commanded/eventstore/master/lib/event_store/notifications/publisher.ex) — tier-1 — github.com/commanded/eventstore (primary source)
- [procrastinate/psycopg_connector.py](https://raw.githubusercontent.com/procrastinate-org/procrastinate/main/procrastinate/psycopg_connector.py) — tier-1 | tier-2 (recorded at different tiers by different findings) — raw.githubusercontent.com (procrastinate-org/procrastinate)
- [procrastinate/worker.py](https://raw.githubusercontent.com/procrastinate-org/procrastinate/main/procrastinate/worker.py) — tier-1 | tier-2 (recorded at different tiers by different findings) — raw.githubusercontent.com (procrastinate-org/procrastinate)
- [Que README](https://github.com/que-rb/que) — tier-1 — github.com (que-rb/que)
- [eventsourcing docs: Persistence (PostgreSQL table lock)](https://eventsourcing.readthedocs.io/en/stable/topics/persistence.html) — tier-1 — eventsourcing.readthedocs.io
- [eventsourcing source: eventsourcing/postgres.py](https://raw.githubusercontent.com/pyeventsourcing/eventsourcing/main/eventsourcing/postgres.py) — tier-1 — github.com/pyeventsourcing/eventsourcing
- [Akka Persistence R2DBC: Query plugin](https://doc.akka.io/libraries/akka-persistence-r2dbc/current/query.html) — tier-1 — doc.akka.io
- [Events from slices (Akka Projections)](https://doc.akka.io/libraries/akka-projection/current/eventsourced.html) — tier-1 — doc.akka.io
- [How Postgres sequences issues can impact your messaging guarantees](https://event-driven.io/en/ordering_in_postgres_outbox/) — tier-2 | tier-3 (recorded at different tiers by different findings) — event-driven.io
- [Postgres sequences can commit out-of-order](https://blog.sequinstream.com/postgres-sequences-can-commit-out-of-order/) — tier-3 — blog.sequinstream.com
- [Generating truly sequential IDs in PostgreSQL](https://www.cloudscale.ch/en/engineering-blog/2025/10/09/generating-truly-sequential-ids-in-postgresql) — tier-3 — cloudscale.ch
- [Fun with MVCC](https://tech.precisionnutrition.com/postgres/databases/2020/03/06/fun-with-mvcc) — tier-3 — tech.precisionnutrition.com
- [postgresql-event-sourcing reference implementation](https://github.com/eugene-khyst/postgresql-event-sourcing) — tier-3 — github.com/eugene-khyst
- [Emmett PostgreSQL append function, appendToStream.ts](https://raw.githubusercontent.com/event-driven-io/emmett/main/src/packages/emmett-postgresql/src/eventStore/schema/appendToStream.ts) — tier-1 — github.com/event-driven-io/emmett
- [Emmett PR #336: Fixed race conditions in PostgreSQL event store consumer message polling](https://github.com/event-driven-io/emmett/pull/336) — tier-1 — github.com (event-driven-io/emmett)
- [GapAwareTrackingToken](https://apidocs.axoniq.io/4.10/org/axonframework/eventhandling/GapAwareTrackingToken.html) — tier-1 — apidocs.axoniq.io (Axon Framework API documentation)
- [pgq.batch_event_sql.sql (PgQ source)](https://raw.githubusercontent.com/pgq/pgq/master/functions/pgq.batch_event_sql.sql) — tier-1 — github.com/pgq/pgq (PgQ source)
- [Picking task from queue – revisit](https://www.depesz.com/2016/05/04/picking-task-from-queue-revisit/) — tier-2 — depesz.com
- [How to do distributed locking](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html) — tier-2 — martin.kleppmann.com
- [DynamoDBCheckpointer.java (amazon-kinesis-client)](https://raw.githubusercontent.com/awslabs/amazon-kinesis-client/master/amazon-kinesis-client/src/main/java/software/amazon/kinesis/checkpoint/dynamodb/DynamoDBCheckpointer.java) — tier-1 — github.com/awslabs (primary source)
- [KIP-848: The Next Generation of the Consumer Rebalance Protocol](https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol) — tier-1 — cwiki.apache.org (Apache Kafka)
- [KIP-447: Producer scalability for exactly once semantics](https://cwiki.apache.org/confluence/display/KAFKA/KIP-447%3A+Producer+scalability+for+exactly+once+semantics) — tier-1 — cwiki.apache.org (Apache Kafka)
- [Supabase docs: Postgres Changes](https://supabase.com/docs/guides/realtime/postgres-changes) — tier-1 — supabase.com
- [Apache Kafka Design: Push vs. pull](https://kafka.apache.org/081/design/design/) — tier-1 — kafka.apache.org (official project documentation)
- [NAPI — The Linux Kernel documentation](https://docs.kernel.org/networking/napi.html) — tier-1 — docs.kernel.org (official documentation)
- [RFC 6202: Known Issues and Best Practices for the Use of Long Polling and Streaming in Bidirectional HTTP](https://www.rfc-editor.org/rfc/rfc6202.html) — tier-1 — rfc-editor.org (IETF informational RFC)
- [Optimizing Storage Performance with Calibrated Interrupts](https://www.usenix.org/conference/osdi21/presentation/tai) — tier-1 — usenix.org (OSDI 2021 peer-reviewed proceedings)
- [Paper: Feeding Frenzy: Selectively Materializing Users' Event Feeds](https://highscalability.com/paper-feeding-frenzy-selectively-materializing-users-event-f/) — tier-2 — highscalability.com (established technical writing summarising the SIGMOD 2010 paper)
- [Client Behavior and Feed Characteristics of RSS, a Publish-Subscribe System for Web Micronews](https://www.usenix.org/legacy/event/imc05/tech/full_papers/liu_hongzhou/liu_hongzhou_html/paper.html) — tier-1 — usenix.org (IMC 2005 peer-reviewed proceedings)
- [Exponential Backoff And Jitter (Marc Brooker)](https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/) — tier-2 — aws.amazon.com (AWS Architecture Blog)
- [Adaptive push-pull: disseminating dynamic Web data](https://doi.org/10.1109/tc.2002.1009150) — tier-1 — IEEE Transactions on Computers
- [Adaptive push-pull](https://doi.org/10.1145/371920.372066) — tier-1 — International World Wide Web Conference (WWW)
- [Eliminating receive livelock in an interrupt-driven kernel](https://doi.org/10.1145/263326.263335) — tier-1 — ACM Transactions on Computer Systems
- [Experience with processes and monitors in Mesa](https://doi.org/10.1145/358818.358824) — tier-1 — Communications of the ACM
- [Distributed snapshots](https://doi.org/10.1145/214451.214456) — tier-1 — ACM Transactions on Computer Systems
- [Synchronization with eventcounts and sequencers](https://doi.org/10.1145/359060.359076) — tier-1 — Communications of the ACM
- [Leases: an efficient fault-tolerant mechanism for distributed file cache consistency](https://doi.org/10.1145/74850.74870) — tier-3 — doi.org (conference paper; venue not recorded)
- [The synchronization of periodic routing messages](https://doi.org/10.1109/90.298431) — tier-1 — IEEE/ACM Transactions on Networking
- [On the quality of service of failure detectors](https://doi.org/10.1109/12.980014) — tier-1 — IEEE Transactions on Computers
- [Unreliable failure detectors for reliable distributed systems](https://doi.org/10.1145/226643.226647) — tier-1 — Journal of the ACM
- [Real-time status: How often should one update?](https://doi.org/10.1109/infcom.2012.6195689) — tier-3 — doi.org (no venue recorded)
- [Effective page refresh policies for Web crawlers](https://doi.org/10.1145/958942.958945) — tier-1 — ACM Transactions on Database Systems
- [Efficient visit frequencies for polling tables: minimization of waiting cost](https://doi.org/10.1007/bf01158795) — tier-1 — Queueing Systems
- [Efficient Monitoring Algorithm for Fast News Alerts](https://doi.org/10.1109/tkde.2007.1041) — tier-1 — IEEE Transactions on Knowledge and Data Engineering
- [Optimal probabilistic cache stampede prevention](https://doi.org/10.14778/2757807.2757813) — tier-1 — Proceedings of the VLDB Endowment
- [The Linux Scalability Project](https://openalex.org/W1521570651) — tier-2 — Deep Blue (University of Michigan)
- [Design and evaluation of a wide-area event notification service](https://doi.org/10.1145/380749.380767) — tier-1 — ACM Transactions on Computer Systems
- [The many faces of publish/subscribe](https://doi.org/10.1145/857076.857078) — tier-1 — ACM Computing Surveys
- [A reliable multicast framework for light-weight sessions and application level framing](https://doi.org/10.1109/90.650139) — tier-1 — IEEE/ACM Transactions on Networking
- [Optimal Operating Policies for M / G /1 Queuing Systems](https://doi.org/10.1287/opre.16.2.362) — tier-1 — Operations Research
- [M/M/1 Queueing Decision Processes with Monotone Hysteretic Optimal Policies](https://doi.org/10.1287/opre.32.5.1116) — tier-1 — Operations Research
- [Serializable snapshot isolation in PostgreSQL](https://doi.org/10.14778/2367502.2367523) — tier-1 — Proceedings of the VLDB Endowment
- [A read-only transaction anomaly under snapshot isolation](https://doi.org/10.1145/1031570.1031573) — tier-1 — ACM SIGMOD Record
- [Staring into the abyss](https://doi.org/10.14778/2735508.2735511) — tier-1 — Proceedings of the VLDB Endowment
- [Aether](https://doi.org/10.14778/1920841.1920928) — tier-1 — Proceedings of the VLDB Endowment
- [CORFU](https://doi.org/10.1145/2535930) — tier-1 — ACM Transactions on Computer Systems
- [Building a replicated logging system with Apache Kafka](https://doi.org/10.14778/2824032.2824063) — tier-1 — Proceedings of the VLDB Endowment
- [The dataflow model](https://doi.org/10.14778/2824032.2824076) — tier-1 — Proceedings of the VLDB Endowment
- [MillWheel](https://doi.org/10.14778/2536222.2536229) — tier-1 — Proceedings of the VLDB Endowment
- [The Chubby lock service for loosely-coupled distributed systems](https://doi.org/10.5555/1298455.1298487) — tier-3 — Operating Systems Design and Implementation (OSDI)
- [End-to-end arguments in system design](https://doi.org/10.1145/357401.357402) — tier-1 — ACM Transactions on Computer Systems
- [Online event processing](https://doi.org/10.1145/3312527) — tier-1 — Communications of the ACM
- [Idempotence is not a medical condition](https://doi.org/10.1145/2160718.2160734) — tier-1 — Communications of the ACM
- [The power of two choices in randomized load balancing](https://doi.org/10.1109/71.963420) — tier-1 — IEEE Transactions on Parallel and Distributed Systems
- [Heavy-traffic Delay Optimality in Pull-based Load Balancing Systems](https://doi.org/10.1145/3287323) — tier-1 — Proceedings of the ACM on Measurement and Analysis of Computing Systems
- [Integrating databases with publish/subscribe](https://doi.org/10.1109/icdcsw.2005.79) — tier-1 — IEEE International Conference on Distributed Computing Systems Workshops (ICDCSW)
