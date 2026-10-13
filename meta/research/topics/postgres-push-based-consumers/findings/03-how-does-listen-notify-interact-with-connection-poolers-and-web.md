---
type: "topic-research"
id: "03-how-does-listen-notify-interact-with-connection-poolers-and-web"
title: "How does `LISTEN`/`NOTIFY` interact with connection poolers and connection limits — PgBouncer session versus transaction mode, other poolers and managed proxies (such as RDS Proxy pinning, Supavisor and PgCat), and the cost of a dedicated session connection per listener — and how does psycopg 3 expose asynchronous notifications and support keeping a long-lived listener connection healthy (keepalives, idle timeouts, reconnection)?"
date: "2026-10-05T13:56:47+00:00"
author: "Toby Clemson"
producer: "research-topic"
status: "complete"
kind: "finding"
round: 1
depth: 3
question: "How does `LISTEN`/`NOTIFY` interact with connection poolers and connection limits — PgBouncer session versus transaction mode, other poolers and managed proxies (such as RDS Proxy pinning, Supavisor and PgCat), and the cost of a dedicated session connection per listener — and how does psycopg 3 expose asynchronous notifications and support keeping a long-lived listener connection healthy (keepalives, idle timeouts, reconnection)?"
source_profile: "web"
tags: ["research", "topic-research", "finding"]
last_updated: "2026-10-05T13:56:47+00:00"
last_updated_by: "Toby Clemson"
schema_version: 1
---

# How does `LISTEN`/`NOTIFY` interact with connection poolers and connection limits — PgBouncer session versus transaction mode, other poolers and managed proxies (such as RDS Proxy pinning, Supavisor and PgCat), and the cost of a dedicated session connection per listener — and how does psycopg 3 expose asynchronous notifications and support keeping a long-lived listener connection healthy (keepalives, idle timeouts, reconnection)?

## Question

How does PostgreSQL's `LISTEN`/`NOTIFY` behave behind connection poolers and
managed proxies? The question covers PgBouncer in session and transaction
mode, Amazon RDS Proxy and its pinning, Supavisor, PgCat and similar poolers.
What does a dedicated session connection for each listener cost against
connection limits? On the client side: how does psycopg 3 expose asynchronous
notifications, and what does it provide, or leave to the application, for
keeping a long-lived listener connection healthy through keepalives, idle
timeouts and reconnection?

## Findings

### Short answer

The two commands are scoped differently, and that decides how each pooler
treats them.

- **`NOTIFY` is transactional.** `NOTIFY`, or `pg_notify()` called from a
  trigger, is queued in the sending transaction and delivered only if that
  transaction commits. It therefore works through any pooler that keeps a
  transaction on one backend, transaction pooling included.
- **`LISTEN` is session state.** It registers the session, and the
  registration lasts until the session ends. It therefore works only where a
  client keeps one backend for the whole subscription: a direct connection, a
  session-mode pool, or a proxy that pins the session, such as RDS Proxy.
- **Transaction pooling does not support it.** That covers transaction mode in
  PgBouncer and Supavisor, Google Cloud SQL Managed Connection Pooling in its
  default mode, and Neon's pooled endpoint. In PgBouncer it fails silently.
- **PgDog is the exception.** It is the only pooler found that emulates
  `LISTEN`/`NOTIFY` in transaction mode. It delivers at most once, and it
  does not tell clients when notifications go missing.

So every working listener is a real PostgreSQL backend counted against
`max_connections`, however many clients the pooler accepts. The memory cost
per backend is small, and so is the CPU cost on PostgreSQL 14 and later. The
connection budget is the cost that matters. One listener connection per
process, listening on a single static channel and fanning out in process,
keeps that budget bounded.

psycopg 3 exposes notifications through the `Connection.notifies()`
generator and its `AsyncConnection` equivalent. They yield `Notify` objects
carrying `channel`, `payload` and `pid`. The generator holds the connection
lock while it runs, which has these consequences:

- Changing registrations and sending heartbeats have to happen between runs,
  each run bounded with `notifies(timeout=...)`.
- Use psycopg 3.2.10 or later, in autocommit mode, with no
  `add_notify_handler()` on the same connection.
- Never close the generator part-way through a batch.

psycopg does not keep the connection alive and does not reconnect it. The
application has to:

- tune libpq keepalives and `tcp_user_timeout`;
- send a heartbeat query more often than the shortest client-request idle
  timer on the path;
- treat any `OperationalError` as the loss of every registration;
- reconnect to the primary, issue `LISTEN` again, and catch up from a durable
  position.

Notifications are lost whenever the session breaks. They can only ever be
wake-up hints on top of a log that consumers read by position.

### Why poolers and `LISTEN` conflict

PostgreSQL scopes the two commands differently:

- **LISTEN is per session.** `LISTEN` "registers the current session as a
  listener" and takes effect at transaction commit; a rolled-back `LISTEN` does
  nothing. "A session's listen registrations are automatically cleared when the
  session ends". One session can listen on any number of channels.
- **NOTIFY is per transaction.** A `NOTIFY` is delivered only if its
  transaction commits. Notifications from one transaction arrive in the order
  they were sent. Identical channel-and-payload pairs within one transaction
  are merged into one.
- **Delivery scope.** A notification reaches only "the sessions currently
  listening on that notification channel" in the same database. A listening
  session that is inside a transaction receives it only after that transaction
  ends.
- **The queue is volatile.** The shared notification queue holds about 8 GB in
  a standard installation. It is not WAL-logged and is wiped at every server
  start. The `async.c` source says: "notifications are not expected to survive
  database crashes".
- **A listener must never sit in an open transaction.** A listener idle inside
  a transaction stops the queue from being cleaned up, the log warns when the
  queue is half full, and when it fills, transactions calling `NOTIFY` fail at
  commit.
- **Primary only.** `LISTEN` and `NOTIFY` raise errors on a hot standby, so a
  listener must connect to the primary.
- **Documented ordering.** To avoid missing changes, execute and commit
  `LISTEN` first, then inspect database state in a new transaction, then rely
  on notifications.

PgBouncer's feature table states the result for transaction pooling: `NOTIFY`
is "Yes" and `LISTEN` is "Never". The other poolers follow the same logic.

### How each pooler and proxy treats a listener

| Pooler or proxy | `LISTEN` through its multiplexed path | Route that supports a listener | Does an idle timer close a listener that only receives? |
| --- | --- | --- | --- |
| PgBouncer | Transaction mode: unsupported, and it fails silently | Session mode (the default `pool_mode`), which can be set per database or user | No. `client_idle_timeout` skips clients linked to a server |
| Amazon RDS Proxy | The session is pinned. It works but is not multiplexed | Every connection; the pin cannot be configured away | Yes. `IdleClientTimeout` (default 30 min) counts only client requests. Hard 24-hour cap |
| Supavisor (Supabase) | Transaction mode (port 6543): unsupported | Session mode on port 5432, or a direct connection | Only if `client_idle_timeout` is set (default 0) |
| PgCat | Transaction mode: not documented; unsafe by inference | Session mode ("Identical to PgBouncer") | Not established |
| Cloud SQL Managed Connection Pooling | Transaction mode (the default): unsupported | Session mode | `client_connection_idle_timeout` defaults to 0 |
| Neon pooled endpoint | PgBouncer in transaction mode: unsupported | Direct connection | Not applicable |
| PgDog | Emulated in transaction mode when `pub_sub_channel_size` is set; at most once | Session mode passes `LISTEN` to the backend | Yes, if the global `client_idle_timeout` is set (default off) |

#### PgBouncer

- **Session pooling** "supports all PostgreSQL features". It releases the
  server connection only when the client disconnects.
- **Transaction pooling** releases the server connection after each
  transaction. It marks `LISTEN`, `SET`/`RESET`, SQL-level `PREPARE` and
  session advisory locks as "Never", and `server_reset_query` is not used in
  that mode.
- **The failure is silent.** A `LISTEN` sent through a transaction pool gets no
  error. The registration stays on a backend the client no longer holds, so
  notifications go to nobody, or "to whichever client happens to hold that
  connection next". A practitioner guide notes that single-client tests usually
  pass and the publish path keeps working, so the fault appears only under
  concurrent load.
- **The usual fix** is a second database alias with `pool_mode=session`, used
  only by listeners. Pool mode can be overridden per database and per user.
  The alternative is to connect listeners directly to PostgreSQL.
- **Native fan-out is not coming soon.** Issue #655, opened in November 2021,
  asks for one upstream listener fanned out to subscribers in transaction mode.
  It is still open.

A session-mode listener holds one server connection for as long as it is
connected. That connection counts against `default_pool_size` (default 20 per
user and database pair) and `max_db_connections` (unlimited by default).

PgBouncer's idle timers leave the listener alone:

- The janitor (`pool_client_maint` in `src/janitor.c`) skips any client that
  has a linked server before it applies `client_idle_timeout`. That setting
  defaults to 0, and the docs say it should be "only used for network
  problems".
- `server_idle_timeout` (600 s) and `server_lifetime` (3600 s) apply only to
  servers not linked to a client.
- Asynchronous `NotificationResponse` messages reach an idle session-mode
  client normally.

What PgBouncer does not do is notice a dead peer quickly. `tcp_keepalive` is
on, but it uses OS defaults; on Linux those are `tcp_keepidle=7200`,
`tcp_keepintvl=75` and `tcp_keepcnt=9`. They can be tuned with
`tcp_keepidle`, `tcp_keepintvl`, `tcp_keepcnt` and `tcp_user_timeout`.

#### Amazon RDS Proxy

**Pinning.** "Listening on a notification channel" is a documented pinning
condition for PostgreSQL. Other conditions include `SET`, session advisory
locks, temporary tables, cursors and `nextval`/`setval`. Pinning filters are
not supported for PostgreSQL, so the pin cannot be configured away.

- A pinned session keeps one database connection "until the session ends",
  and "the session ends when the client connection is dropped".
- No other client can reuse that connection.
- Pinned connections "don't count as idle for the purposes of enforcing
  `MaxIdleConnectionsPercent`".
- RDS Proxy has no state tracking and replay for PostgreSQL, so it has no way
  to re-run `LISTEN` on a different backend.

A listener behind RDS Proxy therefore gets no multiplexing. It behaves like a
direct connection with an extra network hop, which AWS quotes as typically
"low single-digit milliseconds" for queries, and it holds a proxy pool slot
for good.

**How notifications are forwarded.** AWS documents nothing about how the proxy
relays server-initiated `NotificationResponse` messages: no latency figure, no
buffering behaviour, no loss statement. Within one unbroken pinned session, a
notification could only be lost if the proxy dropped bytes from a 1:1 relay,
and nothing documented suggests it does. That conclusion is inferred from the
architecture, not stated by AWS. The proxy also "doesn't currently support
canceling a query from a client by issuing a `CancelRequest`". A listener
therefore needs a client-side way to end a wait, such as the
`notifies(timeout=...)` parameter.

**Ways a pinned listener is closed before 24 hours:**

1. **Idle client timeout.** `IdleClientTimeout` defaults to 1,800 s and can be
   raised to 8 hours. The configuration guide gives the lower bound as
   1 minute; another AWS description gives it as 1 second.
   - AWS counts only requests: a client is idle "when the application doesn't
     submit a new request within the specified time after the previous request
     completed". For the timeout to apply, the connection "must be truly idle
     without sending any queries (even simple health checks such as
     `SELECT 1;`)".
   - So a listener that only receives notifications is closed when the timeout
     expires, however many notifications arrive. AWS does not say how
     server-pushed traffic is treated, but its wording counts only client
     requests.
2. **Database restart.** "Pinned back-end connections might be closed due to a
   database restart."
3. **The 24-hour cap.** "RDS Proxy enforces a maximum life of client
   connections of 24 hours. This value is not configurable." It applies
   "regardless of the activity on the connection". AWS advises setting
   client-side maximum connection lifetimes below 24 hours.

**Liveness checks.** AWS says client-side liveness checks are usually
unnecessary. It accepts them for two purposes:

- deliberately keeping specific connections from timing out at the proxy;
- detecting connections the proxy has dropped.

It recommends ordering timeouts as `client timeout < proxy timeout < database
timeout`. Failover is covered under reconnection below.

#### Supavisor (Supabase)

Supavisor has three pool modes:

- **Transaction:** holds a connection for one transaction.
- **Session:** "assigns a connection to a client for the duration of the
  client connection".
- **Native:** proxies the client as if it were directly connected.

On Supabase:

- Port 5432 is session mode and port 6543 is transaction mode. Session mode on
  port 6543 was deprecated on 28 February 2025.
- Supabase lists `LISTEN`/`NOTIFY` as unsupported in transaction mode, because
  session-level commands are "lost between transactions".
- The paid dedicated pooler is PgBouncer in transaction mode only.
- For persistent backends such as VMs and long-running containers, Supabase
  recommends the direct connection.
- Its FAQ warns that in session mode the first clients to take the backends
  "could keep them, even if they are no longer executing queries".

**Idle timeout.** Supavisor's `client_idle_timeout` defaults to 0, which means
off. The following comes from reading Supavisor's source:

- The idle timer is re-armed only when the database handler reports
  `ReadyForQuery` after a client query.
- In session mode, notifications are written straight to the client socket
  without that signal, so they do not re-arm the timer.
- A non-zero value therefore closes a listener that only receives
  notifications. A client query such as `SELECT 1` resets the timer.

The unit of `client_idle_timeout` is not documented.

**Heartbeat.** `client_heartbeat_interval` (default 60 s) is a probe from the
proxy to the client that finds dead clients behind load balancers. It does not
count the client as active.

#### PgCat

PgCat describes its session pooling as "Identical to PgBouncer". Its
transaction-mode limitations list prepared statements, `SET` and advisory
locks, and do not mention `LISTEN`. It is inferred, not documented, that
`LISTEN` works in session mode and is unsafe in transaction mode, for the same
reason as in PgBouncer.

#### Google Cloud SQL and Neon

**Cloud SQL Managed Connection Pooling:**

- It defaults to transaction mode, where `LISTEN` is unsupported.
- Session mode "uses a dedicated server connection that maintains a session
  state".
- `client_connection_idle_timeout` defaults to 0 and
  `server_connection_idle_timeout` to 600 s.

**Neon:**

- Its pooled endpoint is PgBouncer with `pool_mode=transaction` and
  `max_client_conn=10000`.
- `LISTEN`/`NOTIFY` are not supported on it, and Neon points users who need
  them to direct connections.

#### PgDog: transaction-mode pub/sub with weaker guarantees

**How it works.** PgDog intercepts `LISTEN`, `UNLISTEN` and `NOTIFY` in
transaction mode once `pub_sub_channel_size` is set; the feature is off by
default.

- **Registration.** It registers each channel once per pool (database, user
  and shard) over a standalone server connection.
- **Fan-out.** It distributes each `NotificationResponse` to subscribed
  clients through a Tokio broadcast channel.
- **NOTIFY.** It forwards client `NOTIFY` statements over the same standalone
  connection. A `NOTIFY` inside a transaction is buffered until commit and
  dropped on rollback.
- **Session mode.** PgDog passes the commands straight to the backend; this
  was fixed in PR #800.

Its author's case for the design is that PostgreSQL "only has to send a
message as many times as there are instances of PgDog".

**Trade-offs, from its documentation and source:**

1. **At most once, with silent gaps.** The docs say: "A NOTIFY message can be
   lost if the dedicated connection to Postgres is broken. PgDog will not
   attempt re-delivery". When that connection fails, the listener's error
   branch only logs and sleeps.
   - Client connections stay open and receive no error, notice or other
     signal.
   - A direct listener also loses notifications while disconnected, but it at
     least sees the disconnect and knows to catch up.
2. **Slow subscribers lose messages silently.** A subscriber that falls behind
   the broadcast channel, whose capacity is `pub_sub_channel_size`, loses
   messages. PgDog only increments a "dropped" counter, shown by
   `SHOW LISTENERS` and in OpenMetrics. The counter covers lag, not outages.
3. **`LISTEN` is acknowledged before it takes effect.** The client gets a
   synthetic `LISTEN` reply as soon as the request is queued, before
   PostgreSQL has run it.
   - If another client already listens on the channel, no PostgreSQL `LISTEN`
     is sent at all.
   - The reply also comes while the dedicated connection is down.
   - The "commit `LISTEN`, then read state" ordering therefore no longer
     holds; a commit that lands in that window can be missed.
   - A race in first-time registration was fixed only in June 2026
     (PR #1079).
4. **A client `NOTIFY` is decoupled from the data commit.** It is buffered in
   PgDog's memory, queued after `COMMIT`, and later executed as a separate
   statement. A PgDog crash or redeploy in between loses it while the data
   stays committed. The forwarded statement is built by string interpolation
   with no visible escaping, so payloads containing single quotes, as JSON
   often does, may fail to forward without any error. That last point is
   inferred from the code.
5. **`pg_notify()` from a trigger or function bypasses the interception.**
   - Neither PgDog's regex pre-filter nor its parser treats it as a `NOTIFY`.
     Its parser inspects only the first top-level statement, and trigger
     bodies are invisible to it.
   - So it runs on the pooled backend, with PostgreSQL's native delivery: at
     commit and atomic with the data.
   - It then reaches PgDog's dedicated listener and its subscribed clients.
     This comes from reading the code; no PgDog test covers it.
   - Conditions:
     - **Exact channel names.** An unquoted `LISTEN Events` is folded to
       `events`, but `pg_notify('Events', ...)` is not folded.
     - **Same database.**
     - **Same shard.** Each channel is listened on only on its shard's
       primary.
     - **Sender PID.** The PID clients see is that of the committing pooled
       backend.
   - It keeps the database-wide commit lock that PgDog's buffering was meant
     to avoid (see the channel-layout section below).

**Does PgDog reconnect on its own?** The sources conflict:

- PgDog's documentation says that "upon reconnection, all subsequent messages
  will be delivered over the new connection". Its design notes say the
  listener waits `connect_attempt_delay`, reconnects, issues `LISTEN` again
  for its channels, and keeps working across a primary promotion.
- A reading of `listener.rs` suggests otherwise. The background loop waits for
  a start signal at the top of every iteration, and only `launch()` sends one.
  `launch()` runs at startup, on RELOAD/SIGHUP, on RECONNECT and on replica
  promotion.
- If that reading is right, a listener whose connection drops mid-life stays
  dormant until one of those events. Clients would stay connected and
  silently receive nothing, and client `NOTIFY` statements would queue without
  being forwarded.

This reading has not been checked by running PgDog, and the file has no
reconnect test.

**Restarts and deploys.** On SIGINT or SIGTERM, PgDog disconnects every client
that is not in a transaction, LISTEN-only clients included, with `FATAL 57P01
"PgDog is shutting down"`. It shuts its listeners down last.

PgDog publishes no figure for the notification gap that follows. The gap lasts
until the client has reconnected through a live instance and that instance's
dedicated connection has registered the channel. With several instances it is
probably sub-second to a few seconds. For a single instance restarted in
place, it is at least the full restart time. Both figures are estimates.

RELOAD keeps connections open and swaps the listener (PR #302). Any window
around that swap is not measured.

**Maturity.** The feature shipped in August 2025 (PR #301) under a "bleeding
edge" label. PgDog is still on 0.1.x (v0.1.60, September 2026), and its README
does not say whether pub/sub is stable. Fixes since then:

- **March 2026 (PR #800):** session-mode pools wrongly rejected `LISTEN` and
  `NOTIFY`.
- **April 2026 (PR #925):** the pub/sub listener had to be re-initialised after
  failover.
- **June 2026 (PR #1079):** the first-registration race.
- **July 2026 (PR #1285, v0.1.52):** channels leaked across databases.
- **September 2026 (v0.1.57):** `DISCARD` now clears a client's channels.
- **September 2026 (issue #1512, fixed in v0.1.59):** a pipeline protocol
  violation that broke psycopg 3.

Issue #1462 is still open. `client_idle_timeout` can only be set globally, and
it closes silent `LISTEN` clients; one reporter saw about 8,400 disconnect
errors a day. Per-user and per-database overrides (PR #1467) are not merged.

**When PgDog is acceptable.** PgDog's transaction-mode pub/sub can stand in for
a per-process session listener only if notifications are treated as hints
backed by a bounded catch-up poll. PgDog's authors advise the same: "consider
storing messages in a Postgres table and using a secondary polling mechanism".
In that case:

- run v0.1.59 or later;
- leave `client_idle_timeout` off for listeners;
- size `pub_sub_channel_size` for bursts and alert on the dropped counter.

A consumer could detect a silent outage by periodically sending a `NOTIFY` on
a private channel through PgDog and checking that it comes back. This is a
design inference, not documented practice. Where missed signals must be
detectable, a session-mode connection gives stronger guarantees, either direct
or through a PgDog session pool.

### What a dedicated listener connection costs

**Connection budget.** Every listener that bypasses transaction pooling is a
PostgreSQL backend. It counts against `max_connections` (typically 100), and
some shared memory is sized from that setting. A pooler's high client limit,
such as Neon's `max_client_conn=10000`, does not help: listeners are charged
against the database's own budget. Each pooler adds its own cost:

- **PgBouncer:** the listener also counts against the pool size.
- **RDS Proxy:** each pinned listener permanently holds a proxy slot.
- **Supavisor:** session mode gives each client its own backend.

**Per-connection cost.** Andres Freund's 2020 analysis measured each
connection at "a few MiB", or under 2 MiB with `huge_pages`. CPU was the larger
problem before PostgreSQL 14: idle connections slowed `GetSnapshotData()`, and
5,000 idle connections cut one active connection's throughput by more than
half. pganalyze reports that PostgreSQL 14's snapshot-scalability work raised
throughput from about 15,000 to nearly 35,000 TPS with 10,000 idle connections
plus one active query.

**The cheaper pattern.** One session can listen on any number of channels.
The cheaper pattern is therefore one listener connection per process, fanned
out in process, not one per consumer. Brandur Leach's "notifier" pattern and
the psycopg-based libraries oban-py, Procrastinate and Litestar all work this
way.

### Choosing a channel layout for a shared listener

With one shared listener per process, use one static channel and put the
category, and optionally the stream and position, in the payload. Scope the
channel by schema, as Commanded's `<schema>.events` does. This layout costs
the same as per-category channels on the server, and much less in listener
complexity.

**Writers pay the same either way:**

- **Commit lock.** Every transaction that issued `NOTIFY` takes a
  database-wide `AccessExclusiveLock` at commit. The source comment says this
  is to "Serialize writers ... This ensures that queue entries appear in
  commit order", and notifying commits are serialised as a result.
  - Recall.ai reported that under tens of thousands of concurrent writers the
    lock "effectively serializes all commits".
  - Its later update credits commit 282b1cde9 with removing the bottleneck.
    That commit changed how listeners are woken, and the current master source
    still takes the lock.
  - The lock covers every channel, so splitting channels does not relieve it.
- **Payload limits.** Payloads must be shorter than 8000 bytes.
- **Duplicates.** Duplicates within a transaction collapse only when channel
  and payload are both identical.
- **Notification rate.** Commanded limits it by notifying from a
  statement-level trigger rather than once per row.

**Readers through PostgreSQL 18:**

- The server does not track which backend listens on which channel. Every
  listening backend in the database is signalled for every `NOTIFY`.
- Each woken backend filters entries with a linear scan of its own channel
  list.
- Per-category channels therefore wake no fewer backends. They only save the
  network cost of unwanted notifications, and each match gets more expensive
  as the channel list grows.
- `LISTEN` and `UNLISTEN` take no locks while running. They update
  backend-local state after commit, so churn is cheap on the server. Each
  change still costs a client round trip.

**Readers from PostgreSQL 19:**

- Commit 282b1cde9 (January 2026) is described in the release notes as
  "Improve NOTIFY to only wake up backends that are listening to specified
  notifications". It adds a shared map from channel to the backends listening
  on it.
- Per-category channels then gain a real server-side advantage, but only when
  many listener connections each care about a largely separate set of
  categories, and volume is high. That is an inference.
- `LISTEN`/`UNLISTEN` churn then also touches shared memory.

**Listener complexity strongly favours a static channel:**

- **No interrupted waits.** It never has to leave the `notifies()` generator
  to change registrations.
- **No catch-up race when a consumer starts.** The `LISTEN` was committed when
  the connection opened, so a consumer that registers in process and then
  reads from its checkpoint is safe without extra coordination.
- **Simple reconnect.** A reconnect issues one `LISTEN`.
- **No name mapping.** Category names never have to become channel
  identifiers:
  - Unquoted identifiers are folded to lower case.
  - `LISTEN` silently truncates names longer than 63 bytes, while
    `pg_notify()` rejects names of 64 bytes or more.
  - `LISTEN` cannot take bind parameters, so it needs
    `psycopg.sql.Identifier`.
- **"All events" consumers come free.** They are just another entry in the
  in-process dispatch table.

**Prior art:**

- **Commanded EventStore.** One statement-level trigger notifies one channel
  with payload `stream_uuid,stream_id,from_version,to_version`.
- **Oban and River.** A small fixed set of channels multiplexed over one
  connection.
- **pgmq.** One channel per queue with a NULL payload. It suits standalone
  consumers that each watch one queue, not a shared listener.

If measurement on PostgreSQL 19 ever favours splitting, a fixed set of hashed
bucket channels would keep registration static. This is an inference; no
source describes it.

### How psycopg 3 exposes notifications

**Basics:**

- **Autocommit.** The connection should be in autocommit mode "if you wish to
  receive or send notifications in a timely manner". In autocommit, `LISTEN`
  is active as soon as `execute()` returns, and the listener never sits idle
  in an implicit transaction.
- **Sync or async.** A plain blocking `Connection` works; `AsyncConnection`
  is not required.
- **Notify objects.** Notifications arrive as `Notify` objects with
  `channel`, `payload` and `pid`.

**Two mechanisms; use only one per connection.** Mixing them raises a
`RuntimeWarning` since 3.2.10.

- **The `notifies()` generator**, sync or async, is meant for connections
  dedicated to notifications.
- **`add_notify_handler()` callbacks** fire only "during a connection
  operation, such as a query". psycopg runs no background reader, so on an
  idle listener nothing triggers them.
  - The maintainer's suggested workaround was to `select()` on the socket and
    then run `select 1`. That adds a query for every wake-up.
  - asyncpg's `add_listener()`, by contrast, dispatches from the protocol
    layer.

**Mechanics.**

- **Reading.** psycopg waits for the socket to become readable without using
  CPU, calls `PQconsumeInput`, and drains `PQnotifies`.
- **Failure.** If `PQconsumeInput` fails, it raises
  `OperationalError("consuming input failed: ...")`.
- **`timeout` and `stop_after`.** Available since 3.2 (PR #673, January 2024).
  - `timeout` is a total deadline computed once (`monotonic() + timeout`) and
    not reset by each notification. A loop of `notifies(timeout=T)` therefore
    returns at least once per interval, even under steady traffic.
  - The timeout is a local socket wait and sends nothing to the server.
  - Both limits are checked only after a whole received batch has been
    yielded. `stop_after` "might actually receive more than this number if
    more than one notifications arrives in the same packet".

**The connection lock.** Both `notifies()` implementations run their whole
body inside `with self.lock` or `async with self.lock`, and yield from inside
it. The lock is a `threading.Lock` or an `asyncio.Lock`, and neither is
re-entrant.

- **Inside the loop body.** Calling `conn.execute()` on the same connection
  deadlocks in sync code, and never completes in async code.
- **Other threads and tasks** wait until the generator returns. The
  maintainer's position: "you are not supposed to run queries while you are
  listening".
- **Before 3.2,** stopping the generator required `gen.close()` from another
  task or thread. Issue #340 reports `AsyncConnection.execute()` hanging while
  it ran.

**Versions that matter:**

- **3.2:** `timeout` and `stop_after`.
- **3.2.4:** notifications that arrive while the generator is not running are
  kept in a backlog and yielded first on the next call (#962, PR #975). Before
  this they were lost, which is exactly the gap a "leave, run a statement,
  re-enter" loop creates. The changelog also says server-initiated disconnects
  now raise the specific server error, such as `AdminShutdown`.
- **3.2.10:** fixed a backlog leak (#1091). A connection that had `LISTEN`
  registrations but never called `notifies()` grew by about 200 MB an hour at
  more than 10,000 events a minute. With a handler registered, the backlog is
  no longer filled, and mixing the two mechanisms became deprecated.
- **3.3.6:** the current release.

**Closing the generator mid-batch silently drops the rest.** Every release up
to 3.3.6 behaves this way, according to a reading of the source.

**Why.** The low-level generator drains every pending notification into a
Python list, which removes them from libpq ("it is considered handled and will
be removed from the list of notifications"). The high-level generator then
yields that list one item at a time, with the backlog swapped out to `None`.
If the generator is closed while suspended mid-batch, the rest of the list is
discarded, with no warning, log line or exception.

**Exits that lose the rest of a batch:**

- an explicit `close()` or `aclose()`;
- a `break` that drops the last reference to the generator;
- an exception raised in the loop body;
- a cancelled asyncio task whose abandoned generator is later finalised.

**Exits that are safe:**

- `timeout` and `stop_after`;
- items served from the backlog.

psycopg's own documentation example calls `gen.close()` inside the loop.
Issue #1310 lists "close() and reopen it" as a workaround for changing
registrations, which is exactly the pattern that hits this loss.

**A test that should reproduce it** (sketched, not run, and timing-sensitive):

1. Commit three `NOTIFY` commands with distinct payloads in one transaction
   from a second connection.
2. On the listener, take one notification and close the generator, without
   running any query between the commit and entering `notifies()`.
3. Read the rest. Against 3.0 to 3.3.6 only the first payload is expected to
   arrive.

Two controls make the test convincing:

- **Batch arrival.** `stop_after=1` returning all three payloads shows the
  batch arrived together.
- **Backlog path.** A `SELECT 1` before creating the generator routes the
  notifications through the backlog, so the test should then pass.

### Structuring the listener loop and in-process fan-out

oban-py, Procrastinate and Litestar, all built on psycopg 3, use the same
shape:

1. **A dedicated connection.** One autocommit connection per process, outside
   any pool, owned by a single task or thread.
2. **An alternating loop.** Apply pending `LISTEN`/`UNLISTEN` commands, then
   run `notifies(timeout=T)` until it ends by itself. `T` sets the worst-case
   delay for a registration change or a shutdown. oban-py and Litestar use
   0.1 s; Procrastinate uses 30 s and sends a heartbeat after each run.
3. **Subscriptions go through a queue.**
   - oban-py's `listen(wait=True)` returns only after `LISTEN` has run.
   - `UNLISTEN` is queued only when a channel's last subscriber leaves.
   - On reconnect, every subscribed channel is queued for `LISTEN` again.
4. **Non-blocking dispatch.** Dispatch runs while the lock is held, so it must
   not block:
   - Litestar calls `put_nowait` into a queue.
   - oban-py starts async callbacks with `asyncio.create_task`.
   - Procrastinate only sets an event, with a 5 s fallback poll.
5. **The heartbeat shares the connection.**
   - Procrastinate runs `SELECT 1` after each timed-out run.
   - oban-py runs `SELECT 1` every 30 s from a separate task. That task gets
     the lock because `asyncio.Lock` is fair: "the coroutine that proceeds
     will be the first coroutine that started waiting on the lock".

**A counter-example.** pgqueuer runs one watcher task per channel on one
connection, each iterating `notifies(timeout=1.0, stop_after=1)`. But
`notifies()` yields every channel's notifications, and the watcher does not
check `note.channel`. A notification therefore goes to whichever watcher holds
the lock at the time (inferred from the code). One loop per connection,
routing by channel, avoids this.

**Threaded code.** `threading.Lock` does not define which waiting thread gets
the lock next. A thread looping on `notifies()` can therefore starve another
thread calling `execute()` on the same connection. A sound threaded design,
assembled here rather than taken from a single source:

- the listener thread is the only user of the connection;
- subscription changes reach it through a `queue.Queue`, drained between
  generator runs;
- each command carries a `threading.Event` that is set once its `LISTEN` has
  run;
- fan-out uses `put_nowait` or a per-consumer `threading.Event`;
- the heartbeat is a statement the owner thread runs itself.

**For an event store**, where a notification only means "new events exist;
re-read from your checkpoint":

- **Coalesce per consumer.** Give each consumer an `asyncio.Event` or
  `threading.Event` instead of queueing every notification. Memory stays
  bounded, dispatch under the lock stays O(1), and a slow consumer loses
  nothing that matters. Keep a periodic fallback poll.
- **Subscribe before catching up.** With dynamic channels, a consumer must
  wait until its `LISTEN` is confirmed before its first catch-up read. A
  static channel makes this automatic.
- **On reconnect,** issue `LISTEN` again and wake every consumer.

### Keeping the listener open: idle timers and heartbeats

Every client idle timer examined counts only requests from the client.
Notifications sent by the server do not reset any of them.

| Timer | Default | Does it close a listener that only receives? |
| --- | --- | --- |
| RDS Proxy `IdleClientTimeout` | 1,800 s | Yes |
| PgBouncer `client_idle_timeout` (session mode) | 0 (off) | No. Linked clients are skipped |
| Supavisor `client_idle_timeout` (session mode) | 0 (off) | Yes, if non-zero (inferred from source) |
| PgDog `client_idle_timeout` | off | Yes, if set. Global only (issue #1462) |
| PostgreSQL `idle_session_timeout` (14 and later) | 0 (off) | Yes |

**PostgreSQL's `idle_session_timeout`** terminates "any session that has been
idle (that is, waiting for a client query), but not within an open
transaction". The commit that added it (9877374) and the backend source show
why notifications do not reset it:

- **Start.** The timer starts when the backend sends `ReadyForQuery` outside a
  transaction.
- **Stop.** It is stopped only after the backend reads the next message from
  the client.
- **Delivery.** Notifications are delivered from inside the wait for a client
  read (`ProcessClientReadInterrupt` → `ProcessIncomingNotify`), which never
  touches the timer.
- **Result.** The session ends `idle_session_timeout` milliseconds after the
  client's last command, however many notifications it has received.
  `pg_stat_activity` keeps showing the listener as `idle`.
- **Ordering at expiry.** The termination is checked before pending
  notifications are processed, so a notification pending at that moment is
  lost.
- **Error code.** The error is SQLSTATE `57P05`.

libpq passes an error that arrives while the connection is idle to its notice
processor. So, from the code, a listener blocked in `notifies()` sees a
generic `OperationalError("consuming input failed: ...")` without the
`57P05` attached.

There are two remedies:

- **Disable it for the listener.** The setting has `user` context, so the
  listener can run `SET idle_session_timeout = 0` on its own session.
- **Send heartbeats.** Any query sent more often than the timeout keeps the
  session alive.

The PostgreSQL docs themselves warn against enforcing the timeout on
connections made through poolers.

**The heartbeat.**

- **What to send.** A cheap round-trip query such as `SELECT 1`, sent on the
  autocommit listener connection, resets every one of these timers.
- **No open transaction.** If the heartbeat left a transaction open, the
  session would fall foul of `idle_in_transaction_session_timeout`, or of
  PgBouncer's `idle_transaction_timeout` and `transaction_timeout`.
- **Interval.** At most about half the shortest client-request timer on the
  path. It should also be below any layer-4 device's idle timeout, unless TCP
  keepalives cover that:
  - AWS Network Load Balancer: 350 s by default; adjustable from 60 to
    6,000 s for TCP listeners; fixed at 350 s for TLS listeners.
  - AWS NAT gateway: closes connections idle for 350 s or more, and answers
    later traffic with an RST.

  Roughly 30 s to 5 minutes suits most deployments; AWS's own example pings
  once a minute against the 30-minute default. A shorter interval also
  shortens the time before a silently half-open listener is noticed. Without
  a heartbeat, "no events" and "dead connection" look the same.
- **Idle management still works,** under three conditions:
  - The listener's backend is already dedicated or pinned, so the heartbeat
    keeps nothing the proxy could otherwise reclaim.
  - Heartbeats run only on the listener, never on pooled query connections.
    AWS says to perform them "only for the specific connections that you want
    to keep from timing out".
  - The heartbeat runs from the same loop that consumes notifications, so a
    stuck consumer stops sending heartbeats and the proxy reclaims its
    connection. Keep per-notification work short so the heartbeat keeps to
    schedule.
- **Make it check identity too.**
  `SELECT pg_backend_pid(), array(SELECT pg_listening_channels())` returns the
  backend PID and the channels the current session listens on. A changed PID
  or a missing channel means the registrations are gone. The listener should
  then issue `LISTEN` again and catch up. This catches a proxy that silently
  reattached the client to a new backend, and a reconnect that forgot to
  re-listen.
- **Recycle before RDS Proxy's 24-hour cap.** No heartbeat prevents it.
  Replace the listener on your own schedule: open the new connection and
  commit its `LISTEN` before closing the old one, then catch up.

### Detecting a dead connection: keepalives

**Orderly versus unclean loss.** A readable socket signals failure only if a
FIN or RST actually arrives.

- **Orderly termination** gives an immediate error. This covers reboots,
  manual failover, maintenance and scaling, where PostgreSQL's fast shutdown
  sends SIGTERM to every backend.
- **Unclean loss** gives nothing until TCP gives up. This covers a host crash,
  an AZ loss or a network partition. A `notifies()` call with no timeout could
  then wait forever.

**libpq and Linux defaults:**

- **Keepalives.** libpq enables keepalives by default (`keepalives=1`), but
  with system defaults unless `keepalives_idle`, `keepalives_interval` and
  `keepalives_count` are set.
- **Linux keepalive timing.** Probing starts after 7,200 s idle, followed by
  9 probes at 75 s intervals. A silent dead peer can go undetected for about
  2 h 11 min.
- **After a write**, such as a heartbeat, unacknowledged data is retransmitted
  according to `tcp_retries2` (default 15, "approximately between 13 to 30
  minutes").
- **`tcp_user_timeout`** (in milliseconds) caps that wait. On Linux it "will
  override keepalive to determine when to close a connection due to keepalive
  failure".

**AWS's settings for Aurora PostgreSQL fast failover** are idle 1 s, interval
1 s and count 5. AWS says these "should notify the application within five
seconds", and that a lossy network needs a higher count.

**What keepalives do not do:**

- Keepalive probes end at the proxy's TCP stack and never reset a proxy's idle
  timer; only a protocol-level query does.
- The server-side equivalents are `tcp_keepalives_idle`,
  `tcp_keepalives_interval`, `tcp_keepalives_count` and `tcp_user_timeout`.
- `client_connection_check_interval` applies only while a query is running, so
  it does not help an idle listener.

**psycopg's own detection pattern** registers `conn.fileno()` with `selectors`,
or with `loop.add_reader` under asyncio. When the socket becomes readable it
runs `SELECT 1` and treats `OperationalError` as a lost connection.
`Connection.closed` and `Connection.broken` (closed uncleanly) expose the
connection's state.

Combining moderate keepalives, `tcp_user_timeout` and a periodic heartbeat
brings detection down to seconds or tens of seconds.

### Reconnecting without missing events

**Three loss windows.** No reconnect logic can make a restart or failover
lossless. Notifications are lost in three windows:

1. notifications committed but not yet read when the old server dies (the
   queue is wiped at restart);
2. everything committed while no session is registered;
3. notifications sent before the new connection's `LISTEN` takes effect.

The Elixir driver Postgrex documents the same limit for its own auto-reconnect:
"any notifications that occur during the disconnection period are not queued
and cannot be recovered".

**Managed HA does keep committed data:**

- An RDS Multi-AZ DB cluster failover "completes when both reader DB instances
  have applied outstanding transactions from the failed writer".
- Aurora replicates writes synchronously to six storage nodes.
- Azure zone-redundant HA is documented as zero data loss.

So a catch-up read against the new writer finds every event committed before
the failover. Surviving a failover is a property of the catch-up read, not of
the notification channel.

**The reconnect-and-catch-up loop:**

1. **Connect to a writer only.**
   - Use libpq multi-host syntax with `target_session_attrs=read-write` (or
     `primary`), so standbys are rejected. This matters because `LISTEN` fails
     on a standby, and after an Aurora failover DNS can briefly point at the
     wrong node. AWS's Aurora guidance lists the cluster and reader endpoints
     and requires a primary.
   - Set `connect_timeout`; it applies to each host separately. psycopg
     enforces its own connect timeout: 130 s when `connect_timeout` is unset
     or zero or less, and at least 2 s otherwise. Since 3.2.8 it reports every
     attempt's error.
   - Open the connection with `autocommit=True`.
2. **Issue `LISTEN` before reading anything.** This closes window 3.
3. **Catch up** from the consumer's last processed position to the current
   head.
4. **Wait in `notifies(timeout=T)`.** Each notification triggers another read
   from the last position; payloads are not trusted as content. On timeout,
   send a heartbeat and run a safety poll.
5. **On any `psycopg.OperationalError`,** discard the connection, back off
   with jitter, and return to step 1. Cap the backoff at a few seconds up to
   about 30 s.

Because `LISTEN` comes before the catch-up, overlap can only produce
duplicates, never gaps. The consumer must therefore skip anything at or below
its last processed position.

**Catch-up on a separate connection.** If the catch-up runs on a separate
connection, that connection must also reach the writer. A lagging replica may
not yet show the event a notification announced; this is an inference from
the replication model.

**Which exception to expect.** The 3.2.4 changelog says server-initiated
disconnects raise specific errors such as `AdminShutdown`. A reading of the
`notifies()` path suggests that a blocked listener may instead get the generic
`OperationalError`. Either way it is an `OperationalError`, so one handler
covers both.

A minimal synchronous shape (host names are placeholders; `consumer.catch_up()`
reads from the last processed position on a separate writer connection):

```python
import random, time
import psycopg

CONNINFO = (
    "host=cluster-writer.example,cluster-reader.example "
    "target_session_attrs=read-write connect_timeout=5 "
    "keepalives=1 keepalives_idle=10 keepalives_interval=5 keepalives_count=3 "
    "tcp_user_timeout=30000"
)

def run(consumer, stop):
    delay = 0.5
    while not stop.is_set():
        try:
            with psycopg.connect(CONNINFO, autocommit=True) as conn:
                conn.execute("LISTEN event_appended")
                consumer.catch_up()
                delay = 0.5
                while not stop.is_set():
                    for _ in conn.notifies(timeout=30.0):
                        consumer.catch_up()
                    conn.execute("SELECT 1")
                    consumer.catch_up()
        except psycopg.OperationalError:
            time.sleep(delay * random.uniform(0.5, 1.5))
            delay = min(delay * 2, 30.0)
```

In this minimal form the catch-up runs inside the generator's loop body, which
delays the next socket read and the next heartbeat. With many consumers or
slow reads, the body should instead only set per-consumer events, as described
under fan-out above.

**A simpler alternative.** Brandur Leach's notifier treats an unhealthy
listener as fatal and lets the process restart; he has "only seen it happen
once in a year+ of use". This works when a supervisor restarts the process
quickly. In-process reconnection costs less when failovers recur, for example
during maintenance windows. PostgREST's listener retries forever with
exponential backoff capped at 32 s, and reloads its cached state when it
recovers.

**Documented outage windows,** which the backoff should be tuned against:

| Service | Documented outage | How clients reach the new writer |
| --- | --- | --- |
| RDS Multi-AZ DB instance | "typically 60–120 seconds" | DNS record changed to the standby |
| RDS Multi-AZ DB cluster | "typically under 35 seconds" | DNS record changed to a reader |
| Aurora PostgreSQL with a replica | "typically restored in less than 60 seconds, and often less than 30 seconds"; without a replica, under 10 minutes | Cluster endpoint; DNS update typically under 30 s; reader endpoint TTL 1 s |
| Cloud SQL for PostgreSQL HA | "about sixty seconds"; existing connections "are closed" | Shared static IP |
| Cloud SQL planned maintenance | Enterprise "less than 30 seconds on average"; Enterprise Plus typically under 1 second | Same IP |
| Azure Database for PostgreSQL, zone-redundant HA | "typically completes within 60-120 seconds" | Same server name |

psycopg 3.1 and later resolves host names on each async connect attempt. Any
OS-level or sidecar DNS cache must still respect the endpoints' low TTLs.
Citus measured less than 2 s of impact when reconnecting with
`target_session_attrs=read-write` and pg_auto_failover.

**RDS Proxy failover and pinned listeners.**

- **The documented promise.** RDS Proxy says it keeps idle client connections
  open through a failover: "Only connections that are in the middle of a
  transaction or SQL statement are canceled".
- **It applies to multiplexed connections.** That mechanism borrows a fresh
  database connection for the next transaction, which works only when no
  session state is tied to the old one.
- **The trap for listeners.** A psycopg listener blocked in `notifies()` has
  no transaction or statement in flight, so the "idle connections survive"
  sentence seems to cover it. It should not be relied on.
- **Where AWS does address pinned sessions,** losing their instance means an
  error and a reconnect. The proxy endpoint guides say: "If the connection is
  pinned, the next query on the connection returns an error. However, your
  application can immediately reconnect to the same proxy endpoint". They also
  say "if the session is pinned, your application gets an error and must
  reconnect". Those passages concern reader endpoints.
- **Blue/Green switchover** drops every connection. On RDS for PostgreSQL,
  queries return `AdminShutdown: terminating connection due to administrator
  command`.

The best inference is that a writer failover ends a pinned listener session
rather than silently reattaching it to a backend with no registrations. AWS
does not state this for the writer. What remains open is when an idle listener
finds out: the only explicit wording says the error comes on "the next query".
Hence the bounded `notifies()` wait and the identity-checking heartbeat. The
behaviour can be checked with a forced "Reboot with failover" while watching
the `ClientConnectionsClosed` and `DatabaseConnectionsCurrentlySessionPinned`
metrics.

**The AWS Advanced Python Wrapper** supports psycopg 3 and speeds up Aurora
failover. It keeps a cluster topology cache, refreshed every 2 s by default,
with a 300 s overall failover timeout. It does not watch a listener. A reading
of its source (3.1.0) shows:

- **`notifies()` bypasses the plugin pipeline.** `AwsWrapperConnection` passes
  unknown attributes straight to the current psycopg connection through
  `__getattr__`. As a result:
  - neither failover nor Enhanced Failure Monitoring covers a listener blocked
    in `notifies()`;
  - a closed socket raises psycopg's raw `OperationalError`;
  - a silently dead host leaves the listener blocked until keepalives or the
    `timeout` fire.
- **The next pipeline call triggers failover.** A call such as `execute`,
  `cursor`, `commit` or `rollback`:
  1. classifies the dead connection as a network error;
  2. swaps a new physical connection into the same wrapper object;
  3. raises `FailoverSuccessError`, or `TransactionResolutionUnknownError`, or
     `FailoverFailedError` if no connection was made within
     `failover_timeout_sec` (default 300 s).

  The statement that triggered failover did not run.
- **Little session state carries over.** Only `autocommit`, `read_only` and
  `isolation_level` are transferred. `LISTEN` registrations, notify handlers
  and any generator obtained earlier are gone.
- **Another thread may close the socket.** The default
  `aurora_connection_tracker` plugin can close the listener's socket from
  another thread when it detects a writer change. psycopg's `close()` does not
  take the connection lock. Whether that reliably wakes a blocked `notifies()`
  is undocumented.
- **Limited coverage.** Failover covers Aurora and RDS Multi-AZ DB clusters
  only, not plain RDS instances. `failover_mode` defaults to `strict_writer`,
  except through an Aurora reader endpoint.

The workable pattern:

1. Loop on `notifies(timeout=heartbeat)`.
2. Between waits, send the heartbeat through the wrapper.
3. On any error, call a pipeline method until it raises `FailoverSuccessError`
   or `FailoverFailedError`.
4. Then, on the same wrapper object, issue `LISTEN` again and catch up.

**psycopg_pool is a poor home for the listener.** It offers:

- `check` callbacks, with `ConnectionPool.check_connection` added in 3.2;
- `max_lifetime` (3600 s) and `max_idle` (600 s);
- `reconnect_timeout` (300 s with exponential backoff), followed by a
  `reconnect_failed` callback.

But it manages connections that are borrowed and returned. On return it ends
any open transaction and calls an optional `reset` callback, and the docs
describe no `UNLISTEN` or `DISCARD`. Holding a pooled connection as a
permanent listener bypasses `max_lifetime` and shrinks the pool for good.
Returning one that ran `LISTEN` would leave its registration in place (an
inference). A dedicated `Connection` with its own supervision loop fits the
role better.

### Recommended design for a psycopg 3 event store

- **Writers** keep using whatever pool they already use; transaction pooling
  is fine.
  - Issue `NOTIFY` inside the append transaction, from the statement or from a
    statement-level trigger.
  - Use one static, schema-scoped channel with the category in the payload.
  - Accept that notifying commits serialise on the database-wide commit lock.
- **Each consuming process** opens one autocommit listener connection that
  bypasses transaction pooling. Any of these routes works:
  - a direct connection;
  - a PgBouncer session-mode alias;
  - Supabase port 5432;
  - Cloud SQL session mode;
  - RDS Proxy, accepting the pin;
  - a PgDog session pool.

  The connection should:
  - connect with `target_session_attrs=primary` and a `connect_timeout`;
  - set keepalives below any middlebox idle limit, plus `tcp_user_timeout`;
  - run `SET idle_session_timeout = 0`.
- **The listener loop:**
  - require psycopg 3.2.10 or later;
  - use the generator only, with no notify handlers;
  - loop on `notifies(timeout=T)`;
  - never close the generator mid-batch;
  - dispatch by setting per-consumer events.
- **Heartbeat between generator runs.**
  - Check the backend PID and `pg_listening_channels()`.
  - Run it at no more than half the shortest client-request idle timer on the
    path, and below any layer-4 idle timeout.
  - Behind RDS Proxy, also recycle the connection, with overlap, before the
    24-hour cap.
- **On any `OperationalError` or identity mismatch:**
  - reconnect with jittered backoff capped around 30 s;
  - issue `LISTEN`, then wake every consumer to catch up from its stored
    position.
- **Keep a periodic safety poll** that bounds the latency of any missed
  notification. It matters most behind PgDog, where gaps are silent.

### Unverified points and open questions

- **RDS Proxy:**
  - How it relays `NotificationResponse` messages is undocumented.
  - It is not documented whether server-pushed traffic resets
    `IdleClientTimeout`; the wording counts only client requests.
  - It is unknown whether a pinned listener is closed straight away on writer
    failover, or errors only on its next query.
  - AWS sources disagree on the minimum `IdleClientTimeout`: 1 minute or
    1 second.
- **PgDog:**
  - The documentation says the dedicated connection reconnects automatically;
    the code reading suggests it may stay dormant. This conflict is
    unresolved.
  - The `pg_notify()` trigger path and payload escaping are code readings,
    not tested.
- **Supavisor:**
  - The behaviour of `client_idle_timeout` with listeners is inferred from
    source.
  - Its unit is undocumented.
- **PgCat:** `LISTEN` support in session mode is inferred, not documented.
- **psycopg:**
  - The mid-batch loss comes from reading the source, and the reproduction
    test has not been run.
  - Which exception a server-terminated idle listener receives
    (`AdminShutdown` or a generic `OperationalError`) differs between the
    changelog and a reading of the code.
- **AWS Advanced Python Wrapper:** whether a `close()` from another thread
  promptly wakes a blocked `notifies()` is undocumented.
- **Managed services:** whether any of them set `idle_session_timeout` by
  default was not established.

## Sources

- [LISTEN](https://www.postgresql.org/docs/current/sql-listen.html) — tier-1 — postgresql.org
- [NOTIFY](https://www.postgresql.org/docs/current/sql-notify.html) — tier-1 — postgresql.org
- [libpq: Asynchronous Notification](https://www.postgresql.org/docs/current/libpq-notify.html) — tier-1 — postgresql.org
- [libpq: Database Connection Control Functions (connection parameters)](https://www.postgresql.org/docs/current/libpq-connect.html) — tier-1 — postgresql.org
- [Client Connection Defaults (idle_session_timeout and related)](https://www.postgresql.org/docs/current/runtime-config-client.html) — tier-1 — postgresql.org
- [Connections and Authentication (max_connections, tcp_keepalives_*)](https://www.postgresql.org/docs/current/runtime-config-connection.html) — tier-1 — postgresql.org
- [Hot Standby](https://www.postgresql.org/docs/current/hot-standby.html) — tier-1 — postgresql.org
- [Shutting Down the Server (PostgreSQL docs)](https://www.postgresql.org/docs/current/server-shutdown.html) — tier-1 — postgresql.org
- [System Information Functions (PostgreSQL docs)](https://www.postgresql.org/docs/current/functions-info.html) — tier-1 — postgresql.org
- [PostgreSQL documentation: Message Flow (asynchronous operations)](https://www.postgresql.org/docs/current/protocol-flow.html) — tier-1 — postgresql.org
- [PostgreSQL Documentation: Appendix A, PostgreSQL Error Codes (current)](https://www.postgresql.org/docs/current/errcodes-appendix.html) — tier-1 — postgresql.org
- [PostgreSQL 14 Documentation: Appendix A, PostgreSQL Error Codes](https://www.postgresql.org/docs/14/errcodes-appendix.html) — tier-1 — postgresql.org
- [PostgreSQL Documentation: pg_settings view (context column)](https://www.postgresql.org/docs/current/view-pg-settings.html) — tier-1 — postgresql.org
- [Lexical Structure: identifiers (PostgreSQL documentation, current)](https://www.postgresql.org/docs/current/sql-syntax-lexical.html) — tier-1 — postgresql.org
- [PostgreSQL 19 release notes (devel)](https://www.postgresql.org/docs/devel/release-19.html) — tier-1 — postgresql.org
- [PostgreSQL 19 release notes](https://www.postgresql.org/docs/19/release-19.html) — tier-1 — postgresql.org
- [Commit 282b1cde9: Optimize LISTEN/NOTIFY via shared channel map and direct advancement](https://git.postgresql.org/gitweb/?p=postgresql.git;a=commit;h=282b1cde9) — tier-1 — git.postgresql.org
- [Add idle_session_timeout (commitdiff 9877374)](https://git.postgresql.org/gitweb/?p=postgresql.git;a=commitdiff;h=9877374bef76ef03923f6aa8b955f2dbcbe6c2c7) — tier-1 — git.postgresql.org
- [Add idle_session_timeout (commit patch, postgres mirror)](https://github.com/postgres/postgres/commit/9877374bef76ef03923f6aa8b955f2dbcbe6c2c7.patch) — tier-1 — github.com (postgres/postgres)
- [Terminate the idle sessions (pgsql-hackers thread)](https://www.postgresql.org/message-id/763A0689-F189-459E-946F-F0EC4458980B@hotmail.com) — tier-1 — postgresql.org mailing-list archive
- [src/backend/commands/async.c (PostgreSQL source)](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/commands/async.c) — tier-1 — github.com/postgres/postgres
- [src/backend/commands/async.c (REL_18_STABLE)](https://raw.githubusercontent.com/postgres/postgres/REL_18_STABLE/src/backend/commands/async.c) — tier-1 — github.com (postgres/postgres)
- [src/backend/tcop/postgres.c (master)](https://raw.githubusercontent.com/postgres/postgres/master/src/backend/tcop/postgres.c) — tier-1 — github.com (postgres/postgres)
- [src/interfaces/libpq/fe-protocol3.c (master)](https://raw.githubusercontent.com/postgres/postgres/master/src/interfaces/libpq/fe-protocol3.c) — tier-1 — github.com (postgres/postgres)
- [idle_session_timeout parameter reference](https://postgresqlco.nf/doc/en/param/idle_session_timeout/) — tier-2 — postgresqlco.nf
- [tcp(7) — Linux manual page](https://man7.org/linux/man-pages/man7/tcp.7.html) — tier-1 — man7.org (Linux man-pages project)
- [Psycopg: Asynchronous operations and notifications](https://www.psycopg.org/psycopg3/docs/advanced/async.html) — tier-1 — psycopg.org
- [Psycopg async.rst documentation source](https://raw.githubusercontent.com/psycopg/psycopg/master/docs/advanced/async.rst) — tier-1 — raw.githubusercontent.com (psycopg/psycopg repository)
- [Psycopg: Connection classes](https://www.psycopg.org/psycopg3/docs/api/connections.html) — tier-1 — psycopg.org
- [Psycopg release notes](https://www.psycopg.org/psycopg3/docs/news.html) — tier-1 — psycopg.org
- [psycopg changelog (docs/news.rst)](https://raw.githubusercontent.com/psycopg/psycopg/master/docs/news.rst) — tier-1 — github.com/psycopg/psycopg
- [Psycopg: Connection pools](https://www.psycopg.org/psycopg3/docs/advanced/pool.html) — tier-1 — psycopg.org
- [psycopg_pool API](https://www.psycopg.org/psycopg3/docs/api/pool.html) — tier-1 — psycopg.org
- [DNS functions (psycopg 3 API docs)](https://www.psycopg.org/psycopg3/docs/api/dns.html) — tier-1 — psycopg.org
- [psycopg.sql: SQL string composition (psycopg 3 documentation)](https://www.psycopg.org/psycopg3/docs/api/sql.html) — tier-1 — psycopg.org
- [psycopg connection.py source](https://raw.githubusercontent.com/psycopg/psycopg/master/psycopg/psycopg/connection.py) — tier-1 — raw.githubusercontent.com (psycopg/psycopg repository)
- [psycopg connection.py (3.3.6 tag)](https://raw.githubusercontent.com/psycopg/psycopg/3.3.6/psycopg/psycopg/connection.py) — tier-1 — github.com/psycopg/psycopg
- [psycopg connection.py (3.1.18 tag)](https://raw.githubusercontent.com/psycopg/psycopg/3.1.18/psycopg/psycopg/connection.py) — tier-1 — github.com/psycopg/psycopg
- [psycopg source: connection_async.py](https://raw.githubusercontent.com/psycopg/psycopg/master/psycopg/psycopg/connection_async.py) — tier-1 — raw.githubusercontent.com (psycopg/psycopg repository)
- [psycopg source: _connection_base.py](https://raw.githubusercontent.com/psycopg/psycopg/master/psycopg/psycopg/_connection_base.py) — tier-1 — raw.githubusercontent.com (psycopg/psycopg repository)
- [psycopg source: _acompat.py](https://raw.githubusercontent.com/psycopg/psycopg/master/psycopg/psycopg/_acompat.py) — tier-1 — raw.githubusercontent.com (psycopg/psycopg repository)
- [psycopg generators.py source](https://raw.githubusercontent.com/psycopg/psycopg/master/psycopg/psycopg/generators.py) — tier-1 — raw.githubusercontent.com (psycopg/psycopg repository)
- [psycopg pq_ctypes.py source](https://raw.githubusercontent.com/psycopg/psycopg/master/psycopg/psycopg/pq/pq_ctypes.py) — tier-1 — raw.githubusercontent.com (psycopg/psycopg repository)
- [psycopg conninfo.py source](https://raw.githubusercontent.com/psycopg/psycopg/master/psycopg/psycopg/conninfo.py) — tier-1 — raw.githubusercontent.com (psycopg/psycopg repository)
- [psycopg tests/test_notify.py](https://raw.githubusercontent.com/psycopg/psycopg/master/tests/test_notify.py) — tier-1 — github.com/psycopg/psycopg
- [psycopg PR #673: add timeout and stop_after to notifies()](https://github.com/psycopg/psycopg/pull/673) — tier-1 — github.com (psycopg/psycopg)
- [PR #975: Don't lose notifications while notifies() is not running](https://github.com/psycopg/psycopg/pull/975) — tier-1 — github.com/psycopg/psycopg
- [AsyncConnection.notifies() generator blocks AsyncConnection.execute() (issue #340)](https://github.com/psycopg/psycopg/issues/340) — tier-1 — github.com (psycopg project issue tracker)
- [psycopg issue #340 comments: notifies() blocks execute()](https://api.github.com/repos/psycopg/psycopg/issues/340/comments) — tier-1 — api.github.com (psycopg/psycopg issue tracker)
- [Connection.notifies() generator loses notifications (issue #962)](https://github.com/psycopg/psycopg/issues/962) — tier-1 — github.com (psycopg project issue tracker)
- [psycopg issue #962 comments: notifications lost while notifies() not running](https://api.github.com/repos/psycopg/psycopg/issues/962/comments) — tier-1 — api.github.com (psycopg/psycopg issue tracker)
- [Memory leak when listening to Notifications with long-running connections (issue #1091)](https://github.com/psycopg/psycopg/issues/1091) — tier-1 — github.com (psycopg project issue tracker)
- [Issue #1310: Recommended pattern for notifies() while also running LISTEN/UNLISTEN on the same connection](https://github.com/psycopg/psycopg/issues/1310) — tier-2 — github.com/psycopg/psycopg issue tracker
- [psycopg issue tracker search: notifies](https://github.com/psycopg/psycopg/issues?q=notifies) — tier-2 — github.com/psycopg/psycopg issue tracker
- [psycopg release metadata](https://pypi.org/pypi/psycopg/json) — tier-1 — pypi.org
- [Daniele Varrazzo on improving notifies() (psycopg mailing list, 2023-10-24)](https://www.postgresql.org/message-id/CA%2Bmi_8Z6pSgVOpWgretCKpop8MzEWnJ-R-A3c6KQdodYY4D0YQ%40mail.gmail.com) — tier-1 — postgresql.org (psycopg mailing list archive)
- [asyncio synchronization primitives (Python docs)](https://docs.python.org/3/library/asyncio-sync.html) — tier-1 — docs.python.org
- [threading — Thread-based parallelism (Python docs)](https://docs.python.org/3/library/threading.html) — tier-1 — docs.python.org
- [Python language reference: Expressions (generator and asynchronous generator methods)](https://docs.python.org/3/reference/expressions.html) — tier-1 — docs.python.org
- [asyncpg API reference: Connection.add_listener](https://magicstack.github.io/asyncpg/current/api/index.html) — tier-1 — magicstack.github.io (asyncpg official docs)
- [PgBouncer Features](https://www.pgbouncer.org/features.html) — tier-1 — pgbouncer.org
- [PgBouncer Configuration](https://www.pgbouncer.org/config.html) — tier-1 — pgbouncer.org
- [PgBouncer source: src/janitor.c](https://raw.githubusercontent.com/pgbouncer/pgbouncer/master/src/janitor.c) — tier-1 — github.com (pgbouncer/pgbouncer)
- [PgBouncer source: src/server.c](https://raw.githubusercontent.com/pgbouncer/pgbouncer/master/src/server.c) — tier-1 — github.com (pgbouncer/pgbouncer)
- [Feature: Listen/Notify Support with Transaction Pooling (issue #655)](https://github.com/pgbouncer/pgbouncer/issues/655) — tier-1 — github.com (pgbouncer project issue tracker)
- [PgBouncer LISTEN/NOTIFY in transaction mode](https://www.netdata.cloud/guides/pgbouncer/pgbouncer-listen-notify-transaction-mode/) — tier-3 — netdata.cloud
- [Avoiding pinning an RDS Proxy](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/rds-proxy-pinning.html) — tier-1 — docs.aws.amazon.com
- [Avoiding pinning an RDS Proxy (Aurora User Guide)](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/rds-proxy-pinning.html) — tier-1 — docs.aws.amazon.com
- [RDS Proxy configuration guidelines](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/rds-proxy-best-practices.configuration.html) — tier-1 — docs.aws.amazon.com
- [RDS Proxy connection considerations](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/rds-proxy-connections.html) — tier-1 — docs.aws.amazon.com
- [RDS Proxy concepts and terminology](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/rds-proxy.howitworks.html) — tier-1 — docs.aws.amazon.com
- [RDS Proxy concepts and terminology (Aurora)](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/rds-proxy.howitworks.html) — tier-1 — docs.aws.amazon.com (Amazon Aurora User Guide)
- [Amazon RDS Proxy: quotas and limitations](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/rds-proxy.html) — tier-1 — docs.aws.amazon.com
- [RDS Proxy best practices: application and workload considerations](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/rds-proxy-best-practices.workload-considerations.html) — tier-1 — docs.aws.amazon.com
- [RDS Proxy best practices: common usage scenarios](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/rds-proxy-best-practices.usage-scenarios.html) — tier-1 — docs.aws.amazon.com
- [Working with Amazon RDS Proxy endpoints (RDS)](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/rds-proxy-endpoints.html) — tier-1 — docs.aws.amazon.com (Amazon RDS User Guide)
- [Working with Amazon RDS Proxy endpoints (Aurora)](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/rds-proxy-endpoints.html) — tier-1 — docs.aws.amazon.com (Amazon Aurora User Guide)
- [Monitoring RDS Proxy metrics with Amazon CloudWatch](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/rds-proxy.monitoring.html) — tier-1 — docs.aws.amazon.com
- [Working with RDS Proxy events](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/rds-proxy.events.html) — tier-1 — docs.aws.amazon.com
- [CreateDBProxy API reference](https://docs.aws.amazon.com/AmazonRDS/latest/APIReference/API_CreateDBProxy.html) — tier-1 — docs.aws.amazon.com
- [DBProxy data type (RDS API Reference)](https://docs.aws.amazon.com/AmazonRDS/latest/APIReference/API_DBProxy.html) — tier-1 — docs.aws.amazon.com
- [Amazon RDS Proxy multiplexing support for PostgreSQL Extended Query Protocol](https://aws.amazon.com/blogs/database/amazon-rds-proxy-multiplexing-support-for-postgresql-extended-query-protocol/) — tier-1 — aws.amazon.com (AWS Database Blog)
- [Improving application availability with Amazon RDS Proxy](https://aws.amazon.com/blogs/database/improving-application-availability-with-amazon-rds-proxy/) — tier-1 — aws.amazon.com (AWS Database Blog)
- [Amazon RDS Proxy FAQs](https://aws.amazon.com/rds/proxy/faqs/) — tier-1 — aws.amazon.com
- [Switching a blue/green deployment in Amazon RDS](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/blue-green-deployments-switching.html) — tier-1 — docs.aws.amazon.com (Amazon RDS User Guide)
- [Failing over a Multi-AZ DB instance for Amazon RDS](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Concepts.MultiAZ.Failover.html) — tier-1 — docs.aws.amazon.com
- [Failing over a Multi-AZ DB cluster for Amazon RDS](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/multi-az-db-clusters-concepts-failover.html) — tier-1 — docs.aws.amazon.com
- [High availability for Amazon Aurora](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Concepts.AuroraHighAvailability.html) — tier-1 — docs.aws.amazon.com
- [Fast failover with Amazon Aurora PostgreSQL](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/AuroraPostgreSQL.BestPractices.FastFailover.html) — tier-1 — docs.aws.amazon.com
- [Amazon Aurora endpoint connections](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Aurora.Overview.Endpoints.html) — tier-1 — docs.aws.amazon.com
- [Troubleshoot NAT gateways](https://docs.aws.amazon.com/vpc/latest/userguide/nat-gateway-troubleshooting.html) — tier-1 — docs.aws.amazon.com
- [Update the TCP idle timeout for your Network Load Balancer listener](https://docs.aws.amazon.com/elasticloadbalancing/latest/network/update-idle-timeout.html) — tier-1 — docs.aws.amazon.com
- [AWS Advanced Python Wrapper (README)](https://github.com/aws/aws-advanced-python-wrapper) — tier-1 — github.com/aws
- [AWS Advanced Python Wrapper — Using the Failover Plugin](https://github.com/aws/aws-advanced-python-wrapper/blob/main/docs/using-the-python-wrapper/using-plugins/UsingTheFailoverPlugin.md) — tier-1 — github.com (aws/aws-advanced-python-wrapper official docs)
- [Using the Failover Plugin (AWS Advanced Python Wrapper docs)](https://raw.githubusercontent.com/aws/aws-advanced-python-wrapper/main/docs/using-the-python-wrapper/using-plugins/UsingTheFailoverPlugin.md) — tier-1 — github.com/aws
- [Using the AWS Advanced Python Wrapper](https://github.com/aws/aws-advanced-python-wrapper/blob/main/docs/using-the-python-wrapper/UsingThePythonWrapper.md) — tier-1 — github.com (aws/aws-advanced-python-wrapper official docs)
- [Enhanced Failure Monitoring (Host Monitoring Plugin)](https://raw.githubusercontent.com/aws/aws-advanced-python-wrapper/main/docs/using-the-python-wrapper/using-plugins/UsingTheHostMonitoringPlugin.md) — tier-1 — raw.githubusercontent.com (aws/aws-advanced-python-wrapper official docs)
- [Failover Configuration Guide](https://raw.githubusercontent.com/aws/aws-advanced-python-wrapper/main/docs/using-the-python-wrapper/FailoverConfigurationGuide.md) — tier-1 — raw.githubusercontent.com (aws/aws-advanced-python-wrapper official docs)
- [Session States](https://raw.githubusercontent.com/aws/aws-advanced-python-wrapper/main/docs/using-the-python-wrapper/SessionState.md) — tier-1 — raw.githubusercontent.com (aws/aws-advanced-python-wrapper official docs)
- [wrapper.py — AwsWrapperConnection](https://raw.githubusercontent.com/aws/aws-advanced-python-wrapper/main/aws_advanced_python_wrapper/wrapper.py) — tier-1 — raw.githubusercontent.com (aws/aws-advanced-python-wrapper source)
- [failover_plugin.py](https://raw.githubusercontent.com/aws/aws-advanced-python-wrapper/main/aws_advanced_python_wrapper/failover_plugin.py) — tier-1 — raw.githubusercontent.com (aws/aws-advanced-python-wrapper source)
- [failover_v2_plugin.py](https://raw.githubusercontent.com/aws/aws-advanced-python-wrapper/main/aws_advanced_python_wrapper/failover_v2_plugin.py) — tier-1 — raw.githubusercontent.com (aws/aws-advanced-python-wrapper source)
- [pg_driver_dialect.py](https://raw.githubusercontent.com/aws/aws-advanced-python-wrapper/main/aws_advanced_python_wrapper/pg_driver_dialect.py) — tier-1 — raw.githubusercontent.com (aws/aws-advanced-python-wrapper source)
- [utils/pg_exception_handler.py](https://raw.githubusercontent.com/aws/aws-advanced-python-wrapper/main/aws_advanced_python_wrapper/utils/pg_exception_handler.py) — tier-1 — raw.githubusercontent.com (aws/aws-advanced-python-wrapper source)
- [pep249_methods.py — DbApiMethod](https://raw.githubusercontent.com/aws/aws-advanced-python-wrapper/main/aws_advanced_python_wrapper/pep249_methods.py) — tier-1 — raw.githubusercontent.com (aws/aws-advanced-python-wrapper source)
- [host_monitoring_plugin.py](https://raw.githubusercontent.com/aws/aws-advanced-python-wrapper/main/aws_advanced_python_wrapper/host_monitoring_plugin.py) — tier-1 — raw.githubusercontent.com (aws/aws-advanced-python-wrapper source)
- [host_monitoring_v2_plugin.py](https://raw.githubusercontent.com/aws/aws-advanced-python-wrapper/main/aws_advanced_python_wrapper/host_monitoring_v2_plugin.py) — tier-1 — raw.githubusercontent.com (aws/aws-advanced-python-wrapper source)
- [aurora_connection_tracker_plugin.py](https://raw.githubusercontent.com/aws/aws-advanced-python-wrapper/main/aws_advanced_python_wrapper/aurora_connection_tracker_plugin.py) — tier-1 — raw.githubusercontent.com (aws/aws-advanced-python-wrapper source)
- [aio/wrapper.py — AsyncAwsWrapperConnection](https://raw.githubusercontent.com/aws/aws-advanced-python-wrapper/main/aws_advanced_python_wrapper/aio/wrapper.py) — tier-1 — raw.githubusercontent.com (aws/aws-advanced-python-wrapper source)
- [aio/psycopg.py](https://raw.githubusercontent.com/aws/aws-advanced-python-wrapper/main/aws_advanced_python_wrapper/aio/psycopg.py) — tier-1 — raw.githubusercontent.com (aws/aws-advanced-python-wrapper source)
- [states/session_state_service.py](https://raw.githubusercontent.com/aws/aws-advanced-python-wrapper/main/aws_advanced_python_wrapper/states/session_state_service.py) — tier-1 — raw.githubusercontent.com (aws/aws-advanced-python-wrapper source)
- [CHANGELOG.md](https://raw.githubusercontent.com/aws/aws-advanced-python-wrapper/main/CHANGELOG.md) — tier-1 — raw.githubusercontent.com (aws/aws-advanced-python-wrapper)
- [Issue search: notifies OR LISTEN OR notify](https://github.com/aws/aws-advanced-python-wrapper/issues?q=notifies+OR+LISTEN+OR+notify) — tier-1 — github.com (aws/aws-advanced-python-wrapper issue tracker)
- [About high availability (Cloud SQL for PostgreSQL)](https://docs.cloud.google.com/sql/docs/postgres/high-availability) — tier-1 — docs.cloud.google.com
- [About maintenance on Cloud SQL instances (Cloud SQL for PostgreSQL)](https://docs.cloud.google.com/sql/docs/postgres/maintenance) — tier-1 — docs.cloud.google.com
- [Cloud SQL Managed Connection Pooling overview](https://docs.cloud.google.com/sql/docs/postgres/managed-connection-pooling) — tier-1 — docs.cloud.google.com
- [Reliability in Azure Database for PostgreSQL](https://learn.microsoft.com/en-us/azure/reliability/reliability-postgresql-flexible-server) — tier-1 — learn.microsoft.com
- [Neon: Connection pooling](https://neon.com/docs/connect/connection-pooling) — tier-1 — neon.com
- [Supabase: Connect to your database](https://supabase.com/docs/guides/database/connecting-to-postgres) — tier-1 — supabase.com
- [Supabase Connection Pooler Deprecating Session Mode on Port 6543 on February 28, 2025](https://supabase.com/changelog/32755-supabase-connection-pooler-deprecating-session-mode-on-port-6543-on-february-28-2025) — tier-1 — supabase.com
- [Supavisor FAQ](https://supabase.com/docs/guides/troubleshooting/supavisor-faq-YyP5tI) — tier-1 — supabase.com
- [Supavisor v1.1.2: allow_list and client_heartbeat_interval](https://supabase.com/changelog/20195-supavisor-v1-1-2-allow-list-and-client-heartbeat-interval) — tier-1 — supabase.com
- [Supavisor: Pool Modes](https://supabase.github.io/supavisor/configuration/pool_modes/) — tier-1 — supabase.github.io (Supavisor project documentation)
- [Supavisor tenant configuration](https://supabase.github.io/supavisor/configuration/tenants/) — tier-1 — supabase.github.io
- [Supavisor source: tenants/tenant.ex](https://raw.githubusercontent.com/supabase/supavisor/main/lib/supavisor/tenants/tenant.ex) — tier-1 — github.com (supabase/supavisor)
- [Supavisor source: client_handler.ex](https://raw.githubusercontent.com/supabase/supavisor/main/lib/supavisor/client_handler.ex) — tier-1 — github.com (supabase/supavisor)
- [Supavisor source: db_handler.ex](https://raw.githubusercontent.com/supabase/supavisor/main/lib/supavisor/db_handler.ex) — tier-1 — github.com (supabase/supavisor)
- [Supavisor source: manager.ex](https://raw.githubusercontent.com/supabase/supavisor/main/lib/supavisor/manager.ex) — tier-1 — github.com (supabase/supavisor)
- [PgCat README](https://github.com/postgresml/pgcat) — tier-1 — github.com (postgresml/pgcat project)
- [PgDog: Pub/sub](https://docs.pgdog.dev/features/connection-pooler/pub_sub/) — tier-1 — docs.pgdog.dev
- [PgDog docs: pgdog.toml general settings](https://docs.pgdog.dev/configuration/pgdog.toml/general/) — tier-1 — docs.pgdog.dev
- [PgDog docs: Features](https://docs.pgdog.dev/features/) — tier-1 — docs.pgdog.dev
- [Administration — PgDog documentation](https://docs.pgdog.dev/administration/) — tier-1 — docs.pgdog.dev
- [Configuration overview — PgDog documentation](https://docs.pgdog.dev/configuration/) — tier-1 — docs.pgdog.dev
- [Scaling Postgres LISTEN/NOTIFY](https://pgdog.dev/blog/scaling-postgres-listen-notify) — tier-1 — pgdog.dev (project's official blog)
- [PgDog repository README](https://github.com/pgdogdev/pgdog) — tier-1 — github.com/pgdogdev/pgdog
- [PgDog releases](https://github.com/pgdogdev/pgdog/releases) — tier-1 — github.com/pgdogdev/pgdog
- [PgDog v0.1.59 release notes](https://github.com/pgdogdev/pgdog/releases/tag/v0.1.59) — tier-1 — github.com/pgdogdev/pgdog
- [pub_sub listener.rs source](https://raw.githubusercontent.com/pgdogdev/pgdog/main/pgdog/src/backend/pub_sub/listener.rs) — tier-1 — github.com/pgdogdev/pgdog
- [pgdog/src/backend/pub_sub/listener.rs](https://github.com/pgdogdev/pgdog/blob/main/pgdog/src/backend/pub_sub/listener.rs) — tier-1 — github.com (pgdogdev/pgdog source)
- [Commit history of listener.rs](https://github.com/pgdogdev/pgdog/commits/main/pgdog/src/backend/pub_sub/listener.rs) — tier-1 — github.com (pgdogdev/pgdog)
- [pub_sub client.rs source](https://raw.githubusercontent.com/pgdogdev/pgdog/main/pgdog/src/backend/pub_sub/client.rs) — tier-1 — github.com/pgdogdev/pgdog
- [query_engine pub_sub.rs source](https://raw.githubusercontent.com/pgdogdev/pgdog/main/pgdog/src/frontend/client/query_engine/pub_sub.rs) — tier-1 — github.com/pgdogdev/pgdog
- [query_engine mod.rs source](https://raw.githubusercontent.com/pgdogdev/pgdog/main/pgdog/src/frontend/client/query_engine/mod.rs) — tier-1 — github.com/pgdogdev/pgdog
- [query_engine end_transaction.rs source](https://raw.githubusercontent.com/pgdogdev/pgdog/main/pgdog/src/frontend/client/query_engine/end_transaction.rs) — tier-1 — github.com/pgdogdev/pgdog
- [PgDog source: notify buffer](https://raw.githubusercontent.com/pgdogdev/pgdog/main/pgdog/src/frontend/client/query_engine/notify_buffer.rs) — tier-1 — raw.githubusercontent.com (pgdogdev/pgdog repository)
- [PgDog source: router parser query module (ListenStmt and NotifyStmt routing)](https://raw.githubusercontent.com/pgdogdev/pgdog/main/pgdog/src/frontend/router/parser/query/mod.rs) — tier-1 — raw.githubusercontent.com (pgdogdev/pgdog repository)
- [PgDog source: router parser Command enum](https://raw.githubusercontent.com/pgdogdev/pgdog/main/pgdog/src/frontend/router/parser/command.rs) — tier-1 — raw.githubusercontent.com (pgdogdev/pgdog repository)
- [PgDog source: regex pre-parser](https://raw.githubusercontent.com/pgdogdev/pgdog/main/pgdog/src/frontend/regex_parser.rs) — tier-1 — raw.githubusercontent.com (pgdogdev/pgdog repository)
- [PgDog source: cluster query-parser selection](https://raw.githubusercontent.com/pgdogdev/pgdog/main/pgdog/src/backend/pool/cluster.rs) — tier-1 — raw.githubusercontent.com (pgdogdev/pgdog repository)
- [pgdog/src/backend/pool/connection/mod.rs](https://raw.githubusercontent.com/pgdogdev/pgdog/main/pgdog/src/backend/pool/connection/mod.rs) — tier-1 — raw.githubusercontent.com (pgdogdev/pgdog source)
- [pgdog/src/backend/pool/shard/mod.rs](https://raw.githubusercontent.com/pgdogdev/pgdog/main/pgdog/src/backend/pool/shard/mod.rs) — tier-1 — raw.githubusercontent.com (pgdogdev/pgdog source)
- [pgdog/src/backend/databases.rs](https://raw.githubusercontent.com/pgdogdev/pgdog/main/pgdog/src/backend/databases.rs) — tier-1 — raw.githubusercontent.com (pgdogdev/pgdog source)
- [pgdog/src/frontend/client/mod.rs](https://raw.githubusercontent.com/pgdogdev/pgdog/main/pgdog/src/frontend/client/mod.rs) — tier-1 — raw.githubusercontent.com (pgdogdev/pgdog source)
- [pgdog/src/frontend/listener.rs](https://raw.githubusercontent.com/pgdogdev/pgdog/main/pgdog/src/frontend/listener.rs) — tier-1 — raw.githubusercontent.com (pgdogdev/pgdog source)
- [pgdog/src/util.rs (safe_sleep, armable)](https://raw.githubusercontent.com/pgdogdev/pgdog/main/pgdog/src/util.rs) — tier-1 — raw.githubusercontent.com (pgdogdev/pgdog source)
- [pgdog/src/net/messages/error_response.rs](https://raw.githubusercontent.com/pgdogdev/pgdog/main/pgdog/src/net/messages/error_response.rs) — tier-1 — raw.githubusercontent.com (pgdogdev/pgdog source)
- [PgDog integration tests: NOTIFY](https://raw.githubusercontent.com/pgdogdev/pgdog/main/integration/rust/tests/integration/notify.rs) — tier-1 — raw.githubusercontent.com (pgdogdev/pgdog repository)
- [Issue #294: Support LISTEN/NOTIFY in transaction mode](https://github.com/pgdogdev/pgdog/issues/294) — tier-1 — github.com/pgdogdev/pgdog
- [PR #301: LISTEN/NOTIFY in transaction mode](https://github.com/pgdogdev/pgdog/pull/301) — tier-1 — github.com/pgdogdev/pgdog
- [Listener survive reload (PR #302)](https://github.com/pgdogdev/pgdog/pull/302) — tier-1 — github.com (pgdogdev/pgdog)
- [Issue #449: Buffer NOTIFY until transaction end](https://github.com/pgdogdev/pgdog/issues/449) — tier-1 — github.com/pgdogdev/pgdog
- [PR #800: Forward LISTEN/NOTIFY/UNLISTEN to backend in session mode](https://github.com/pgdogdev/pgdog/pull/800) — tier-1 — github.com/pgdogdev/pgdog
- [PR #925: Re-initialize pub/sub channel after failover](https://github.com/pgdogdev/pgdog/pull/925) — tier-1 — github.com/pgdogdev/pgdog
- [PR #1078: Enable parser for LISTEN/NOTIFY/UNLISTEN specifically](https://github.com/pgdogdev/pgdog/pull/1078) — tier-1 — github.com/pgdogdev/pgdog
- [PR #1079: Listen/notify stats; fix race condition in LISTEN](https://github.com/pgdogdev/pgdog/pull/1079) — tier-1 — github.com/pgdogdev/pgdog
- [PR #1285: Scope pub/sub channels to their pool](https://github.com/pgdogdev/pgdog/pull/1285) — tier-1 — github.com/pgdogdev/pgdog
- [Issue #1462: client_idle_timeout is global-only for long-idle LISTEN clients](https://github.com/pgdogdev/pgdog/issues/1462) — tier-1 — github.com/pgdogdev/pgdog
- [PR #1467: Per-user/per-database override for client_idle_timeout](https://github.com/pgdogdev/pgdog/pull/1467) — tier-1 — github.com/pgdogdev/pgdog
- [Issue #1512: Transaction-pool pipeline LISTEN sends ReadyForQuery before Sync](https://github.com/pgdogdev/pgdog/issues/1512) — tier-1 — github.com/pgdogdev/pgdog
- [PR #1557: Fix premature ReadyForQuery in pub/sub pipelines](https://github.com/pgdogdev/pgdog/pull/1557) — tier-1 — github.com/pgdogdev/pgdog
- [Postgrex.Notifications](https://postgrex.hexdocs.pm/Postgrex.Notifications.html) — tier-1 — hexdocs.pm
- [PostgREST documentation: Listener](https://docs.postgrest.org/en/stable/references/listener.html) — tier-1 — docs.postgrest.org
- [Commanded EventStore init.ex (notify_events trigger)](https://raw.githubusercontent.com/commanded/eventstore/master/lib/event_store/sql/init.ex) — tier-1 — github.com/commanded/eventstore
- [Commanded EventStore notifications listener.ex](https://raw.githubusercontent.com/commanded/eventstore/master/lib/event_store/notifications/listener.ex) — tier-1 — github.com/commanded/eventstore
- [Commanded EventStore notifications publisher.ex](https://raw.githubusercontent.com/commanded/eventstore/master/lib/event_store/notifications/publisher.ex) — tier-1 — github.com/commanded/eventstore
- [Oban Postgres notifier (postgres.ex)](https://raw.githubusercontent.com/oban-bg/oban/main/lib/oban/notifiers/postgres.ex) — tier-1 — github.com/oban-bg/oban
- [Oban Notifier (notifier.ex)](https://raw.githubusercontent.com/oban-bg/oban/main/lib/oban/notifier.ex) — tier-1 — github.com/oban-bg/oban
- [Oban Stager (stager.ex)](https://raw.githubusercontent.com/oban-bg/oban/main/lib/oban/stager.ex) — tier-1 — github.com/oban-bg/oban
- [River internal notifier (notifier.go)](https://raw.githubusercontent.com/riverqueue/river/master/internal/notifier/notifier.go) — tier-1 — github.com/riverqueue/river
- [pgmq extension SQL (enable_notify_insert)](https://raw.githubusercontent.com/pgmq/pgmq/main/pgmq-extension/sql/pgmq.sql) — tier-1 — github.com/pgmq/pgmq
- [oban-py source: _notifier.py](https://raw.githubusercontent.com/oban-bg/oban-py/main/oban/_notifier.py) — tier-2 — raw.githubusercontent.com (oban-bg/oban-py repository)
- [oban-py repository](https://github.com/oban-bg/oban-py/tree/main/oban) — tier-2 — github.com (oban-bg)
- [Litestar source: channels psycopg backend](https://raw.githubusercontent.com/litestar-org/litestar/main/litestar/channels/backends/psycopg.py) — tier-2 — raw.githubusercontent.com (litestar-org/litestar repository)
- [Procrastinate source: psycopg_connector.py](https://raw.githubusercontent.com/procrastinate-org/procrastinate/main/procrastinate/psycopg_connector.py) — tier-2 — raw.githubusercontent.com (procrastinate-org/procrastinate repository)
- [Procrastinate source: worker.py](https://raw.githubusercontent.com/procrastinate-org/procrastinate/main/procrastinate/worker.py) — tier-2 — raw.githubusercontent.com (procrastinate-org/procrastinate repository)
- [Procrastinate source: connector.py](https://raw.githubusercontent.com/procrastinate-org/procrastinate/main/procrastinate/connector.py) — tier-2 — raw.githubusercontent.com (procrastinate-org/procrastinate repository)
- [pgqueuer source: psycopg driver](https://raw.githubusercontent.com/janbjorge/pgqueuer/main/pgqueuer/adapters/drivers/psycopg.py) — tier-3 — raw.githubusercontent.com (janbjorge/pgqueuer repository)
- [The Notifier Pattern for Applications That Use Postgres](https://brandur.org/notifier) — tier-2 — brandur.org
- [Reconnecting your application after a Postgres failover](https://www.citusdata.com/blog/2021/02/12/reconnecting-your-app-after-a-postgres-failover/) — tier-2 — citusdata.com
- [Analyzing the Limits of Connection Scalability in Postgres](https://www.citusdata.com/blog/2020/10/08/analyzing-connection-scalability/) — tier-2 — citusdata.com
- [A look at Postgres 14: Performance and Monitoring Improvements](https://pganalyze.com/blog/postgres-14-performance-monitoring) — tier-2 — pganalyze.com
- [Postgres LISTEN/NOTIFY does not scale](https://www.recall.ai/blog/postgres-listen-notify-does-not-scale) — tier-3 — recall.ai (company engineering blog)
