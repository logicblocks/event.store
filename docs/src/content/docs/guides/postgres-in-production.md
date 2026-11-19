---
title: Running PostgreSQL in production
description: Keep writes fast and stop one slow writer from blocking the rest when running the event store on PostgreSQL.
---

The [PostgreSQL guide](/event.store/guides/postgres/) covers creating the
tables and configuring the adapters. This guide covers how to set up and run
the event store so that writes stay fast as your tables grow, and what to
look at when they don't.

Most of it comes down to the write lock. Every write holds a lock that other
writes wait for, so anything that makes one write slow makes every write that
shares its lock slow too.

## How a write works

Every `publish`, including each save of a subscriber's state, runs as one
transaction in `PostgresEventStorageAdapter`:

1. Take a transaction-scoped advisory lock with `pg_advisory_xact_lock`.
2. Read the latest event in the stream, to find its position.
3. Check the write condition against that event.
4. Insert the events with `INSERT … RETURNING *`.
5. Commit, which releases the lock.

The read in step 2 is:

```sql
SELECT * FROM events
WHERE category = $1 AND stream = $2
ORDER BY sequence_number DESC
LIMIT $3;
```

The lock is held from step 1 until the commit. Anything slow inside that
transaction is time that every other writer on the same lock spends waiting.
That includes a slow query, a busy event loop between `await`s, and a slow
commit. Reads take no lock, so they keep working while writes queue.

## Keep the schema in step with the library

The library does not create or migrate your tables. You copy the SQL from
[`sql/`](https://github.com/logicblocks/event.store/tree/main/sql) into your
own migrations, as described in
[Creating the schema](/event.store/guides/postgres/#creating-the-schema). That
copy does not change when you upgrade the package.

### Apply every index

Check that your events table has every index in
[`create_events_indices.sql`](https://github.com/logicblocks/event.store/blob/main/sql/create_events_indices.sql)
for the version you run:

```sql
SELECT indexname, indexdef
FROM pg_indexes
WHERE tablename = 'events';
```

The most important is `events_category_stream_sequence_number_index` on
`(category, stream, sequence_number DESC)`. It turns the read before every
write into a one-row index lookup, however long the stream is. It was added in
version 0.1.10a17, so tables created from earlier copies of the SQL don't have
it.

### Check the SQL on every upgrade

Upgrades include automated dependency bumps. Before you upgrade, compare the
SQL between the version you run and the one you are moving to:

```bash
git diff v0.1.11 v0.1.12a6 -- sql/
```

Add anything new to your migrations.

### Add new columns carefully

The adapter reads events with `SELECT *` and inserts them with
`RETURNING *`, then loads each row into a `StoredEvent`. A column it doesn't
know about makes reads and writes fail, and so does a column it expects that
is missing. For example, version 0.1.12a1 added a `metadata` column to the
events table, so versions before it fail against a table that has the column,
and versions from it onwards fail against a table that doesn't.

Plan these upgrades so that no version of your service runs against a schema
it doesn't expect. One way is to stop all instances, run the migration, then
start the new version. With rolling deploys, add the column in a separate
step from the upgrade, with a temporary workaround in the old version that
ignores the new column.

### Build indices without blocking writes

To add an index to a live events table, use `CREATE INDEX CONCURRENTLY`. A
plain `CREATE INDEX` blocks every insert until the build finishes, which can
take minutes on a large table.

```sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS events_category_stream_sequence_number_index
    ON events (category, stream, sequence_number DESC);
```

- `CONCURRENTLY` can't run inside a transaction. Most migration tools run
  each migration in one, so check how yours lets you opt out. In Alembic, use
  `op.get_context().autocommit_block()`.
- The build waits for every transaction that started before it. Sessions
  left `idle in transaction` stop it finishing.
- Pausing your subscribers during the build takes load off the write lock.

## Choose the serialisation guarantee

The serialisation guarantee sets which writes share a lock. The default is
`LOG`, which puts every write to the table behind one lock:

| Guarantee | Lock name | Writes that share it |
| --- | --- | --- |
| `LOG` | `events` | Every write to the table |
| `CATEGORY` | `events.<category>` | Every write to one category |
| `STREAM` | `events.<category>.<stream>` | Every write to one stream |

The lock also controls the order in which events are committed, and
subscribers rely on that order. A subscriber remembers the last sequence
number it processed and asks for events after it. If two writes in what it
reads commit out of order, it can move past an event that commits a moment
later and never process it. The
[concurrency guide](/event.store/guides/concurrency/#serialisation-guarantees)
explains this in more detail.

So choose the narrowest guarantee that is still at least as wide as anything
your subscribers read:

- If every subscriber reads a single category, use `CATEGORY`.
- If any subscriber reads the whole log, you need `LOG`.
- Only use `STREAM` if nothing reads a whole category or the log.

Every process that writes to a table must use the same guarantee. Writes
under different guarantees take different locks, so they don't wait for each
other.

### Switching guarantee

During a rolling deploy, old instances take one lock and new instances take
another, so for a few minutes writes are not serialised and subscribers can
skip events. Don't run both at once: stop every instance before starting the
new version, or pause your subscribers until the deploy has finished.

## Keep subscriber state small

A subscriber made with `make_event_store_subscriber` saves its progress by
publishing a `state-changed` event to the `default` stream of its state
category, as described in
[Track and resume progress](/event.store/guides/event-processing/#track-and-resume-progress).
Each save is an ordinary write, with the same lock and the same read before
the insert.

Only the latest state event is ever read, and nothing removes the older ones,
so the state stream grows for as long as the subscriber runs. A subscriber
that polls every 200 milliseconds and finds a few events each time saves its
state about five times a second, and can write millions of state events
within weeks.

Check how many state events a subscriber has saved. This uses
`events_category_stream_position_index`, so it is cheap to run:

```sql
SELECT max(position) + 1 AS state_events
FROM events
WHERE category = 'welcome-emails-state'
  AND stream = 'default';
```

To reduce the load these saves put on your writes:

- Make sure you have every index, so that each save stays fast however long
  the stream gets.
- Keep subscriber state in its own table, through a separate
  `PostgresEventStorageAdapter` with its own `TableSettings`. Lock names
  include the table name, so state saves no longer share a lock with your
  other writes, and the events table stays smaller. Create the second table
  and its indices from the same SQL, with the table name changed.
- Save less often, by raising `subscriber_state_persistence_interval` or
  setting `save_state_after_consumption=False`. More events are then processed
  again after a restart.

## Keep slow writers apart

Writes in the same lock scope wait for each other, so keep the writes that
callers wait for, such as HTTP request handlers, out of the categories that
background work writes to.

For example, a webhook handler that writes each item of a large batch to an
existing stream needs a read and an insert under the lock for every item,
and the caller may time out and retry the whole batch. Instead:

1. Store the raw payload as one event in a new stream, in a category of its
   own. A new stream has no events to read, so the write is quick.
2. Respond straight away.
3. Use a subscriber on that category to write the individual events where
   they belong.

To keep the events a caller sees in the same stream as before, the subscriber
can copy each event on with the `stream_is_empty()` write condition, so a
repeated copy changes nothing. Reads then need to fall back to the first
stream until the copy has happened.

### Keep transactions short

The lock is held across several `await`s. If your event loop is busy between
them, for example validating large payloads or running many pollers, the
transaction holding the lock waits for its turn. PostgreSQL shows that
session as `idle in transaction`, waiting on `ClientRead`, while every other
writer waits for it.

- Avoid heavy CPU work on the event loop that runs your writes.
- Run pollers for busy subscribers in a separate process from your request
  handlers.
- Give the process enough CPU.

## Make event processors safe against bad events

When `process_event` raises, the event is not marked as processed, and the
subscriber tries the same event again on its next poll. There is no
dead-letter queue, so an event that can never be processed stops the
subscriber for good.

- Log and skip events you can't parse or don't recognise, so the subscriber
  keeps moving.
- Only raise for errors that trying again can fix, such as a database
  connection failure.
- Don't write events of an unexpected type into a category that a strict
  processor reads. Give them their own category.

See [Handling errors](/event.store/guides/services-and-error-handling/#handling-errors)
for how the polling service responds to errors.

## Monitor your writes

Measure how long writes take, so you can see lock contention building up
before it becomes an outage. One way is to subclass the adapter and time
`save`, tagged by category:

```python
import time

from logicblocks.event.store import PostgresEventStorageAdapter


class TimedPostgresEventStorageAdapter(PostgresEventStorageAdapter):
    async def save(self, *, target, **kwargs):
        started = time.perf_counter()
        try:
            return await super().save(target=target, **kwargs)
        finally:
            record_write_duration(
                category=target.category,
                seconds=time.perf_counter() - started,
            )
```

Replace `record_write_duration` with a call to your metrics client. Record a
distribution rather than an average, and alert on the 95th or 99th
percentile. Tagging each write with the part of your service that made it
makes it easier to see who is holding the lock.

Also run your tests against a real PostgreSQL database. The in-memory adapter
has no advisory locks shared between processes and no query planner, so it
can't show lock contention or a slow plan.

## Diagnosing slow writes

If writes slow down or time out while reads keep working, look at the advisory
locks first. Sessions holding a lock show `granted = true`, and sessions
waiting for one show `false`:

```sql
SELECT l.granted, a.pid, a.state, a.wait_event,
       now() - a.xact_start AS transaction_age,
       left(a.query, 80) AS query
FROM pg_locks l
JOIN pg_stat_activity a USING (pid)
WHERE l.locktype = 'advisory'
ORDER BY l.granted DESC, a.xact_start;
```

Many waiters behind one holder that is running a query means that query is
slow. A holder that is `idle in transaction` means your process is slow
between the queries in its transaction.

### Check the plan your application gets

psycopg prepares a statement once it has run it five times on a connection.
After a few runs, PostgreSQL may switch a prepared statement to a generic plan
that ignores the actual values. A generic plan suits most streams, which are
short, and can be very slow for the few that are long.

Running `EXPLAIN` on the query with literal values gives a different plan, so
it can look fast when it isn't. Check the generic plan instead. `EXPLAIN`
without `ANALYZE` only plans the query, so this is safe to run in production:

```sql
SET plan_cache_mode = force_generic_plan;
PREPARE read_last(text, text, int) AS
  SELECT * FROM events
  WHERE category = $1 AND stream = $2
  ORDER BY sequence_number DESC
  LIMIT $3;
EXPLAIN EXECUTE read_last('welcome-emails-state', 'default', 1);
```

A `Sort` node in the plan means you are missing
`events_category_stream_sequence_number_index`.

The index is the fix. If you also want to stop psycopg preparing statements,
pass `prepare_threshold=None` to the connections in your pool:

```python
from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

pool = AsyncConnectionPool[AsyncConnection](
    connection_string,
    kwargs={"prepare_threshold": None},
    open=False,
)
```

## Common pitfalls

**A missing index makes long streams slow to write to.** Without
`events_category_stream_sequence_number_index`, the read before each write
can sort every event in the stream. Combined with the `LOG` guarantee and a
subscriber state stream of millions of events, each state save holds the
only write lock for minutes and every other write in the service queues
behind it. It gets slower gradually as the stream grows, then stops writes
altogether once saves take longer than the queue behind them needs to clear.
See [Apply every index](#apply-every-index).

**Upgrading the package without updating the schema.** New indices and
columns only reach your database through your own migrations. See
[Check the SQL on every upgrade](#check-the-sql-on-every-upgrade).

**Leaving the default `LOG` guarantee without deciding to.** Every write to
the table shares one lock, so the slowest writer sets the pace for all of
them. See [Choose the serialisation guarantee](#choose-the-serialisation-guarantee).

**Using `STREAM` with category subscribers.** Writes to different streams in
the same category can commit out of order, and subscribers skip events
without any error.

**Switching guarantee during a rolling deploy.** Old and new instances take
different locks, so writes aren't serialised until the deploy finishes. See
[Switching guarantee](#switching-guarantee).

**Testing the query plan with literal values.** `EXPLAIN` with literals gives
a custom plan, which can be fast when the generic plan your application uses
is slow. See [Check the plan your application gets](#check-the-plan-your-application-gets).

**Restarting the database to fix slow writes.** New connections start with
custom plans, so a slow generic plan can seem fixed for a while, then return
once the connections switch to it.

**Making the connection pool bigger.** Extra connections only join the queue
for the same lock, so they don't help with lock contention.

**Retrying an interrupted concurrent index build.** An interrupted
`CREATE INDEX CONCURRENTLY` leaves an `INVALID` index behind, and
`IF NOT EXISTS` then skips it without an error. Drop it with
`DROP INDEX CONCURRENTLY` before trying again.

**Raising on events a processor doesn't recognise.** The subscriber retries
the same event forever and processes nothing after it. See
[Make event processors safe against bad events](#make-event-processors-safe-against-bad-events).

**Only testing against the in-memory adapter.** Lock contention, query plans
and anything that depends on the database's behaviour only show up against
PostgreSQL.
