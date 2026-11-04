---
title: Using PostgreSQL
description: Run the event store and projection store on PostgreSQL.
---

The library includes PostgreSQL adapters for the event store, the projection
store and the distributed event broker. They use
[psycopg](https://www.psycopg.org/psycopg3/) 3 and its connection pool,
`psycopg_pool`. This guide covers creating the tables, connecting, and
configuring each adapter.

## Creating the schema

The SQL that creates the tables is in the
[`sql/`](https://github.com/logicblocks/event.store/tree/main/sql) directory of
the repository. These files are not included in the Python package, and the
library has no helper to create or migrate tables. Copy the files you need
into your own migration tooling and apply them there.

Which files you need depends on the features you use:

| Feature | Files |
| --- | --- |
| Event store | [`create_events_table.sql`](https://github.com/logicblocks/event.store/blob/main/sql/create_events_table.sql), [`create_events_indices.sql`](https://github.com/logicblocks/event.store/blob/main/sql/create_events_indices.sql) |
| Projection store | [`create_projections_table.sql`](https://github.com/logicblocks/event.store/blob/main/sql/create_projections_table.sql), [`create_projections_indices.sql`](https://github.com/logicblocks/event.store/blob/main/sql/create_projections_indices.sql) |
| Distributed event broker | [`create_subscribers_table.sql`](https://github.com/logicblocks/event.store/blob/main/sql/create_subscribers_table.sql), [`create_subscribers_indices.sql`](https://github.com/logicblocks/event.store/blob/main/sql/create_subscribers_indices.sql), [`create_subscriptions_table.sql`](https://github.com/logicblocks/event.store/blob/main/sql/create_subscriptions_table.sql), [`create_subscriptions_indices.sql`](https://github.com/logicblocks/event.store/blob/main/sql/create_subscriptions_indices.sql) |
| `Similarity` query function | [`enable_pg_trgm_extension.sql`](https://github.com/logicblocks/event.store/blob/main/sql/enable_pg_trgm_extension.sql) |

Apply each table file before its indices file. The files use plain
`CREATE TABLE` and `CREATE INDEX` statements, so apply each one only once.

The distributed event broker also reads events through the event store, so it
needs the events table too. The singleton event broker only needs the events
table.

[`Similarity`](/event.store/reference/query/#logicblocks.event.query.Similarity)
scores text using the `similarity` function from PostgreSQL's `pg_trgm`
extension. You only need the extension if you sort projection searches by
`Similarity`.

### Table names

By default the adapters use tables named `events`, `projections`,
`subscribers` and `subscriptions`, which match the SQL files. The event and
projection storage adapters take a
[`TableSettings`](/event.store/reference/persistence/#logicblocks.event.persistence.postgres.TableSettings)
to use a different table. If you change a name, change it in the SQL you apply
as well, including in the index definitions.

`make_event_broker` always uses the default `subscribers` and `subscriptions`
table names.

## Connecting

Both storage adapters take a `connection_source`, which is either a
[`ConnectionSettings`](/event.store/reference/persistence/#logicblocks.event.persistence.postgres.ConnectionSettings)
or a `psycopg_pool.AsyncConnectionPool`. The
[`ConnectionSource`](/event.store/reference/persistence/#logicblocks.event.persistence.postgres.ConnectionSource)
type covers both.

### From connection settings

When you pass `ConnectionSettings`, the adapter creates its own connection pool
with psycopg's default pool settings. You must call `open()` on the adapter
before using it and `close()` when you are done. Using the adapter before
opening it raises `psycopg_pool.PoolClosed`.

```python
import asyncio

from logicblocks.event.persistence.postgres import ConnectionSettings
from logicblocks.event.store import EventStore, PostgresEventStorageAdapter
from logicblocks.event.types import NewEvent

connection_settings = ConnectionSettings(
    host="localhost",
    port=5432,
    dbname="event-store",
    user="admin",
    password="super-secret",
)


async def main():
    adapter = PostgresEventStorageAdapter(connection_source=connection_settings)
    await adapter.open()

    try:
        store = EventStore(adapter)
        stream = store.stream(category="profiles", stream="joe.bloggs")

        await stream.publish(
            events=[
                NewEvent(
                    name="profile-created",
                    payload={"name": "Joe Bloggs"},
                    metadata=None,
                )
            ]
        )

        latest = await stream.latest()
        print(latest.name, latest.payload)
        # profile-created {'name': 'Joe Bloggs'}
    finally:
        await adapter.close()


asyncio.run(main())
```

The `repr` of `ConnectionSettings` masks the password, so it is safe to log.

:::caution
`ConnectionSettings` builds a `postgresql://` URI without URL-encoding the
user or password. Credentials that contain characters such as `@`, `/` or `%`
produce a URI that fails to parse or points at the wrong host. To avoid this
for the storage adapters, create the pool yourself from a connection string
built with `psycopg.conninfo.make_conninfo`. The Postgres lock manager, which
the distributed event broker uses, only accepts `ConnectionSettings`.
:::

### From a connection pool

When you pass an `AsyncConnectionPool`, the adapter uses it as it is. You open
and close the pool, and the adapter's `open()` and `close()` do nothing. This
lets several adapters share one pool, and lets you choose the pool's size and
other options.

This example shares one pool between an event store and a projection store. It
uses a projector like the one in the
[basic example](/event.store/getting-started/#basic-example):

```python
import asyncio

from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from logicblocks.event.persistence.postgres import ConnectionSettings
from logicblocks.event.projection import (
    PostgresProjectionStorageAdapter,
    ProjectionStore,
    Projector,
)
from logicblocks.event.store import EventStore, PostgresEventStorageAdapter
from logicblocks.event.types import JsonValue, NewEvent, StreamIdentifier

connection_settings = ConnectionSettings(
    host="localhost",
    port=5432,
    dbname="event-store",
    user="admin",
    password="super-secret",
)


class ProfileProjector(Projector[StreamIdentifier, dict[str, str]]):
    name = "profile"

    def initial_state_factory(self) -> dict[str, str]:
        return {}

    def initial_metadata_factory(self) -> JsonValue:
        return {}

    def id_factory(self, state, source: StreamIdentifier) -> str:
        return source.stream

    def profile_created(self, state, event):
        state["name"] = event.payload["name"]
        state["email"] = event.payload["email"]
        return state


async def main():
    async with AsyncConnectionPool[AsyncConnection](
        connection_settings.to_connection_string(), open=False
    ) as pool:
        event_store = EventStore(
            PostgresEventStorageAdapter(connection_source=pool)
        )
        projection_store = ProjectionStore(
            PostgresProjectionStorageAdapter(connection_source=pool)
        )

        stream = event_store.stream(category="profiles", stream="jane.doe")
        await stream.publish(
            events=[
                NewEvent(
                    name="profile-created",
                    payload={"name": "Jane Doe", "email": "jane@example.com"},
                    metadata=None,
                )
            ]
        )

        projection = await ProfileProjector().project(source=stream)
        await projection_store.save(projection=projection)

        loaded = await projection_store.locate(
            source=stream.identifier, name="profile"
        )
        print(loaded.id, loaded.state)
        # jane.doe {'name': 'Jane Doe', 'email': 'jane@example.com'}


asyncio.run(main())
```

`async with` opens the pool on entry and closes it on exit.

## The event storage adapter

[`PostgresEventStorageAdapter`](/event.store/reference/store/#logicblocks.event.store.PostgresEventStorageAdapter)
stores events in the events table. Pass it to
[`EventStore`](/event.store/reference/store/#logicblocks.event.store.EventStore)
as you would the in-memory adapter. Apart from `connection_source`, its main
options are:

- `serialisation_guarantee`: an
  [`EventSerialisationGuarantee`](/event.store/reference/store/#logicblocks.event.store.adapters.EventSerialisationGuarantee),
  which defaults to `EventSerialisationGuarantee.LOG`;
- `query_settings`: a
  [`PostgresQuerySettings`](/event.store/reference/store/#logicblocks.event.store.adapters.PostgresQuerySettings),
  whose `scan_query_page_size` sets how many events each scan query fetches.
  It defaults to 100;
- `table_settings`: a `TableSettings`, which defaults to the `events` table.

```python
from logicblocks.event.persistence.postgres import (
    ConnectionSettings,
    TableSettings,
)
from logicblocks.event.store import PostgresEventStorageAdapter
from logicblocks.event.store.adapters import (
    EventSerialisationGuarantee,
    PostgresQuerySettings,
)

connection_settings = ConnectionSettings(
    host="localhost",
    port=5432,
    dbname="event-store",
    user="admin",
    password="super-secret",
)

adapter = PostgresEventStorageAdapter(
    connection_source=connection_settings,
    serialisation_guarantee=EventSerialisationGuarantee.STREAM,
    query_settings=PostgresQuerySettings(scan_query_page_size=500),
    table_settings=TableSettings(table_name="account_events"),
)
```

### Serialisation guarantee

Before each write, the adapter takes a transaction-scoped PostgreSQL advisory
lock, then checks any write conditions and inserts the events. The
serialisation guarantee sets the scope of that lock:

- `LOG` serialises all writes to the table;
- `CATEGORY` serialises writes to the same category;
- `STREAM` serialises writes to the same stream.

Lock names include the table name, so adapters on different tables do not
block each other. Every process that writes to a table should use the same
guarantee, because writes under different guarantees take different locks and
are not serialised against each other. The
[concurrency guide](/event.store/guides/concurrency/) explains the trade-offs.

### Scanning

Scans read events in pages ordered by sequence number, one query per page. A
long-running scan can therefore yield events that were saved after it started.

## The projection storage adapter

[`PostgresProjectionStorageAdapter`](/event.store/reference/projection/#logicblocks.event.projection.PostgresProjectionStorageAdapter)
stores projections in the projections table. Pass it to
[`ProjectionStore`](/event.store/reference/projection/#logicblocks.event.projection.ProjectionStore),
as in the [connection pool example](#from-a-connection-pool). It takes a
`connection_source` and, optionally, a `table_settings` that defaults to the
`projections` table:

```python
from logicblocks.event.persistence.postgres import (
    ConnectionSettings,
    TableSettings,
)
from logicblocks.event.projection import (
    PostgresProjectionStorageAdapter,
    ProjectionStore,
)

connection_settings = ConnectionSettings(
    host="localhost",
    port=5432,
    dbname="event-store",
    user="admin",
    password="super-secret",
)

projection_store = ProjectionStore(
    PostgresProjectionStorageAdapter(
        connection_source=connection_settings,
        table_settings=TableSettings(table_name="account_projections"),
    )
)
```

As with the event storage adapter, call `open()` and `close()` on the adapter
when you pass `ConnectionSettings`.

The adapter stores each projection's state, metadata and source as `jsonb`.
Saving inserts the projection or, if one with the same name and id exists,
replaces its state and metadata. Only projections whose source is a log,
category or stream identifier can be read back.

See [the projection store guide](/event.store/guides/projection-store/) for
saving, loading and searching projections.

## Event processing

The event broker can keep its state in PostgreSQL. Pass
`EventBrokerStorageType.Postgres` to
[`make_event_broker`](/event.store/reference/processing/#logicblocks.event.processing.make_event_broker)
with both a `ConnectionSettings` and an open `AsyncConnectionPool`. Here,
`connection_settings` and `pool` are as in the
[connection pool example](#from-a-connection-pool):

```python
from logicblocks.event.processing import (
    DistributedEventBrokerSettings,
    EventBrokerStorageType,
    EventBrokerType,
    make_event_broker,
)

event_broker = make_event_broker(
    node_id="node-1",
    broker_type=EventBrokerType.Distributed,
    storage_type=EventBrokerStorageType.Postgres,
    connection_settings=connection_settings,
    connection_pool=pool,
    settings=DistributedEventBrokerSettings(),
)
```

The distributed broker:

- stores subscribers and subscriptions in the `subscribers` and
  `subscriptions` tables, using the pool;
- reads events through a `PostgresEventStorageAdapter` on the pool, unless
  you pass your own `adapter`;
- uses a
  [`PostgresLockManager`](/event.store/reference/processing/#logicblocks.event.processing.PostgresLockManager)
  to choose which node coordinates the subscriber groups. The lock manager
  opens its own connections from `ConnectionSettings` rather than using the
  pool, and holds a PostgreSQL advisory lock for the duration of each
  connection's transaction.

The singleton broker with Postgres storage keeps no state in the database. It
reads events through a `PostgresEventStorageAdapter` on the pool, unless you
pass your own `adapter`. It requires `connection_settings` but does not use
it.

See [the event processing guide](/event.store/guides/event-processing/) for
subscribers, event processors and running the broker.
