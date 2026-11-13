---
title: Processing events asynchronously
description: Consume events in the background with an event broker, subscribers and event processors, and keep projections up to date.
---

The `logicblocks.event.processing` package runs work in the background as
events arrive. It has four parts:

- an **event processor** handles one event at a time;
- a **subscriber** asks for an event source, such as a category or the log,
  and passes its events to a processor. It records how far it has got in a
  consumer state store;
- an **event broker** decides which subscriber consumes which event source,
  and hands each source to its subscriber;
- **services** run the broker and poll the subscribers until you stop them.

The broker only allocates event sources. It does not read events itself.
Each subscriber reads events when its `consume_all` method is called, so you
need to call it repeatedly, usually with a `PollingService`.

## Write an event processor

Subclass
[`EventProcessor`](/event.store/reference/processing/#logicblocks.event.processing.EventProcessor)
and implement `process_event`. This processor collects the email address
from each `profile-created` event:

```python
from logicblocks.event.processing import EventProcessor
from logicblocks.event.types import StoredEvent


class WelcomeEmailProcessor(EventProcessor[StoredEvent]):
    def __init__(self):
        self.sent_to: list[str] = []

    async def process_event(self, event: StoredEvent) -> None:
        if event.name == "profile-created":
            self.sent_to.append(event.payload["email"])
```

The subscriber receives every event in its source, so the processor ignores
the events it doesn't handle. Each event counts as processed once
`process_event` returns. If `process_event` raises, the exception is logged
and re-raised, and the event is not marked as processed.

## Run a processor in the background

This example runs the processor against the `profiles` category, using
in-memory storage:

```python
import asyncio
from datetime import timedelta

from logicblocks.event.processing import (
    CallableService,
    EventBrokerStorageType,
    EventBrokerType,
    EventProcessor,
    PollingService,
    ServiceManager,
    SingletonEventBrokerSettings,
    make_event_broker,
    make_event_store_subscriber,
)
from logicblocks.event.store import EventStore, adapters
from logicblocks.event.types import CategoryIdentifier, NewEvent, StoredEvent


class WelcomeEmailProcessor(EventProcessor[StoredEvent]):
    def __init__(self):
        self.sent_to: list[str] = []

    async def process_event(self, event: StoredEvent) -> None:
        if event.name == "profile-created":
            self.sent_to.append(event.payload["email"])


async def main():
    adapter = adapters.InMemoryEventStorageAdapter()
    event_store = EventStore(adapter)

    processor = WelcomeEmailProcessor()
    subscriber = make_event_store_subscriber(
        subscriber_group="welcome-emails",
        subscription_request=CategoryIdentifier(category="profiles"),
        subscriber_state_category=event_store.category(
            category="welcome-emails-state"
        ),
        event_processor=processor,
    )

    event_broker = make_event_broker(
        node_id="node-1",
        broker_type=EventBrokerType.Singleton,
        storage_type=EventBrokerStorageType.InMemory,
        settings=SingletonEventBrokerSettings(),
        adapter=adapter,
    )
    await event_broker.register(subscriber)

    service_manager = ServiceManager()
    service_manager.register(event_broker)
    service_manager.register(
        PollingService(
            service=CallableService(subscriber.consume_all),
            poll_interval=timedelta(milliseconds=200),
        )
    )

    await service_manager.start()
    try:
        await event_store.stream(
            category="profiles", stream="joe.bloggs"
        ).publish(
            events=[
                NewEvent(
                    name="profile-created",
                    payload={
                        "name": "Joe Bloggs",
                        "email": "joe.bloggs@example.com",
                    },
                    metadata=None,
                )
            ]
        )

        await asyncio.sleep(1)

        print(processor.sent_to)
        # ['joe.bloggs@example.com']
    finally:
        await service_manager.stop()


asyncio.run(main())
```

The example logs every read and write. To show only warnings, configure
structlog as described in
[Getting started](/event.store/getting-started/#basic-example).

The rest of this page explains each step.

## Create a subscriber

[`make_event_store_subscriber`](/event.store/reference/processing/#logicblocks.event.processing.make_event_store_subscriber)
creates a subscriber for events read from an event store. It takes:

- `subscriber_group`, the name of the kind of work the subscriber does.
  Subscribers that do the same work on different nodes share a group;
- `subscriber_id`, which identifies this instance within its group. It
  defaults to a random UUID, so each process gets a new one;
- `subscription_request`, the identifier of the one event source the
  subscriber wants;
- `subscriber_state_category`, the event category in which the subscriber
  records its progress;
- `subscriber_state_persistence_interval`, how many processed events to
  record in memory before saving them. It defaults to `EventCount(100)`;
- `save_state_after_consumption`, whether to save progress at the end of
  each `consume_all` call. It defaults to `True`;
- `event_processor`, the processor to pass events to.

[`make_subscriber`](/event.store/reference/processing/#logicblocks.event.processing.make_subscriber)
takes the same arguments plus a `subscriber_state_converter`, which decides
what progress to record for each event and how to resume from it.
`make_event_store_subscriber` uses
[`StoredEventEventConsumerStateConverter`](/event.store/reference/processing/#logicblocks.event.processing.consumers.StoredEventEventConsumerStateConverter),
which records the sequence number of the last processed event. This call
does the same as the `make_event_store_subscriber` call in the example:

```python
from logicblocks.event.processing import make_subscriber
from logicblocks.event.processing.consumers import (
    StoredEventEventConsumerStateConverter,
)

subscriber = make_subscriber(
    subscriber_group="welcome-emails",
    subscription_request=CategoryIdentifier(category="profiles"),
    subscriber_state_category=event_store.category(
        category="welcome-emails-state"
    ),
    subscriber_state_converter=StoredEventEventConsumerStateConverter(),
    event_processor=processor,
)
```

Register each subscriber with the broker using `register` before you start
the broker. The distributed broker only records the subscribers that are
registered when it starts.

## Consume from a category or the log

The subscription request decides which events the subscriber sees:

- `CategoryIdentifier(category="profiles")` gives every event in every
  stream of the `profiles` category;
- `LogIdentifier()` gives every event in the store, in sequence number
  order;
- `StreamIdentifier(category="profiles", stream="joe.bloggs")` gives the
  events in one stream.

See the [streams, categories and logs guide](/event.store/guides/streams-categories-and-logs/)
for more on these sources.

:::caution
A subscriber saves its progress as events in its state category. If it
consumes the log and its state category is in the same event store, it sees
its own `state-changed` events. Each one counts as a processed event, so
every poll saves another state event and the subscriber never goes idle.
When you consume the log, keep subscriber state in a separate event store:

```python
from logicblocks.event.types import LogIdentifier

state_store = EventStore(adapters.InMemoryEventStorageAdapter())

subscriber = make_event_store_subscriber(
    subscriber_group="audit-trail",
    subscription_request=LogIdentifier(),
    subscriber_state_category=state_store.category(
        category="audit-trail-state"
    ),
    event_processor=processor,
)
```
:::

## Track and resume progress

Each subscriber keeps an
[`EventConsumerStateStore`](/event.store/reference/processing/#logicblocks.event.processing.EventConsumerStateStore)
for its progress. When an event is processed, the store records the state
that the converter produces for that event in memory. For
`make_event_store_subscriber` that state looks like
`{"last_sequence_number": 4}`.

The store saves the state by publishing a `state-changed` event to the
stream named `default` in the subscriber state category. The event payload
looks like this:

```python
{"state": {"last_sequence_number": 4}}
```

A save happens when the number of unsaved events reaches
`subscriber_state_persistence_interval`. With
`save_state_after_consumption=True`, a save also happens at the end of each
`consume_all` call that processed events. The write requires the stream to
be at the position the store last saw, so if two writers save to the same
stream, the later one fails with an
[`UnmetWriteConditionError`](/event.store/reference/store/#logicblocks.event.store.UnmetWriteConditionError)
instead of overwriting the other.

When `consume_all` runs, the subscriber loads the latest state, from memory
or else from the stream, and reads only events after the recorded sequence
number. So a restarted process carries on where the last save left off,
provided it uses the same state category. Events processed after the last
save are processed again after a crash. Make processors safe to repeat
where you can.

Because the state stream is always called `default`, give each subscriber
group its own state category. Two groups that share a state category write
to the same stream.

Old state events are never removed, so the state stream grows for as long as
the subscriber runs. On PostgreSQL, see
[Keep subscriber state small](/event.store/guides/postgres-in-production/#keep-subscriber-state-small).

## Project events into a projection store

[`ProjectionEventProcessor`](/event.store/reference/processing/#logicblocks.event.processing.ProjectionEventProcessor)
keeps projections up to date as events arrive. For each event, it finds the
current projection for the event's stream in a
[`ProjectionStore`](/event.store/reference/projection/#logicblocks.event.projection.ProjectionStore),
applies the event with a projector and saves the result.

This example uses the `ProfileProjector` from
[Getting started](/event.store/getting-started/#basic-example). To run it,
replace the processor and subscriber in the example above:

```python
from logicblocks.event.processing import ProjectionEventProcessor
from logicblocks.event.projection import (
    InMemoryProjectionStorageAdapter,
    ProjectionStore,
)
from logicblocks.event.types import StreamIdentifier

projection_store = ProjectionStore(adapter=InMemoryProjectionStorageAdapter())

subscriber = make_event_store_subscriber(
    subscriber_group="profile-projection",
    subscription_request=CategoryIdentifier(category="profiles"),
    subscriber_state_category=event_store.category(
        category="profile-projection-state"
    ),
    event_processor=ProjectionEventProcessor(
        projector=ProfileProjector(),
        projection_store=projection_store,
    ),
)
```

Once the event is processed, find the projection by its source and name:

```python
projection = await projection_store.locate(
    source=StreamIdentifier(category="profiles", stream="joe.bloggs"),
    name="profile",
)
print(projection.state)
# {'name': 'Joe Bloggs', 'email': 'joe.bloggs@example.com'}
```

The projection is named after the projector, so `ProfileProjector` gives
`profile`. To load state as a type other than a JSON value, pass
`state_type` and `metadata_type` to `ProjectionEventProcessor`. See the
[projection store guide](/event.store/guides/projection-store/) for more on
storing and querying projections.

## Choose a broker type

[`make_event_broker`](/event.store/reference/processing/#logicblocks.event.processing.make_event_broker)
takes a node ID, a broker type and a storage type. The node ID names the
process the broker runs in.

Use
[`EventBrokerType.Singleton`](/event.store/reference/processing/#logicblocks.event.processing.EventBrokerType.Singleton)
when one process consumes the events. The singleton broker hands every
registered subscriber each source it requested, then does so again after
each `distribution_interval`. Sources are never withdrawn. It doesn't
coordinate with other nodes, so if you run it on several nodes, every node
consumes every source.

Use
[`EventBrokerType.Distributed`](/event.store/reference/processing/#logicblocks.event.processing.EventBrokerType.Distributed)
when several nodes run the same subscribers. The distributed broker
allocates each requested source to one live subscriber in each subscriber
group, and moves it to another subscriber if that one goes away.
[How the distributed event broker works](/event.store/concepts/distributed-event-broker/)
explains the details.

The settings must match the broker type: pass
[`SingletonEventBrokerSettings`](/event.store/reference/processing/#logicblocks.event.processing.SingletonEventBrokerSettings)
to a singleton broker and
[`DistributedEventBrokerSettings`](/event.store/reference/processing/#logicblocks.event.processing.DistributedEventBrokerSettings)
to a distributed one. `make_event_broker` has an overload for each
combination of broker type and storage type, so a type checker reports
missing arguments.

## Choose the broker storage

The storage type decides where the broker keeps its own state and which
arguments it needs:

| Broker type   | Storage type | Required arguments                                          | Optional arguments        |
| ------------- | ------------ | ----------------------------------------------------------- | ------------------------- |
| `Singleton`   | `InMemory`   | `settings`, `adapter`                                       | `error_handler`           |
| `Singleton`   | `Postgres`   | `settings`, `connection_settings`, `connection_pool`        | `adapter`, `error_handler` |
| `Distributed` | `InMemory`   | `settings`, `adapter`                                       | `error_handler`           |
| `Distributed` | `Postgres`   | `settings`, `connection_settings`, `connection_pool`        | `adapter`, `error_handler` |

The broker reads event sources through `adapter`. With Postgres storage,
`adapter` defaults to a `PostgresEventStorageAdapter` on `connection_pool`.

`EventBrokerStorageType.InMemory` keeps broker state inside the broker
object. Use it for tests and single-process deployments. In-memory brokers
don't share state, even with other brokers in the same process. A
distributed broker with in-memory storage behaves like a single node, which
is useful for testing code that uses the distributed broker:

```python
from logicblocks.event.processing import DistributedEventBrokerSettings

event_broker = make_event_broker(
    node_id="node-1",
    broker_type=EventBrokerType.Distributed,
    storage_type=EventBrokerStorageType.InMemory,
    settings=DistributedEventBrokerSettings(
        coordinator_distribution_interval=timedelta(milliseconds=100),
        observer_synchronisation_interval=timedelta(milliseconds=100),
    ),
    adapter=adapter,
)
```

`EventBrokerStorageType.Postgres` is required when several nodes coordinate.
The distributed broker keeps subscribers and subscriptions in the
`subscribers` and `subscriptions` tables, and elects a leader with a
Postgres advisory lock taken on a separate connection made from
`connection_settings`. Create the tables from
`sql/create_subscribers_table.sql` and `sql/create_subscriptions_table.sql`,
and their index files, before you start the broker. See the
[PostgreSQL guide](/event.store/guides/postgres/) for setting up the
database.

```python
from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from logicblocks.event.persistence.postgres import ConnectionSettings
from logicblocks.event.store import PostgresEventStorageAdapter

connection_settings = ConnectionSettings(
    host="localhost",
    port=5432,
    dbname="my-database",
    user="admin",
    password="super-secret",
)


async def main():
    async with AsyncConnectionPool[AsyncConnection](
        connection_settings.to_connection_string(), open=False
    ) as connection_pool:
        event_store = EventStore(
            PostgresEventStorageAdapter(connection_source=connection_pool)
        )

        # create the processor and subscriber as before

        event_broker = make_event_broker(
            node_id="node-1",
            broker_type=EventBrokerType.Distributed,
            storage_type=EventBrokerStorageType.Postgres,
            settings=DistributedEventBrokerSettings(),
            connection_settings=connection_settings,
            connection_pool=connection_pool,
        )

        # register the subscriber and run the services as before
```

With the default settings, the distributed broker can take around 20
seconds to hand a source to a new subscriber, because the coordinator and
the observer each run every 20 seconds. Lower
`coordinator_distribution_interval` and `observer_synchronisation_interval`
if you need it sooner.

The singleton broker doesn't use the `subscribers` or `subscriptions`
tables, and ignores `connection_settings`. With Postgres storage, it uses
the connection pool only to build the default `adapter`.

## Run the broker as a service

The broker and the subscribers are
[services](/event.store/reference/processing/#logicblocks.event.processing.Service).
Register them with a
[`ServiceManager`](/event.store/reference/processing/#logicblocks.event.processing.ServiceManager)
and start it. Wrap each subscriber's `consume_all` in a
[`PollingService`](/event.store/reference/processing/#logicblocks.event.processing.PollingService)
so it runs repeatedly. The poll interval defaults to 200 milliseconds.

Errors need some care:

- a `PollingService` stops at the first exception, so one failing event
  stops the subscriber;
- the broker retries after an exception, and by default it retries
  immediately without waiting. A persistent error, such as a closed
  connection pool, then keeps the broker busy retrying.

Wrap the polling service in an `ErrorHandlingService`, and give both a
retry strategy that waits. Inside `main` from the first example:

```python
import signal

from logicblocks.event.processing import (
    ConstantRetryStrategy,
    ErrorHandlingService,
    ExecutionMode,
    RetryErrorHandler,
)

retry_after_five_seconds = RetryErrorHandler(
    retry_strategy=ConstantRetryStrategy(time=timedelta(seconds=5))
)

event_broker = make_event_broker(
    node_id="node-1",
    broker_type=EventBrokerType.Singleton,
    storage_type=EventBrokerStorageType.InMemory,
    settings=SingletonEventBrokerSettings(),
    adapter=adapter,
    error_handler=retry_after_five_seconds,
)
await event_broker.register(subscriber)

service_manager = (
    ServiceManager()
    .register(event_broker, execution_mode=ExecutionMode.FOREGROUND)
    .register(
        ErrorHandlingService(
            service=PollingService(
                service=CallableService(subscriber.consume_all)
            ),
            error_handler=retry_after_five_seconds,
        )
    )
    .stop_on([signal.SIGINT, signal.SIGTERM])
)

await service_manager.start()
```

Registering the broker as a `FOREGROUND` service makes `start` wait until
the broker stops. `stop_on` stops the manager when the process receives one
of the given signals. When a failed event is retried, the subscriber
carries on from the last event it processed. See the
[services and error handling guide](/event.store/guides/services-and-error-handling/)
for the other error handlers and execution modes.

## Shut down

`ServiceManager.stop` cancels every running service. When the broker is
cancelled, its status becomes `stopped`. A distributed broker also removes
its subscribers from the `subscribers` table, so the leader moves their
sources to subscribers on other nodes at its next distribution.

Each subscriber saves its progress at the end of every `consume_all` call
that processed events. Progress recorded since the last save is lost if the
subscriber is cancelled part way through a call, and those events are
processed again on the next run.

To check on a running broker, read its `status`, a
[`ProcessStatus`](/event.store/reference/processing/#logicblocks.event.processing.ProcessStatus)
such as `running`, `waiting` or `errored`.
