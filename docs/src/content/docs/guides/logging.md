---
title: Logging
description: What the library logs, at which levels, and how to filter or route it.
---

The library logs through [structlog](https://www.structlog.org/). It doesn't
configure structlog itself, so its log lines go wherever your structlog
configuration sends them. With structlog's defaults, they are printed to
standard output, including debug lines for every publish and read.

## Loggers

Each part of the library gets its logger with `structlog.get_logger(name)`:

| Logger name | Used by |
| --- | --- |
| `logicblocks.event.store` | `EventStore` and the streams, categories and logs obtained from it |
| `logicblocks.event.projection` | `ProjectionStore` |
| `logicblocks.event.processing.broker` | the singleton and distributed event brokers |
| `logicblocks.event.processing.consumers` | `EventSourceConsumer` and `EventSubscriptionConsumer` |

The name only shows up if your logger factory uses it, for example when you
[route structlog through standard library logging](#routing-through-standard-library-logging).
structlog's default logger factory ignores it.

The storage adapters, projectors and `ProjectionEventProcessor` don't log
directly. Their work is logged by the store, projection store or consumer
that calls them.

## Event names

Log event names are dot-separated, start with `event.` and name the component
followed by what happened, in kebab case. An action in progress uses the
present participle, such as `event.stream.publishing`. A completed action
uses the past tense, such as `event.stream.published`.

Some context is bound to the logger, so it appears on every line:
`category` and `stream` for a stream, `category` for a category, `node` for
broker components, `subscriber` for a subscription consumer and `source` for
an event source consumer.

### Store

| Event | Level | When |
| --- | --- | --- |
| `event.stream.publishing` | debug | before publishing to a stream, with the events |
| `event.stream.published` | info | after publishing to a stream |
| `event.stream.publish-failed` | warning | a write condition wasn't met, with the reason |
| `event.stream.reading-latest` | debug | reading a stream's latest event |
| `event.stream.iterating` | debug | starting to read a stream |
| `event.category.reading-latest` | debug | reading a category's latest event |
| `event.category.iterating` | debug | starting to read a category |
| `event.log.reading-latest` | debug | reading the log's latest event |
| `event.log.iterating` | debug | starting to read the log |

Publishing to several streams at once through `EventCategory.publish` isn't
logged.

### Projection store

| Event | Level | When |
| --- | --- | --- |
| `event.projection.saved` | info | after saving a projection |
| `event.projection.locating` | debug | `locate` was called |
| `event.projection.loading` | debug | `load` was called |
| `event.projection.searching` | debug | `search` was called |

### Event brokers

The singleton broker logs `event.processing.broker.starting`, `running` and
`stopped` at info. The distributed broker is made of three processes, each
with its own prefix:

| Prefix | Info | Debug | Error |
| --- | --- | --- | --- |
| `event.processing.broker.coordinator.` | `starting`, `running`, `stopped` | `distribution.starting`, `distribution.existing-status`, `distribution.latest-status`, `distribution.updated-status`, `distribution.complete` | `failed` |
| `event.processing.broker.observer.` | `starting`, `stopped` | `synchronisation.starting`, `synchronisation.complete` | `failed` |
| `event.processing.broker.subscriber-manager.` | `starting`, `stopped`, `registering-subscriber`, `unregistering-subscriber` | `sending-heartbeats`, `subscriber-healthy`, `purging-subscribers` | `subscriber-unhealthy` |

The singleton broker also logs `event.processing.broker.failed`. All the
`failed` events are logged with `exception`, so they are at error level and
include the traceback.

### Consumers

| Event | Level | When |
| --- | --- | --- |
| `event.consumer.subscription.accepting-source` | info | a source was allocated to the subscriber |
| `event.consumer.subscription.reaccepting-source` | info | a source already held was allocated again |
| `event.consumer.subscription.withdrawing-source` | info | a source was taken away from the subscriber |
| `event.consumer.subscription.missing-source` | warning | a source the subscriber didn't hold was withdrawn |
| `event.consumer.subscription.starting-consume` | debug | starting a round of consumption |
| `event.consumer.subscription.consuming-source` | debug | consuming one source |
| `event.consumer.subscription.completed-consume` | debug | finished a round of consumption |
| `event.consumer.source.starting-consume` | debug | starting to consume a source |
| `event.consumer.source.consuming-event` | debug | about to process an event, with its summary |
| `event.consumer.source.completed-consume` | debug | finished, with counts of consumed and processed events |
| `event.consumer.source.processor-failed` | error | an event processor raised, with the traceback |

## Levels

The levels follow a pattern:

- **debug**: reads, queries, and per-event and per-cycle progress;
- **info**: writes and changes in what is running, such as starting and
  stopping processes or allocating sources;
- **warning**: expected failures the caller handles, such as an unmet write
  condition;
- **error**: unhealthy subscribers and unexpected exceptions, with
  tracebacks.

`event.stream.published` and `event.projection.saved` are info lines, but
their content depends on whether the logger has debug enabled. With debug
enabled, they include the full events or projection, including payloads,
metadata and state. Otherwise they include a summary: ids, names, positions
and timestamps for events, and the id, name and source for a projection.

## Setting the level

To show only warnings and above, configure structlog with a filtering bound
logger before using the library:

```python
import logging

import structlog

structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(logging.WARNING)
)
```

The repository's own component tests do the same in `conftest.py`, with
`logging.DEBUG`. Putting this in your `conftest.py` with `logging.WARNING`
quietens tests that use the library.

:::caution
Configure structlog before creating streams, consumers or brokers. Many of
the library's objects bind context to their logger when they are created,
which fixes the structlog configuration they use. A stream created before
you call `structlog.configure` keeps logging with the old configuration.
:::

## Changing the output format

The library only produces structured events. structlog's processors decide
how they are rendered. For example, to write JSON lines at info and above:

```python
import asyncio
import logging

import structlog

structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    processors=[
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ],
)

from logicblocks.event.store import EventStore, adapters
from logicblocks.event.store.conditions import stream_is_empty
from logicblocks.event.store.exceptions import UnmetWriteConditionError
from logicblocks.event.types import NewEvent


async def main():
    store = EventStore(adapters.InMemoryEventStorageAdapter())
    stream = store.stream(category="profiles", stream="joe.bloggs")
    event = NewEvent(
        name="profile-created", payload={"name": "Joe Bloggs"}, metadata=None
    )

    await stream.publish(events=[event], condition=stream_is_empty())
    try:
        await stream.publish(events=[event], condition=stream_is_empty())
    except UnmetWriteConditionError:
        pass


asyncio.run(main())
```

This prints two lines, shown here wrapped:

```json
{"category": "profiles", "stream": "joe.bloggs",
 "events": [{"id": "c699ddc18aed4d6fa1c07e01fdb7bb5c",
             "name": "profile-created", "stream": "joe.bloggs",
             "category": "profiles", "position": 0, "sequence_number": 0,
             "observed_at": "2026-11-04T13:38:13.438674+00:00",
             "occurred_at": "2026-11-04T13:38:13.438674+00:00"}],
 "event": "event.stream.published", "level": "info",
 "timestamp": "2026-11-04T13:38:13.439314Z"}
{"category": "profiles", "stream": "joe.bloggs",
 "events": [{"name": "profile-created",
             "observed_at": "2026-11-04T13:38:13.438674+00:00",
             "occurred_at": "2026-11-04T13:38:13.438674+00:00"}],
 "reason": "UnmetWriteConditionError(stream is not empty)",
 "event": "event.stream.publish-failed", "level": "warning",
 "timestamp": "2026-11-04T13:38:13.439551Z"}
```

The level is info, so the published events are summarised and their payloads
left out.

## Routing through standard library logging

To send the library's logs to standard library handlers, and to set levels
per logger name, use structlog's standard library logger factory. Keep a
filtering bound logger as the wrapper class:

```python
import asyncio
import logging

import structlog

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s %(name)s %(message)s"
)
logging.getLogger("logicblocks.event.store").setLevel(logging.WARNING)

structlog.configure(
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.JSONRenderer(),
    ],
)

from logicblocks.event.projection import (
    InMemoryProjectionStorageAdapter,
    ProjectionStore,
)
from logicblocks.event.store import EventStore, adapters
from logicblocks.event.types import NewEvent, Projection


async def main():
    store = EventStore(adapters.InMemoryEventStorageAdapter())
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

    projection_store = ProjectionStore(
        adapter=InMemoryProjectionStorageAdapter()
    )
    await projection_store.save(
        projection=Projection(
            id="joe.bloggs",
            name="profile",
            source=stream.identifier,
            state={"name": "Joe Bloggs"},
            metadata={},
        )
    )


asyncio.run(main())
```

The store's info line is dropped by the `logicblocks.event.store` level, and
the projection store's line gets through:

```
INFO logicblocks.event.projection {"projection": {"id": "joe.bloggs", "name": "profile", "source": {"type": "stream", "category": "profiles", "stream": "joe.bloggs"}}, "event": "event.projection.saved", "logger": "logicblocks.event.projection", "level": "info"}
```

The wrapper class's level applies first, before the standard library levels,
and it also decides whether `published` and `saved` lines carry full
payloads. Set it to the lowest level you want from any logger.

:::caution
Don't use `structlog.stdlib.BoundLogger` as the wrapper class. Publishing to
a stream and saving a projection call the logger's `is_enabled_for` method,
which that class doesn't have, so they fail with an `AttributeError`.
:::

## Passing your own logger

These accept a `logger` argument, to use instead of the default for that
part of the library:

- `EventStore`, which passes it to the streams, categories and logs you get
  from it, and `ProjectionStore`;
- `EventSourceConsumer`, `EventSubscriptionConsumer`, `make_subscriber` and
  `make_event_store_subscriber`;
- the broker classes, such as `SingletonEventBroker` and the distributed
  broker's coordinator, observer and subscriber manager.

The `make_event_broker` family of factory functions doesn't take a logger, so
brokers made with them use `logicblocks.event.processing.broker`.

A logger with bound context adds that context to every line:

```python
import asyncio

import structlog

from logicblocks.event.store import EventStore, adapters
from logicblocks.event.types import NewEvent


async def main():
    logger = structlog.get_logger("profiles.events").bind(service="profiles")
    store = EventStore(adapters.InMemoryEventStorageAdapter(), logger=logger)
    stream = store.stream(category="profiles", stream="joe.bloggs")

    await stream.publish(
        events=[NewEvent(name="profile-created", payload={}, metadata=None)]
    )
    # 2026-11-04 13:40:02 [info     ] event.stream.published         category=profiles events=[...] service=profiles stream=joe.bloggs


asyncio.run(main())
```
