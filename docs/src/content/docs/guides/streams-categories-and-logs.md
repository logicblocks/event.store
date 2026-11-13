---
title: Streams, categories and the log
description: How events are organised in the store, and how to publish and read them.
---

Events in an `EventStore` are organised at three levels:

- a **stream** is a sequence of events about one thing, such as one order or
  one account. A stream is identified by a category name and a stream name.
- a **category** groups streams that share some characteristics, such as all
  orders. It is identified by its name.
- the **log** holds every event in the store, across all categories.

You get each level from the store. None of these calls do any IO, and
categories and streams don't need creating before you use them:

```python
stream = store.stream(category="orders", stream="order-1")
category = store.category(category="orders")
log = store.log()
```

`stream()` returns an
[`EventStream`](/event.store/reference/store/#logicblocks.event.store.EventStream),
`category()` returns an
[`EventCategory`](/event.store/reference/store/#logicblocks.event.store.EventCategory)
and `log()` returns an
[`EventLog`](/event.store/reference/store/#logicblocks.event.store.EventLog).
You can also get a stream from its category with
`category.stream(stream="order-1")`.

The examples on this page use `InMemoryEventStorageAdapter`, so they run
without a database. For production storage, see the
[PostgreSQL guide](/event.store/guides/postgres/). Running them also prints
log lines from the library; see
[Getting started](/event.store/getting-started/) for how to quieten them.

## Ordering: positions and sequence numbers

Every stored event has two numbers:

- `position` is the event's place within its stream. Positions start at 0
  and have no gaps.
- `sequence_number` orders the event across the whole log. It increases with
  the order in which events are saved, but it can have gaps, so don't rely
  on it being contiguous.

Reading a stream returns events in position order. Reading a category or the
log returns events in sequence number order. This example writes to three
streams in two categories, then reads each level:

```python
import asyncio

from logicblocks.event.store import EventStore, adapters
from logicblocks.event.types import NewEvent


def event(name, payload):
    return NewEvent(name=name, payload=payload, metadata=None)


async def main():
    store = EventStore(adapters.InMemoryEventStorageAdapter())

    joe = store.stream(category="profiles", stream="joe")
    jane = store.stream(category="profiles", stream="jane")
    order = store.stream(category="orders", stream="order-1")

    await joe.publish(events=[event("profile-created", {"name": "Joe"})])
    await order.publish(events=[event("order-placed", {"total": 42})])
    await jane.publish(events=[event("profile-created", {"name": "Jane"})])
    await joe.publish(events=[event("email-changed", {"email": "joe@example.com"})])

    print("stream:")
    async for stored in joe:
        print(" ", stored.name, stored.position, stored.sequence_number)

    print("category:")
    async for stored in store.category(category="profiles"):
        print(" ", stored.stream, stored.name, stored.position, stored.sequence_number)

    print("log:")
    async for stored in store.log():
        print(" ", stored.category, stored.stream, stored.name, stored.sequence_number)


asyncio.run(main())
```

This prints:

```text
stream:
  profile-created 0 0
  email-changed 1 3
category:
  joe profile-created 0 0
  jane profile-created 0 2
  joe email-changed 1 3
log:
  profiles joe profile-created 0
  orders order-1 order-placed 1
  profiles jane profile-created 2
  profiles joe email-changed 3
```

The values of sequence numbers depend on the storage adapter. The in-memory
adapter starts at 0.

## Publishing to a stream

Call `publish` on a stream with a list of
[`NewEvent`](/event.store/reference/types/#logicblocks.event.types.NewEvent)s.
The events are written in order, and either all of them are stored or none
are. `publish` returns the stored events:

```python
import asyncio
from datetime import UTC, datetime

from logicblocks.event.store import EventStore, adapters
from logicblocks.event.types import NewEvent


async def main():
    store = EventStore(adapters.InMemoryEventStorageAdapter())
    account = store.stream(category="accounts", stream="acc-1")

    stored = await account.publish(
        events=[
            NewEvent(
                name="account-opened",
                payload={"owner": "Joe"},
                metadata={"actor": "user-123"},
                occurred_at=datetime(2026, 1, 5, 9, 30, tzinfo=UTC),
            ),
            NewEvent(
                name="funds-deposited",
                payload={"amount": 100},
                metadata=None,
            ),
        ]
    )

    opened = stored[0]
    print(opened.category, opened.stream, opened.position, opened.sequence_number)
    # accounts acc-1 0 0
    print(opened.occurred_at)
    # 2026-01-05 09:30:00+00:00
    print(opened.metadata)
    # {'actor': 'user-123'}
    print([event.position for event in stored])
    # [0, 1]


asyncio.run(main())
```

`publish` also takes a `condition=` argument, which only writes the events
if the stream is in the state you expect. See
[Concurrency and write conditions](/event.store/guides/concurrency/).

## Publishing to several streams at once

To write to several streams in the same category in one atomic operation,
call `publish` on the category. Pass a mapping from stream name to a
[`StreamPublishDefinition`](/event.store/reference/store/#logicblocks.event.store.StreamPublishDefinition),
which holds the events for that stream and, optionally, a write condition.
You can build a definition with
[`stream_publish_definition`](/event.store/reference/store/#logicblocks.event.store.stream_publish_definition)
or write it as a plain dictionary. A stream without a condition is written
unconditionally.

If any stream's condition is not met, nothing is written to any of the
streams. The result maps each stream name to its stored events:

```python
import asyncio

from logicblocks.event.store import (
    EventStore,
    UnmetWriteConditionError,
    adapters,
    conditions,
    stream_publish_definition,
)
from logicblocks.event.types import NewEvent


def event(name, amount):
    return NewEvent(name=name, payload={"amount": amount}, metadata=None)


async def main():
    store = EventStore(adapters.InMemoryEventStorageAdapter())
    accounts = store.category(category="accounts")

    await accounts.stream(stream="acc-1").publish(
        events=[event("funds-deposited", 100)]
    )

    stored = await accounts.publish(
        streams={
            "acc-1": stream_publish_definition(
                events=[event("funds-withdrawn", 30)],
                condition=conditions.position_is(0),
            ),
            "acc-2": {"events": [event("funds-deposited", 30)]},
        }
    )
    for stream, events in stored.items():
        print(stream, [(e.name, e.position) for e in events])
    # acc-1 [('funds-withdrawn', 1)]
    # acc-2 [('funds-deposited', 0)]

    try:
        await accounts.publish(
            streams={
                "acc-1": stream_publish_definition(
                    events=[event("funds-withdrawn", 5)],
                ),
                "acc-2": stream_publish_definition(
                    events=[event("funds-deposited", 5)],
                    condition=conditions.stream_is_empty(),
                ),
            }
        )
    except UnmetWriteConditionError as error:
        print("rejected:", error.message)
    # rejected: stream is not empty

    print(len(await store.log().read()))
    # 3


asyncio.run(main())
```

The second publish is rejected because `acc-2` is not empty, so the
withdrawal from `acc-1` is not written either. The log still holds three
events.

## Reading events

Streams, categories and the log are all
[event sources](/event.store/reference/sources/#logicblocks.event.sources.EventSource),
so they share the same ways of reading:

- `async for event in source` iterates over every event.
- `source.iterate(constraints=...)` returns an async iterator, optionally
  filtered by [constraints](#reading-from-a-point-with-constraints).
- `await source.read(constraints=...)` reads the matching events into a list.
- `await source.latest()` returns the most recent event, or `None` if there
  are no events.

Reading a stream that has never been written to is not an error. It has no
events:

```python
import asyncio

from logicblocks.event.store import EventStore, adapters
from logicblocks.event.types import NewEvent


async def main():
    store = EventStore(adapters.InMemoryEventStorageAdapter())
    account = store.stream(category="accounts", stream="acc-1")

    await account.publish(
        events=[
            NewEvent(name="account-opened", payload={"owner": "Joe"}, metadata=None),
            NewEvent(name="funds-deposited", payload={"amount": 100}, metadata=None),
        ]
    )

    events = await account.read()
    print([event.name for event in events])
    # ['account-opened', 'funds-deposited']

    latest = await account.latest()
    print(latest.name, latest.position)
    # funds-deposited 1

    unused = store.stream(category="accounts", stream="acc-2")
    print(await unused.latest(), await unused.read())
    # None []


asyncio.run(main())
```

Reads are lazy and go to the storage adapter each time, so reading the same
source twice can return different events if something was published in
between.

## Reading from a point with constraints

Pass a set of query constraints from
[`logicblocks.event.sources.constraints`](/event.store/reference/sources/#logicblocks.event.sources.constraints)
to `iterate` or `read` to filter the events returned. The built-in
constraint is
[`sequence_number_after`](/event.store/reference/sources/#logicblocks.event.sources.constraints.sequence_number_after),
which returns only events with a sequence number greater than the one given.
It lets you remember how far you have read and pick up from there later:

```python
import asyncio

from logicblocks.event.sources import constraints
from logicblocks.event.store import EventStore, adapters
from logicblocks.event.types import NewEvent


async def main():
    store = EventStore(adapters.InMemoryEventStorageAdapter())
    log = store.log()

    for n in range(3):
        await store.stream(category="tickets", stream=f"ticket-{n}").publish(
            events=[NewEvent(name="ticket-raised", payload={}, metadata=None)]
        )

    seen = await log.read()
    checkpoint = seen[-1].sequence_number
    print("checkpoint:", checkpoint)
    # checkpoint: 2

    await store.stream(category="tickets", stream="ticket-0").publish(
        events=[NewEvent(name="ticket-closed", payload={}, metadata=None)]
    )

    async for event in log.iterate(
        constraints={constraints.sequence_number_after(checkpoint)}
    ):
        print(event.stream, event.name, event.sequence_number)
    # ticket-0 ticket-closed 3


asyncio.run(main())
```

Constraints work the same way on streams and categories. Storage adapters
convert each constraint type into their own query, and may raise an error
for constraint types they don't support.

Event processors use this to resume from a checkpoint; see
[Event processing](/event.store/guides/event-processing/).

## New events and stored events

You publish
[`NewEvent`](/event.store/reference/types/#logicblocks.event.types.NewEvent)s
and read back
[`StoredEvent`](/event.store/reference/types/#logicblocks.event.types.StoredEvent)s.
Both are frozen dataclasses.

A `NewEvent` has these fields:

| Field         | Description                                                                                          |
| ------------- | ---------------------------------------------------------------------------------------------------- |
| `name`        | The event name, such as `"order-placed"`.                                                            |
| `payload`     | The event data.                                                                                      |
| `metadata`    | Data about the event, such as who caused it. Required: pass `metadata=None` if there is none.        |
| `observed_at` | When the event was recorded. Defaults to the current time in UTC.                                    |
| `occurred_at` | When the event happened in the domain. Defaults to `observed_at`. Set it when recording past events. |

You can also pass `clock=` to control the default for `observed_at`, which
helps in tests.

A `StoredEvent` has the same fields, plus the ones the store assigns:

| Field             | Description                                        |
| ----------------- | -------------------------------------------------- |
| `id`              | A unique id for the event.                         |
| `category`        | The category of the event's stream.                |
| `stream`          | The name of the event's stream.                    |
| `position`        | The event's position in its stream, starting at 0. |
| `sequence_number` | The event's place in the whole log.                |

The store keeps the `observed_at` and `occurred_at` values you set on the
`NewEvent`.

## Payloads and metadata

`name` can be a string or any value that can be converted to one. `payload`
and `metadata` can be any
[`JsonPersistable`](/event.store/reference/types/#logicblocks.event.types.JsonPersistable)
value: plain JSON values such as dictionaries, lists, strings, numbers,
booleans and `None`, or your own types that know how to serialise
themselves.

The stored events returned by `publish` hold the values you passed in.
Events you read back hold the serialised JSON values, so a custom payload
type comes back as a dictionary. Values that can't be serialised make
`publish` raise a `ValueError`. See
[Serialisation](/event.store/guides/serialisation/) for using your own types.
