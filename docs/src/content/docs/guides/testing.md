---
title: Testing with the library
description: Build test events and projections, control time, and test against in-memory storage.
---

The library ships helpers for testing code that uses it, in
`logicblocks.event.testing` and `logicblocks.event.utils`. This guide covers
the builders, random data generators and clocks, testing against the
in-memory storage adapters, and unit-testing a projector.

The examples are [pytest](https://docs.pytest.org/) tests. The async ones
use [pytest-asyncio](https://pytest-asyncio.readthedocs.io/) for the
`@pytest.mark.asyncio` marker. Neither is a dependency of the library.

## Building events

[`NewEventBuilder`](/event.store/reference/testing/#logicblocks.event.testing.NewEventBuilder)
builds `NewEvent` instances, and
[`StoredEventBuilder`](/event.store/reference/testing/#logicblocks.event.testing.StoredEventBuilder)
builds `StoredEvent` instances. Builders are immutable: each `with_*` method
returns a new builder with that field replaced. Fields you don't set get
random values.

```python
from datetime import UTC, datetime

from logicblocks.event.testing import NewEventBuilder, StoredEventBuilder

new_event = (
    NewEventBuilder()
    .with_name("profile-created")
    .with_payload({"name": "Joe Bloggs"})
    .with_occurred_at(datetime(2025, 1, 1, tzinfo=UTC))
    .build()
)

stored_event = (
    StoredEventBuilder()
    .from_new_event(new_event)
    .with_category("profiles")
    .with_stream("joe.bloggs")
    .with_position(0)
    .build()
)

print(stored_event.name, stored_event.payload, stored_event.position)
# profile-created {'name': 'Joe Bloggs'} 0
```

`from_new_event` copies the name, payload, metadata and timestamps from a
new event, and leaves the id, stream, category, position and sequence number
as they were.

Timestamps work differently in the two builders:

- `NewEventBuilder` leaves `occurred_at` and `observed_at` as `None` unless
  you set them, so `NewEvent` stamps them with the current system time when
  you call `build()`.
- `StoredEventBuilder` sets `observed_at` from a clock when the builder is
  created, and `occurred_at` defaults to `observed_at`. Pass
  `clock=` to control it (see [Clocks](#clocks)).

Keep these in mind:

- Random defaults are drawn once, when a builder is created. Builders derived
  from it with `with_*` share those values. For example, two events built
  from the same `StoredEventBuilder` have the same id and sequence number
  unless you set them.
- The builders treat `None` as "not given". Passing `metadata=None`, calling
  `with_metadata(None)` or calling `from_new_event` on an event whose metadata
  is `None` all give random metadata, not `None`. The same applies to
  `payload`. If you need `None` metadata, construct `NewEvent` or
  `StoredEvent` directly.

## Building projections

[`BaseProjectionBuilder`](/event.store/reference/testing/#logicblocks.event.testing.BaseProjectionBuilder)
builds `Projection` instances. Subclass it and implement
`default_state_factory` and `default_metadata_factory` to supply the state and
metadata used when none is given. The id and name default to random values,
and the source defaults to a `StreamIdentifier` with random names.

```python
from collections.abc import Mapping
from typing import Any

from logicblocks.event.testing import BaseProjectionBuilder
from logicblocks.event.types import StreamIdentifier


class ProfileProjectionBuilder(
    BaseProjectionBuilder[dict[str, str], Mapping[str, Any]]
):
    def default_state_factory(self) -> dict[str, str]:
        return {"name": "Joe Bloggs", "email": "joe.bloggs@example.com"}

    def default_metadata_factory(self) -> Mapping[str, Any]:
        return {}


projection = (
    ProfileProjectionBuilder()
    .with_name("profile")
    .with_id("joe.bloggs")
    .with_source(StreamIdentifier(category="profiles", stream="joe.bloggs"))
    .build()
)

print(projection.state)
# {'name': 'Joe Bloggs', 'email': 'joe.bloggs@example.com'}
```

The `with_*` methods return an instance of your subclass, so you can add your
own `with_*` methods to it.

[`MappingProjectionBuilder`](/event.store/reference/testing/#logicblocks.event.testing.MappingProjectionBuilder)
is a ready-made subclass whose state and metadata default to random
string-to-string mappings. Use it when the shape of the state doesn't matter.

## Random test data

[`logicblocks.event.testing.data`](/event.store/reference/testing/#logicblocks.event.testing.data)
has the generators the builders use. They aren't seeded, so values differ
between runs. They fall into four groups:

- general values: `random_int`, `random_string` and variants for lowercase,
  uppercase, alphanumeric and hyphenated strings, and `random_uuid4_string`;
- event fields: `random_event_category_name`, `random_event_stream_name`,
  `random_event_name`, `random_event_id`, `random_event_position`,
  `random_event_sequence_number`, `random_event_payload` and
  `random_event_metadata`;
- projection fields: `random_projection_id`, `random_projection_name`,
  `random_projection_state` and `random_projection_metadata`;
- event processing: `random_node_id`, `random_subscriber_group` and
  `random_subscriber_id`.

Generated names are lowercase ASCII letters and hyphens, and never start or
end with a hyphen. Generated payloads, metadata and states are mappings of 1
to 10 entries, each a 10 character lowercase key and a 20 character
alphanumeric value.

```python
from logicblocks.event.testing import data

category = data.random_event_category_name()
stream = data.random_event_stream_name()
print(len(category), len(stream))
# 10 42
```

Random category and stream names are useful for keeping tests that share a
store, such as a Postgres database, apart from each other.

## Clocks

The library reads the current time through a
[`Clock`](/event.store/reference/utils/#logicblocks.event.utils.Clock), which
you can replace in tests. Clocks are in `logicblocks.event.utils`:

- [`SystemClock`](/event.store/reference/utils/#logicblocks.event.utils.SystemClock)
  returns the system time. It is the default everywhere a clock is accepted.
- [`StaticClock`](/event.store/reference/utils/#logicblocks.event.utils.StaticClock)
  returns a fixed time until you change it with `set()`.
- [`TimezoneRequiredStaticClock`](/event.store/reference/utils/#logicblocks.event.utils.TimezoneRequiredStaticClock)
  is a `StaticClock` that only answers when asked for one time zone. It raises
  a `ValueError` for any other time zone, or none, so you can check that code
  asks for times in the right zone.

These accept a `clock` argument:

- `NewEvent`, which uses it to default `observed_at`;
- `StoredEventBuilder`, which uses it to default `observed_at`;
- the subscriber state stores of the distributed event broker,
  `InMemoryEventSubscriberStateStore` and `PostgresEventSubscriberStateStore`,
  which use it to record when subscribers were last seen.

The storage adapters keep the timestamps of the `NewEvent`s they are given,
so a clock passed to `NewEvent` controls the timestamps of stored events.
To make your own code testable, accept a `Clock` and default it to
`SystemClock()`:

```python
from datetime import UTC, datetime

from logicblocks.event.types import NewEvent
from logicblocks.event.utils import Clock, StaticClock, SystemClock


def profile_created(name: str, clock: Clock = SystemClock()) -> NewEvent:
    return NewEvent(
        name="profile-created",
        payload={"name": name},
        metadata={},
        clock=clock,
    )


clock = StaticClock(datetime(2025, 1, 1, 12, 0, tzinfo=UTC))
first = profile_created("Joe Bloggs", clock=clock)

clock.set(datetime(2025, 1, 1, 12, 5, tzinfo=UTC))
second = profile_created("Jane Doe", clock=clock)

print(first.observed_at, second.observed_at)
# 2025-01-01 12:00:00+00:00 2025-01-01 12:05:00+00:00
```

`StaticClock.now()` ignores its time zone argument and returns the time
exactly as you set it. Give it a time zone aware `datetime`, otherwise events
get naive timestamps. `TimezoneRequiredStaticClock` converts the time you give
it to its time zone:

```python
from datetime import UTC, datetime, timedelta, timezone

from logicblocks.event.types import NewEvent
from logicblocks.event.utils import TimezoneRequiredStaticClock

british_summer_time = timezone(timedelta(hours=1))
clock = TimezoneRequiredStaticClock(
    datetime(2025, 6, 1, 13, 0, tzinfo=british_summer_time), UTC
)

event = NewEvent(name="profile-created", payload={}, metadata={}, clock=clock)
print(event.observed_at)
# 2025-06-01 12:00:00+00:00

try:
    clock.now()
except ValueError as error:
    print(error)
    # Clock supports only UTC which must be provided.
```

## Testing against in-memory storage

[`InMemoryEventStorageAdapter`](/event.store/reference/store/#logicblocks.event.store.InMemoryEventStorageAdapter)
and
[`InMemoryProjectionStorageAdapter`](/event.store/reference/projection/#logicblocks.event.projection.InMemoryProjectionStorageAdapter)
keep everything in process memory. They behave like the Postgres adapters,
including write conditions, locking, ordering and queries, so they suit fast
tests. Create a new adapter in each test to keep tests isolated.

This test publishes an event with a static clock and compares the stored
event with one built by `StoredEventBuilder`. The id is assigned by the
adapter, so the test copies it across. The in-memory adapter numbers events
from 0, so the first event's sequence number is 0.

```python
from datetime import UTC, datetime

import pytest

from logicblocks.event.store import EventStore, InMemoryEventStorageAdapter
from logicblocks.event.testing import StoredEventBuilder
from logicblocks.event.types import NewEvent
from logicblocks.event.utils import StaticClock


@pytest.mark.asyncio
async def test_publishes_profile_created_event():
    clock = StaticClock(datetime(2025, 1, 1, 12, 0, tzinfo=UTC))
    store = EventStore(InMemoryEventStorageAdapter())
    stream = store.stream(category="profiles", stream="joe.bloggs")

    new_event = NewEvent(
        name="profile-created",
        payload={"name": "Joe Bloggs"},
        metadata={"actor": "user-123"},
        clock=clock,
    )
    await stream.publish(events=[new_event])

    stored_event = await stream.latest()
    assert stored_event is not None

    expected = (
        StoredEventBuilder()
        .from_new_event(new_event)
        .with_id(stored_event.id)
        .with_category("profiles")
        .with_stream("joe.bloggs")
        .with_position(0)
        .with_sequence_number(0)
        .build()
    )

    assert stored_event == expected
```

The projection side works the same way. Build projections, save them to a
`ProjectionStore` backed by the in-memory adapter, then read them back:

```python
import pytest

from logicblocks.event.projection import (
    InMemoryProjectionStorageAdapter,
    ProjectionStore,
)
from logicblocks.event.query import (
    FilterClause,
    OffsetPagingClause,
    Operator,
    Path,
    SortClause,
    SortField,
    SortOrder,
)
from logicblocks.event.testing import MappingProjectionBuilder


@pytest.mark.asyncio
async def test_finds_projections_by_state():
    store = ProjectionStore(adapter=InMemoryProjectionStorageAdapter())
    profiles = MappingProjectionBuilder().with_name("profile")
    joe = profiles.with_id("joe").with_state({"town": "Leeds"}).build()
    jane = profiles.with_id("jane").with_state({"town": "York"}).build()

    await store.save(projection=joe)
    await store.save(projection=jane)

    found = await store.search(
        filters=[FilterClause(Operator.EQUAL, Path("state", "town"), "York")],
        sort=SortClause(fields=[SortField(Path("id"), SortOrder.ASC)]),
        paging=OffsetPagingClause(item_count=10),
    )

    assert found == [jane]
```

Each projection gets its own id here. Both builders derive from `profiles`,
so without `with_id` they would share a random id, and the second save would
replace the first.

:::caution
The in-memory adapters store payloads, metadata and projection state by
reference rather than copying them. If a test changes a dictionary after
publishing or saving it, the stored value changes too. The Postgres adapters
don't behave this way, so avoid mutating values you have handed to a store.
:::

The library logs every publish and read, so tests that use a store print log
lines. See [Logging](/event.store/guides/logging/) to quieten them.

## Unit-testing a projector

A [`Projector`](/event.store/reference/projection/#logicblocks.event.projection.Projector)
doesn't need a store. To test a single handler, call `apply()` with an event
and a starting state. To test a whole projection, give `project()` an
[`InMemoryStoredEventSource`](/event.store/reference/store/#logicblocks.event.store.InMemoryStoredEventSource),
which yields the events you give it.

```python
import pytest

from logicblocks.event.projection import Projector
from logicblocks.event.store import InMemoryStoredEventSource
from logicblocks.event.testing import StoredEventBuilder
from logicblocks.event.types import JsonValue, Projection, StreamIdentifier


class ProfileProjector(Projector[StreamIdentifier, dict[str, str]]):
    def initial_state_factory(self) -> dict[str, str]:
        return {}

    def initial_metadata_factory(self) -> JsonValue:
        return {}

    def id_factory(self, state, source: StreamIdentifier) -> str:
        return source.stream

    def profile_created(self, state, event):
        return {**state, "name": event.payload["name"]}

    def date_of_birth_set(self, state, event):
        return {**state, "dob": event.payload["dob"]}


def test_applies_date_of_birth_set():
    event = (
        StoredEventBuilder()
        .with_name("date-of-birth-set")
        .with_payload({"dob": "1992-07-10"})
        .build()
    )

    state = ProfileProjector().apply(event=event, state={"name": "Joe Bloggs"})

    assert state == {"name": "Joe Bloggs", "dob": "1992-07-10"}


@pytest.mark.asyncio
async def test_projects_all_events_in_stream():
    identifier = StreamIdentifier(category="profiles", stream="joe.bloggs")
    events = StoredEventBuilder().with_category("profiles").with_stream(
        "joe.bloggs"
    )
    source = InMemoryStoredEventSource(
        events=[
            events.with_name("profile-created")
            .with_payload({"name": "Joe Bloggs"})
            .with_position(0)
            .with_sequence_number(0)
            .build(),
            events.with_name("date-of-birth-set")
            .with_payload({"dob": "1992-07-10"})
            .with_position(1)
            .with_sequence_number(1)
            .build(),
        ],
        identifier=identifier,
    )

    projection = await ProfileProjector().project(source=source)

    assert projection == Projection(
        id="joe.bloggs",
        name="profile",
        source=identifier,
        state={"name": "Joe Bloggs", "dob": "1992-07-10"},
        metadata={},
    )
```

`apply()` doesn't call `update_metadata` or `finalise_state`; `project()`
calls both. Test those through `project()`. See
[Finalising state](/event.store/guides/finalising-state/) for what
`finalise_state` should and shouldn't do.

When the stored payloads are custom types, events read from a store carry
their JSON form, so build test events with JSON payloads too. See
[Custom payload and state types](/event.store/guides/serialisation/).
