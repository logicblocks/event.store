---
title: Writing a storage adapter
description: Implement your own event or projection storage backend.
---

`EventStore` and `ProjectionStore` delegate storage to adapters. The library
ships in-memory and Postgres adapters. To store events or projections
somewhere else, subclass
[`EventStorageAdapter`](/event.store/reference/store/#logicblocks.event.store.EventStorageAdapter)
or
[`ProjectionStorageAdapter`](/event.store/reference/projection/#logicblocks.event.projection.ProjectionStorageAdapter).

This guide describes what each adapter must do, walks through a small event
storage adapter, and covers the converter machinery the built-in adapters
use to support queries. The in-memory adapters in
`logicblocks.event.store.adapters.memory` and
`logicblocks.event.projection.store.adapters.memory` are the simplest
complete implementations to read alongside it.

## Event storage adapters

`EventStorageAdapter` has three abstract methods. `EventStore` and the
streams, categories and logs it hands out call them as follows:

| Adapter method | Called by |
| --- | --- |
| `save(target=StreamIdentifier, events=..., condition=...)` | `EventStream.publish` |
| `save(target=CategoryIdentifier, streams=...)` | `EventCategory.publish` |
| `scan(target=..., constraints=...)` | `iterate`, `read` and `async for` on a stream, category or log |
| `latest(target=...)` | `latest` on a stream, category or log |

### Contract

Every adapter must guarantee that:

- events in a stream have contiguous, zero-based positions, assigned in the
  order they are saved;
- every stored event has a sequence number that is unique across the whole
  store and increases with the order events are saved;
- a save is atomic: all of its events are stored, or none are;
- write conditions are checked after acquiring the adapter's write lock or
  locks, so no conflicting write can happen between the check and the write;
- `scan` and `latest` order events by sequence number.

### `save`

`save` has two forms, chosen by the type of `target`:

- With a
  [`StreamIdentifier`](/event.store/reference/types/#logicblocks.event.types.StreamIdentifier),
  append `events` to the stream if `condition` is met, and return the stored
  events in the order given.
- With a
  [`CategoryIdentifier`](/event.store/reference/types/#logicblocks.event.types.CategoryIdentifier),
  `streams` maps stream names to a
  [`StreamPublishDefinition`](/event.store/reference/store/#logicblocks.event.store.StreamPublishDefinition):
  a dictionary with `events` and an optional `condition`, which defaults to
  `NoCondition`. Write all the streams in one atomic operation. If any
  stream's condition isn't met, write nothing. Return a mapping from stream
  name to that stream's stored events.

Raise `ValueError` if the argument for the target type is missing or the
target type isn't supported.

For each new event, assign an id, the next position in its stream and the
next sequence number. Keep the event's `observed_at` and `occurred_at`. Store
the name with `serialise_to_string`, and the payload and metadata with
`serialise_to_json_value`; these raise `ValueError` for values that can't be
serialised. See
[Custom payload and state types](/event.store/guides/serialisation/) for how
those functions work. The stored events you return keep the original,
unserialised name, payload and metadata.

### Write conditions

Conditions are the classes in
[`logicblocks.event.store.conditions`](/event.store/reference/store/#logicblocks.event.store.conditions).
Check them against the latest event in the target stream:

- [`NoCondition`](/event.store/reference/store/#logicblocks.event.store.conditions.NoCondition)
  is always met.
- [`PositionIsCondition`](/event.store/reference/store/#logicblocks.event.store.conditions.PositionIsCondition)
  is met when the latest event's position equals `position`. A `position` of
  `None` is met only by an empty stream.
- [`EmptyStreamCondition`](/event.store/reference/store/#logicblocks.event.store.conditions.EmptyStreamCondition)
  is met when the stream has no events.
- [`AndCondition`](/event.store/reference/store/#logicblocks.event.store.conditions.AndCondition)
  and
  [`OrCondition`](/event.store/reference/store/#logicblocks.event.store.conditions.OrCondition)
  hold a frozen set of `conditions`, all or at least one of which must be
  met.

When a condition isn't met, raise
[`UnmetWriteConditionError`](/event.store/reference/store/#logicblocks.event.store.UnmetWriteConditionError)
and store nothing. `EventStream.publish` logs a warning for it and re-raises
it.

### Serialisation guarantees

[`EventSerialisationGuarantee`](/event.store/reference/store/#logicblocks.event.store.adapters.EventSerialisationGuarantee)
describes how widely an adapter serialises writes: `LOG` (all writes),
`CATEGORY` (writes to the same category) or `STREAM` (writes to the same
stream). Each has a `lock_name(namespace, target)` method that returns the
name of the lock to hold for a target.

It isn't part of the abstract interface. The built-in adapters take it as a
`serialisation_guarantee` constructor argument and default to `LOG`. With
`STREAM`, a category-level save needs a lock per stream: the in-memory
adapter takes them in stream name order, so concurrent saves can't
deadlock. Narrower guarantees allow more concurrent writes, but events may
then be committed out of sequence number order across categories or
streams.

### `scan` and `latest`

`scan` takes a `LogIdentifier`, `CategoryIdentifier` or `StreamIdentifier`
target and a set of query constraints, and returns an async iterator over the
matching events. `latest` returns the event with the highest sequence number
in the target, or `None`. Both return events in serialised form, as
`StoredEvent[str, JsonValue, JsonValue]`.

The only built-in constraint is
[`SequenceNumberAfterConstraint`](/event.store/reference/sources/#logicblocks.event.sources.constraints.SequenceNumberAfterConstraint),
which event processing uses to resume from the last processed event. Adapters
may raise for constraint types they don't support. The in-memory adapter
iterates over a snapshot taken when the scan starts, so events saved during a
scan aren't yielded by it.

### Example

This adapter keeps events in a list. It holds a single lock for every write,
which gives the `LOG` guarantee, and reuses the in-memory adapter's
constraint converter for `scan`. It passes the repository's contract tests
(see [Testing your adapter](#testing-your-adapter)).

```python
import asyncio
from collections.abc import AsyncIterator, Mapping, Sequence, Set
from uuid import uuid4

from aiologic import Lock

from logicblocks.event.sources.constraints import QueryConstraint
from logicblocks.event.store import EventStore, StreamPublishDefinition
from logicblocks.event.store.adapters import EventStorageAdapter
from logicblocks.event.store.adapters.memory import (
    InMemoryTypeRegistryConstraintConverter,
)
from logicblocks.event.store.conditions import (
    AndCondition,
    EmptyStreamCondition,
    NoCondition,
    OrCondition,
    PositionIsCondition,
    WriteCondition,
    stream_is_empty,
)
from logicblocks.event.store.exceptions import UnmetWriteConditionError
from logicblocks.event.types import (
    CategoryIdentifier,
    JsonValue,
    LogIdentifier,
    NewEvent,
    StoredEvent,
    StreamIdentifier,
    serialise_to_json_value,
    serialise_to_string,
)

type SerialisedEvent = StoredEvent[str, JsonValue, JsonValue]
type Target = LogIdentifier | CategoryIdentifier | StreamIdentifier


def check_condition(
    condition: WriteCondition, latest: SerialisedEvent | None
) -> None:
    match condition:
        case NoCondition():
            return
        case PositionIsCondition(position=position):
            if (latest.position if latest else None) != position:
                raise UnmetWriteConditionError("unexpected stream position")
        case EmptyStreamCondition():
            if latest is not None:
                raise UnmetWriteConditionError("stream is not empty")
        case AndCondition(conditions=conditions):
            for inner in conditions:
                check_condition(inner, latest)
        case OrCondition(conditions=conditions):
            errors = []
            for inner in conditions:
                try:
                    check_condition(inner, latest)
                    return
                except UnmetWriteConditionError as error:
                    errors.append(error)
            raise errors[0]
        case _:
            raise ValueError(f"Unsupported condition: {condition!r}")


def in_target(event: SerialisedEvent, target: Target) -> bool:
    match target:
        case LogIdentifier():
            return True
        case CategoryIdentifier(category=category):
            return event.category == category
        case StreamIdentifier(category=category, stream=stream):
            return event.category == category and event.stream == stream
        case _:
            raise ValueError(f"Unsupported target: {target!r}")


class ListEventStorageAdapter(EventStorageAdapter):
    def __init__(self):
        self._lock = Lock()
        self._events: list[SerialisedEvent] = []
        self._constraint_converter = (
            InMemoryTypeRegistryConstraintConverter()
            .with_default_constraint_converters()
        )

    async def save(
        self,
        *,
        target,
        events=None,
        condition: WriteCondition = NoCondition(),
        streams=None,
    ):
        match target:
            case StreamIdentifier(category=category, stream=stream):
                if events is None:
                    raise ValueError("events must be provided")
                definition = {"events": events, "condition": condition}
                saved = await self._save(category, {stream: definition})
                return saved[stream]
            case CategoryIdentifier(category=category):
                if streams is None:
                    raise ValueError("streams must be provided")
                return await self._save(category, streams)
            case _:
                raise ValueError(f"Unsupported target type: {type(target)}")

    async def _save(
        self, category: str, streams: Mapping[str, StreamPublishDefinition]
    ) -> Mapping[str, Sequence[StoredEvent]]:
        async with self._lock:
            pending: list[SerialisedEvent] = []
            results: dict[str, list[StoredEvent]] = {}
            for stream_name in sorted(streams):
                definition = streams[stream_name]
                identifier = StreamIdentifier(
                    category=category, stream=stream_name
                )
                latest = self._latest(identifier)
                check_condition(
                    definition.get("condition", NoCondition()), latest
                )
                first_position = latest.position + 1 if latest else 0
                results[stream_name] = [
                    self._prepare(
                        identifier, event, first_position + offset, pending
                    )
                    for offset, event in enumerate(definition["events"])
                ]
            self._events.extend(pending)
            return results

    def _prepare(
        self,
        identifier: StreamIdentifier,
        event: NewEvent,
        position: int,
        pending: list[SerialisedEvent],
    ) -> StoredEvent:
        fields = dict(
            id=uuid4().hex,
            stream=identifier.stream,
            category=identifier.category,
            position=position,
            sequence_number=len(self._events) + len(pending),
            observed_at=event.observed_at,
            occurred_at=event.occurred_at,
        )
        pending.append(
            StoredEvent(
                name=serialise_to_string(event.name),
                payload=serialise_to_json_value(event.payload),
                metadata=serialise_to_json_value(event.metadata),
                **fields,
            )
        )
        return StoredEvent(
            name=event.name,
            payload=event.payload,
            metadata=event.metadata,
            **fields,
        )

    def _latest(self, target: Target) -> SerialisedEvent | None:
        matching = [e for e in self._events if in_target(e, target)]
        return matching[-1] if matching else None

    async def latest(self, *, target: Target) -> SerialisedEvent | None:
        return self._latest(target)

    async def scan(
        self,
        *,
        target: Target = LogIdentifier(),
        constraints: Set[QueryConstraint] = frozenset(),
    ) -> AsyncIterator[SerialisedEvent]:
        checks = [self._constraint_converter.convert(c) for c in constraints]
        for event in list(self._events):
            if in_target(event, target) and all(c(event) for c in checks):
                yield event


async def main():
    store = EventStore(ListEventStorageAdapter())
    stream = store.stream(category="profiles", stream="joe.bloggs")
    event = NewEvent(name="profile-created", payload={}, metadata=None)

    await stream.publish(events=[event], condition=stream_is_empty())
    try:
        await stream.publish(events=[event], condition=stream_is_empty())
    except UnmetWriteConditionError as error:
        print(repr(error))
        # UnmetWriteConditionError(stream is not empty)

    print([(e.name, e.position) for e in await stream.read()])
    # [('profile-created', 0)]


if __name__ == "__main__":
    asyncio.run(main())
```

Some notes on the example:

- It builds every event before storing any, so a failed condition or a
  payload that can't be serialised leaves the store unchanged.
- It uses `aiologic`'s `Lock`, as the built-in in-memory adapter does,
  rather than `asyncio.Lock`. The contract tests call one adapter from
  several threads, each with its own event loop, and an `asyncio.Lock` can't
  be shared that way. A database-backed adapter would use the database's
  locking instead.
- `EventStorageAdapter` declares `@overload`s for the two forms of `save`.
  The example leaves them out for brevity. Copy them from the base class if
  you want a type checker to check calls to your adapter.

## Projection storage adapters

`ProjectionStorageAdapter` has four abstract methods. `ProjectionStore`
calls them as follows:

| Adapter method | Called by |
| --- | --- |
| `save(projection=...)` | `ProjectionStore.save` |
| `find_one(lookup=..., state_type=..., metadata_type=...)` | `locate` and `load` |
| `find_many(search=..., state_type=..., metadata_type=...)` | `search` |
| `count(search=...)` | nothing in `ProjectionStore`; call it on the adapter |

Implementations must:

- key projections by name and id together. `save` inserts a projection, or
  replaces the state and metadata of an existing one with the same name and
  id;
- store state and metadata as JSON values. `serialise_projection` does this
  for a whole projection;
- deserialise state and metadata into `state_type` and `metadata_type` on
  the way out, for example with `deserialise_projection`;
- make `find_one` return `None` when nothing matches and raise `ValueError`
  when more than one projection matches.

Queries are [`Lookup`](/event.store/reference/query/#logicblocks.event.query.Lookup)
for `find_one` and [`Search`](/event.store/reference/query/#logicblocks.event.query.Search)
for `find_many` and `count`. They refer to the fields `id`, `name`, `source`,
`state` and `metadata` by [`Path`](/event.store/reference/query/#logicblocks.event.query.Path),
and can index into `state` and `metadata`, as in `Path("state", "town")`.
`ProjectionStore.locate` filters on `Path("source")` with an identifier
object as the value, so the adapter must be able to compare stored sources
with identifiers.

To be a full replacement for the built-in adapters, an adapter also needs to
support every `Operator` of
[`FilterClause`](/event.store/reference/query/#logicblocks.event.query.FilterClause),
[`SortClause`](/event.store/reference/query/#logicblocks.event.query.SortClause),
both paging clauses, and the
[`Similarity`](/event.store/reference/query/#logicblocks.event.query.Similarity)
function in sort fields.

The adapter's two type parameters, `ItemQuery` and `CollectionQuery`, default
to `Lookup` and `Search`. `ProjectionStore` always passes `Lookup` and
`Search`, so other query types are only reachable by calling the adapter
directly.

## Converters and type registries

The built-in adapters turn queries, clauses, constraints and conditions into
something they can execute, using
[`Converter`](/event.store/reference/types/#logicblocks.event.types.Converter)
objects. A [`TypeRegistryConverter`](/event.store/reference/persistence/#logicblocks.event.persistence.TypeRegistryConverter)
picks the converter registered for an item's class:

- the lookup uses the item's exact class, so a converter registered for a
  class isn't used for its subclasses;
- registering a type again replaces its converter;
- `register` returns the registry, so calls can be chained;
- converting an item with no registered converter raises `ValueError`.

Subclasses implement `register`, usually by calling `_register`, to narrow
the types accepted.

### In-memory query converters

[`logicblocks.event.persistence.memory`](/event.store/reference/persistence/#logicblocks.event.persistence.memory)
evaluates queries in process. Each query, clause and function converts into
a `ResultSetTransformer`, a function from one
[`ResultSet`](/event.store/reference/persistence/#logicblocks.event.persistence.memory.ResultSet)
to another. Records in a result set must have an `id` attribute, which
key-set paging uses, and paths are resolved against their attributes.
[`DelegatingQueryConverter`](/event.store/reference/persistence/#logicblocks.event.persistence.memory.DelegatingQueryConverter)
holds three registries, for functions, clauses and queries. They start
empty: `with_default_converters()` registers converters for every built-in
query type.

`InMemoryProjectionStorageAdapter` takes a `query_converter`. Pass one with
extra converters registered to support your own clauses. This adds a clause
that matches projections whose state has a given key:

```python
import asyncio
from dataclasses import dataclass

from logicblocks.event.persistence.memory import (
    ClauseConverter,
    DelegatingQueryConverter,
    ResultSet,
    ResultSetTransformer,
)
from logicblocks.event.projection import InMemoryProjectionStorageAdapter
from logicblocks.event.query import Clause, Path, Search
from logicblocks.event.types import JsonValue, Projection, StreamIdentifier

type StoredProjection = Projection[JsonValue, JsonValue]


@dataclass(frozen=True)
class HasKeyClause(Clause):
    field: Path
    key: str


class HasKeyClauseConverter(ClauseConverter[StoredProjection, HasKeyClause]):
    def convert(
        self, item: HasKeyClause
    ) -> ResultSetTransformer[StoredProjection]:
        def transform(
            result_set: ResultSet[StoredProjection],
        ) -> ResultSet[StoredProjection]:
            return result_set.with_results(
                [
                    result
                    for result in result_set.results
                    if item.key in result.lookup(item.field)
                ]
            )

        return transform


async def main():
    query_converter = (
        DelegatingQueryConverter[StoredProjection]()
        .with_default_converters()
        .register_clause_converter(HasKeyClause, HasKeyClauseConverter())
    )
    adapter = InMemoryProjectionStorageAdapter(query_converter=query_converter)

    for stream, state in [
        ("joe.bloggs", {"name": "Joe Bloggs", "dob": "1992-07-10"}),
        ("jane.doe", {"name": "Jane Doe"}),
    ]:
        await adapter.save(
            projection=Projection(
                id=stream,
                name="profile",
                source=StreamIdentifier(category="profiles", stream=stream),
                state=state,
                metadata={},
            )
        )

    found = await adapter.find_many(
        search=Search(filters=[HasKeyClause(Path("state"), "dob")])
    )
    print([projection.id for projection in found])
    # ['joe.bloggs']


asyncio.run(main())
```

`ProjectionStore.search` is typed to take `FilterClause`s, so custom clauses
are passed to the adapter directly, as here. The in-memory adapter applies
the converter to projections in their serialised form.

These converters work on any records with attributes, not only projections,
so an adapter that loads records into memory can reuse them.

### Event query constraints

The event side has its own registry for query constraints.
[`InMemoryTypeRegistryConstraintConverter`](/event.store/reference/store/#logicblocks.event.store.adapters.memory.InMemoryTypeRegistryConstraintConverter)
converts each constraint into a predicate over a `StoredEvent`. The predicates
don't depend on how events are stored, which is why the example adapter can
reuse it. `InMemoryEventStorageAdapter` and `InMemoryStoredEventSource` take
one as `constraint_converter`, so you can support your own constraints:

```python
import asyncio
from dataclasses import dataclass

from logicblocks.event.sources.constraints import QueryConstraint
from logicblocks.event.store import EventStore
from logicblocks.event.store.adapters import InMemoryEventStorageAdapter
from logicblocks.event.store.adapters.memory import (
    InMemoryQueryConstraintCheck,
    InMemoryTypeRegistryConstraintConverter,
)
from logicblocks.event.types import Converter, NewEvent


@dataclass(frozen=True)
class NameIsConstraint(QueryConstraint):
    name: str


class NameIsConstraintConverter(
    Converter[NameIsConstraint, InMemoryQueryConstraintCheck]
):
    def convert(self, item: NameIsConstraint) -> InMemoryQueryConstraintCheck:
        return lambda event: event.name == item.name


async def main():
    adapter = InMemoryEventStorageAdapter(
        constraint_converter=(
            InMemoryTypeRegistryConstraintConverter()
            .with_default_constraint_converters()
            .register(NameIsConstraint, NameIsConstraintConverter())
        )
    )
    stream = EventStore(adapter).stream(
        category="profiles", stream="joe.bloggs"
    )
    await stream.publish(
        events=[
            NewEvent(name="profile-created", payload={}, metadata=None),
            NewEvent(name="date-of-birth-set", payload={}, metadata=None),
        ]
    )

    events = await stream.read(
        constraints={NameIsConstraint("date-of-birth-set")}
    )
    print([event.name for event in events])
    # ['date-of-birth-set']


asyncio.run(main())
```

The in-memory adapter also takes a `condition_converter` for write
conditions. Its enforcers check conditions against the in-memory adapter's
own transaction type and aren't exported from the package, so treat them as
internal to that adapter.

### Postgres converters

[`logicblocks.event.persistence.postgres`](/event.store/reference/persistence/#logicblocks.event.persistence.postgres)
is the SQL equivalent, used by the Postgres adapters. Its converters turn
queries and clauses into parameterised SQL, with type registries of the same
kind. It is the place to start if your backend speaks SQL, but it is shaped
around the Postgres adapters' tables, so expect to read its source.

## Testing your adapter

The built-in adapters are tested against shared contract test cases in the
repository, under
[`tests/shared/logicblocks/event/testcases`](https://github.com/logicblocks/event.store/tree/main/tests/shared/logicblocks/event/testcases).
They aren't part of the published package, so to use them, copy them into
your project or check out the repository. They need pytest, pytest-asyncio
with `asyncio_mode = "auto"`, and pytest-unordered.

`store/adapters.py` has `EventStorageAdapterCases` and
`projection/store/adapters.py` has `ProjectionStorageAdapterCases`. Subclass
one and implement its abstract members. With the `testcases` directory's
parent on the Python path, as `tests/shared` is in the repository, this runs
the event adapter cases against the example adapter:

```python
from collections.abc import Sequence

from logicblocks.event.testcases.store.adapters import (
    ConcurrencyParameters,
    EventStorageAdapterCases,
)

from logicblocks.event.store.adapters import EventStorageAdapter
from logicblocks.event.types import StoredEvent, identifier

from list_adapter import ListEventStorageAdapter


class TestListEventStorageAdapter(EventStorageAdapterCases):
    @property
    def concurrency_parameters(self) -> ConcurrencyParameters:
        return ConcurrencyParameters(concurrent_writes=2, repeats=1)

    def construct_storage_adapter(
        self, *, serialisation_guarantee=None
    ) -> EventStorageAdapter:
        return ListEventStorageAdapter()

    async def clear_storage(self) -> None:
        pass

    async def retrieve_events(
        self,
        *,
        adapter: EventStorageAdapter,
        category: str | None = None,
        stream: str | None = None,
    ) -> Sequence[StoredEvent]:
        return [
            event
            async for event in adapter.scan(
                target=identifier.target(category=category, stream=stream)
            )
        ]
```

`construct_storage_adapter` receives a `serialisation_guarantee`. The example
adapter ignores it because it always serialises every write. An adapter that
supports several guarantees should honour it. `clear_storage` should empty
any storage that outlives an adapter instance, such as database tables.

The in-memory adapters' tests in `tests/unit` and the Postgres adapters'
tests in `tests/integration` show complete setups.
