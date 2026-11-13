---
title: Storing and querying projections
description: Save projections to a projection store, load them back and query them with filters, sorting and paging.
---

A projector turns events into a projection, but it doesn't keep it anywhere.
A [`ProjectionStore`](/event.store/reference/projection/#logicblocks.event.projection.ProjectionStore)
saves projections and finds them again later. It delegates storage to an
adapter: `InMemoryProjectionStorageAdapter` for tests and prototypes, or
`PostgresProjectionStorageAdapter` for production.

The examples below use the `ProfileProjector` from
[Getting started](/event.store/getting-started/#basic-example). As there,
the library logs each operation through structlog. The output comments show
only what the examples print.

## Saving and loading a projection

```python
import asyncio

from logicblocks.event.projection import (
    InMemoryProjectionStorageAdapter,
    ProjectionStore,
    Projector,
)
from logicblocks.event.store import EventStore, adapters
from logicblocks.event.types import JsonValue, NewEvent, StreamIdentifier


class ProfileProjector(Projector[StreamIdentifier, dict[str, str]]):
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

    def date_of_birth_set(self, state, event):
        state["dob"] = event.payload["dob"]
        return state


async def main():
    event_store = EventStore(adapters.InMemoryEventStorageAdapter())
    stream = event_store.stream(category="profiles", stream="joe.bloggs")
    await stream.publish(
        events=[
            NewEvent(
                name="profile-created",
                payload={"name": "Joe Bloggs", "email": "joe.bloggs@example.com"},
                metadata=None,
            ),
            NewEvent(
                name="date-of-birth-set",
                payload={"dob": "1992-07-10"},
                metadata=None,
            ),
        ]
    )

    projection = await ProfileProjector().project(source=stream)

    projection_store = ProjectionStore(InMemoryProjectionStorageAdapter())
    await projection_store.save(projection=projection)

    loaded = await projection_store.load(
        id="joe.bloggs",
        name="profile",
        state_type=dict[str, str],
        metadata_type=dict[str, str],
    )
    print(loaded.state)
    # {'name': 'Joe Bloggs', 'email': 'joe.bloggs@example.com',
    #  'dob': '1992-07-10'}

    located = await projection_store.locate(
        source=StreamIdentifier(category="profiles", stream="joe.bloggs"),
        name="profile",
    )
    print(located == loaded)
    # True

    missing = await projection_store.load(
        id="jane.doe",
        name="profile",
        state_type=dict[str, str],
        metadata_type=dict[str, str],
    )
    print(missing)
    # None


asyncio.run(main())
```

`ProjectionStore` has four methods:

- `save(projection=...)` inserts the projection, or updates it if one with
  the same name and id is already stored.
- `load(id=..., name=..., state_type=..., metadata_type=...)` finds a
  projection by id and name. Both type arguments are required.
- `locate(source=..., name=...)` finds a projection by the identifier of the
  event source it was projected from. `state_type` and `metadata_type` are
  optional and default to plain JSON values.
- `search(filters=..., sort=..., paging=...)` returns a list of matching
  projections. See [Searching](#searching).

`load` and `locate` return `None` when nothing matches. They raise a
`ValueError` if more than one projection matches, which can happen with
`locate` if a source has several projections under the same name but
different ids.

The store keeps state and metadata as JSON. `state_type` and `metadata_type`
say what to turn them back into: a JSON type such as `dict[str, str]`, or a
class that implements `JsonValueDeserialisable`. See the
[serialisation guide](/event.store/guides/serialisation/) for custom state
types.

## The `Projection` type

A [`Projection`](/event.store/reference/types/#logicblocks.event.types.Projection)
is a frozen dataclass with five fields:

| Field      | Description                                                                                                   |
| ---------- | ------------------------------------------------------------------------------------------------------------- |
| `id`       | Unique among projections with the same name. `Projector.id_factory` sets it.                                  |
| `name`     | Usually shared by every projection from one projector. Defaults to the class name without `Projector`, in kebab case, so `ProfileProjector` gives `profile`. |
| `source`   | The identifier of the event source projected from, for example a `StreamIdentifier`.                          |
| `state`    | The projected state.                                                                                          |
| `metadata` | Data about the projection itself, separate from the metadata on each event.                                   |

The store identifies a projection by its name and id together. Saving a
projection with a name and id that are already stored replaces its state and
metadata. The stored `source` is kept, even if the new projection has a
different one.

You don't need a projector to make a projection. You can construct one
directly and save it:

```python
Projection(
    id="joe.bloggs",
    name="profile",
    source=StreamIdentifier(category="profiles", stream="joe.bloggs"),
    state={"name": "Joe Bloggs", "email": "joe.bloggs@example.com"},
    metadata={},
)
```

## Projection metadata

A projector starts metadata from `initial_metadata_factory` and passes it
through `update_metadata` after each event. The default `update_metadata`
returns the metadata unchanged. Override it to record facts about the
projection, such as how many events it has seen:

```python
class TrackedProfileProjector(ProfileProjector):
    name = "profile"

    def update_metadata(self, state, metadata, event):
        return {
            "event_count": metadata.get("event_count", 0) + 1,
            "last_event_position": event.position,
        }
```

Projecting the two events from the first example with this projector gives:

```python
projection = await TrackedProfileProjector().project(source=stream)
print(projection.metadata)
# {'event_count': 2, 'last_event_position': 1}
```

Setting `name` keeps the projection name as `profile`. Without it, the name
would be `tracked-profile`.

The store saves metadata alongside state, and you can query it with paths
such as `Path("metadata", "event_count")`. When you resume a projection,
pass the stored metadata back to `project(metadata=...)` along with the
stored state. `ProjectionEventProcessor` does this for you. See
[Keeping projections up to date](#keeping-projections-up-to-date).

## Searching

`ProjectionStore.search` takes a list of filter clauses, a sort clause and a
paging clause. All three are required. This example adds a helper that
projects and saves several profiles:

```python
import asyncio

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
from logicblocks.event.store import EventStore, adapters
from logicblocks.event.types import NewEvent

# ProfileProjector as defined above

PROFILES = [
    ("joe.bloggs", "Joe Bloggs", "joe.bloggs@example.com", "1992-07-10"),
    ("jane.doe", "Jane Doe", "jane.doe@example.com", "1988-03-14"),
    ("sam.smith", "Sam Smith", "sam.smith@example.org", "2001-11-30"),
    ("alex.jones", "Alex Jones", "alex.jones@example.org", "1995-05-02"),
]


async def save_profiles(projection_store: ProjectionStore) -> None:
    event_store = EventStore(adapters.InMemoryEventStorageAdapter())
    projector = ProfileProjector()

    for stream_name, name, email, dob in PROFILES:
        stream = event_store.stream(category="profiles", stream=stream_name)
        await stream.publish(
            events=[
                NewEvent(
                    name="profile-created",
                    payload={"name": name, "email": email},
                    metadata=None,
                ),
                NewEvent(
                    name="date-of-birth-set",
                    payload={"dob": dob},
                    metadata=None,
                ),
            ]
        )
        projection = await projector.project(source=stream)
        await projection_store.save(projection=projection)


async def main():
    projection_store = ProjectionStore(InMemoryProjectionStorageAdapter())
    await save_profiles(projection_store)

    born_since_1990 = await projection_store.search(
        filters=[
            FilterClause(Operator.EQUAL, Path("name"), "profile"),
            FilterClause(
                Operator.GREATER_THAN_OR_EQUAL,
                Path("state", "dob"),
                "1990-01-01",
            ),
        ],
        sort=SortClause(
            fields=[SortField(Path("state", "dob"), SortOrder.DESC)]
        ),
        paging=OffsetPagingClause(page_number=1, item_count=10),
        state_type=dict[str, str],
    )

    for projection in born_since_1990:
        print(projection.id, projection.state["dob"])
    # sam.smith 2001-11-30
    # alex.jones 1995-05-02
    # joe.bloggs 1992-07-10


asyncio.run(main())
```

A search applies every filter first, then sorts, then takes a page. A
projection must match all filters. There is no way to combine filters with
"or".

The rest of this page shows snippets that run inside `main()` after
`save_profiles`.

## `Lookup` and `Search`

The query types in
[`logicblocks.event.query`](/event.store/reference/query/) describe what to
find. The store builds them for you, but you can also pass them to the
adapter directly:

- [`Lookup`](/event.store/reference/query/#logicblocks.event.query.Lookup)
  has only `filters`. It's for finding a single projection.
- [`Search`](/event.store/reference/query/#logicblocks.event.query.Search)
  has `filters`, `sort` and `paging`, all optional.

Each adapter has three query methods:

| Method                                  | Query    | Returns                                                       |
| --------------------------------------- | -------- | ------------------------------------------------------------- |
| `find_one(lookup=..., state_type=...)`  | `Lookup` | The match or `None`. Raises `ValueError` on several matches.  |
| `find_many(search=..., state_type=...)` | `Search` | A list of matches, possibly empty.                            |
| `count(search=...)`                     | `Search` | The number of matches.                                        |

`ProjectionStore.load` and `locate` call `find_one`, and `search` calls
`find_many`. The store has no count method, so call `count` on the adapter:

```python
adapter = InMemoryProjectionStorageAdapter()
projection_store = ProjectionStore(adapter)
await save_profiles(projection_store)

jane = await adapter.find_one(
    lookup=Lookup(
        filters=[
            FilterClause(Operator.EQUAL, Path("name"), "profile"),
            FilterClause(
                Operator.EQUAL, Path("state", "email"), "jane.doe@example.com"
            ),
        ]
    ),
    state_type=dict[str, str],
)
print(jane.id)
# jane.doe

org_profiles = Search(
    filters=[
        FilterClause(
            Operator.REGEX_MATCHES, Path("state", "email"), r".*@example\.org$"
        )
    ],
    sort=SortClause(fields=[SortField(Path("id"), SortOrder.ASC)]),
)
print([p.id for p in await adapter.find_many(search=org_profiles)])
# ['alex.jones', 'sam.smith']
print(await adapter.count(search=org_profiles))
# 2
```

`count` applies the search's paging too. A search with
`OffsetPagingClause(item_count=1)` counts at most one projection. Leave
`paging` out to count every match.

## Filters

A [`FilterClause`](/event.store/reference/query/#logicblocks.event.query.FilterClause)
takes an operator, a path and a value:
`FilterClause(Operator.EQUAL, Path("state", "name"), "Jane Doe")`.

### Paths

A [`Path`](/event.store/reference/query/#logicblocks.event.query.Path) starts
with one of the projection's fields: `id`, `name`, `source`, `state` or
`metadata`. Further arguments index into nested JSON, so
`Path("state", "address", "postcode")` refers to
`state["address"]["postcode"]`.

Use `Path("source")` with `Operator.EQUAL` and a whole identifier to filter
by source. This is how `locate` works:

```python
FilterClause(
    Operator.EQUAL,
    Path("source"),
    StreamIdentifier(category="profiles", stream="sam.smith"),
)
```

### Operators

[`Operator`](/event.store/reference/query/#logicblocks.event.query.Operator)
has ten members. The value at the path is on the left of each comparison
and the clause's value is on the right.

| Operator                | Matches when the value at the path...                                  |
| ----------------------- | ---------------------------------------------------------------------- |
| `EQUAL`                 | equals the value.                                                      |
| `NOT_EQUAL`             | doesn't equal the value.                                               |
| `GREATER_THAN`          | is greater than the value.                                             |
| `GREATER_THAN_OR_EQUAL` | is greater than or equal to the value.                                 |
| `LESS_THAN`             | is less than the value.                                                |
| `LESS_THAN_OR_EQUAL`    | is less than or equal to the value.                                    |
| `IN`                    | is one of the items in the value, which must be a list.                |
| `CONTAINS`              | is a list that contains the value.                                     |
| `REGEX_MATCHES`         | is a string that matches the regular expression in the value.          |
| `NOT_REGEX_MATCHES`     | is a string that doesn't match the regular expression in the value.   |

For example, `FilterClause(Operator.IN, Path("id"), ["jane.doe", "sam.smith"])`
matches those two profiles.

There are no separate null-check operators. To check for null, use `EQUAL`
or `NOT_EQUAL` with a value of `None`. On Postgres these become `IS NULL`
and `IS NOT NULL`.

A projection that lacks a nested key in a filter's path doesn't match that
filter. The one exception is `EQUAL` with `None` on Postgres, described in
[In-memory and Postgres differences](#in-memory-and-postgres-differences).

## Sorting

A [`SortClause`](/event.store/reference/query/#logicblocks.event.query.SortClause)
holds a list of [`SortField`](/event.store/reference/query/#logicblocks.event.query.SortField)s.
Each has a path and a
[`SortOrder`](/event.store/reference/query/#logicblocks.event.query.SortOrder),
`ASC` or `DESC`. Later fields only break ties between earlier ones:

```python
SortClause(
    fields=[
        SortField(Path("name"), SortOrder.ASC),
        SortField(Path("state", "dob"), SortOrder.DESC),
    ]
)
```

Without a sort clause, results come back in no particular order. Add a sort
clause whenever the order matters, and always when you page.

### Sorting by similarity

[`Similarity`](/event.store/reference/query/#logicblocks.event.query.Similarity)
scores how alike a string field is to some text, from 0 to 1, using
trigrams. Use it in place of a path in a `SortField` to put the closest
matches first:

```python
closest_names = await projection_store.search(
    filters=[],
    sort=SortClause(
        fields=[
            SortField(
                Similarity(
                    alias="name_similarity",
                    left=Path("state", "name"),
                    right="Jo Blogs",
                ),
                SortOrder.DESC,
            )
        ]
    ),
    paging=OffsetPagingClause(item_count=2),
)
print([p.id for p in closest_names])
# ['joe.bloggs', 'alex.jones']
```

`Similarity` works only in sort fields. You can't filter on the score.

On Postgres, `Similarity` calls the `similarity` function from the `pg_trgm`
extension. Without the extension, the search fails with
`psycopg.errors.UndefinedFunction: function similarity(text, unknown) does
not exist`. The in-memory adapter has its own approximation of `pg_trgm`, so
scores, and the order of close matches, can differ between the two.

## Paging

Both paging clauses take `item_count`, the most projections to return. It
defaults to 10.

### Key-set paging

[`KeySetPagingClause`](/event.store/reference/query/#logicblocks.event.query.KeySetPagingClause)
finds a page relative to the id of a known projection. Pages stay consistent
when projections are added or removed between requests. With no `last_id`,
it returns the first page. To move forwards, pass the id of the last
projection on the current page. To move backwards, pass the id of the first
projection and set `direction` to
[`PagingDirection.BACKWARDS`](/event.store/reference/query/#logicblocks.event.query.PagingDirection).
The projection with `last_id` itself isn't included.

```python
by_name = SortClause(fields=[SortField(Path("state", "name"), SortOrder.ASC)])

first_page = await projection_store.search(
    filters=[], sort=by_name, paging=KeySetPagingClause(item_count=2)
)
print([p.id for p in first_page])
# ['alex.jones', 'jane.doe']

second_page = await projection_store.search(
    filters=[],
    sort=by_name,
    paging=KeySetPagingClause(last_id=first_page[-1].id, item_count=2),
)
print([p.id for p in second_page])
# ['joe.bloggs', 'sam.smith']

previous_page = await projection_store.search(
    filters=[],
    sort=by_name,
    paging=KeySetPagingClause(
        last_id=second_page[0].id,
        direction=PagingDirection.BACKWARDS,
        item_count=2,
    ),
)
print([p.id for p in previous_page])
# ['alex.jones', 'jane.doe']
```

Backwards pages are still in sort order. Paging uses only the id, so filter
by `name` as well if the store holds projections from several projectors
whose ids could overlap.

### Offset paging

[`OffsetPagingClause`](/event.store/reference/query/#logicblocks.event.query.OffsetPagingClause)
finds a page by number, starting at 1. Pages shift if projections are added
or removed between requests.

```python
page_two = await projection_store.search(
    filters=[],
    sort=by_name,
    paging=OffsetPagingClause(page_number=2, item_count=3),
)
print([p.id for p in page_two])
# ['sam.smith']
```

## In-memory and Postgres differences

The in-memory adapter supports every operator, both paging clauses and
`Similarity`. It runs queries in Python, though, and some results differ
from Postgres. The examples on this page give the same results on both.
Keep to the same patterns if you test with the in-memory adapter and run
on Postgres:

- `REGEX_MATCHES` and `NOT_REGEX_MATCHES` use Python's `re.match` in memory,
  which only matches at the start of the string. Postgres uses its `~`
  operator, which matches anywhere. A pattern such as `@example\.org$`
  matches on Postgres but not in memory. Start patterns with `.*` or `^` so
  they behave the same on both.
- `CONTAINS` uses Python's `in` operator in memory, so with a string field
  it matches substrings. Postgres uses the JSONB `@>` operator, where a
  string only contains an equal string. Use `CONTAINS` only to check whether
  a list holds a single value.
- `EQUAL` with `None` on a nested path also matches projections that lack
  the key on Postgres. In memory, it matches only keys whose value is null.
- Sorting by a nested path that some projections lack raises an error in
  memory. Postgres sorts those projections last in ascending order.
- Paths into `source`, such as `Path("source", "category")`, work on
  Postgres but raise a `TypeError` in memory. Integer path segments, for
  list indices, fail on Postgres.
- Without a sort clause, the in-memory adapter returns projections in the
  order they were first saved. Postgres doesn't guarantee an order, except
  that key-set paging orders by id.
- Key-set paging backwards with no `last_id` returns an empty page in
  memory and the first page on Postgres. With a `last_id` that isn't in the
  results and no sort clause, it returns an empty page in memory. On
  Postgres it returns the projections whose ids sort after `last_id`.

## Using Postgres

`PostgresProjectionStorageAdapter` stores projections in a table, named
`projections` by default. Create it with `sql/create_projections_table.sql`
and `sql/create_projections_indices.sql` from the library's repository. If
you use `Similarity`, also run `sql/enable_pg_trgm_extension.sql`.

```python
import asyncio

from logicblocks.event.persistence.postgres import ConnectionSettings
from logicblocks.event.projection import (
    PostgresProjectionStorageAdapter,
    ProjectionStore,
)
from logicblocks.event.types import Projection, StreamIdentifier


async def main():
    adapter = PostgresProjectionStorageAdapter(
        connection_source=ConnectionSettings(
            host="localhost",
            port=5432,
            dbname="some-database",
            user="admin",
            password="super-secret",
        )
    )
    await adapter.open()
    try:
        projection_store = ProjectionStore(adapter)
        await projection_store.save(
            projection=Projection(
                id="joe.bloggs",
                name="profile",
                source=StreamIdentifier(category="profiles", stream="joe.bloggs"),
                state={"name": "Joe Bloggs", "email": "joe.bloggs@example.com"},
                metadata={},
            )
        )
        loaded = await projection_store.load(
            id="joe.bloggs",
            name="profile",
            state_type=dict[str, str],
            metadata_type=dict[str, str],
        )
        print(loaded.state)
        # {'name': 'Joe Bloggs', 'email': 'joe.bloggs@example.com'}
    finally:
        await adapter.close()


asyncio.run(main())
```

When the adapter is given `ConnectionSettings`, it creates its own
connection pool, which you must `open()` before use and `close()`
afterwards. You can pass an existing `AsyncConnectionPool` as
`connection_source` instead. The adapter then leaves opening and closing
the pool to you. Pass `table_settings=TableSettings(table_name=...)` to use
a different table. The Postgres adapter can only read back projections whose
source is a log, category or stream identifier.

See the [Postgres guide](/event.store/guides/postgres/) for connection
settings and schema set-up.

## Keeping projections up to date

So far, the examples call `project` and `save` by hand. A
[`ProjectionEventProcessor`](/event.store/reference/processing/#logicblocks.event.processing.ProjectionEventProcessor)
does this for each event as it arrives. For every event, it locates the
stream's projection by source and projector name, projects the event onto
the stored state and metadata, and saves the result. Continuing the first
example with `TrackedProfileProjector`:

```python
from logicblocks.event.processing import ProjectionEventProcessor

processor = ProjectionEventProcessor(
    projector=TrackedProfileProjector(),
    projection_store=projection_store,
    state_type=dict[str, str],
    metadata_type=dict[str, JsonValue],
)

async for event in stream:
    await processor.process_event(event)

projection = await projection_store.load(
    id="joe.bloggs",
    name="profile",
    state_type=dict[str, str],
    metadata_type=dict[str, JsonValue],
)
print(projection.metadata)
# {'event_count': 2, 'last_event_position': 1}
```

The metadata counts both events, even though the processor projected them
one at a time, because it passes the stored metadata back into the
projector. In an application, an event consumer feeds the processor
instead of a loop. See the
[event processing guide](/event.store/guides/event-processing/).
