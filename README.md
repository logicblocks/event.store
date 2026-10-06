logicblocks.event.store
=======================

![PyPI - Version](https://img.shields.io/pypi/v/logicblocks.event.store)
![Python - Version](https://img.shields.io/pypi/pyversions/logicblocks.event.store)
![Documentation Status](https://readthedocs.org/projects/eventstore/badge/?version=latest)
![CircleCI](https://img.shields.io/circleci/build/github/logicblocks/event.store)

Eventing infrastructure for event-sourced architectures.

Table of Contents
-----------------

- [Installation](#installation)
- [Usage](#usage)
- [Features](#features)
- [Documentation](#documentation)
- [Development](#development)
- [Contributing](#contributing)
- [License](#license)

Installation
------------

```shell
pip install logicblocks-event-store
```

Usage
-----

### Basic Example

```python
import asyncio

from logicblocks.event.store import EventStore, adapters
from logicblocks.event.types import NewEvent, StreamIdentifier
from logicblocks.event.projection import Projector


class ProfileProjector(
    Projector[StreamIdentifier, dict[str, str], dict[str, str]]
):
    def initial_state_factory(self) -> dict[str, str]:
        return {}

    def initial_metadata_factory(self) -> dict[str, str]:
        return {}

    def id_factory(self, state, source: StreamIdentifier) -> str:
        return source.stream

    def profile_created(self, state, event):
        state['name'] = event.payload['name']
        state['email'] = event.payload['email']
        return state

    def date_of_birth_set(self, state, event):
        state['dob'] = event.payload['dob']
        return state


async def main():
    adapter = adapters.InMemoryEventStorageAdapter()
    store = EventStore(adapter)

    stream = store.stream(category="profiles", stream="joe.bloggs")
    # metadata is required; pass metadata=None when the event has no metadata
    profile_created_event = NewEvent(name="profile-created",
                                     payload={"name": "Joe Bloggs", "email": "joe.bloggs@example.com"},
                                     metadata=None)
    date_of_birth_set_event = NewEvent(name="date-of-birth-set", payload={"dob": "1992-07-10"},
                                       metadata={"actor": "user-123"})

    await stream.publish(
        events=[
            profile_created_event
        ])
    await stream.publish(
        events=[
            date_of_birth_set_event
        ]
    )

    projector = ProfileProjector()
    projection = await projector.project(source=stream)
    profile = projection.state


asyncio.run(main())


# profile == {
#   "name": "Joe Bloggs", 
#   "email": "joe.bloggs@example.com", 
#   "dob": "1992-07-10"
# }
```

### Finalising State

Override `finalise_state` to do work once per projection rather than in every
event handler, for example validation or normalisation. Handlers can then make
cheap, unvalidated changes and leave the expensive work to the end:

```python
class ValidatedProfileProjector(ProfileProjector):
    def finalise_state(self, state: dict[str, str]) -> dict[str, str]:
        if "email" not in state:
            raise ValueError("profile has no email")
        return {**state, "email": state["email"].strip()}
```

`finalise_state` is called once at the end of each `project()` call, after
all events are applied and before `id_factory` derives the projection id,
including when the source has no events. Keep in mind that:

- with `ProjectionEventProcessor`, which projects one event at a time, it
  runs once per processed event, so the saving comes from multi-event folds
  such as rebuilds;
- its output is passed back in as the starting state when a projection is
  resumed (via `state=`, and always by `ProjectionEventProcessor`), so it must
  not change what later handlers, `update_metadata` or `id_factory` compute.
  Validation and recomputing derived fields are fine; lossy normalisation,
  such as clamping or truncation, isn't;
- its output must survive a round trip through the projection store, and it
  must not change fields `id_factory` uses for projections that are already
  stored, otherwise those projections need rebuilding;
- `update_metadata` and `apply()` see unfinalised state;
- handlers mustn't rely on coercion or defaults that validation would apply;
- `finalise_state` is a reserved name, so no event may be named
  `finalise-state`;
- subclasses that override `project()` must call `finalise_state`
  themselves.

### Indexing Projections in Postgres

The Postgres projection store keeps every projection type in one
`projections` table, with each projection's state in a `jsonb` column.
Searches filter and sort on paths into `state`, so index those paths with
expression indexes, in addition to the indexes in
`sql/create_projections_indices.sql`. Lead with `name`, and build the index
concurrently on a live table:

```sql
CREATE INDEX CONCURRENTLY projections_name_account_id_created_at_index
    ON projections (
        name,
        jsonb_extract_path(state, 'account_id'),
        jsonb_extract_path(state, 'created_at') DESC
    );

ANALYZE projections;
```

The trailing sort expression lets queries that sort with a limit stop early.
`CONCURRENTLY` can't run inside a transaction, and a failed build leaves an
`INVALID` index that must be dropped and recreated.

Index expressions must match the SQL the store renders exactly, so `->` and
`->>` won't do:

| Filter operators | Index expression |
|---|---|
| `EQUAL`, `NOT_EQUAL`, `LESS_THAN`, `GREATER_THAN`, `IN`, `CONTAINS` (and their variants), and sorting | `jsonb_extract_path(state, 'key', …)` |
| `REGEX_MATCHES`, `NOT_REGEX_MATCHES`, and `EQUAL` / `NOT_EQUAL` with `None` | `jsonb_extract_path_text(state, 'key', …)` |

Keep in mind that:

- partial indexes (`… WHERE name = 'profile'`) mislead the planner when a
  query filters on more than one indexed path. Postgres doesn't use a partial
  index's statistics to estimate how many rows match, so it estimates every
  equality filter at 0.5% of the rows and every range filter at a third, and
  can walk a far less selective index. Partial indexes are fine when only one
  index can serve the query, or when the index only provides the order;
- btree indexes suit keys holding small scalar values, such as ids, enums and
  timestamps. Index entries larger than about 2.7kB are rejected, so indexing
  a key that can hold a large object or array makes saving such a projection
  fail. For `CONTAINS` on larger values, use a GIN index with `jsonb_path_ops`;
- path keys and `name` are sent as bind parameters. Once psycopg has run a
  query five times on a connection it prepares it, and Postgres may then
  switch to a generic plan, which can use neither expression indexes nor
  partial indexes. If that happens, set `plan_cache_mode = force_custom_plan`
  for the database role, or pass a connection pool that doesn't prepare
  queries:

  ```python
  from psycopg_pool import AsyncConnectionPool

  from logicblocks.event.projection.store import (
      PostgresProjectionStorageAdapter,
  )

  pool = AsyncConnectionPool(
      conninfo, kwargs={"prepare_threshold": None}, open=False
  )
  adapter = PostgresProjectionStorageAdapter(connection_source=pool)
  ```

- `CREATE STATISTICS` on expressions over large `state` documents can use a
  lot of memory during `ANALYZE`, so try it on a production-sized copy first.

Features
--------

- **Event modelling**:
  - _Log / category / stream based_: events are grouped into logs of
    categories of streams.
  - _Arbitrary payloads and metadata_: events can have arbitrary payloads and
    metadata limited only by what the underlying storage backend can support.
  - _Bi-temporality support_: events included timestamps for both the time the
    event occurred and the time the event was recorded in the log.
- **Event storage**:
  - _Immutable and append only_: the event store is modelled as an append-only
    log of immutable events.
  - _Consistency guarantees_: concurrent stream updates can optionally be 
    handled with optimistic concurrency control.
  - _Write conditions_: an extensible write condition system allows 
    pre-conditions to be evaluated before publish.
  - _Ordering guarantees_: event writes are serialised (at log level by default,
    but customisable) to guarantee consistent ordering at scan time.
  - _`asyncio` support_: the event store is implemented using `asyncio` and can 
    be used in cooperative multitasking applications.
- **Storage adapters**: 
  - _Storage adapter abstraction_: adapters are provided for different storage
    backends, currently including:
    - an _in-memory_ implementation for testing and experimentation; and 
    - a _PostgreSQL_ backed implementation for production use.
  - _Extensible to other backends_: the storage adapter abstract base class is 
    designed to be relatively easily implemented to support other storage
    backends.
- **Projections**:
  - _Reduction_: event sequences can be reduced to a single value, a projection,
    using a projector.
  - _Metadata_: projections have metadata for keeping track of things like 
    update timestamps, versions, etc.
  - _Storage_: a general purpose projection store allows easy management of 
    projections for the majority of use cases, utilising the same adapter 
    architecture as the event store, with a rich and customisable query language
    providing store search.
  - _Snapshotting_: coming soon.
- **Types**:
  - _Type hints_: includes type hints for all public classes and functions. 
  - _Value types_: includes serialisable value types for identifiers, events and
    projections.
  - _Pydantic support_: coming soon.
- **Testing utilities**:
  - _Builders_: includes builders for events to simplify testing.
  - _Data generators_: includes random data generators for events and event
    attributes.
  - _Storage adapter tests_: includes tests for storage adapters to ensure
    consistency across implementations.

Documentation
-------------

- [API docs](https://eventstore.readthedocs.io/en/latest/)

Development
-----------

This project uses [mise](https://mise.jdx.dev/) for tool management. To get
started:

```shell
mise install
mise run
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for detailed development instructions.

Contributing
------------

Bug reports and pull requests are welcome on GitHub at
https://github.com/logicblocks/event.store.

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on:

- Reporting bugs and requesting features
- Setting up your development environment
- Running tests and code quality checks
- Submitting pull requests

This project is intended to be a safe, welcoming space for collaboration, and
contributors are expected to adhere to the
[code of conduct](CODE_OF_CONDUCT.md).

License
-------

Copyright &copy; 2025 LogicBlocks Maintainers

Distributed under the terms of the
[MIT License](http://opensource.org/licenses/MIT).
