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
    profile_created_event = NewEvent(name="profile-created",
                                     payload={"name": "Joe Bloggs", "email": "joe.bloggs@example.com"})
    date_of_birth_set_event = NewEvent(name="date-of-birth-set", payload={"dob": "1992-07-10"})

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
