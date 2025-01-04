logicblocks.event.store
=======================

![PyPI - Version](https://img.shields.io/pypi/v/logicblocks.event.store)
![Python - Version](https://img.shields.io/pypi/pyversions/logicblocks.event.store)
![Documentation Status](https://readthedocs.org/projects/eventstore/badge/?version=latest)
![CircleCI](https://img.shields.io/circleci/build/github/logicblocks/event.store)

Eventing infrastructure for event sourced architectures.

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
pip install logicblocks.event.store
```

Usage
-----

### Basic Example

```python
from logicblocks.event.store import EventStore, adapters
from logicblocks.event.types import NewEvent
from logicblocks.event.projection import Projector

adapter = adapters.InMemoryEventStorageAdapter()
store = EventStore(adapter)

stream = store.stream(category="profiles", stream="joe.bloggs")
stream.publish(
    events=[
        NewEvent(
            name="profile-created",
            payload={
                "name": "Joe Bloggs",
                "email": "joe.bloggs@example.com"
            }
        )
    ])
stream.publish(
    events=[
        NewEvent(
            name="date-of-birth-set",
            payload={
                "dob": "1992-07-10"
            }
        )
    ]
)

projector = Projector(
    handlers={
        "profile-created": lambda state, event: state.merge({
            "name": event.payload["name"],
            "email": event.payload["email"]
        }),
        "date-of-birth-set": lambda state, event: state.merge({
            "dob": event.payload["dob"]
        })
    }
)
profile = projector.project({}, stream.read())

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
    event was created and the time the event was recorded in the log.
- **Event storage**:
  - _Immutable and append only_: the event store is modelled as an append-only
    log of immutable events.
  - _Consistency guarantees_: concurrent stream updates can optionally be 
    handled with optimistic concurrency control.
  - _Write conditions_: an extensible write condition system allows 
    pre-conditions to be evaluated before publish.
  - _Ordering guarantees_: event writes are serialised (currently at log level)
    to guarantee consistent ordering at scan time.
  - _Thread safety_: the event store is thread safe and can be used in 
    multithreaded applications.
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
  - _Versioning_: projections are versioned based on some attribute of the last
    event processed (position, sequence number, etc).
  - _Storage_: coming soon.
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

To run the full pre-commit build:

```shell
./go
```

To run tests:

```shell
./go library:test:all          # all tests
./go library:test:unit         # unit tests
./go library:test:integration  # integration tests
```

To perform linting:

```shell
./go library:lint:check  # check linting rules are met
./go library:lint:fix    # attempt to fix linting issues
```

To format code:

```shell
./go library:format:check  # check code formatting
./go library:format:fix    # attempt to fix code formatting
```

To run type checking:

```shell
./go library:type:check  # check type hints
```

To build packages:

```shell
./go library:build
```

To see all available tasks:

```shell
./go -T
```

Contributing
------------

Bug reports and pull requests are welcome on GitHub at 
https://github.com/logicblocks/event.store. This project is intended to be a 
safe, welcoming space for collaboration, and contributors are expected to 
adhere to the [Contributor Covenant](http://contributor-covenant.org) code of 
conduct.

License
-------

Copyright &copy; 2024 LogicBlocks Maintainers

Distributed under the terms of the
[MIT License](http://opensource.org/licenses/MIT).
