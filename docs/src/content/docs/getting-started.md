---
title: Getting started
description: Publish events to a stream and project them into state.
---

## Basic example

```python
import asyncio

from logicblocks.event.projection import Projector
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
    adapter = adapters.InMemoryEventStorageAdapter()
    store = EventStore(adapter)

    stream = store.stream(category="profiles", stream="joe.bloggs")

    # metadata is required; pass metadata=None when the event has no metadata
    profile_created_event = NewEvent(
        name="profile-created",
        payload={"name": "Joe Bloggs", "email": "joe.bloggs@example.com"},
        metadata=None,
    )
    date_of_birth_set_event = NewEvent(
        name="date-of-birth-set",
        payload={"dob": "1992-07-10"},
        metadata={"actor": "user-123"},
    )

    await stream.publish(events=[profile_created_event])
    await stream.publish(events=[date_of_birth_set_event])

    projector = ProfileProjector()
    projection = await projector.project(source=stream)

    print(projection.state)
    # {'name': 'Joe Bloggs', 'email': 'joe.bloggs@example.com',
    #  'dob': '1992-07-10'}


asyncio.run(main())
```

`Projector` takes the source identifier type, the state type and, optionally,
the type of the projection's own metadata, which defaults to `JsonValue`. The
projection's metadata is separate from the metadata on each event.

The library logs through [structlog](https://www.structlog.org/), so running
the example also prints debug and info lines for each publish and read. To
show only warnings and above, configure structlog before running it:

```python
import logging

import structlog

structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(logging.WARNING)
)
```
