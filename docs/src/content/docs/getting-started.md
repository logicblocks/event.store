---
title: Getting started
description: Publish events to a stream and project them into state.
---

## Basic example

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
