# logicblocks.event.store

## Example

```python linenums="1"
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

## Reference

### ::: logicblocks.event.store

::: logicblocks.event.store.EventStore
::: logicblocks.event.store.EventCategory
::: logicblocks.event.store.EventStream
::: logicblocks.event.store.adapters.StorageAdapter
::: logicblocks.event.store.adapters.InMemoryStorageAdapter
::: logicblocks.event.store.adapters.PostgresStorageAdapter
::: logicblocks.event.store.conditions.WriteCondition
::: logicblocks.event.store.conditions.PositionIsCondition
::: logicblocks.event.store.conditions.EmptyStreamCondition
::: logicblocks.event.store.conditions.position_is
::: logicblocks.event.store.conditions.stream_is_empty

### ::: logicblocks.event.types

::: logicblocks.event.types.identifier.Log
::: logicblocks.event.types.identifier.Category
::: logicblocks.event.types.identifier.Stream
::: logicblocks.event.types.NewEvent
::: logicblocks.event.types.StoredEvent
::: logicblocks.event.types.Projection

### ::: logicblocks.event.projection

::: logicblocks.event.projection.Projector

### ::: logicblocks.event.testing

::: logicblocks.event.testing.builders
::: logicblocks.event.testing.data
