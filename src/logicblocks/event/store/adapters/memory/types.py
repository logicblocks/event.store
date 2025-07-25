from logicblocks.event.sources.constraints import QueryConstraintCheck
from logicblocks.event.types import (
    StoredEvent,
)

type InMemoryQueryConstraintCheck[E: StoredEvent = StoredEvent] = (
    QueryConstraintCheck[E]
)
