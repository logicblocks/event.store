from logicblocks.event.sources import constraints
from logicblocks.event.types import (
    StoredEvent,
)

type InMemoryQueryConstraintCheck[E: StoredEvent = StoredEvent] = (
    constraints.QueryConstraintCheck[E]
)
