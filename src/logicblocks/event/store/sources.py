from logicblocks.event.sources import InMemoryEventSource, constraints
from logicblocks.event.types import (
    Converter,
    EventSourceIdentifier,
    StoredEvent,
)

from .adapters.memory import InMemoryQueryConstraintCheck
from .adapters.memory.converters import TypeRegistryConstraintConverter


class InMemoryStoredEventSource[
    I: EventSourceIdentifier,
    E: StoredEvent = StoredEvent,
](InMemoryEventSource[I, E]):
    def _get_default_constraint_converter(
        self,
    ) -> Converter[
        constraints.QueryConstraint, InMemoryQueryConstraintCheck[E]
    ]:
        return TypeRegistryConstraintConverter().with_default_constraint_converters()
