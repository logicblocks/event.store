from typing import cast

from logicblocks.event.sources import InMemoryEventSource, constraints
from logicblocks.event.sources.memory import InMemoryEventSourceQueryApplier
from logicblocks.event.types import (
    Converter,
    EventSourceIdentifier,
    StoredEvent,
)

from .adapters.memory.converters import TypeRegistryConstraintConverter


class InMemoryStoredEventSource[
    I: EventSourceIdentifier,
    E: StoredEvent = StoredEvent,
](InMemoryEventSource[I, E]):
    def _get_default_constraint_converter(
        self,
    ) -> Converter[
        constraints.QueryConstraint, InMemoryEventSourceQueryApplier[E]
    ]:
        return cast(
            Converter[
                constraints.QueryConstraint,
                InMemoryEventSourceQueryApplier[E],
            ],
            TypeRegistryConstraintConverter().with_default_constraint_converters(),
        )
