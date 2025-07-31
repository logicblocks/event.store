from collections.abc import Sequence

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
    def __init__(
        self,
        events: Sequence[E],
        identifier: I,
        constraint_converter: Converter[
            constraints.QueryConstraint, InMemoryQueryConstraintCheck[E]
        ]
        | None = None,
    ):
        super().__init__(
            events,
            identifier,
            constraint_converter=(
                constraint_converter
                if constraint_converter is not None
                else (
                    TypeRegistryConstraintConverter().with_default_constraint_converters()
                )
            ),
        )
