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
    """An in-memory event source of stored events.

    By default, supports the query constraints understood by the in-memory
    storage adapter, such as `SequenceNumberAfterConstraint`.

    Args:
        events: The stored events in the source, in order.
        identifier: The identifier of the source.
        constraint_converter: Converts query constraints into predicates
            over events. Defaults to the in-memory adapter's converter.
    """

    def _get_default_constraint_converter(
        self,
    ) -> Converter[
        constraints.QueryConstraint, InMemoryQueryConstraintCheck[E]
    ]:
        return TypeRegistryConstraintConverter().with_default_constraint_converters()
