from collections.abc import Callable, MutableMapping
from typing import Any, Self

from logicblocks.event.sources import EventSource, EventSourceFactory
from logicblocks.event.types import (
    CategoryIdentifier,
    EventSourceIdentifier,
    LogIdentifier,
    StoredEvent,
    StreamIdentifier,
)

from .store import (
    EventCategory,
    EventLog,
    EventStorageAdapter,
    EventStream,
)


def construct_event_log(
    identifier: LogIdentifier, adapter: EventStorageAdapter
) -> EventLog:
    return EventLog(adapter, identifier)


def construct_event_category(
    identifier: CategoryIdentifier, adapter: EventStorageAdapter
) -> EventCategory:
    return EventCategory(adapter, identifier)


def construct_event_stream(
    identifier: StreamIdentifier, adapter: EventStorageAdapter
) -> EventStream:
    return EventStream(adapter, identifier)


type EventSourceConstructor[I: EventSourceIdentifier] = Callable[
    [I, EventStorageAdapter], EventSource[I, StoredEvent]
]


class EventStoreEventSourceFactory(EventSourceFactory[StoredEvent]):
    """Constructs event store sources backed by a storage adapter.

    Out of the box, constructs an `EventLog` for a `LogIdentifier`, an
    `EventCategory` for a `CategoryIdentifier` and an `EventStream` for a
    `StreamIdentifier`. Further identifier types can be supported with
    `register_constructor`.

    Args:
        adapter: The storage adapter passed to every constructed source.
    """

    def __init__(self, adapter: EventStorageAdapter):
        self._constructors: MutableMapping[
            type[EventSourceIdentifier],
            EventSourceConstructor[Any],
        ] = {}
        self._adapter = adapter
        self.register_constructor(LogIdentifier, construct_event_log)
        self.register_constructor(CategoryIdentifier, construct_event_category)
        self.register_constructor(StreamIdentifier, construct_event_stream)

    @property
    def storage_adapter(self) -> EventStorageAdapter:
        """The storage adapter used by constructed sources."""
        return self._adapter

    def register_constructor[I: EventSourceIdentifier](
        self,
        identifier_type: type[I],
        constructor: EventSourceConstructor[I],
    ) -> Self:
        """Register a constructor for an identifier type.

        Replaces any constructor already registered for the type. Lookup is
        by exact type, so subclasses must be registered separately.

        Args:
            identifier_type: The identifier type to construct sources for.
            constructor: A callable taking the identifier and the storage
                adapter and returning an event source.

        Returns:
            this factory, to allow chaining.
        """
        self._constructors[identifier_type] = constructor
        return self

    def construct[I: EventSourceIdentifier](
        self, identifier: I
    ) -> EventSource[I, StoredEvent]:
        """Construct an event source for an identifier.

        Args:
            identifier: The identifier of the event source to construct.

        Returns:
            an event source for the identifier.

        Raises:
            KeyError: If no constructor is registered for the identifier's
                type.
        """
        return self._constructors[type(identifier)](
            identifier, self.storage_adapter
        )
