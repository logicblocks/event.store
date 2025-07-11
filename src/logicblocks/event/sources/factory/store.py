from collections.abc import Callable, MutableMapping
from typing import Any, Self

from logicblocks.event.store import (
    EventCategory,
    EventLog,
    EventStorageAdapter,
    EventStream,
)
from logicblocks.event.types import (
    CategoryIdentifier,
    EventSourceIdentifier,
    LogIdentifier,
    StoredEvent,
    StreamIdentifier,
)

from ..base import EventSource
from .base import EventSourceFactory


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
        return self._adapter

    def register_constructor[I: EventSourceIdentifier](
        self,
        identifier_type: type[I],
        constructor: EventSourceConstructor[I],
    ) -> Self:
        self._constructors[identifier_type] = constructor
        return self

    def construct[I: EventSourceIdentifier](
        self, identifier: I
    ) -> EventSource[I, StoredEvent]:
        return self._constructors[type(identifier)](
            identifier, self.storage_adapter
        )
