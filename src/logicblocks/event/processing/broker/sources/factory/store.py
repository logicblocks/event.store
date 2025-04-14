from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import Any, MutableMapping, Self

from logicblocks.event.store import (
    EventCategory,
    EventSource,
    EventStorageAdapter,
    EventStream,
    InMemoryEventStorageAdapter,
    PostgresEventStorageAdapter,
)
from logicblocks.event.types import (
    CategoryIdentifier,
    EventSourceIdentifier,
    StreamIdentifier,
)

from .base import EventSourceFactory


def construct_event_category(
    identifier: CategoryIdentifier, adapter: EventStorageAdapter
) -> EventCategory:
    return EventCategory(adapter, identifier)


def construct_event_stream(
    identifier: StreamIdentifier, adapter: EventStorageAdapter
) -> EventStream:
    return EventStream(adapter, identifier)


type EventSourceConstructor[I: EventSourceIdentifier] = Callable[
    [I, EventStorageAdapter], EventSource[I]
]


class EventStoreEventSourceFactory(
    EventSourceFactory[EventStorageAdapter], ABC
):
    def __init__(self):
        self._constructors: MutableMapping[
            type[EventSourceIdentifier],
            EventSourceConstructor[Any],
        ] = {}

        self.register_constructor(CategoryIdentifier, construct_event_category)
        self.register_constructor(StreamIdentifier, construct_event_stream)

    @property
    @abstractmethod
    def storage_adapter(self) -> EventStorageAdapter:
        raise NotImplementedError()

    def register_constructor[I: EventSourceIdentifier](
        self,
        identifier_type: type[I],
        constructor: EventSourceConstructor[I],
    ) -> Self:
        self._constructors[identifier_type] = constructor
        return self

    def construct[I: EventSourceIdentifier](
        self, identifier: I
    ) -> EventSource[I]:
        return self._constructors[type(identifier)](
            identifier, self.storage_adapter
        )


class InMemoryEventStoreEventSourceFactory(EventStoreEventSourceFactory):
    def __init__(self, adapter: InMemoryEventStorageAdapter):
        super().__init__()
        self._adapter = adapter

    @property
    def storage_adapter(self) -> EventStorageAdapter:
        return self._adapter


class PostgresEventStoreEventSourceFactory(EventStoreEventSourceFactory):
    def __init__(self, adapter: PostgresEventStorageAdapter):
        super().__init__()
        self._adapter = adapter

    @property
    def storage_adapter(self) -> EventStorageAdapter:
        return self._adapter
