from typing import Any, MutableMapping, Self

from logicblocks.event.sources import ConstrainedEventSource
from logicblocks.event.store import (
    EventCategory,
    EventLog,
    EventSource,
    EventStorageAdapter,
    EventStream,
)
from logicblocks.event.types import (
    CategoryIdentifier,
    CategoryPartitionIdentifier,
    EventSourceIdentifier,
    LogIdentifier,
    LogPartitionIdentifier,
    StreamIdentifier,
)

from ..partitions import PartitionConstraintFactory
from .base import (
    EventSourceConstructor,
    EventSourceConstructorRegistry,
    EventSourceFactory,
)


class EventCategoryConstructor(
    EventSourceConstructor[CategoryIdentifier, [EventStorageAdapter]]
):
    def construct(
        self, identifier: CategoryIdentifier, adapter: EventStorageAdapter
    ) -> EventCategory:
        return EventCategory(adapter, identifier)


class EventStreamConstructor(
    EventSourceConstructor[StreamIdentifier, [EventStorageAdapter]]
):
    def construct(
        self, identifier: StreamIdentifier, adapter: EventStorageAdapter
    ) -> EventStream:
        return EventStream(adapter, identifier)


class EventLogConstructor(
    EventSourceConstructor[LogIdentifier, [EventStorageAdapter]]
):
    def construct(
        self, identifier: LogIdentifier, adapter: EventStorageAdapter
    ) -> EventLog:
        return EventLog(adapter)


class CategoryPartitionConstructor(
    EventSourceConstructor[CategoryPartitionIdentifier, [EventStorageAdapter]]
):
    def __init__(
        self,
        constraint_factory: PartitionConstraintFactory,
        category_constructor: EventSourceConstructor[
            CategoryIdentifier, [EventStorageAdapter]
        ],
    ):
        self._constraint_factory = constraint_factory
        self._category_constructor = category_constructor

    def construct(
        self,
        identifier: CategoryPartitionIdentifier,
        adapter: EventStorageAdapter,
    ) -> ConstrainedEventSource[CategoryPartitionIdentifier]:
        category_identifier = CategoryIdentifier(category=identifier.category)
        unconstrained_source = self._category_constructor.construct(
            category_identifier, adapter
        )
        constraint = self._constraint_factory.construct(identifier.partition)

        return ConstrainedEventSource(
            identifier=identifier,
            delegate=unconstrained_source,
            constraints={constraint},
        )


class LogPartitionConstructor(
    EventSourceConstructor[LogPartitionIdentifier, [EventStorageAdapter]]
):
    def __init__(
        self,
        constraint_factory: PartitionConstraintFactory,
        log_constructor: EventSourceConstructor[
            LogIdentifier, [EventStorageAdapter]
        ],
    ):
        self._constraint_factory = constraint_factory
        self._log_constructor = log_constructor

    def construct(
        self, identifier: LogPartitionIdentifier, adapter: EventStorageAdapter
    ) -> ConstrainedEventSource[LogPartitionIdentifier]:
        log_identifier = LogIdentifier()
        unconstrained_source = self._log_constructor.construct(
            log_identifier, adapter
        )
        constraint = self._constraint_factory.construct(identifier.partition)

        return ConstrainedEventSource(
            identifier=identifier,
            delegate=unconstrained_source,
            constraints={constraint},
        )


class EventStoreEventSourceFactory(
    EventSourceFactory,
    EventSourceConstructorRegistry[[EventStorageAdapter]],
):
    def __init__(
        self,
        adapter: EventStorageAdapter,
        constraint_factory: PartitionConstraintFactory | None = None,
    ):
        self._constructors: MutableMapping[
            type[EventSourceIdentifier],
            EventSourceConstructor[Any, [EventStorageAdapter]],
        ] = {}
        self._adapter = adapter
        self._constraint_factory = (
            constraint_factory
            if constraint_factory is not None
            else PartitionConstraintFactory()
        )
        self._register_default_constructors()

    def _register_default_constructors(self):
        category_constructor = EventCategoryConstructor()
        stream_constructor = EventStreamConstructor()
        log_constructor = EventLogConstructor()

        self.register_constructor(CategoryIdentifier, category_constructor)
        self.register_constructor(StreamIdentifier, stream_constructor)
        self.register_constructor(LogIdentifier, log_constructor)
        self.register_constructor(
            CategoryPartitionIdentifier,
            CategoryPartitionConstructor(
                self._constraint_factory, category_constructor
            ),
        )
        self.register_constructor(
            LogPartitionIdentifier,
            LogPartitionConstructor(self._constraint_factory, log_constructor),
        )

    @property
    def storage_adapter(self) -> EventStorageAdapter:
        return self._adapter

    def register_constructor[I: EventSourceIdentifier](
        self,
        identifier_type: type[I],
        constructor: EventSourceConstructor[I, [EventStorageAdapter]],
    ) -> Self:
        self._constructors[identifier_type] = constructor
        return self

    def construct[I: EventSourceIdentifier](
        self, identifier: I
    ) -> EventSource[I]:
        return self._constructors[type(identifier)].construct(
            identifier, self.storage_adapter
        )
