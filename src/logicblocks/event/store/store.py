from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Sequence, Set

from logicblocks.event.store.adapters import EventStorageAdapter
from logicblocks.event.store.conditions import WriteCondition
from logicblocks.event.store.constraints import QueryConstraint
from logicblocks.event.types import (
    CategoryIdentifier,
    EventSequenceIdentifier,
    NewEvent,
    StoredEvent,
    StreamIdentifier,
)


class EventSource(ABC):
    @property
    @abstractmethod
    def identifier(self) -> EventSequenceIdentifier:
        raise NotImplementedError()

    @abstractmethod
    async def latest(self) -> StoredEvent | None:
        pass

    async def read(
        self,
        *,
        constraints: Set[QueryConstraint] = frozenset(),
    ) -> Sequence[StoredEvent]:
        return [event async for event in self.iterate(constraints=constraints)]

    @abstractmethod
    def iterate(
        self, *, constraints: Set[QueryConstraint] = frozenset()
    ) -> AsyncIterator[StoredEvent]:
        raise NotImplementedError()

    def __aiter__(self) -> AsyncIterator[StoredEvent]:
        return self.iterate()


class EventStream(EventSource):
    """A class for interacting with a specific stream of events.

    Events can be published into the stream using the `publish` method, and
    the entire stream can be read using the `read` method. Streams are also
    iterable, supporting `aiter`.
    """

    def __init__(self, adapter: EventStorageAdapter, stream: StreamIdentifier):
        self.adapter: EventStorageAdapter = adapter
        self._identifier: StreamIdentifier = stream

    @property
    def identifier(self) -> EventSequenceIdentifier:
        return self._identifier

    async def latest(self) -> StoredEvent | None:
        return await self.adapter.latest(target=self._identifier)

    async def publish(
        self,
        *,
        events: Sequence[NewEvent],
        conditions: Set[WriteCondition] = frozenset(),
    ) -> Sequence[StoredEvent]:
        """Publish a sequence of events into the stream."""
        return await self.adapter.save(
            target=self._identifier,
            events=events,
            conditions=conditions,
        )

    def iterate(
        self, *, constraints: Set[QueryConstraint] = frozenset()
    ) -> AsyncIterator[StoredEvent]:
        """Iterate over the events in the stream.

        Args:
            constraints: A set of query constraints defining which events to
                   include in the iteration

        Returns:
            an async iterator over the events in the stream.
        """
        return self.adapter.scan(
            target=self.identifier,
            constraints=constraints,
        )


class EventCategory(EventSource):
    """A class for interacting with a specific category of events.

    Since a category consists of zero or more streams, the category
    can be narrowed to a specific stream using the `stream` method.

    Events in the category can be read using the `read` method. Categories are
    also iterable, supporting `iter`.
    """

    def __init__(
        self, adapter: EventStorageAdapter, category: CategoryIdentifier
    ):
        self._adapter: EventStorageAdapter = adapter
        self._identifier: CategoryIdentifier = category

    @property
    def identifier(self) -> EventSequenceIdentifier:
        return self._identifier

    async def latest(self) -> StoredEvent | None:
        pass

    def stream(self, *, stream: str) -> EventStream:
        """Get a stream of events in the category.

        Args:
            stream (str): The name of the stream.

        Returns:
            an event store scoped to the specified stream.
        """
        return EventStream(
            adapter=self._adapter,
            stream=StreamIdentifier(
                category=self._identifier.category, stream=stream
            ),
        )

    def iterate(
        self, *, constraints: Set[QueryConstraint] = frozenset()
    ) -> AsyncIterator[StoredEvent]:
        """Iterate over the events in the category.

        Args:
            constraints: A set of query constraints defining which events to
                   include in the iteration

        Returns:
            an async iterator over the events in the category.
        """
        return self._adapter.scan(
            target=self._identifier,
            constraints=constraints,
        )


class EventStore:
    """The primary interface into the store of events.

    An [`EventStore`][logicblocks.event.store.EventStore] is backed by a
    [`StorageAdapter`][logicblocks.event.store.adapters.StorageAdapter]
    which implements event storage. Typically, events are stored in an immutable
    append only log, the details of which are storage implementation specific.

    The event store is partitioned into _streams_, a sequence of events relating
    to the same "thing", such as an entity, a process or a state machine, and
    _categories_, a logical grouping of streams that share some characteristics.

    For example, a stream might exist for each order in a commerce system, with
    the category of such streams being "orders".

    Streams and categories are each identified by a string name. The combination
    of a category name and a stream name acts as an identifier for a specific
    stream of events.
    """

    adapter: EventStorageAdapter

    def __init__(self, adapter: EventStorageAdapter):
        self.adapter = adapter

    def stream(self, *, category: str, stream: str) -> EventStream:
        """Get a stream of events from the store.

        This method alone doesn't result in any IO, it instead returns a scoped
        event store for the stream identified by the category and stream names,
        as part of a fluent interface.

        Categories and streams implicitly exist, i.e., calling this method for a
        category or stream that has never been written to will not result in an
        error.

        Args:
            category (str): The name of the category of the stream.
            stream (str): The name of the stream.

        Returns:
            an event store scoped to the specified stream.
        """
        return EventStream(
            adapter=self.adapter,
            stream=StreamIdentifier(category=category, stream=stream),
        )

    def category(self, *, category: str) -> EventCategory:
        """Get a category of events from the store.

        This method alone doesn't result in any IO, it instead returns a scoped
        event store for the category identified by the category name,
        as part of a fluent interface.

        Categories implicitly exist, i.e., calling this method for a category
        that has never been written to will not result in an error.

        Args:
            category (str): The name of the category.

        Returns:
            an event store scoped to the specified category.
        """
        return EventCategory(
            adapter=self.adapter,
            category=CategoryIdentifier(category=category),
        )
