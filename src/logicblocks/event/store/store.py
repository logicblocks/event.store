from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Sequence, Set

from logicblocks.event.store.adapters import EventStorageAdapter
from logicblocks.event.store.conditions import WriteCondition
from logicblocks.event.store.constraints import QueryConstraint
from logicblocks.event.types import NewEvent, StoredEvent, identifier


class EventSource(ABC):
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

    Attributes:
        adapter: The storage adapter to use for interacting with the stream.
        category: The name of the category of the stream.
        stream: The name of the stream.
    """

    adapter: EventStorageAdapter
    category: str
    stream: str

    def __init__(
        self, adapter: EventStorageAdapter, category: str, stream: str
    ):
        self.adapter = adapter
        self.category = category
        self.stream = stream

    async def publish(
        self,
        *,
        events: Sequence[NewEvent],
        conditions: Set[WriteCondition] = frozenset(),
    ) -> Sequence[StoredEvent]:
        """Publish a sequence of events into the stream."""
        target = identifier.Stream(category=self.category, stream=self.stream)

        return await self.adapter.save(
            target=target,
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
            target=identifier.Stream(
                category=self.category, stream=self.stream
            ),
            constraints=constraints,
        )


class EventCategory(EventSource):
    """A class for interacting with a specific category of events.

    Since a category consists of zero or more streams, the category
    can be narrowed to a specific stream using the `stream` method.

    Events in the category can be read using the `read` method. Categories are
    also iterable, supporting `iter`.

    Attributes:
        adapter: The storage adapter to use for interacting with the category.
        category: the name of the category.
    """

    adapter: EventStorageAdapter
    category: str

    def __init__(self, adapter: EventStorageAdapter, category: str):
        self.adapter = adapter
        self.category = category

    def stream(self, *, stream: str) -> EventStream:
        """Get a stream of events in the category.

        Args:
            stream (str): The name of the stream.

        Returns:
            an event store scoped to the specified stream.
        """
        return EventStream(
            adapter=self.adapter, category=self.category, stream=stream
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
        return self.adapter.scan(
            target=identifier.Category(category=self.category),
            constraints=constraints,
        )


class EventStore(object):
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
            adapter=self.adapter, category=category, stream=stream
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
        return EventCategory(adapter=self.adapter, category=category)
