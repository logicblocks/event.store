import logging
from collections.abc import AsyncIterator, Mapping, Sequence, Set
from typing import Any

import structlog
from structlog.typing import FilteringBoundLogger

from logicblocks.event.sources import EventSource, constraints
from logicblocks.event.types import (
    CategoryIdentifier,
    JsonPersistable,
    LogIdentifier,
    NewEvent,
    StoredEvent,
    StreamIdentifier,
    StringPersistable,
    str_serialisation_fallback,
)

from .adapters import EventStorageAdapter
from .conditions import NoCondition, WriteCondition
from .exceptions import UnmetWriteConditionError
from .types import StreamPublishDefinition

_default_logger = structlog.get_logger("logicblocks.event.store")


class EventStream(EventSource[StreamIdentifier, StoredEvent]):
    """A class for interacting with a specific stream of events.

    Events can be published into the stream using the `publish` method, and
    the stream can be read using the `read` or `iterate` methods. Streams
    are also async iterable, supporting `aiter` and `async for`.

    Typically obtained via `EventStore.stream` or `EventCategory.stream`
    rather than constructed directly.

    Args:
        adapter: The storage adapter backing the stream.
        stream: The identifier of the stream.
        logger: The logger to use.
    """

    def __init__(
        self,
        adapter: EventStorageAdapter,
        stream: StreamIdentifier,
        logger: FilteringBoundLogger = _default_logger,
    ):
        self._adapter = adapter
        self._logger = logger.bind(
            category=stream.category, stream=stream.stream
        )
        self._identifier = stream

    @property
    def identifier(self) -> StreamIdentifier:
        """The identifier of the stream."""
        return self._identifier

    async def latest(self) -> StoredEvent | None:
        """Get the latest event in the stream.

        Returns:
            the event at the highest position in the stream, or `None` if
                the stream is empty.
        """
        await self._logger.adebug("event.stream.reading-latest")
        return await self._adapter.latest(target=self._identifier)

    async def publish[
        Name: StringPersistable,
        Payload: JsonPersistable,
        Metadata: JsonPersistable,
    ](
        self,
        *,
        events: Sequence[NewEvent[Name, Payload, Metadata]],
        condition: WriteCondition = NoCondition(),
    ) -> Sequence[StoredEvent[Name, Payload, Metadata]]:
        """Publish a sequence of events into the stream.

        Args:
            events: The events to publish, in order.
            condition: A write condition that must be met for the events to
                be written. Defaults to no condition.

        Returns:
            the stored events, in the order they were published.

        Raises:
            UnmetWriteConditionError: If the condition is not met, in which
                case no events are written.
        """
        await self._logger.adebug(
            "event.stream.publishing",
            category=self._identifier.category,
            stream=self._identifier.stream,
            events=[
                event.serialise(fallback=str_serialisation_fallback)
                for event in events
            ],
            conditions=condition,
        )

        try:
            stored_events = await self._adapter.save(
                target=self._identifier,
                events=events,
                condition=condition,
            )

            if self._logger.is_enabled_for(logging.DEBUG):
                await self._logger.ainfo(
                    "event.stream.published",
                    events=[
                        event.serialise(fallback=str_serialisation_fallback)
                        for event in stored_events
                    ],
                )
            else:
                await self._logger.ainfo(
                    "event.stream.published",
                    events=[event.summarise() for event in stored_events],
                )

            return stored_events
        except UnmetWriteConditionError as ex:
            await self._logger.awarning(
                "event.stream.publish-failed",
                category=self._identifier.category,
                stream=self._identifier.stream,
                events=[event.summarise() for event in events],
                reason=repr(ex),
            )
            raise

    def iterate(
        self, *, constraints: Set[constraints.QueryConstraint] = frozenset()
    ) -> AsyncIterator[StoredEvent]:
        """Iterate over the events in the stream.

        Args:
            constraints: A set of query constraints defining which events to
                include in the iteration.

        Returns:
            an async iterator over the events in the stream, in position
                order.
        """
        self._logger.debug(
            "event.stream.iterating", constraints=list(constraints)
        )

        return self._adapter.scan(
            target=self._identifier,
            constraints=constraints,
        )

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, EventStream):
            return NotImplemented
        return (
            self._adapter == other._adapter
            and self._identifier == other._identifier
        )


class EventCategory(EventSource[CategoryIdentifier, StoredEvent]):
    """A class for interacting with a specific category of events.

    Since a category consists of zero or more streams, the category
    can be narrowed to a specific stream using the `stream` method.

    Events in the category can be read using the `read` or `iterate`
    methods. Categories are also async iterable, supporting `aiter` and
    `async for`. Events can be published to multiple streams in the category
    atomically using the `publish` method.

    Typically obtained via `EventStore.category` rather than constructed
    directly.

    Args:
        adapter: The storage adapter backing the category.
        category: The identifier of the category.
        logger: The logger to use.
    """

    def __init__(
        self,
        adapter: EventStorageAdapter,
        category: CategoryIdentifier,
        logger: FilteringBoundLogger = _default_logger,
    ):
        self._adapter = adapter
        self._logger = logger.bind(category=category.category)
        self._identifier = category

    @property
    def identifier(self) -> CategoryIdentifier:
        """The identifier of the category."""
        return self._identifier

    async def latest(self) -> StoredEvent | None:
        """Get the latest event in the category, across all its streams.

        Returns:
            the event with the highest sequence number in the category, or
                `None` if the category is empty.
        """
        await self._logger.adebug("event.category.reading-latest")
        return await self._adapter.latest(target=self._identifier)

    def stream(self, *, stream: str) -> EventStream:
        """Get a stream of events in the category.

        Args:
            stream (str): The name of the stream.

        Returns:
            an `EventStream` for the specified stream.
        """
        return EventStream(
            adapter=self._adapter,
            logger=self._logger,
            stream=StreamIdentifier(
                category=self._identifier.category, stream=stream
            ),
        )

    def iterate(
        self, *, constraints: Set[constraints.QueryConstraint] = frozenset()
    ) -> AsyncIterator[StoredEvent]:
        """Iterate over the events in the category.

        Args:
            constraints: A set of query constraints defining which events to
                include in the iteration.

        Returns:
            an async iterator over the events in the category, in sequence
                number order.
        """
        self._logger.debug(
            "event.category.iterating", constraints=list(constraints)
        )
        return self._adapter.scan(
            target=self._identifier,
            constraints=constraints,
        )

    async def publish[
        Name: StringPersistable,
        Payload: JsonPersistable,
        Metadata: JsonPersistable,
    ](
        self,
        *,
        streams: Mapping[
            str, StreamPublishDefinition[Name, Payload, Metadata]
        ],
    ) -> Mapping[str, Sequence[StoredEvent[Name, Payload, Metadata]]]:
        """Publish events to multiple streams in the category atomically.

        Args:
            streams: A mapping from stream name to the definition of what to
                publish to that stream. Each definition holds the events to
                publish and, optionally, the write condition for that stream.
                Streams without a condition are written unconditionally.

        Returns:
            a mapping from stream name to the stored events for that stream.

        Raises:
            UnmetWriteConditionError: If any stream's condition is not met, in
                which case no events are written to any stream.
        """
        return await self._adapter.save(
            target=self._identifier, streams=streams
        )

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, EventCategory):
            return NotImplemented
        return (
            self._adapter == other._adapter
            and self._identifier == other._identifier
        )


class EventLog(EventSource[LogIdentifier, StoredEvent]):
    """A class for interacting with the entire event log.

    This class allows reading and iterating over all events in the log,
    without scoping to a specific category or stream.

    Typically obtained via `EventStore.log` rather than constructed directly.

    Args:
        adapter: The storage adapter backing the log.
        log: The identifier of the log.
        logger: The logger to use.
    """

    def __init__(
        self,
        adapter: EventStorageAdapter,
        log: LogIdentifier = LogIdentifier(),
        logger: FilteringBoundLogger = _default_logger,
    ):
        self._adapter = adapter
        self._logger = logger.bind()
        self._identifier = log

    @property
    def identifier(self) -> LogIdentifier:
        """The identifier of the log."""
        return self._identifier

    async def latest(self) -> StoredEvent | None:
        """Get the latest event in the log.

        Returns:
            the event with the highest sequence number in the log, or `None`
                if the log is empty.
        """
        await self._logger.adebug("event.log.reading-latest")
        return await self._adapter.latest(target=self._identifier)

    def iterate(
        self, *, constraints: Set[constraints.QueryConstraint] = frozenset()
    ) -> AsyncIterator[StoredEvent]:
        """Iterate over all events in the log.

        Args:
            constraints: A set of query constraints defining which events to
                include in the iteration.

        Returns:
            an async iterator over the events in the log, in sequence number
                order.
        """
        self._logger.debug(
            "event.log.iterating", constraints=list(constraints)
        )
        return self._adapter.scan(
            target=self._identifier,
            constraints=constraints,
        )

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, EventLog):
            return NotImplemented
        return self._adapter == other._adapter


class EventStore:
    """The primary interface into the store of events.

    An [`EventStore`][logicblocks.event.store.EventStore] is backed by a
    [`EventStorageAdapter`][logicblocks.event.store.adapters.EventStorageAdapter]
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

    Args:
        adapter: The storage adapter in which events are stored.
        logger: The logger to use. Streams, categories and logs obtained
            from the store use the same logger.
    """

    def __init__(
        self,
        adapter: EventStorageAdapter,
        logger: FilteringBoundLogger = _default_logger,
    ):
        self._adapter = adapter
        self._logger = logger

    def stream(self, *, category: str, stream: str) -> EventStream:
        """Get a stream of events from the store.

        This method alone doesn't result in any IO, it instead returns an
        `EventStream` for the stream identified by the category and stream
        names, as part of a fluent interface.

        Categories and streams implicitly exist, i.e., calling this method for a
        category or stream that has never been written to will not result in an
        error.

        Args:
            category (str): The name of the category of the stream.
            stream (str): The name of the stream.

        Returns:
            an `EventStream` for the specified stream.
        """
        return EventStream(
            adapter=self._adapter,
            logger=self._logger,
            stream=StreamIdentifier(category=category, stream=stream),
        )

    def category(self, *, category: str) -> EventCategory:
        """Get a category of events from the store.

        This method alone doesn't result in any IO, it instead returns an
        `EventCategory` for the category identified by the category name,
        as part of a fluent interface.

        Categories implicitly exist, i.e., calling this method for a category
        that has never been written to will not result in an error.

        Args:
            category (str): The name of the category.

        Returns:
            an `EventCategory` for the specified category.
        """
        return EventCategory(
            adapter=self._adapter,
            logger=self._logger,
            category=CategoryIdentifier(category=category),
        )

    def log(self) -> EventLog:
        """Get an event log from the store.

        This method alone doesn't result in any IO, it instead returns an event
        log, as part of a fluent interface.

        Returns:
            an `EventLog` over all events in the store.
        """
        return EventLog(
            adapter=self._adapter,
            logger=self._logger,
        )
