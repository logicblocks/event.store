import time
from collections.abc import Mapping, Sequence, Set
from dataclasses import replace

import structlog
from structlog.typing import FilteringBoundLogger

from logicblocks.event.types import JsonValue, NewEvent

from ..types import StreamPublishDefinition
from .middleware import (
    AnyNewEvent,
    AnyStoredEvent,
    AnyStreamPublishDefinition,
    CategoryPublishHook,
    CategoryPublishRequest,
    EventStoreWriteMiddleware,
    StreamPublishHook,
    StreamPublishRequest,
)


class DisallowedEventError(ValueError):
    pass


class AllowedEventNames(EventStoreWriteMiddleware):
    def __init__(self, allowed: Set[str]):
        self._allowed = allowed

    async def publish_to_stream(
        self, request: StreamPublishRequest
    ) -> StreamPublishHook:
        self._check(request.events)
        yield request

    async def publish_to_category(
        self, request: CategoryPublishRequest
    ) -> CategoryPublishHook:
        for definition in request.streams.values():
            self._check(definition["events"])
        yield request

    def _check(self, events: Sequence[AnyNewEvent]) -> None:
        disallowed = {str(e.name) for e in events} - self._allowed
        if disallowed:
            raise DisallowedEventError(
                f"Disallowed event names: {sorted(disallowed)}"
            )


class StampMetadata(EventStoreWriteMiddleware):
    def __init__(self, entries: Mapping[str, JsonValue]):
        self._entries = entries

    async def publish_to_stream(
        self, request: StreamPublishRequest
    ) -> StreamPublishHook:
        yield replace(request, events=self._stamp_all(request.events))

    async def publish_to_category(
        self, request: CategoryPublishRequest
    ) -> CategoryPublishHook:
        yield replace(
            request,
            streams={
                stream: self._stamp_definition(definition)
                for stream, definition in request.streams.items()
            },
        )

    def _stamp_definition(
        self, definition: AnyStreamPublishDefinition
    ) -> AnyStreamPublishDefinition:
        stamped: AnyStreamPublishDefinition = StreamPublishDefinition(
            **definition
        )
        stamped["events"] = self._stamp_all(definition["events"])
        return stamped

    def _stamp_all(
        self, events: Sequence[AnyNewEvent]
    ) -> Sequence[AnyNewEvent]:
        return [self._stamp(event) for event in events]

    def _stamp(self, event: AnyNewEvent) -> AnyNewEvent:
        if not isinstance(event.metadata, Mapping):
            return event
        return NewEvent(
            name=event.name,
            payload=event.payload,
            metadata={**event.metadata, **self._entries},
            observed_at=event.observed_at,
            occurred_at=event.occurred_at,
        )


class PublishTiming(EventStoreWriteMiddleware):
    def __init__(
        self,
        logger: FilteringBoundLogger = structlog.get_logger(
            "logicblocks.event.store.timing"
        ),
    ):
        self._logger = logger

    async def publish_to_stream(
        self, request: StreamPublishRequest
    ) -> StreamPublishHook:
        started = time.perf_counter()
        outcome = "failed"
        try:
            yield request
            outcome = "succeeded"
        finally:
            await self._logger.ainfo(
                "event.store.publish-timed",
                category=request.target.category,
                stream=request.target.stream,
                outcome=outcome,
                duration_ms=(time.perf_counter() - started) * 1000,
            )


class InMemoryOutbox(EventStoreWriteMiddleware):
    def __init__(self):
        self.pending: list[AnyStoredEvent] = []

    async def publish_to_stream(
        self, request: StreamPublishRequest
    ) -> StreamPublishHook:
        stored = yield request
        self.pending.extend(stored)

    async def publish_to_category(
        self, request: CategoryPublishRequest
    ) -> CategoryPublishHook:
        stored = yield request
        for events in stored.values():
            self.pending.extend(events)

    def drain(self) -> Sequence[AnyStoredEvent]:
        drained, self.pending = self.pending, []
        return drained
