from abc import ABC
from collections.abc import Sequence
from dataclasses import dataclass

from logicblocks.event.types import (
    JsonPersistable,
    NewEvent,
    StreamIdentifier,
    StringPersistable,
)

from .conditions import WriteCondition


@dataclass(frozen=True, kw_only=True)
class PublishRequest:
    stream: StreamIdentifier
    events: Sequence[
        NewEvent[StringPersistable, JsonPersistable, JsonPersistable]
    ]
    condition: WriteCondition


@dataclass(frozen=True, kw_only=True)
class PublishResponse:
    events: Sequence[
        NewEvent[StringPersistable, JsonPersistable, JsonPersistable]
    ]
    condition: WriteCondition


class EventStoreMiddleware(ABC):
    async def on_publish(self, request: PublishRequest) -> PublishResponse:
        return PublishResponse(
            events=request.events, condition=request.condition
        )


class EventStoreMiddlewareRegistry:
    def __init__(self, middleware: Sequence[EventStoreMiddleware]):
        self._middleware = list(middleware)

    async def on_publish(self, request: PublishRequest) -> PublishResponse:
        response = PublishResponse(
            events=request.events, condition=request.condition
        )
        for middleware in self._middleware:
            next_request = PublishRequest(
                stream=request.stream,
                events=response.events,
                condition=response.condition,
            )
            response = await middleware.on_publish(next_request)

        return response
