from abc import ABC
from collections.abc import AsyncGenerator, Sequence
from contextlib import (
    AsyncExitStack,
    asynccontextmanager,
)
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


class EventStoreWriteHooks(ABC):
    @asynccontextmanager
    async def on_publish(
        self, request: PublishRequest
    ) -> AsyncGenerator[PublishResponse]:
        # Something before
        yield PublishResponse(
            events=request.events, condition=request.condition
        )
        # Something After


class EventStoreWriteHooksRegistry:
    def __init__(self, hooks: Sequence[EventStoreWriteHooks]):
        self._hooks = list(hooks)

    @asynccontextmanager
    async def on_publish(
        self, request: PublishRequest
    ) -> AsyncGenerator[PublishResponse]:
        response = PublishResponse(
            events=request.events, condition=request.condition
        )
        async with AsyncExitStack() as stack:
            for middleware in self._hooks:
                response = await stack.enter_async_context(
                    middleware.on_publish(
                        PublishRequest(
                            stream=request.stream,
                            events=response.events,
                            condition=response.condition,
                        )
                    )
                )

            yield response
