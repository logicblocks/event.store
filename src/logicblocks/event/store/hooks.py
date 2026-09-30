import functools
from abc import ABC
from collections.abc import AsyncGenerator, Callable, Sequence
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
    async def on_publish(
        self, request: PublishRequest
    ) -> AsyncGenerator[PublishResponse]:
        # Something before
        yield PublishResponse(
            events=request.events, condition=request.condition
        )
        # Something After


class _DefaultEventStoreWriteHooks(EventStoreWriteHooks):
    pass


class EventStoreHookRunner[Res]:
    def __init__(self, gen: AsyncGenerator[Res]):
        self._gen = gen

    async def start(self) -> Res:
        return await self._gen.asend(None)

    async def end(self) -> None:
        try:
            await self._gen.asend(None)
        except StopAsyncIteration:
            pass


def hook_runner[**P, Res](
    fn: Callable[P, AsyncGenerator[Res]],
) -> Callable[P, EventStoreHookRunner[Res]]:
    @functools.wraps(fn)
    def wrapper(
        *args: P.args, **kwargs: P.kwargs
    ) -> EventStoreHookRunner[Res]:
        return EventStoreHookRunner(fn(*args, **kwargs))

    return wrapper


class EventStoreWriteHooksRegistry:
    def __init__(self, hooks: Sequence[EventStoreWriteHooks]):
        self._hooks = list(hooks)

    @hook_runner
    async def on_publish(
        self, request: PublishRequest
    ) -> AsyncGenerator[PublishResponse]:
        response = PublishResponse(
            events=request.events, condition=request.condition
        )
        hooks = []
        for middleware in self._hooks:
            hook = middleware.on_publish(
                PublishRequest(
                    stream=request.stream,
                    events=response.events,
                    condition=response.condition,
                )
            )
            response = await hook.asend(None)
            hooks.append(hook)

        yield response

        for hook in reversed(hooks):
            await hook.asend(None)
