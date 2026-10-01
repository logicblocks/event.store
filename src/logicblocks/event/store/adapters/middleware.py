from collections.abc import (
    AsyncGenerator,
    AsyncIterator,
    Awaitable,
    Callable,
    Mapping,
    Sequence,
    Set,
)
from dataclasses import dataclass
from typing import Any, cast, overload

from logicblocks.event.sources import constraints
from logicblocks.event.types import (
    CategoryIdentifier,
    JsonPersistable,
    JsonValue,
    LogIdentifier,
    NewEvent,
    StoredEvent,
    StreamIdentifier,
    StringPersistable,
)

from ..conditions import NoCondition, WriteCondition
from ..types import StreamPublishDefinition
from .base import EventStorageAdapter, Latestable, Saveable, Scannable

type AnyNewEvent = NewEvent[StringPersistable, JsonPersistable, JsonPersistable]
type AnyStoredEvent = StoredEvent[
    StringPersistable, JsonPersistable, JsonPersistable
]
type AnyStreamPublishDefinition = StreamPublishDefinition[
    StringPersistable, JsonPersistable, JsonPersistable
]


@dataclass(frozen=True, kw_only=True)
class StreamPublishRequest:
    target: StreamIdentifier
    events: Sequence[AnyNewEvent]
    condition: WriteCondition = NoCondition()


@dataclass(frozen=True, kw_only=True)
class CategoryPublishRequest:
    target: CategoryIdentifier
    streams: Mapping[str, AnyStreamPublishDefinition]


type StreamPublishHook = AsyncGenerator[
    StreamPublishRequest, Sequence[AnyStoredEvent]
]
type CategoryPublishHook = AsyncGenerator[
    CategoryPublishRequest, Mapping[str, Sequence[AnyStoredEvent]]
]


class EventStoreWriteMiddleware:
    async def publish_to_stream(
        self, request: StreamPublishRequest
    ) -> StreamPublishHook:
        yield request

    async def publish_to_category(
        self, request: CategoryPublishRequest
    ) -> CategoryPublishHook:
        yield request


class HookProtocolError(RuntimeError):
    pass


type Hook[Request, Result] = Callable[
    [Request], AsyncGenerator[Request, Result]
]
type Save[Request, Result] = Callable[[Request], Awaitable[Result]]


class EventStoreWriteMiddlewareRegistry:
    def __init__(self, middleware: Sequence[EventStoreWriteMiddleware] = ()):
        self._middleware = list(middleware)

    def register(self, middleware: EventStoreWriteMiddleware) -> None:
        self._middleware.append(middleware)

    async def publish_to_stream(
        self,
        request: StreamPublishRequest,
        save: Save[StreamPublishRequest, Sequence[AnyStoredEvent]],
    ) -> Sequence[AnyStoredEvent]:
        return await self._run(
            [m.publish_to_stream for m in self._middleware], request, save
        )

    async def publish_to_category(
        self,
        request: CategoryPublishRequest,
        save: Save[
            CategoryPublishRequest, Mapping[str, Sequence[AnyStoredEvent]]
        ],
    ) -> Mapping[str, Sequence[AnyStoredEvent]]:
        return await self._run(
            [m.publish_to_category for m in self._middleware], request, save
        )

    async def _run[Request, Result](
        self,
        hooks: Sequence[Hook[Request, Result]],
        request: Request,
        save: Save[Request, Result],
    ) -> Result:
        if not hooks:
            return await save(request)

        first, *rest = hooks
        hook = first(request)
        try:
            request = await anext(hook)
        except StopAsyncIteration:
            raise HookProtocolError("Write hook must yield exactly once.")

        try:
            result = await self._run(rest, request, save)
        except BaseException as error:
            await self._resume_with_error(hook, error)
            raise
        await self._resume_with_result(hook, result)
        return result

    @staticmethod
    async def _resume_with_result[Result](
        hook: AsyncGenerator[Any, Result], result: Result
    ) -> None:
        try:
            await hook.asend(result)
        except StopAsyncIteration:
            return
        await hook.aclose()
        raise HookProtocolError("Write hook must yield exactly once.")

    @staticmethod
    async def _resume_with_error(
        hook: AsyncGenerator[Any, Any], error: BaseException
    ) -> None:
        try:
            await hook.athrow(error)
        except StopAsyncIteration:
            return
        except BaseException as raised:
            if raised is error:
                return
            raise
        await hook.aclose()
        raise HookProtocolError("Write hook must yield exactly once.")


class MiddlewareEventStorageAdapter(EventStorageAdapter):
    def __init__(
        self,
        adapter: EventStorageAdapter,
        middleware: EventStoreWriteMiddlewareRegistry,
    ):
        self._adapter = adapter
        self._middleware = middleware

    @overload
    async def save[
        Name: StringPersistable,
        Payload: JsonPersistable,
        Metadata: JsonPersistable,
    ](
        self,
        *,
        target: StreamIdentifier,
        events: Sequence[NewEvent[Name, Payload, Metadata]],
        condition: WriteCondition = NoCondition(),
    ) -> Sequence[StoredEvent[Name, Payload, Metadata]]: ...

    @overload
    async def save[
        Name: StringPersistable,
        Payload: JsonPersistable,
        Metadata: JsonPersistable,
    ](
        self,
        *,
        target: CategoryIdentifier,
        streams: Mapping[
            str, StreamPublishDefinition[Name, Payload, Metadata]
        ],
    ) -> Mapping[str, Sequence[StoredEvent[Name, Payload, Metadata]]]: ...

    async def save[
        Name: StringPersistable,
        Payload: JsonPersistable,
        Metadata: JsonPersistable,
    ](
        self,
        *,
        target: Saveable,
        events: Sequence[NewEvent[Name, Payload, Metadata]] | None = None,
        condition: WriteCondition = NoCondition(),
        streams: Mapping[str, StreamPublishDefinition[Name, Payload, Metadata]]
        | None = None,
    ) -> (
        Sequence[StoredEvent[Name, Payload, Metadata]]
        | Mapping[str, Sequence[StoredEvent[Name, Payload, Metadata]]]
    ):
        # Middleware may rewrite events, so the caller's generic types are
        # trusted rather than proven from here on.
        match target:
            case StreamIdentifier():
                stored = await self._middleware.publish_to_stream(
                    StreamPublishRequest(
                        target=target,
                        events=events or [],
                        condition=condition,
                    ),
                    self._save_to_stream,
                )
                return cast(Any, stored)
            case CategoryIdentifier():
                stored = await self._middleware.publish_to_category(
                    CategoryPublishRequest(
                        target=target, streams=cast(Any, streams or {})
                    ),
                    self._save_to_category,
                )
                return cast(Any, stored)

    async def latest(
        self, *, target: Latestable
    ) -> StoredEvent[str, JsonValue, JsonValue] | None:
        return await self._adapter.latest(target=target)

    def scan(
        self,
        *,
        target: Scannable = LogIdentifier(),
        constraints: Set[constraints.QueryConstraint] = frozenset(),
    ) -> AsyncIterator[StoredEvent[str, JsonValue, JsonValue]]:
        return self._adapter.scan(target=target, constraints=constraints)

    async def _save_to_stream(
        self, request: StreamPublishRequest
    ) -> Sequence[AnyStoredEvent]:
        return await self._adapter.save(
            target=request.target,
            events=request.events,
            condition=request.condition,
        )

    async def _save_to_category(
        self, request: CategoryPublishRequest
    ) -> Mapping[str, Sequence[AnyStoredEvent]]:
        return await self._adapter.save(
            target=request.target, streams=request.streams
        )
