import asyncio
from collections import defaultdict
from collections.abc import (
    AsyncGenerator,
    AsyncIterator,
    Mapping,
    Sequence,
    Set,
)
from typing import overload
from uuid import uuid4

from aiologic import Lock

from logicblocks.event.sources.constraints import QueryConstraint
from logicblocks.event.types import (
    CategoryIdentifier,
    Converter,
    JsonPersistable,
    JsonValue,
    LogIdentifier,
    NewEvent,
    StoredEvent,
    StreamIdentifier,
    StringPersistable,
    serialise_to_json_value,
    serialise_to_string,
)

from ...conditions import (
    NoCondition,
    WriteCondition,
)
from ...types import StreamPublishDefinition
from ..base import (
    EventSerialisationGuarantee,
    EventStorageAdapter,
    Latestable,
    Saveable,
    Scannable,
)
from .converters import (
    TypeRegistryConditionConverter,
    TypeRegistryConstraintConverter,
    WriteConditionEnforcer,
    WriteConditionEnforcerContext,
)
from .db import InMemoryEventsDB, InMemorySequence
from .locks import MultiLock
from .types import QueryConstraintCheck


class InMemoryEventStorageAdapter(EventStorageAdapter):
    def __init__(
        self,
        *,
        serialisation_guarantee: EventSerialisationGuarantee[
            CategoryIdentifier | StreamIdentifier | LogIdentifier
        ] = EventSerialisationGuarantee.LOG,
        constraint_converter: Converter[
            QueryConstraint, QueryConstraintCheck[StoredEvent]
        ]
        | None = None,
        condition_converter: Converter[WriteCondition, WriteConditionEnforcer]
        | None = None,
    ):
        self._constraint_converter = (
            constraint_converter
            if constraint_converter is not None
            else (
                TypeRegistryConstraintConverter().with_default_constraint_converters()
            )
        )
        self._condition_converter = (
            condition_converter
            if condition_converter is not None
            else (
                TypeRegistryConditionConverter().with_default_condition_converters()
            )
        )
        self._locks: dict[str, Lock] = defaultdict(lambda: Lock())
        self._sequence = InMemorySequence()
        self._db = InMemoryEventsDB(
            events=None,
            log_index=None,
            category_index=None,
            stream_index=None,
            constraint_converter=self._constraint_converter,
        )
        self._serialisation_guarantee = serialisation_guarantee

    def _lock_name(self, target: Saveable) -> str:
        return self._serialisation_guarantee.lock_name(
            namespace="memory", target=target
        )

    def _determine_required_locks(
        self,
        target: CategoryIdentifier,
        streams: Mapping[
            str, StreamPublishDefinition[StringPersistable, JsonPersistable]
        ],
    ) -> list[str]:
        match self._serialisation_guarantee:
            case EventSerialisationGuarantee.STREAM:
                return [
                    self._lock_name(
                        StreamIdentifier(
                            category=target.category, stream=stream_name
                        )
                    )
                    for stream_name in sorted(streams.keys())
                ]
            case _:
                return [self._lock_name(target)]

    @overload
    async def save[Name: StringPersistable, Payload: JsonPersistable](
        self,
        *,
        target: StreamIdentifier,
        events: Sequence[NewEvent[Name, Payload]],
        condition: WriteCondition = NoCondition(),
    ) -> Sequence[StoredEvent[Name, Payload]]: ...

    @overload
    async def save[Name: StringPersistable, Payload: JsonPersistable](
        self,
        *,
        target: CategoryIdentifier,
        streams: Mapping[str, StreamPublishDefinition[Name, Payload]],
    ) -> Mapping[str, Sequence[StoredEvent[Name, Payload]]]: ...

    async def save[Name: StringPersistable, Payload: JsonPersistable](
        self,
        *,
        target: Saveable,
        events: Sequence[NewEvent[Name, Payload]] | None = None,
        condition: WriteCondition = NoCondition(),
        streams: Mapping[str, StreamPublishDefinition[Name, Payload]]
        | None = None,
    ) -> (
        Sequence[StoredEvent[Name, Payload]]
        | Mapping[str, Sequence[StoredEvent[Name, Payload]]]
    ):
        match target:
            case StreamIdentifier():
                if events is None:
                    raise ValueError(
                        "The `events` parameter must be provided for "
                        "stream level publish."
                    )
                return await self._save_to_stream(
                    target=target, events=events, condition=condition
                )
            case CategoryIdentifier():
                if streams is None:
                    raise ValueError(
                        "The `streams` parameter must be provided for "
                        "category level publish."
                    )
                return await self._save_to_category(
                    target=target, streams=streams
                )
            case _:
                raise ValueError(f"Unsupported target type: {type(target)}")

    async def _save_to_stream[
        Name: StringPersistable,
        Payload: JsonPersistable,
    ](
        self,
        *,
        target: StreamIdentifier,
        events: Sequence[NewEvent[Name, Payload]],
        condition: WriteCondition = NoCondition(),
    ) -> Sequence[StoredEvent[Name, Payload]]:
        # note: we call `asyncio.sleep(0)` to yield the event loop at similar
        #       points in the save operation as a DB backed implementation would
        #       in order to keep the implementations as equivalent as possible.
        async with self._locks[self._lock_name(target=target)]:
            transaction = self._db.transaction()
            await asyncio.sleep(0)

            last_stream_event = transaction.last_stream_event(target)
            await asyncio.sleep(0)

            enforcer = self._condition_converter.convert(condition)
            enforcer.assert_satisfied(
                context=WriteConditionEnforcerContext(
                    identifier=target, latest_event=last_stream_event
                ),
                transaction=transaction,
            )

            last_stream_position = transaction.last_stream_position(target)

            new_stored_events: list[StoredEvent[Name, Payload]] = []
            for new_event, count in zip(events, range(len(events))):
                new_stored_event = StoredEvent[Name, Payload](
                    id=uuid4().hex,
                    name=new_event.name,
                    stream=target.stream,
                    category=target.category,
                    position=last_stream_position + count + 1,
                    sequence_number=next(self._sequence),
                    payload=new_event.payload,
                    observed_at=new_event.observed_at,
                    occurred_at=new_event.occurred_at,
                )
                serialised_stored_event = StoredEvent[str, JsonValue](
                    id=new_stored_event.id,
                    name=serialise_to_string(new_stored_event.name),
                    stream=new_stored_event.stream,
                    category=new_stored_event.category,
                    position=new_stored_event.position,
                    sequence_number=new_stored_event.sequence_number,
                    payload=serialise_to_json_value(new_stored_event.payload),
                    observed_at=new_stored_event.observed_at,
                    occurred_at=new_stored_event.occurred_at,
                )
                transaction.add(serialised_stored_event)
                new_stored_events.append(new_stored_event)
                await asyncio.sleep(0)

            transaction.commit()

            return new_stored_events

    async def _save_to_category[
        Name: StringPersistable,
        Payload: JsonPersistable,
    ](
        self,
        *,
        target: CategoryIdentifier,
        streams: Mapping[str, StreamPublishDefinition[Name, Payload]],
    ) -> Mapping[str, Sequence[StoredEvent[Name, Payload]]]:
        # note: we call `asyncio.sleep(0)` to yield the event loop at similar
        #       points in the save operation as a DB backed implementation would
        #       in order to keep the implementations as equivalent as possible.
        required_lock_names = self._determine_required_locks(target, streams)
        required_locks = [
            self._locks[lock_name] for lock_name in required_lock_names
        ]
        async with MultiLock(required_locks):
            transaction = self._db.transaction()
            await asyncio.sleep(0)

            results: dict[str, Sequence[StoredEvent[Name, Payload]]] = {}

            for stream_name in sorted(streams.keys()):
                stream_request = streams[stream_name]
                stream_target = StreamIdentifier(
                    category=target.category, stream=stream_name
                )

                condition = stream_request.get("condition", NoCondition())
                events = stream_request["events"]

                last_stream_event = transaction.last_stream_event(
                    stream_target
                )
                await asyncio.sleep(0)

                enforcer = self._condition_converter.convert(condition)
                enforcer.assert_satisfied(
                    context=WriteConditionEnforcerContext(
                        identifier=stream_target,
                        latest_event=last_stream_event,
                    ),
                    transaction=transaction,
                )

                last_stream_position = transaction.last_stream_position(
                    stream_target
                )

                new_stored_events: list[StoredEvent[Name, Payload]] = []
                for new_event, count in zip(events, range(len(events))):
                    new_stored_event = StoredEvent[Name, Payload](
                        id=uuid4().hex,
                        name=new_event.name,
                        stream=stream_target.stream,
                        category=stream_target.category,
                        position=last_stream_position + count + 1,
                        sequence_number=next(self._sequence),
                        payload=new_event.payload,
                        observed_at=new_event.observed_at,
                        occurred_at=new_event.occurred_at,
                    )
                    serialised_stored_event = StoredEvent[str, JsonValue](
                        id=new_stored_event.id,
                        name=serialise_to_string(new_stored_event.name),
                        stream=new_stored_event.stream,
                        category=new_stored_event.category,
                        position=new_stored_event.position,
                        sequence_number=new_stored_event.sequence_number,
                        payload=serialise_to_json_value(
                            new_stored_event.payload
                        ),
                        observed_at=new_stored_event.observed_at,
                        occurred_at=new_stored_event.occurred_at,
                    )
                    transaction.add(serialised_stored_event)
                    new_stored_events.append(new_stored_event)
                    await asyncio.sleep(0)

                results[stream_name] = new_stored_events

            transaction.commit()

            return results

    async def latest(
        self, *, target: Latestable
    ) -> StoredEvent[str, JsonValue] | None:
        snapshot = self._db.snapshot()
        await asyncio.sleep(0)

        return snapshot.last_event(target)

    async def scan(
        self,
        *,
        target: Scannable = LogIdentifier(),
        constraints: Set[QueryConstraint] = frozenset(),
    ) -> AsyncIterator[StoredEvent[str, JsonValue]]:
        snapshot = self._db.snapshot()

        async_generator = snapshot.scan_events(target, constraints)
        try:
            async for event in async_generator:
                await asyncio.sleep(0)
                yield event
        finally:
            if isinstance(async_generator, AsyncGenerator):
                await async_generator.aclose()
