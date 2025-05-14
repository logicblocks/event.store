import asyncio
from collections import defaultdict
from collections.abc import AsyncIterator, Sequence, Set
from uuid import uuid4

from aiologic import Lock

from logicblocks.event.types import (
    Converter,
    JsonPersistable,
    JsonValue,
    LogIdentifier,
    NewEvent,
    StoredEvent,
    StringPersistable,
    serialise_to_json_value,
    serialise_to_string,
)

from ...conditions import (
    NoCondition,
    WriteCondition,
)
from ...constraints import QueryConstraint
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
from .types import QueryConstraintCheck


class InMemoryEventStorageAdapter(EventStorageAdapter):
    def __init__(
        self,
        *,
        serialisation_guarantee: EventSerialisationGuarantee = EventSerialisationGuarantee.LOG,
        constraint_converter: Converter[QueryConstraint, QueryConstraintCheck]
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

    async def save[Name: StringPersistable, Payload: JsonPersistable](
        self,
        *,
        target: Saveable,
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

        async for event in snapshot.scan_events(target, constraints):
            await asyncio.sleep(0)
            yield event
