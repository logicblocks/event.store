import asyncio
import copy
from collections import defaultdict
from collections.abc import AsyncIterator, Sequence, Set
from typing import Self, cast
from uuid import uuid4

from aiologic import Lock

from logicblocks.event.types import (
    CategoryIdentifier,
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

from ..conditions import (
    NoCondition,
    WriteCondition,
)
from ..constraints import QueryConstraint
from .base import (
    EventSerialisationGuarantee,
    EventStorageAdapter,
    Latestable,
    Saveable,
    Scannable,
)

type StreamKey = tuple[str, str]
type CategoryKey = str
type EventPositionList = list[int]
type EventIndexDict[T] = defaultdict[T, EventPositionList]


class InMemorySequence:
    def __init__(self, initial: int = 0):
        self._value = initial

    def __next__(self) -> int:
        value = self._value
        self._value += 1
        return value


class InMemoryEventsDB:
    def __init__(
        self,
        *,
        events: list[StoredEvent[str, JsonValue] | None] | None,
        log_index: EventPositionList | None,
        category_index: EventIndexDict[CategoryKey] | None,
        stream_index: EventIndexDict[StreamKey] | None,
    ):
        self._events: list[StoredEvent[str, JsonValue] | None] = (
            events if events is not None else []
        )
        self._log_index: EventPositionList = (
            log_index if log_index is not None else []
        )
        self._category_index: EventIndexDict[CategoryKey] = (
            category_index
            if category_index is not None
            else defaultdict(lambda: [])
        )
        self._stream_index: EventIndexDict[StreamKey] = (
            stream_index
            if stream_index is not None
            else defaultdict(lambda: [])
        )

    def snapshot(self) -> Self:
        return self.__class__(
            events=list(self._events),
            log_index=list(self._log_index),
            category_index=copy.deepcopy(self._category_index),
            stream_index=copy.deepcopy(self._stream_index),
        )

    def transaction(self) -> "InMemoryEventsDBTransaction":
        return InMemoryEventsDBTransaction(db=self)

    def stream_events(
        self, target: Saveable
    ) -> list[StoredEvent[str, JsonValue]]:
        stream_key = (target.category, target.stream)
        events = [self._events[i] for i in self._stream_index[stream_key]]
        if any(event is None for event in events):
            raise ValueError(
                f"Invalid state: stream {target.category}/{target.stream} "
                f"contains None values."
            )

        return cast(list[StoredEvent[str, JsonValue]], events)

    def last_stream_event(
        self, target: Saveable
    ) -> StoredEvent[str, JsonValue] | None:
        stream_events = self.stream_events(target)
        return stream_events[-1] if stream_events else None

    def last_stream_position(self, target: Saveable) -> int:
        last_stream_event = self.last_stream_event(target)
        return -1 if last_stream_event is None else last_stream_event.position

    def add(self, event: StoredEvent[str, JsonValue]) -> None:
        category_key = event.category
        stream_key = (event.category, event.stream)
        if len(self._events) <= event.sequence_number:
            self._events += [None] * (
                event.sequence_number - len(self._events) + 1
            )
        self._events[event.sequence_number] = event
        self._log_index += [event.sequence_number]
        self._stream_index[stream_key] += [event.sequence_number]
        self._category_index[category_key] += [event.sequence_number]

    def last_event(
        self, target: Latestable
    ) -> StoredEvent[str, JsonValue] | None:
        index = self._select_index(target)

        return self._events[index[-1]] if index else None

    async def scan_events(
        self,
        target: Scannable,
        constraints: Set[QueryConstraint] = frozenset(),
    ) -> AsyncIterator[StoredEvent[str, JsonValue]]:
        index = self._select_index(target)

        for sequence_number in index:
            event = self._events[sequence_number]
            if event is None:
                raise ValueError(
                    f"Invalid state: event at sequence number {sequence_number} "
                    f"is None"
                )
            if not all(
                constraint.met_by(event=event) for constraint in constraints
            ):
                continue
            yield event

    def _select_index(self, target: Scannable) -> EventPositionList:
        match target:
            case LogIdentifier():
                return self._log_index
            case CategoryIdentifier(category):
                return self._category_index[category]
            case StreamIdentifier(category, stream):
                return self._stream_index[(category, stream)]
            case _:  # pragma: no cover
                raise ValueError(f"Unknown target: {target}")


class InMemoryEventsDBTransaction:
    def __init__(self, db: InMemoryEventsDB):
        self._db = db
        self._added_events: list[StoredEvent[str, JsonValue]] = []

    def add(self, event: StoredEvent[str, JsonValue]) -> None:
        self._added_events.append(event)

    def commit(self) -> None:
        for event in self._added_events:
            self._db.add(event)

    def last_stream_event(
        self, target: Saveable
    ) -> StoredEvent[str, JsonValue] | None:
        return self._db.last_stream_event(target)

    def last_stream_position(self, target: Saveable) -> int:
        return self._db.last_stream_position(target)


class InMemoryEventStorageAdapter(EventStorageAdapter):
    def __init__(
        self,
        *,
        serialisation_guarantee: EventSerialisationGuarantee = EventSerialisationGuarantee.LOG,
    ):
        self._locks: dict[str, Lock] = defaultdict(lambda: Lock())
        self._sequence = InMemorySequence()
        self._db = InMemoryEventsDB(
            events=None,
            log_index=None,
            category_index=None,
            stream_index=None,
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
        condition: WriteCondition = NoCondition,
    ) -> Sequence[StoredEvent[Name, Payload]]:
        # note: we call `asyncio.sleep(0)` to yield the event loop at similar
        #       points in the save operation as a DB backed implementation would
        #       in order to keep the implementations as equivalent as possible.
        async with self._locks[self._lock_name(target=target)]:
            transaction = self._db.transaction()
            await asyncio.sleep(0)

            last_stream_event = transaction.last_stream_event(target)
            await asyncio.sleep(0)

            condition.assert_met_by(last_event=last_stream_event)

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
