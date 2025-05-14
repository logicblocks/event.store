import copy
from collections import defaultdict
from collections.abc import AsyncIterator, Set
from typing import Self, cast

from logicblocks.event.types import (
    CategoryIdentifier,
    Converter,
    JsonValue,
    LogIdentifier,
    StoredEvent,
    StreamIdentifier,
)

from ...constraints import QueryConstraint
from ..base import (
    Latestable,
    Saveable,
    Scannable,
)
from .types import QueryConstraintCheck

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
        events: list[StoredEvent[str, JsonValue] | None] | None = None,
        log_index: EventPositionList | None = None,
        category_index: EventIndexDict[CategoryKey] | None = None,
        stream_index: EventIndexDict[StreamKey] | None = None,
        constraint_converter: Converter[QueryConstraint, QueryConstraintCheck],
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
        self._constraint_converter = constraint_converter

    def snapshot(self) -> Self:
        return self.__class__(
            events=list(self._events),
            log_index=list(self._log_index),
            category_index=copy.deepcopy(self._category_index),
            stream_index=copy.deepcopy(self._stream_index),
            constraint_converter=self._constraint_converter,
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
                self._constraint_converter.convert(constraint)(event)
                for constraint in constraints
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
