from typing import Self
from uuid import uuid4
from collections import defaultdict
from collections.abc import Iterator, Sequence, Set

from logicblocks.event.store.adapters.base import StorageAdapter
from logicblocks.event.store.conditions import (
    WriteCondition,
    Target,
    Operator,
    WriteConditionDescriptor,
)
from logicblocks.event.store.exceptions import UnmetWriteConditionError
from logicblocks.event.types import NewEvent, StoredEvent

type StreamKey = tuple[str, str]
type CategoryKey = str
type EventPositionList = list[int]
type EventIndexDict[T] = defaultdict[T, EventPositionList]


class InMemoryStorageAdapter(StorageAdapter):
    _events: list[StoredEvent]
    _stream_index: EventIndexDict[StreamKey]
    _category_index: EventIndexDict[CategoryKey]

    def __init__(self):
        self._events = []
        self._stream_index = defaultdict(lambda: [])
        self._category_index = defaultdict(lambda: [])

    def save(
        self,
        *,
        category: str,
        stream: str,
        events: Sequence[NewEvent],
        conditions: Set[WriteCondition] = frozenset(),
    ) -> Sequence[StoredEvent]:
        category_key = category
        stream_key = (category, stream)

        stream_indices = self._stream_index[stream_key]
        stream_events = [self._events[i] for i in stream_indices]

        for condition in conditions:
            _assert_condition_met(condition, stream_events)

        last_global_position = len(self._events)
        last_stream_position = (
            -1 if len(stream_events) == 0 else stream_events[-1].position
        )

        new_global_positions = [
            last_global_position + i for i in range(len(events))
        ]
        new_stored_events = [
            StoredEvent(
                id=uuid4().hex,
                name=event.name,
                stream=stream,
                category=category,
                position=last_stream_position + count + 1,
                payload=event.payload,
                observed_at=event.observed_at,
                occurred_at=event.occurred_at,
            )
            for event, count in zip(events, range(len(events)))
        ]

        self._events += new_stored_events
        self._stream_index[stream_key] += new_global_positions
        self._category_index[category_key] += new_global_positions

        return new_stored_events

    def scan_stream(
        self, *, category: str, stream: str
    ) -> Iterator[StoredEvent]:
        for global_position in self._stream_index[(category, stream)]:
            yield self._events[global_position]

    def scan_category(self, *, category: str) -> Iterator[StoredEvent]:
        for global_position in self._category_index[category]:
            yield self._events[global_position]

    def scan_all(self) -> Iterator[StoredEvent]:
        return iter(self._events)


def _assert_condition_met(
    condition: WriteCondition, events: Sequence[StoredEvent]
) -> None:
    (condition.describe(_InMemoryWriteConditionDescriptor()).check(events))


class _InMemoryWriteConditionDescriptor(WriteConditionDescriptor):
    _target: Target | None = None
    _attribute: str | None = None
    _operator: Operator | None = None
    _value: object | None = None

    def __init__(self):
        self._target = None
        self._attribute = None
        self._operator = None

    def target(self, target: Target) -> Self:
        self._target = target
        return self

    def attribute(self, attribute: str) -> Self:
        self._attribute = attribute
        return self

    def operator(self, operator: Operator) -> Self:
        self._operator = operator
        return self

    def value(self, value: object) -> Self:
        self._value = value
        return self

    def check(self, events: Sequence[StoredEvent]) -> None:
        if not self._target:
            raise ValueError("target not set")
        if (
            self._attribute is None
            or self._operator is None
            or self._value is None
        ):
            raise ValueError("attribute, operator, or value not set")
        if not self._operator == "equals":
            raise ValueError("unsupported operator")

        match self._target:
            case "last_event":
                if len(events) == 0:
                    raise UnmetWriteConditionError("no events")
                if not getattr(events[-1], self._attribute) == self._value:
                    raise UnmetWriteConditionError(
                        "last event does not satisfy condition"
                    )
                return
            case "stream":
                if not self._attribute == "length":
                    raise ValueError("unsupported stream attribute")
                if not len(events) == self._value:
                    raise UnmetWriteConditionError(
                        "last event does not satisfy condition"
                    )
