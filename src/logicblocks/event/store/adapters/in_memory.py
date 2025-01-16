import threading
from collections import defaultdict
from collections.abc import AsyncIterator, Sequence, Set
from uuid import uuid4

from logicblocks.event.store.adapters.base import (
    EventStorageAdapter,
    Saveable,
    Scannable,
)
from logicblocks.event.store.conditions import (
    WriteCondition,
)
from logicblocks.event.store.constraints import QueryConstraint
from logicblocks.event.types import (
    CategoryIdentifier,
    LogIdentifier,
    NewEvent,
    StoredEvent,
    StreamIdentifier,
)

type StreamKey = tuple[str, str]
type CategoryKey = str
type EventPositionList = list[int]
type EventIndexDict[T] = defaultdict[T, EventPositionList]


class InMemoryEventStorageAdapter(EventStorageAdapter):
    _lock: threading.Lock
    _events: list[StoredEvent]
    _log_index: EventPositionList
    _stream_index: EventIndexDict[StreamKey]
    _category_index: EventIndexDict[CategoryKey]

    def __init__(self):
        self._lock = threading.Lock()
        self._events = []
        self._log_index = []
        self._stream_index = defaultdict(lambda: [])
        self._category_index = defaultdict(lambda: [])

    async def save(
        self,
        *,
        target: Saveable,
        events: Sequence[NewEvent],
        conditions: Set[WriteCondition] = frozenset(),
    ) -> Sequence[StoredEvent]:
        category_key = target.category
        stream_key = (target.category, target.stream)

        with self._lock:
            stream_indices = self._stream_index[stream_key]
            stream_events = [self._events[i] for i in stream_indices]

            last_event = stream_events[-1] if stream_events else None

            for condition in conditions:
                condition.assert_met_by(last_event=last_event)

            last_sequence_number = len(self._events)
            last_stream_position = (
                -1 if len(stream_events) == 0 else stream_events[-1].position
            )

            new_sequence_numbers = [
                last_sequence_number + i for i in range(len(events))
            ]
            new_stored_events = [
                StoredEvent(
                    id=uuid4().hex,
                    name=event.name,
                    stream=target.stream,
                    category=target.category,
                    position=last_stream_position + count + 1,
                    sequence_number=last_sequence_number + count,
                    payload=event.payload,
                    observed_at=event.observed_at,
                    occurred_at=event.occurred_at,
                )
                for event, count in zip(events, range(len(events)))
            ]

            self._events += new_stored_events
            self._log_index += new_sequence_numbers
            self._stream_index[stream_key] += new_sequence_numbers
            self._category_index[category_key] += new_sequence_numbers

            return new_stored_events

    async def scan(
        self,
        *,
        target: Scannable = LogIdentifier(),
        constraints: Set[QueryConstraint] = frozenset(),
    ) -> AsyncIterator[StoredEvent]:
        index = self._select_index(target)
        for sequence_number in index:
            event = self._events[sequence_number]
            if not all(
                constraint.met_by(event=event) for constraint in constraints
            ):
                continue
            yield self._events[sequence_number]

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
