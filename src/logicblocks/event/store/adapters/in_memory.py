import threading

from uuid import uuid4
from collections import defaultdict
from collections.abc import Iterator, Sequence, Set

from logicblocks.event.store.adapters.base import (
    StorageAdapter,
    Scannable,
    Saveable,
)
from logicblocks.event.store.conditions import (
    WriteCondition,
)
from logicblocks.event.types import (
    NewEvent,
    StoredEvent,
    identifier,
)

type StreamKey = tuple[str, str]
type CategoryKey = str
type EventPositionList = list[int]
type EventIndexDict[T] = defaultdict[T, EventPositionList]


class InMemoryStorageAdapter(StorageAdapter):
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

    def save(
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
                condition.ensure(last_event)

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

    def scan(
        self, *, target: Scannable = identifier.Log()
    ) -> Iterator[StoredEvent]:
        index = self._select_index(target)
        for sequence_number in index:
            yield self._events[sequence_number]

    def _select_index(self, target: Scannable) -> EventPositionList:
        if isinstance(target, identifier.Category):
            return self._category_index[target.category]

        if isinstance(target, identifier.Stream):
            return self._stream_index[(target.category, target.stream)]

        return self._log_index
