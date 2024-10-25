from uuid import uuid4
from collections import defaultdict
from collections.abc import Iterator, Sequence, Set

from logicblocks.event.store.adapters.base import StorageAdapter
from logicblocks.event.store.conditions import (
    WriteCondition,
)
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

        last_event = self._events[-1] if self._events else None

        for condition in conditions:
            condition.evaluate(last_event)

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
