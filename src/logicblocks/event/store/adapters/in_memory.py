from uuid import uuid4
from collections.abc import Iterator
from typing import Sequence, Tuple, DefaultDict, List, NewType, TypeVar
from collections import defaultdict

from .base import StorageAdapter
from ..types import NewEvent, StoredEvent

StreamKey = NewType("StreamKey", Tuple[str, str])
CategoryKey = NewType("CategoryKey", str)
EventPositionList = NewType("EventPositionList", List[int])
KeyType = TypeVar("KeyType")
EventIndexDict = DefaultDict[KeyType, EventPositionList]


class InMemoryStorageAdapter(StorageAdapter):
    _events: list[StoredEvent]
    _stream_index: EventIndexDict[StreamKey]
    _category_index: EventIndexDict[CategoryKey]

    def __init__(self):
        self._events = []
        self._stream_index = defaultdict(lambda: EventPositionList([]))
        self._category_index = defaultdict(lambda: EventPositionList([]))

    def save(
        self, *, category: str, stream: str, events: Sequence[NewEvent]
    ) -> Sequence[StoredEvent]:
        category_key = CategoryKey(category)
        stream_key = StreamKey((category, stream))

        stream_indices = self._stream_index[stream_key]

        last_global_position = len(self._events)
        last_stream_position = (
            -1
            if len(stream_indices) == 0
            else self._events[stream_indices[-1]].position
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
        for global_position in self._stream_index[
            StreamKey((category, stream))
        ]:
            yield self._events[global_position]

    def scan_category(self, *, category: str) -> Iterator[StoredEvent]:
        for global_position in self._category_index[CategoryKey(category)]:
            yield self._events[global_position]

    def scan_all(self) -> Iterator[StoredEvent]:
        return iter(self._events)
