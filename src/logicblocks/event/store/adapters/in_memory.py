from typing import Sequence, Tuple, DefaultDict, List, NewType
from collections import defaultdict

from .base import StorageAdapter
from ..types import NewEvent, StoredEvent

StreamKey = NewType("StreamKey", Tuple[str, str])
StoredEventList = NewType("StoredEventList", List[StoredEvent])
EventStoreStateDict = DefaultDict[StreamKey, StoredEventList]


class InMemoryStorageAdapter(StorageAdapter):
    _state: EventStoreStateDict

    def __init__(self):
        self._state = defaultdict(lambda: StoredEventList([]))

    def save(
        self, *, category: str, stream: str, events: Sequence[NewEvent]
    ) -> None:
        stream_key = StreamKey((category, stream))
        self._state[stream_key].extend(
            [
                StoredEvent(
                    name=event.name,
                    stream=stream,
                    category=category,
                    position=0,
                    payload=event.payload,
                    observed_at=event.observed_at,
                    occurred_at=event.occurred_at,
                )
                for event in events
            ]
        )

    def get_for_stream(
        self, *, category: str, stream: str
    ) -> Sequence[StoredEvent]:
        return self._state[StreamKey((category, stream))]

    def get_for_category(self, *, category: str) -> Sequence[StoredEvent]:
        return self._state[StreamKey((category, ""))]

    def get_all(self) -> Sequence[StoredEvent]:
        return self._state[StreamKey(("", ""))]
