from typing import Sequence, MutableSequence

from .base import StorageAdapter
from ..types import NewEvent, StoredEvent


class InMemoryStorageAdapter(StorageAdapter):
    def __init__(self):
        self._events: MutableSequence[StoredEvent] = []

    def save(
        self, *, category: str, stream: str, events: Sequence[NewEvent]
    ) -> None:
        self._events.extend(
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
        return self._events

    def get_for_category(self, *, category: str) -> Sequence[StoredEvent]:
        return self._events

    def get_all(self) -> Sequence[StoredEvent]:
        return self._events
