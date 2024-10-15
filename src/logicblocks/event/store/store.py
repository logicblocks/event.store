from typing import Sequence

from logicblocks.event.store.adapters import StorageAdapter
from logicblocks.event.store.types import NewEvent, StoredEvent


class StreamEventStore(object):
    adapter: StorageAdapter
    category: str
    stream: str

    def __init__(self, adapter: StorageAdapter, category: str, stream: str):
        self.adapter = adapter
        self.category = category
        self.stream = stream

    def publish(self, *, events: Sequence[NewEvent]) -> None:
        self.adapter.save(
            category=self.category, stream=self.stream, events=events
        )

    def read(self) -> Sequence[StoredEvent]:
        return self.adapter.get_for_stream(
            category=self.category, stream=self.stream
        )


class EventStore(object):
    def __init__(self, adapter: StorageAdapter):
        self.adapter = adapter

    def stream(self, *, category: str, stream: str) -> StreamEventStore:
        return StreamEventStore(
            adapter=self.adapter, category=category, stream=stream
        )
