from typing import Sequence

from logicblocks.event.store.adapters import StorageAdapter
from logicblocks.event.store.types import NewEvent


class EventStore(object):
    def __init__(self, adapter: StorageAdapter):
        self.adapter = adapter

    def publish(
        self, *, category: str, stream: str, events: Sequence[NewEvent]
    ) -> None:
        pass
