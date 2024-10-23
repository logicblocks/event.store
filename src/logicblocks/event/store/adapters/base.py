from abc import ABC, abstractmethod
from collections.abc import Sequence, Set, Iterator

from logicblocks.event.store.conditions import WriteCondition
from logicblocks.event.types import NewEvent, StoredEvent


class StorageAdapter(ABC):
    @abstractmethod
    def save(
        self,
        *,
        category: str,
        stream: str,
        events: Sequence[NewEvent],
        conditions: Set[WriteCondition] = frozenset(),
    ) -> Sequence[StoredEvent]:
        raise NotImplementedError()

    @abstractmethod
    def scan_stream(
        self, *, category: str, stream: str
    ) -> Iterator[StoredEvent]:
        raise NotImplementedError()

    @abstractmethod
    def scan_category(self, *, category: str) -> Iterator[StoredEvent]:
        raise NotImplementedError()

    @abstractmethod
    def scan_all(self) -> Iterator[StoredEvent]:
        raise NotImplementedError()
