from abc import ABC, abstractmethod
from typing import Sequence, Iterator

from ..types import NewEvent, StoredEvent


class StorageAdapter(ABC):
    @abstractmethod
    def save(
        self, *, category: str, stream: str, events: Sequence[NewEvent]
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
