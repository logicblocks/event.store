from abc import ABC, abstractmethod
from typing import Sequence

from ..types import NewEvent, StoredEvent


class StorageAdapter(ABC):
    @abstractmethod
    def save(
        self, *, category: str, stream: str, events: Sequence[NewEvent]
    ) -> None:
        raise NotImplementedError()

    @abstractmethod
    def get_for_stream(
        self, *, category: str, stream: str
    ) -> Sequence[StoredEvent]:
        raise NotImplementedError()

    @abstractmethod
    def get_for_category(self, *, category: str) -> Sequence[StoredEvent]:
        raise NotImplementedError()

    @abstractmethod
    def get_all(self) -> Sequence[StoredEvent]:
        raise NotImplementedError()
