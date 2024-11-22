from abc import ABC, abstractmethod
from collections.abc import Sequence, Set, Iterator

from logicblocks.event.store.conditions import WriteCondition
from logicblocks.event.types import identifier, NewEvent, StoredEvent

# Listable = identifier.Categories | identifier.Streams
# Readable = identifier.Log | identifier.Category | identifier.Stream
Saveable = identifier.Stream
Scannable = identifier.Log | identifier.Category | identifier.Stream


class StorageAdapter(ABC):
    @abstractmethod
    def save(
        self,
        *,
        target: Saveable,
        events: Sequence[NewEvent],
        conditions: Set[WriteCondition] = frozenset(),
    ) -> Sequence[StoredEvent]:
        raise NotImplementedError()

    @abstractmethod
    def scan(
        self,
        *,
        target: Scannable = identifier.Log(),
    ) -> Iterator[StoredEvent]:
        raise NotImplementedError()
