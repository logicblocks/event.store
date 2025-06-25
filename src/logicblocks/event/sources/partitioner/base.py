from abc import ABC, abstractmethod
from collections.abc import Sequence

from logicblocks.event.types import EventSourceIdentifier


class EventSourcePartitioner(ABC):
    @abstractmethod
    def partition(
        self, identifiers: Sequence[EventSourceIdentifier]
    ) -> Sequence[EventSourceIdentifier]:
        raise NotImplementedError
