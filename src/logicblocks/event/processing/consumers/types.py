from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Sequence

from logicblocks.event.types import Event


class EventConsumer(ABC):
    @abstractmethod
    async def consume_all(self) -> None:
        raise NotImplementedError()


class EventProcessor[E: Event](ABC):
    @abstractmethod
    async def process_event(self, event: E) -> None:
        raise NotImplementedError()


class EventProcessorManager[E: Event](ABC):
    @abstractmethod
    def acknowledge(self, events: E | Sequence[E]) -> None:
        pass

    @abstractmethod
    async def commit(self, *, force: bool = False) -> None:
        pass


type EventIterator[E: Event] = AsyncIterator[E]


class AutoCommitEventIteratorProcessor[E: Event](ABC):
    @abstractmethod
    async def process(self, events: EventIterator[E]) -> None:
        raise NotImplementedError()


class ManagedEventIteratorProcessor[E: Event](ABC):
    @abstractmethod
    async def process(
        self, events: EventIterator[E], manager: EventProcessorManager[E]
    ) -> None:
        raise NotImplementedError()


type SupportedProcessors[E: Event] = (
    EventProcessor[E]
    | AutoCommitEventIteratorProcessor[E]
    | ManagedEventIteratorProcessor[E]
)
