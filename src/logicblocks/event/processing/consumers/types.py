from abc import ABC, abstractmethod

from logicblocks.event.store import EventSource
from logicblocks.event.types.event import StoredEvent


class EventBroker(ABC):
    @abstractmethod
    async def register(self, subscriber: "EventSubscriber") -> None:
        raise NotImplementedError()


class EventConsumer(ABC):
    @abstractmethod
    async def consume_all(self) -> None:
        raise NotImplementedError()


class EventSubscriber(ABC):
    @abstractmethod
    async def subscribe(self, broker: EventBroker) -> None:
        raise NotImplementedError()

    @abstractmethod
    async def receive(self, source: EventSource) -> None:
        raise NotImplementedError()

    @abstractmethod
    async def revoke(self, source: EventSource) -> None:
        raise NotImplementedError()


class EventProcessor(ABC):
    @abstractmethod
    async def process_event(self, event: StoredEvent) -> None:
        raise NotImplementedError()
