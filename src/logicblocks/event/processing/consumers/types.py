from abc import ABC, abstractmethod

from logicblocks.event.sources.base import BaseEvent


class EventConsumer[E: BaseEvent](ABC):
    @abstractmethod
    async def consume_all(self) -> None:
        raise NotImplementedError()


class EventProcessor[E: BaseEvent](ABC):
    @abstractmethod
    async def process_event(self, event: E) -> None:
        raise NotImplementedError()
