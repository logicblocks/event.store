from abc import abstractmethod
from types import NoneType

from logicblocks.event.types import BaseEvent

from ..process import Process
from ..services import Service
from .types import EventSubscriber


class EventBroker[E: BaseEvent](Service[NoneType], Process):
    @abstractmethod
    async def register(self, subscriber: EventSubscriber[E]) -> None:
        raise NotImplementedError
