from abc import abstractmethod
from types import NoneType

from logicblocks.event.types import Event

from ..process import Process
from ..services import Service
from .types import EventSubscriber


class EventBroker[E: Event](Service[NoneType], Process):
    @abstractmethod
    async def register(self, subscriber: EventSubscriber[E]) -> None:
        raise NotImplementedError
