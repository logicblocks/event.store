from abc import abstractmethod
from types import NoneType

from logicblocks.event.types import BaseEvent

from ..process import Process
from ..services import Service
from .types import EventSubscriber


class EventBroker(Service[NoneType], Process):
    @abstractmethod
    async def register[E: BaseEvent](
        self, subscriber: EventSubscriber[E]
    ) -> None:
        raise NotImplementedError
