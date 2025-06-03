from abc import abstractmethod
from types import NoneType

from ..process import Process
from ..services import Service
from .types import EventSubscriber


class EventBroker(Service[NoneType], Process):
    @abstractmethod
    async def register(self, subscriber: EventSubscriber) -> None:
        raise NotImplementedError
