from abc import abstractmethod
from types import NoneType

from ...services import Service
from ..process import Process
from ..types import EventSubscriber


class EventBroker(Service[NoneType], Process):
    @abstractmethod
    async def register(self, subscriber: EventSubscriber) -> None:
        raise NotImplementedError
