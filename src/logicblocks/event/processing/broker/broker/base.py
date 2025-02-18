from abc import abstractmethod
from types import NoneType

from ...services import Service
from ..types import EventSubscriber


class EventBroker(Service[NoneType]):
    @abstractmethod
    async def register(self, subscriber: EventSubscriber) -> None:
        raise NotImplementedError
