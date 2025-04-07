from abc import abstractmethod
from enum import StrEnum
from types import NoneType

from ...services import Service
from ..types import EventSubscriber


class EventBrokerStatus(StrEnum):
    INITIALISED = "initialised"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERRORED = "errored"


class EventBroker(Service[NoneType]):
    @property
    def status(self) -> EventBrokerStatus:
        raise NotImplementedError

    @abstractmethod
    async def register(self, subscriber: EventSubscriber) -> None:
        raise NotImplementedError
