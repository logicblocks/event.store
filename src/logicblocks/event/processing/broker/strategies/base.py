from abc import abstractmethod
from enum import StrEnum
from types import NoneType

from ...services import Service
from ..process import Process
from ..types import EventSubscriber


class ProcessStatus(StrEnum):
    INITIALISED = "initialised"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERRORED = "errored"


class EventBroker(Service[NoneType], Process):
    @abstractmethod
    async def register(self, subscriber: EventSubscriber) -> None:
        raise NotImplementedError
