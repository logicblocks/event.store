from abc import ABC, abstractmethod
from enum import StrEnum
from typing import Protocol, runtime_checkable


class ProcessStatus(StrEnum):
    INITIALISED = "initialised"
    STARTING = "starting"
    WAITING = "waiting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERRORED = "errored"


@runtime_checkable
class HasProcessStatus(Protocol):
    @property
    def status(self) -> ProcessStatus: ...


class Process(ABC):
    @property
    @abstractmethod
    def status(self) -> ProcessStatus:
        raise NotImplementedError
