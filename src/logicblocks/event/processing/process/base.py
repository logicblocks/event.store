from abc import abstractmethod
from enum import StrEnum


class ProcessStatus(StrEnum):
    INITIALISED = "initialised"
    STARTING = "starting"
    WAITING = "waiting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERRORED = "errored"


class Process:
    @property
    @abstractmethod
    def status(self) -> ProcessStatus:
        raise NotImplementedError
