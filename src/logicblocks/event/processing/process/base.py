from abc import ABC, abstractmethod
from enum import StrEnum
from typing import Protocol, runtime_checkable


class ProcessStatus(StrEnum):
    """The lifecycle status of a process."""

    INITIALISED = "initialised"
    STARTING = "starting"
    WAITING = "waiting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"
    ERRORED = "errored"
    UNKNOWN = "unknown"


@runtime_checkable
class HasProcessStatus(Protocol):
    """Protocol for objects that report a process status."""

    @property
    def status(self) -> ProcessStatus: ...


class Process(ABC):
    """Base class for processes that report a status."""

    @property
    @abstractmethod
    def status(self) -> ProcessStatus:
        """The current status of the process."""
        raise NotImplementedError
