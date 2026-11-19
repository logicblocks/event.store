from abc import ABC, abstractmethod
from enum import Enum, auto
from typing import Any, Coroutine

from ..process import ProcessStatus
from .deferred_future import DeferredFuture


class Service[T = Any](ABC):
    """Base class for long-running or one-off units of work.

    Services are typically registered with and run by a `ServiceManager`.
    """

    @abstractmethod
    async def execute(self) -> T:
        """Run the service.

        Returns:
            the result of the service, if any.
        """
        raise NotImplementedError()


class ExecutionMode(Enum):
    """Whether a `ServiceManager` waits for a service when starting.

    Attributes:
        FOREGROUND: `ServiceManager.start` waits for the service to finish.
        BACKGROUND: The service runs without `ServiceManager.start` waiting
            for it.
    """

    FOREGROUND = auto()
    BACKGROUND = auto()


class IsolationMode(Enum):
    """Which thread and event loop a `ServiceManager` runs a service on.

    Attributes:
        MAIN_THREAD: The event loop that started the service manager.
        SHARED_THREAD: A separate thread and event loop shared by all
            services with this isolation mode.
        DEDICATED_THREAD: A separate thread and event loop for this service
            alone.
    """

    MAIN_THREAD = auto()
    SHARED_THREAD = auto()
    DEDICATED_THREAD = auto()


class ManagedServiceState[T](ABC):
    """The state of a service registered with a `ServiceManager`."""

    @property
    @abstractmethod
    def service(self) -> Service[T]: ...

    @property
    @abstractmethod
    def name(self) -> str: ...

    @property
    @abstractmethod
    def execution_mode(self) -> ExecutionMode: ...

    @property
    @abstractmethod
    def isolation_mode(self) -> IsolationMode: ...

    @property
    @abstractmethod
    def service_status(self) -> ProcessStatus:
        """The service's status, or `UNKNOWN` if it does not report one."""
        ...

    @property
    @abstractmethod
    def future(self) -> DeferredFuture[T]:
        """A future for the result of the service once it is scheduled."""
        ...


class ServiceDefinition[T](ABC):
    """Describes how a service executor should run a service."""

    @property
    @abstractmethod
    def name(self) -> str: ...

    @property
    @abstractmethod
    def execution_mode(self) -> ExecutionMode: ...

    @property
    @abstractmethod
    def isolation_mode(self) -> IsolationMode: ...

    @abstractmethod
    def coroutine(self) -> Coroutine[Any, Any, T]:
        """Return a new coroutine executing the service."""
        ...
