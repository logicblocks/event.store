from abc import ABC, abstractmethod
from enum import Enum, auto
from typing import Any, Coroutine

from ..process import ProcessStatus
from .deferred_future import DeferredFuture


class Service[T = Any](ABC):
    @abstractmethod
    async def execute(self) -> T:
        raise NotImplementedError()


class ExecutionMode(Enum):
    FOREGROUND = auto()
    BACKGROUND = auto()


class IsolationMode(Enum):
    MAIN_THREAD = auto()
    SHARED_THREAD = auto()
    DEDICATED_THREAD = auto()


class ManagedServiceState[T](ABC):
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
    def service_status(self) -> ProcessStatus: ...

    @property
    @abstractmethod
    def future(self) -> DeferredFuture[T]: ...


class ServiceDefinition[T](ABC):
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
    def coroutine(self) -> Coroutine[Any, Any, T]: ...
