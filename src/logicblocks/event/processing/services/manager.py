import asyncio
import threading
from abc import ABC, abstractmethod
from asyncio import Future, Task
from collections.abc import Mapping, Sequence
from types import TracebackType
from typing import Any, Self, override
from uuid import uuid4

import uvloop

from ..process import (
    HasProcessStatus,
    ProcessStatus,
)
from .base import (
    DeferredFuture,
    ExecutionMode,
    IsolationMode,
    ManagedServiceState,
    Service,
    ServiceDefinition,
)


class ExecutableManagedServiceState[T](
    ManagedServiceState[T], ServiceDefinition[T]
):
    def __init__(
        self,
        service: Service[T],
        name: str,
        execution_mode: ExecutionMode,
        isolation_mode: IsolationMode,
    ):
        self._service = service
        self._name = name
        self._execution_mode = execution_mode
        self._isolation_mode = isolation_mode
        self._future: DeferredFuture[T] = DeferredFuture(name)

    @property
    def service(self) -> Service[T]:
        return self._service

    @property
    def name(self) -> str:
        return self._name

    @property
    def execution_mode(self) -> ExecutionMode:
        return self._execution_mode

    @property
    def isolation_mode(self) -> IsolationMode:
        return self._isolation_mode

    @property
    def service_status(self) -> ProcessStatus:
        return (
            self._service.status
            if isinstance(self._service, HasProcessStatus)
            else ProcessStatus.UNKNOWN
        )

    @property
    def future(self) -> DeferredFuture[T]:
        return self._future

    def coroutine(self):
        return self._service.execute()

    def register_future(self, fut: Future[T]):
        self._future._resolve(fut)

    def __repr__(self):
        return (
            f"{self.__class__.__name__}("
            f"name={self.name}, "
            f"service={self._service!r}, "
            f"execution_mode={self.execution_mode}, "
            f"isolation_mode={self.isolation_mode}"
            ")"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ManagedServiceState):
            return NotImplemented

        return (
            self.name == other.name
            and self._service == other.service
            and self.execution_mode == other.execution_mode
            and self.isolation_mode == other.isolation_mode
        )

    def __hash__(self) -> int:
        return hash((self.name, self.execution_mode, self.isolation_mode))


class ServiceExecutor(ABC):
    @abstractmethod
    async def start(self) -> Self:
        raise NotImplementedError

    @abstractmethod
    async def schedule[R = Any](
        self, definition: ServiceDefinition[R]
    ) -> Future[R]:
        raise NotImplementedError

    @abstractmethod
    async def stop(self) -> Self:
        raise NotImplementedError


class MainThreadServiceExecutor(ServiceExecutor):
    def __init__(self):
        self.service_tasks: set[Task[Any]] = set()

    @override
    async def start(self) -> Self:
        return self

    @override
    async def schedule[R = Any](
        self, definition: ServiceDefinition[R]
    ) -> Future[R]:
        task = asyncio.create_task(definition.coroutine())

        self.service_tasks.add(task)

        task.add_done_callback(self.service_tasks.discard)

        return task

    @override
    async def stop(self) -> Self:
        for task in self.service_tasks:
            task.cancel()
        await asyncio.gather(*self.service_tasks, return_exceptions=True)
        return self


class IsolatedThreadServiceExecutor(ServiceExecutor):
    def __init__(self):
        self._loop = uvloop.new_event_loop()
        self._thread = threading.Thread(target=self._start_event_loop)

    @override
    async def start(self) -> Self:
        self._thread.start()
        return self

    @override
    async def schedule[R = Any](
        self, definition: ServiceDefinition[R]
    ) -> Future[R]:
        return asyncio.wrap_future(
            asyncio.run_coroutine_threadsafe(
                definition.coroutine(), self._loop
            )
        )

    @override
    async def stop(self) -> Self:
        await asyncio.wrap_future(
            asyncio.run_coroutine_threadsafe(
                self._shutdown_services(), self._loop
            )
        )
        self._loop.call_soon_threadsafe(self._loop.stop)
        while self._loop.is_running():
            await asyncio.sleep(0)
        self._loop.close()
        self._thread.join()
        return self

    def _start_event_loop(self):
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    async def _shutdown_services(self):
        service_tasks = [
            task
            for task in asyncio.all_tasks(self._loop)
            if task is not asyncio.current_task()
        ]
        for task in service_tasks:
            task.cancel()
        await asyncio.gather(*service_tasks, return_exceptions=True)


class IsolationModeAwareServiceExecutor(ServiceExecutor):
    def __init__(self):
        self._main_executor = MainThreadServiceExecutor()
        self._shared_executor = IsolatedThreadServiceExecutor()
        self._all_executors: list[ServiceExecutor] = [
            self._main_executor,
            self._shared_executor,
        ]

    async def start(self) -> Self:
        await asyncio.gather(
            *[executor.start() for executor in self._all_executors]
        )
        return self

    async def schedule[R = Any](
        self, definition: ServiceDefinition[R]
    ) -> Future[R]:
        match definition.isolation_mode:
            case IsolationMode.MAIN_THREAD:
                return await self._main_executor.schedule(definition)
            case IsolationMode.SHARED_THREAD:
                return await self._shared_executor.schedule(definition)
            case IsolationMode.DEDICATED_THREAD:
                dedicated_executor = await self._prepare_dedicated_executor()
                return await dedicated_executor.schedule(definition)

    async def stop(self) -> Self:
        await asyncio.gather(
            *[executor.stop() for executor in self._all_executors]
        )
        return self

    async def _prepare_dedicated_executor(self):
        executor = IsolatedThreadServiceExecutor()
        await executor.start()

        self._all_executors.append(executor)

        return executor


class ServiceManager:
    def __init__(self):
        self._service_states: dict[
            str, ExecutableManagedServiceState[Any]
        ] = {}
        self._stop_on_signals: list[int] = []
        self._service_executor = IsolationModeAwareServiceExecutor()

    @property
    def services(self) -> Mapping[str, ManagedServiceState[Any]]:
        return dict(self._service_states)

    def service(self, name: str) -> ManagedServiceState[Any] | None:
        return self._service_states.get(name)

    def _generate_default_service_name(self, service: Service[Any]) -> str:
        return uuid4().hex

    def register[T](
        self,
        service: Service[T],
        *,
        name: str | None = None,
        execution_mode: ExecutionMode = ExecutionMode.BACKGROUND,
        isolation_mode: IsolationMode = IsolationMode.MAIN_THREAD,
    ) -> Self:
        name = name or self._generate_default_service_name(service)

        if name in self._service_states:
            raise ValueError(
                f"Service with name '{name}' is already registered."
            )

        self._service_states[name] = ExecutableManagedServiceState[T](
            service=service,
            name=name,
            execution_mode=execution_mode,
            isolation_mode=isolation_mode,
        )

        return self

    def stop_on(self, signals: Sequence[int]) -> Self:
        self._stop_on_signals = [*self._stop_on_signals, *signals]
        return self

    async def __aenter__(self) -> Self:
        return await self.start()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None = None,
        exc_value: BaseException | None = None,
        traceback: TracebackType | None = None,
    ) -> bool:
        await self.stop()
        return False

    async def _execute_service(
        self, state: ExecutableManagedServiceState[Any]
    ):
        fut = await self._service_executor.schedule(state)
        state.register_future(fut)
        return fut

    async def start(self) -> Self:
        loop = asyncio.get_event_loop()
        for sig in self._stop_on_signals:
            loop.add_signal_handler(
                sig, lambda: asyncio.create_task(self.stop())
            )

        await self._service_executor.start()

        all_futures = {
            name: await self._execute_service(service_state)
            for name, service_state in self._service_states.items()
        }

        blocking_futures = (
            future
            for name, future in all_futures.items()
            if self._service_states[name].execution_mode
            == ExecutionMode.FOREGROUND
        )

        await asyncio.gather(*blocking_futures, return_exceptions=True)

        return self

    async def stop(self) -> Self:
        await self._service_executor.stop()
        return self
