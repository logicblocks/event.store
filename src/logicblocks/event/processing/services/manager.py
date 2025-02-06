import asyncio
import threading
from asyncio import Future, Task
from collections.abc import Coroutine, Sequence
from enum import Enum, auto
from typing import Any, Self

import uvloop

from .types import Service


class ExecutionMode(Enum):
    FOREGROUND = auto()
    BACKGROUND = auto()


class IsolationMode(Enum):
    MAIN_THREAD = auto()
    SHARED_THREAD = auto()
    DEDICATED_THREAD = auto()


class ServiceDefinition[T]:
    def __init__(
        self,
        service: Service[T],
        execution_mode: ExecutionMode,
        isolation_mode: IsolationMode,
    ):
        self.service = service
        self.execution_mode = execution_mode
        self.isolation_mode = isolation_mode

    def coroutine(self) -> Coroutine[Any, Any, T]:
        return self.service.execute()


class MainThreadServiceExecutor:
    def __init__(self):
        self.service_tasks: set[Task[Any]] = set()

    async def start(self) -> Self:
        return self

    async def schedule[R = Any](
        self, definition: ServiceDefinition[R]
    ) -> Future[R]:
        task = asyncio.create_task(definition.coroutine())

        self.service_tasks.add(task)

        task.add_done_callback(self.service_tasks.discard)

        return task

    async def stop(self) -> Self:
        for task in self.service_tasks:
            task.cancel()
        await asyncio.gather(*self.service_tasks, return_exceptions=True)
        return self


class IsolatedThreadServiceExecutor:
    def __init__(self):
        self._loop = uvloop.new_event_loop()
        self._thread = threading.Thread(target=self._start_event_loop)

    async def start(self) -> Self:
        self._thread.start()
        return self

    async def schedule[R = Any](
        self, definition: ServiceDefinition[R]
    ) -> Future[R]:
        return asyncio.wrap_future(
            asyncio.run_coroutine_threadsafe(
                definition.coroutine(), self._loop
            )
        )

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


class IsolationModeAwareServiceExecutor:
    def __init__(self):
        self._main_executor = MainThreadServiceExecutor()
        self._shared_executor = IsolatedThreadServiceExecutor()
        self._all_executors = [self._main_executor, self._shared_executor]

    async def start(self) -> Self:
        await asyncio.gather(
            *[executor.start() for executor in self._all_executors]
        )
        return self

    async def schedule[R = Any](
        self, definition: ServiceDefinition[R]
    ) -> Future[R]:
        if definition.isolation_mode == IsolationMode.MAIN_THREAD:
            return await self._main_executor.schedule(definition)
        if definition.isolation_mode == IsolationMode.SHARED_THREAD:
            return await self._shared_executor.schedule(definition)

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
        self._service_definitions: list[ServiceDefinition[Any]] = []
        self._stop_on_signals: list[int] = []
        self._service_executor = IsolationModeAwareServiceExecutor()

    def register(
        self,
        service: Service,
        *,
        execution_mode: ExecutionMode = ExecutionMode.BACKGROUND,
        isolation_mode: IsolationMode = IsolationMode.MAIN_THREAD,
    ) -> Self:
        self._service_definitions.append(
            ServiceDefinition(service, execution_mode, isolation_mode)
        )
        return self

    def stop_on(self, signals: Sequence[int]) -> Self:
        self._stop_on_signals = [*self._stop_on_signals, *signals]
        return self

    async def start(self) -> list[Future[Any]]:
        loop = asyncio.get_event_loop()
        for sig in self._stop_on_signals:
            loop.add_signal_handler(
                sig, lambda: asyncio.create_task(self.stop())
            )

        await self._service_executor.start()

        all_futures = [
            await self._service_executor.schedule(service_definition)
            for service_definition in self._service_definitions
        ]
        blocking_futures = [
            future
            for future, definition in zip(
                all_futures, self._service_definitions
            )
            if definition.execution_mode == ExecutionMode.FOREGROUND
        ]

        await asyncio.gather(*blocking_futures, return_exceptions=True)

        return all_futures

    async def stop(self) -> Self:
        await self._service_executor.stop()

        return self
