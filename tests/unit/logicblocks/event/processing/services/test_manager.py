import asyncio
import os
import signal
import sys
import threading
import time
from asyncio import CancelledError
from collections.abc import Mapping
from contextlib import contextmanager
from typing import Any, cast
from uuid import UUID

import pytest

from logicblocks.event.processing import (
    ProcessStatus,
    Service,
    ServiceManager,
    StatusTrackingService,
)
from logicblocks.event.processing.services import (
    ExecutionMode,
    IsolationMode,
    ManagedServiceState,
)
from logicblocks.event.processing.services.manager import (
    ExecutableManagedServiceState,
)

# supervision
# schedules
# contexts
# service start and stop?


class TestServiceManagerExecutionModes:
    async def test_foreground_execution_mode_waits_for_completion(self):
        times = {}

        class TimeCapturingService(Service):
            async def execute(self):
                times["during"] = time.monotonic_ns()

        manager = ServiceManager()
        manager.register(
            TimeCapturingService(),
            execution_mode=ExecutionMode.FOREGROUND,
        )

        times["before"] = time.monotonic_ns()
        await manager.start()
        times["after"] = time.monotonic_ns()

        await manager.stop()

        assert times["during"] > times["before"]
        assert times["during"] < times["after"]

    async def test_future_is_available_before_start(self):
        class SimpleService(Service[int]):
            async def execute(self) -> int:
                return 42

        manager = ServiceManager()
        manager.register(
            SimpleService(),
            name="svc",
            execution_mode=ExecutionMode.FOREGROUND,
        )

        state = manager.service("svc")
        assert state is not None
        assert state.future is not None

        await manager.start()
        await manager.stop()

        assert state.future.result() == 42

    async def test_future_awaited_before_start_raises(self):
        class SimpleService(Service[int]):
            async def execute(self) -> int:
                return 42

        manager = ServiceManager()
        manager.register(
            SimpleService(),
            name="svc",
            execution_mode=ExecutionMode.FOREGROUND,
        )

        state = manager.service("svc")
        assert state is not None

        with pytest.raises(RuntimeError, match="has not been scheduled"):
            await state.future

    async def test_background_execution_mode_continues_before_completion(self):
        times = {}
        control = asyncio.Event()

        class TimeCapturingService(Service):
            async def execute(self):
                await asyncio.wait_for(control.wait(), timeout=0.1)
                times["during"] = time.monotonic_ns()

        manager = ServiceManager()
        manager.register(
            TimeCapturingService(),
            name="bg",
            execution_mode=ExecutionMode.BACKGROUND,
        )

        times["before"] = time.monotonic_ns()
        await manager.start()
        times["after"] = time.monotonic_ns()
        control.set()

        await manager.services["bg"].future
        await manager.stop()

        assert times["during"] > times["before"]
        assert times["during"] > times["after"]


class TestDeferredFuture:
    async def test_result_before_scheduling_raises(self):
        class SimpleService(Service[int]):
            async def execute(self) -> int:
                return 42

        manager = ServiceManager()
        manager.register(SimpleService(), name="svc")

        with pytest.raises(RuntimeError, match="has not been scheduled"):
            manager.services["svc"].future.result()

    async def test_exception_before_scheduling_raises(self):
        class SimpleService(Service[int]):
            async def execute(self) -> int:
                return 42

        manager = ServiceManager()
        manager.register(SimpleService(), name="svc")

        with pytest.raises(RuntimeError, match="has not been scheduled"):
            manager.services["svc"].future.exception()

    async def test_done_returns_false_before_scheduling(self):
        class SimpleService(Service[int]):
            async def execute(self) -> int:
                return 42

        manager = ServiceManager()
        manager.register(SimpleService(), name="svc")

        assert manager.services["svc"].future.done() is False

    async def test_done_returns_true_after_service_completes(self):
        class SimpleService(Service[int]):
            async def execute(self) -> int:
                return 42

        manager = ServiceManager()
        manager.register(
            SimpleService(),
            name="svc",
            execution_mode=ExecutionMode.FOREGROUND,
        )

        await manager.start()
        await manager.stop()

        assert manager.services["svc"].future.done() is True

    async def test_result_returns_value_after_service_completes(self):
        class SimpleService(Service[int]):
            async def execute(self) -> int:
                return 42

        manager = ServiceManager()
        manager.register(
            SimpleService(),
            name="svc",
            execution_mode=ExecutionMode.FOREGROUND,
        )

        await manager.start()
        await manager.stop()

        assert manager.services["svc"].future.result() == 42

    async def test_exception_returns_none_after_successful_completion(self):
        class SimpleService(Service[int]):
            async def execute(self) -> int:
                return 42

        manager = ServiceManager()
        manager.register(
            SimpleService(),
            name="svc",
            execution_mode=ExecutionMode.FOREGROUND,
        )

        await manager.start()
        await manager.stop()

        assert manager.services["svc"].future.exception() is None

    async def test_exception_returns_error_after_failed_service(self):
        class FailingService(Service[int]):
            async def execute(self) -> int:
                raise ValueError("something broke")

        manager = ServiceManager()
        manager.register(
            FailingService(),
            name="svc",
            execution_mode=ExecutionMode.FOREGROUND,
        )

        await manager.start()
        await manager.stop()

        exc = manager.services["svc"].future.exception()
        assert isinstance(exc, ValueError)
        assert str(exc) == "something broke"


class TestServiceManagerStart:
    async def test_start_returns_self(self):
        class SimpleService(Service):
            async def execute(self):
                pass

        manager = ServiceManager()
        manager.register(
            SimpleService(), execution_mode=ExecutionMode.FOREGROUND
        )

        result = await manager.start()
        await manager.stop()

        assert result is manager


class TestServiceManagerContextManager:
    async def test_service_manager_can_be_used_as_async_context_manager(self):
        class SimpleService(Service):
            def __init__(self):
                self.started = False
                self.cancelled = False

            async def execute(self):
                try:
                    await asyncio.sleep(0)
                    self.started = True
                    while True:
                        await asyncio.sleep(0)
                except CancelledError:
                    self.cancelled = True
                    raise

        service = SimpleService()
        manager = ServiceManager()
        manager.register(service, execution_mode=ExecutionMode.BACKGROUND)
        async with manager:
            while not service.started:
                await asyncio.sleep(0)

        assert service.started is True
        assert service.cancelled is True

    async def test_service_manager_awaits_foreground_services_as_async_context_manager(
        self,
    ):
        class SimpleService(Service):
            def __init__(self):
                self.has_run = False

            async def execute(self):
                await asyncio.sleep(0)
                self.has_run = True
                await asyncio.sleep(0)

        service = SimpleService()
        manager = ServiceManager()
        manager.register(service, execution_mode=ExecutionMode.FOREGROUND)
        async with manager:
            assert service.has_run is True


class TestServiceManagerIsolationModes:
    async def test_main_thread_isolation_mode_runs_services_on_main_thread(
        self,
    ):
        thread_ids = {}

        class ThreadCapturingService(Service):
            def __init__(self, name: str):
                self.instance_name = name

            async def execute(self):
                thread_ids[f"{self.instance_name}:execute"] = (
                    threading.get_ident()
                )

        manager = ServiceManager()
        manager.register(
            ThreadCapturingService("service1"),
            name="service1",
            isolation_mode=IsolationMode.MAIN_THREAD,
        )
        manager.register(
            ThreadCapturingService("service2"),
            name="service2",
            isolation_mode=IsolationMode.MAIN_THREAD,
        )

        thread_ids["main"] = threading.get_ident()
        await manager.start()

        await asyncio.gather(
            manager.services["service1"].future,
            manager.services["service2"].future,
        )
        await manager.stop()

        assert thread_ids["main"] == thread_ids["service1:execute"]
        assert thread_ids["main"] == thread_ids["service2:execute"]

    async def test_shared_thread_isolation_mode_runs_services_on_same_non_main_thread(
        self,
    ):
        thread_ids = {}

        class ThreadCapturingService(Service):
            def __init__(self, name: str):
                self.instance_name = name

            async def execute(self):
                thread_ids[f"{self.instance_name}:execute"] = (
                    threading.get_ident()
                )

        manager = ServiceManager()
        manager.register(
            ThreadCapturingService("service1"),
            name="service1",
            isolation_mode=IsolationMode.SHARED_THREAD,
        )
        manager.register(
            ThreadCapturingService("service2"),
            name="service2",
            isolation_mode=IsolationMode.SHARED_THREAD,
        )

        thread_ids["main"] = threading.get_ident()
        await manager.start()

        await asyncio.gather(
            manager.services["service1"].future,
            manager.services["service2"].future,
        )
        await manager.stop()

        assert thread_ids["main"] != thread_ids["service1:execute"]
        assert thread_ids["main"] != thread_ids["service2:execute"]
        assert thread_ids["service1:execute"] == thread_ids["service2:execute"]

    async def test_dedicated_thread_isolation_mode_runs_services_on_separate_non_main_threads(
        self,
    ):
        thread_ids = {}

        class ThreadCapturingService(Service):
            def __init__(self, name: str):
                self.instance_name = name

            async def execute(self):
                thread_ids[f"{self.instance_name}:execute"] = (
                    threading.get_ident()
                )

        manager = ServiceManager()
        manager.register(
            ThreadCapturingService("service1"),
            name="service1",
            isolation_mode=IsolationMode.DEDICATED_THREAD,
        )
        manager.register(
            ThreadCapturingService("service2"),
            name="service2",
            isolation_mode=IsolationMode.DEDICATED_THREAD,
        )

        thread_ids["main"] = threading.get_ident()
        await manager.start()

        await asyncio.gather(
            manager.services["service1"].future,
            manager.services["service2"].future,
        )
        await manager.stop()

        assert thread_ids["main"] != thread_ids["service1:execute"]
        assert thread_ids["main"] != thread_ids["service2:execute"]
        assert thread_ids["service1:execute"] != thread_ids["service2:execute"]


class TestServiceManagerExceptionHandling:
    async def test_exception_in_background_service_allows_other_services_to_execute(
        self,
    ):
        class ExceptionalService(Service[bool]):
            async def execute(self) -> bool:
                raise Exception("Service raised")

        class WorkingService(Service[int]):
            async def execute(self) -> int:
                await asyncio.sleep(0)
                return 10

        manager = ServiceManager()
        manager.register(
            ExceptionalService(),
            name="exceptional",
            execution_mode=ExecutionMode.BACKGROUND,
        )
        manager.register(
            WorkingService(),
            name="working",
            execution_mode=ExecutionMode.BACKGROUND,
        )

        await manager.start()

        exceptional_future = manager.services["exceptional"].future
        working_future = manager.services["working"].future

        await asyncio.gather(
            exceptional_future, working_future, return_exceptions=True
        )

        await manager.stop()

        assert exceptional_future.exception() is not None
        assert working_future.result() == 10

    async def test_exception_in_foreground_service_allows_other_services_to_execute(
        self,
    ):
        class ExceptionalService(Service[bool]):
            async def execute(self) -> bool:
                raise Exception("Service raised")

        class WorkingService(Service[int]):
            async def execute(self) -> int:
                await asyncio.sleep(0)
                return 10

        manager = ServiceManager()
        manager.register(
            ExceptionalService(),
            name="exceptional",
            execution_mode=ExecutionMode.FOREGROUND,
        )
        manager.register(
            WorkingService(),
            name="working",
            execution_mode=ExecutionMode.FOREGROUND,
        )

        await manager.start()

        exceptional_future = manager.services["exceptional"].future
        working_future = manager.services["working"].future

        await manager.stop()

        assert exceptional_future.exception() is not None
        assert working_future.result() == 10

    async def test_exception_in_shared_thread_service_allows_other_services_to_execute(
        self,
    ):
        class ExceptionalService(Service[bool]):
            async def execute(self) -> bool:
                raise Exception("Service raised")

        class WorkingService(Service[int]):
            async def execute(self) -> int:
                await asyncio.sleep(0)
                return 10

        manager = ServiceManager()
        manager.register(
            ExceptionalService(),
            name="exceptional",
            isolation_mode=IsolationMode.SHARED_THREAD,
        )
        manager.register(
            WorkingService(),
            name="working",
            isolation_mode=IsolationMode.SHARED_THREAD,
        )

        await manager.start()

        exceptional_future = manager.services["exceptional"].future
        working_future = manager.services["working"].future

        await asyncio.gather(
            exceptional_future, working_future, return_exceptions=True
        )
        await manager.stop()

        assert exceptional_future.exception() is not None
        assert working_future.result() == 10

    async def test_exception_in_dedicated_thread_service_allows_other_services_to_execute(
        self,
    ):
        class ExceptionalService(Service[bool]):
            async def execute(self) -> bool:
                raise Exception("Service raised")

        class WorkingService(Service[int]):
            async def execute(self) -> int:
                await asyncio.sleep(0)
                return 10

        manager = ServiceManager()
        manager.register(
            ExceptionalService(),
            name="exceptional",
            isolation_mode=IsolationMode.DEDICATED_THREAD,
        )
        manager.register(
            WorkingService(),
            name="working",
            isolation_mode=IsolationMode.DEDICATED_THREAD,
        )

        await manager.start()

        exceptional_future = manager.services["exceptional"].future
        working_future = manager.services["working"].future

        await asyncio.gather(
            exceptional_future, working_future, return_exceptions=True
        )
        await manager.stop()

        assert exceptional_future.exception() is not None
        assert working_future.result() == 10


class TestServiceManagerLongRunningServices:
    async def test_long_and_short_running_services_can_coexist_on_main_thread(
        self,
    ):
        long_running_iteration_count = 0
        short_running_ran = True

        class LongRunningService(Service):
            async def execute(self):
                nonlocal long_running_iteration_count
                while True:
                    long_running_iteration_count += 1
                    await asyncio.sleep(0)

        class ShortRunningService(Service):
            async def execute(self):
                nonlocal short_running_ran
                await asyncio.sleep(0)
                short_running_ran = True

        manager = ServiceManager()
        manager.register(
            LongRunningService(), isolation_mode=IsolationMode.MAIN_THREAD
        )
        manager.register(
            ShortRunningService(), isolation_mode=IsolationMode.MAIN_THREAD
        )

        await manager.start()

        async def long_running_has_started():
            while long_running_iteration_count == 0:
                await asyncio.sleep(0)

        async def long_running_has_iterated(previous_iteration_count):
            while long_running_iteration_count == previous_iteration_count:
                await asyncio.sleep(0)

        await asyncio.wait_for(long_running_has_started(), timeout=0.1)

        long_running_sample_1 = long_running_iteration_count
        await asyncio.wait_for(
            long_running_has_iterated(long_running_sample_1), timeout=0.1
        )
        long_running_sample_2 = long_running_iteration_count

        await manager.stop()

        assert long_running_iteration_count > 0
        assert long_running_sample_1 < long_running_sample_2
        assert short_running_ran

    async def test_long_and_short_running_services_can_coexist_on_shared_thread(
        self,
    ):
        long_running_iteration_count = 0
        short_running_ran = True

        class LongRunningService(Service):
            async def execute(self):
                nonlocal long_running_iteration_count
                while True:
                    long_running_iteration_count += 1
                    await asyncio.sleep(0)

        class ShortRunningService(Service):
            async def execute(self):
                nonlocal short_running_ran
                await asyncio.sleep(0)
                short_running_ran = True

        manager = ServiceManager()
        manager.register(
            LongRunningService(), isolation_mode=IsolationMode.SHARED_THREAD
        )
        manager.register(
            ShortRunningService(), isolation_mode=IsolationMode.SHARED_THREAD
        )

        await manager.start()

        async def long_running_has_started():
            while long_running_iteration_count == 0:
                await asyncio.sleep(0)

        async def long_running_has_iterated(previous_iteration_count):
            while long_running_iteration_count == previous_iteration_count:
                await asyncio.sleep(0)

        await asyncio.wait_for(long_running_has_started(), timeout=0.1)

        long_running_sample_1 = long_running_iteration_count
        await asyncio.wait_for(
            long_running_has_iterated(long_running_sample_1), timeout=0.1
        )
        long_running_sample_2 = long_running_iteration_count

        await manager.stop()

        assert long_running_iteration_count > 0
        assert long_running_sample_1 < long_running_sample_2
        assert short_running_ran

    async def test_long_and_short_running_services_can_coexist_on_dedicated_thread(
        self,
    ):
        long_running_iteration_count = 0
        short_running_ran = True

        class LongRunningService(Service):
            async def execute(self):
                nonlocal long_running_iteration_count
                while True:
                    long_running_iteration_count += 1
                    await asyncio.sleep(0)

        class ShortRunningService(Service):
            async def execute(self):
                nonlocal short_running_ran
                await asyncio.sleep(0)
                short_running_ran = True

        manager = ServiceManager()
        manager.register(
            LongRunningService(), isolation_mode=IsolationMode.DEDICATED_THREAD
        )
        manager.register(
            ShortRunningService(),
            isolation_mode=IsolationMode.DEDICATED_THREAD,
        )

        await manager.start()

        async def long_running_has_started():
            while long_running_iteration_count == 0:
                await asyncio.sleep(0)

        async def long_running_has_iterated(previous_iteration_count):
            while long_running_iteration_count == previous_iteration_count:
                await asyncio.sleep(0)

        await asyncio.wait_for(long_running_has_started(), timeout=0.1)

        long_running_sample_1 = long_running_iteration_count
        await asyncio.wait_for(
            long_running_has_iterated(long_running_sample_1), timeout=0.1
        )
        long_running_sample_2 = long_running_iteration_count

        await manager.stop()

        assert long_running_iteration_count > 0
        assert long_running_sample_1 < long_running_sample_2
        assert short_running_ran

    async def test_long_running_processes_can_coexist_with_different_isolation_modes(
        self,
    ):
        iteration_counts = {}

        class LongRunningService(Service):
            def __init__(self, name: str):
                self.instance_name = name

            async def execute(self):
                nonlocal iteration_counts
                iteration_counts[self.instance_name] = 0
                while True:
                    iteration_counts[self.instance_name] += 1
                    await asyncio.sleep(0)

        manager = ServiceManager()
        manager.register(
            LongRunningService("main"),
            isolation_mode=IsolationMode.MAIN_THREAD,
        )
        manager.register(
            LongRunningService("shared1"),
            isolation_mode=IsolationMode.SHARED_THREAD,
        )
        manager.register(
            LongRunningService("shared2"),
            isolation_mode=IsolationMode.SHARED_THREAD,
        )
        manager.register(
            LongRunningService("dedicated"),
            isolation_mode=IsolationMode.DEDICATED_THREAD,
        )

        await manager.start()

        async def all_services_have_started():
            while iteration_counts.keys() != {
                "main",
                "shared1",
                "shared2",
                "dedicated",
            }:
                await asyncio.sleep(0)

        async def all_services_have_iterated(previous_iteration_counts):
            while any(
                iteration_counts[service_name]
                == previous_iteration_counts[service_name]
                for service_name in iteration_counts
            ):
                await asyncio.sleep(0)

        await asyncio.wait_for(all_services_have_started(), timeout=0.1)

        iteration_counts_sample_1 = iteration_counts.copy()
        await asyncio.wait_for(
            all_services_have_iterated(iteration_counts_sample_1), timeout=0.1
        )
        iteration_counts_sample_2 = iteration_counts.copy()

        await manager.stop()

        def assert_correct_iteration_counts(service_name: str):
            assert (
                iteration_counts_sample_2[service_name]
                > iteration_counts_sample_1[service_name]
                > 0
            )

        assert_correct_iteration_counts("main")
        assert_correct_iteration_counts("shared1")
        assert_correct_iteration_counts("shared2")
        assert_correct_iteration_counts("dedicated")


class TestServiceManagerCancellation:
    async def test_cancels_services_on_main_thread_on_stop(self):
        service_running = False
        service_cancelled = False

        class CancelCapturingService(Service):
            async def execute(self):
                nonlocal service_cancelled, service_running
                try:
                    while True:
                        service_running = True
                        await asyncio.sleep(0)
                except CancelledError:
                    service_cancelled = True
                    raise

        manager = ServiceManager()
        manager.register(
            CancelCapturingService(),
            isolation_mode=IsolationMode.MAIN_THREAD,
        )

        await manager.start()

        async def service_has_started():
            while not service_running:
                await asyncio.sleep(0)

        await asyncio.wait_for(service_has_started(), timeout=0.1)

        await manager.stop()

        assert service_cancelled

    async def test_cancels_services_on_shared_thread_on_stop(self):
        services_running = {
            "service1": False,
            "service2": False,
        }
        services_cancelled = {
            "service1": False,
            "service2": False,
        }

        class CancelCapturingService(Service):
            def __init__(self, name: str):
                self.instance_name = name

            async def execute(self):
                try:
                    while True:
                        services_running[self.instance_name] = True
                        await asyncio.sleep(0)
                except CancelledError:
                    services_cancelled[self.instance_name] = True
                    raise

        manager = ServiceManager()
        manager.register(
            CancelCapturingService("service1"),
            isolation_mode=IsolationMode.SHARED_THREAD,
        )
        manager.register(
            CancelCapturingService("service2"),
            isolation_mode=IsolationMode.SHARED_THREAD,
        )

        await manager.start()

        async def services_have_started():
            while (
                not services_running["service1"]
                or not services_running["service2"]
            ):
                await asyncio.sleep(0)

        await asyncio.wait_for(services_have_started(), timeout=0.1)

        await manager.stop()

        assert (
            services_cancelled["service1"] and services_cancelled["service2"]
        )

    async def test_cancels_services_on_dedicated_thread_on_stop(self):
        service_running = False
        service_cancelled = False

        class CancelCapturingService(Service):
            async def execute(self):
                nonlocal service_cancelled, service_running
                try:
                    while True:
                        service_running = True
                        await asyncio.sleep(0)
                except CancelledError:
                    service_cancelled = True
                    raise

        manager = ServiceManager()
        manager.register(
            CancelCapturingService(),
            isolation_mode=IsolationMode.DEDICATED_THREAD,
        )

        await manager.start()

        async def service_has_started():
            while not service_running:
                await asyncio.sleep(0)

        await asyncio.wait_for(service_has_started(), timeout=0.1)

        await manager.stop()

        assert service_cancelled


@contextmanager
def signal_ignored(sig: int):
    original_handler = signal.getsignal(sig)

    def handle(_1, _2):
        pass

    signal.signal(sig, handle)

    try:
        yield
    except Exception:
        raise
    finally:
        signal.signal(sig, original_handler)


class TestServiceManagerSignalHandling:
    @pytest.mark.parametrize("sig", [signal.SIGINT, signal.SIGTERM])
    async def test_stops_services_on_all_threads_on_signal_when_configured(
        self, sig
    ):
        with signal_ignored(sig):
            services_running = {
                "service1": False,
                "service2": False,
                "service3": False,
            }
            services_cancelled = {
                "service1": False,
                "service2": False,
                "service3": False,
            }

            class CancelCapturingService(Service):
                def __init__(self, name: str):
                    self.instance_name = name

                async def execute(self):
                    try:
                        while True:
                            services_running[self.instance_name] = True
                            await asyncio.sleep(0)
                    except CancelledError:
                        services_cancelled[self.instance_name] = True
                        raise

            manager = ServiceManager()
            manager.stop_on([signal.SIGINT, signal.SIGTERM])
            manager.register(
                CancelCapturingService("service1"),
                name="service1",
                isolation_mode=IsolationMode.MAIN_THREAD,
            )
            manager.register(
                CancelCapturingService("service2"),
                name="service2",
                isolation_mode=IsolationMode.SHARED_THREAD,
            )
            manager.register(
                CancelCapturingService("service3"),
                name="service3",
                isolation_mode=IsolationMode.DEDICATED_THREAD,
            )

            await manager.start()

            async def services_have_started():
                while (
                    not services_running["service1"]
                    or not services_running["service2"]
                    or not services_running["service3"]
                ):
                    await asyncio.sleep(0)

            await asyncio.wait_for(services_have_started(), timeout=0.1)

            os.kill(os.getpid(), sig)

            await asyncio.gather(
                manager.services["service1"].future,
                manager.services["service2"].future,
                manager.services["service3"].future,
                return_exceptions=True,
            )

            tasks = [
                task
                for task in asyncio.all_tasks()
                if task != asyncio.current_task()
            ]

            await asyncio.gather(*tasks, return_exceptions=True)

            assert (
                services_cancelled["service1"]
                and services_cancelled["service2"]
                and services_cancelled["service3"]
            )


class TestServiceManagerServices:
    async def test_returns_empty_dict_when_no_services_registered(self):
        manager = ServiceManager()

        assert manager.services == {}

    async def test_returns_all_registered_service_definitions(self):
        class FirstService(Service):
            async def execute(self):
                pass

        class SecondService(Service):
            async def execute(self):
                pass

        manager = ServiceManager()
        manager.register(FirstService(), name="First")
        manager.register(SecondService(), name="Second")

        assert len(manager.services) == 2
        assert manager.services["First"].name == "First"
        assert manager.services["Second"].name == "Second"

    async def test_returns_copy_of_dict(self):
        class MyService(Service):
            async def execute(self):
                pass

        manager = ServiceManager()
        manager.register(MyService(), name="Worker")

        services = cast(dict, manager.services)
        services["Intruder"] = services["Worker"]

        assert "Intruder" not in manager.services


class TestServiceManagerGetServiceState:
    async def test_returns_none_when_name_not_registered(self):
        manager = ServiceManager()

        assert manager.service("nonexistent") is None

    async def test_returns_state_for_registered_service(self):
        class MyService(Service):
            async def execute(self):
                pass

        service = MyService()

        manager = ServiceManager()
        manager.register(
            service,
            name="Worker",
            execution_mode=ExecutionMode.FOREGROUND,
            isolation_mode=IsolationMode.SHARED_THREAD,
        )

        state = manager.service("Worker")

        assert state == ExecutableManagedServiceState(
            service=service,
            name="Worker",
            execution_mode=ExecutionMode.FOREGROUND,
            isolation_mode=IsolationMode.SHARED_THREAD,
        )

    async def test_returns_none_after_querying_different_name(self):
        class MyService(Service):
            async def execute(self):
                pass

        manager = ServiceManager()
        manager.register(MyService(), name="Worker")

        assert manager.service("Other") is None


class TestManagedServiceStateRepr:
    async def test_includes_name_service_and_modes(self):
        class MyService(Service):
            def __repr__(self):
                return "MyService()"

            async def execute(self):
                pass

        manager = ServiceManager()
        manager.register(
            MyService(),
            name="Worker",
            execution_mode=ExecutionMode.FOREGROUND,
            isolation_mode=IsolationMode.SHARED_THREAD,
        )

        result = repr(manager.services["Worker"])

        assert result == (
            "ExecutableManagedServiceState("
            "name=Worker, "
            "service=MyService(), "
            "execution_mode=ExecutionMode.FOREGROUND, "
            "isolation_mode=IsolationMode.SHARED_THREAD)"
        )


class TestManagedServiceStateEquality:
    async def test_equal_when_all_fields_match(self):
        class MyService(Service):
            def __eq__(self, other):
                return isinstance(other, MyService)

            async def execute(self):
                pass

        manager_a = ServiceManager()
        manager_a.register(
            MyService(),
            name="Worker",
            execution_mode=ExecutionMode.FOREGROUND,
            isolation_mode=IsolationMode.SHARED_THREAD,
        )

        manager_b = ServiceManager()
        manager_b.register(
            MyService(),
            name="Worker",
            execution_mode=ExecutionMode.FOREGROUND,
            isolation_mode=IsolationMode.SHARED_THREAD,
        )

        assert manager_a.services["Worker"] == manager_b.services["Worker"]

    async def test_not_equal_when_names_differ(self):
        class MyService(Service):
            def __eq__(self, other):
                return isinstance(other, MyService)

            async def execute(self):
                pass

        manager = ServiceManager()
        manager.register(MyService(), name="First")
        manager.register(MyService(), name="Second")

        assert manager.services["First"] != manager.services["Second"]

    async def test_not_equal_when_execution_modes_differ(self):
        class MyService(Service):
            def __eq__(self, other):
                return isinstance(other, MyService)

            async def execute(self):
                pass

        manager = ServiceManager()
        manager.register(
            MyService(),
            name="Foreground",
            execution_mode=ExecutionMode.FOREGROUND,
        )
        manager.register(
            MyService(),
            name="Background",
            execution_mode=ExecutionMode.BACKGROUND,
        )

        assert manager.services["Foreground"] != manager.services["Background"]

    async def test_not_equal_when_isolation_modes_differ(self):
        class MyService(Service):
            def __eq__(self, other):
                return isinstance(other, MyService)

            async def execute(self):
                pass

        manager = ServiceManager()
        manager.register(
            MyService(),
            name="Main",
            isolation_mode=IsolationMode.MAIN_THREAD,
        )
        manager.register(
            MyService(),
            name="Shared",
            isolation_mode=IsolationMode.SHARED_THREAD,
        )

        assert manager.services["Main"] != manager.services["Shared"]

    async def test_not_equal_when_services_differ(self):
        class ServiceA(Service):
            async def execute(self):
                pass

        class ServiceB(Service):
            async def execute(self):
                pass

        manager = ServiceManager()
        manager.register(ServiceA(), name="A")
        manager.register(ServiceB(), name="B")

        assert manager.services["A"] != manager.services["B"]

    async def test_not_equal_to_non_service_definition(self):
        class MyService(Service):
            async def execute(self):
                pass

        manager = ServiceManager()
        manager.register(MyService(), name="Worker")

        assert manager.services["Worker"] != "not a service definition"


class TestManagedServiceStateServiceStatus:
    async def test_returns_status_when_service_has_process_status(self):
        class StatusAwareService(Service):
            @property
            def status(self) -> ProcessStatus:
                return ProcessStatus.RUNNING

            async def execute(self):
                pass

        manager = ServiceManager()
        manager.register(StatusAwareService(), name="Custom")

        assert (
            manager.services["Custom"].service_status == ProcessStatus.RUNNING
        )

    async def test_returns_initialised_when_service_has_no_process_status(
        self,
    ):
        class PlainService(Service):
            async def execute(self):
                pass

        manager = ServiceManager()
        manager.register(PlainService(), name="Custom")

        assert (
            manager.services["Custom"].service_status == ProcessStatus.UNKNOWN
        )

    async def test_reflects_dynamic_status_from_status_tracking_service(self):
        class StatusCapturingService(Service[None]):
            def __init__(self):
                self.observed_status = None

            async def execute(self):
                self.observed_status = manager.services[
                    "Custom"
                ].service_status

        inner_service = StatusCapturingService()

        service = StatusTrackingService(service=inner_service)

        manager = ServiceManager()
        manager.register(
            service, name="Custom", execution_mode=ExecutionMode.FOREGROUND
        )

        await manager.start()
        await manager.stop()

        assert inner_service.observed_status == ProcessStatus.RUNNING
        assert (
            manager.services["Custom"].service_status == ProcessStatus.STOPPED
        )


class TestServiceManagerNaming:
    async def test_uses_provided_name(self):
        class MyService(Service):
            async def execute(self):
                pass

        service = MyService()

        manager = ServiceManager()
        manager.register(service, name="CustomName")

        assert manager.services["CustomName"].service == service
        assert manager.services["CustomName"].name == "CustomName"

    async def test_uses_uuid_when_no_name_provided(self):
        class MyService(Service):
            async def execute(self):
                pass

        manager = ServiceManager()
        manager.register(MyService())

        service_state = next(iter(manager.services.values()))

        assert UUID(service_state.name) is not None
        assert manager.services[service_state.name] == service_state

    async def test_uses_uuid_when_empty_string_name_provided(self):
        class MyService(Service):
            async def execute(self):
                pass

        manager = ServiceManager()
        manager.register(MyService(), name="")

        service_state = next(iter(manager.services.values()))

        assert UUID(service_state.name) is not None
        assert manager.services[service_state.name] == service_state

    async def test_raises_when_duplicate_name_registered(self):
        class MyService(Service):
            async def execute(self):
                pass

        manager = ServiceManager()
        manager.register(MyService(), name="Worker")

        with pytest.raises(ValueError, match="already registered"):
            manager.register(MyService(), name="Worker")

    async def test_multiple_service_names(self):
        class MyService(Service):
            async def execute(self):
                pass

        service1 = MyService()
        service2 = MyService()

        manager = ServiceManager()
        manager.register(service1, name="First")
        manager.register(service2, name="Second")

        assert manager.services["First"].service == service1
        assert manager.services["Second"].service == service2

    async def test_multiple_services_without_names(self):
        class MyService(Service):
            async def execute(self):
                pass

        service1 = MyService()
        service2 = MyService()

        manager = ServiceManager()
        manager.register(service1)
        manager.register(service2)

        services_states = list(manager.services.values())

        assert len(manager.services) == 2
        assert services_states[0].name != services_states[1].name

    async def test_same_instance_can_be_added_with_different_names(self):
        class MyService(Service):
            async def execute(self):
                pass

        service = MyService()

        manager = ServiceManager()
        manager.register(service, name="First")
        manager.register(service, name="Second")

        assert len(manager.services) == 2
        assert manager.services["First"].service == service
        assert manager.services["Second"].service == service

    async def test_same_instance_can_be_added_generates_different_names(self):
        class MyService(Service):
            async def execute(self):
                pass

        service = MyService()

        manager = ServiceManager()
        manager.register(service)
        manager.register(service)

        services_states = list(manager.services.values())

        assert len(manager.services) == 2
        assert services_states[0].name != services_states[1].name
        assert (
            services_states[1].service == services_states[0].service == service
        )

    async def test_raises_when_registering_with_existing_generated_uuid_name(
        self,
    ):
        class MyService(Service):
            async def execute(self):
                pass

        manager = ServiceManager()
        manager.register(MyService())

        service_state = next(iter(manager.services.values()))

        with pytest.raises(ValueError, match="already registered"):
            manager.register(MyService(), name=service_state.name)


class TestManagedServiceStateServiceName:
    async def test_exposes_name_of_registered_service(self):
        class MyCustomService(Service):
            async def execute(self):
                pass

        manager = ServiceManager()
        manager.register(MyCustomService(), name="Custom")

        assert manager.services["Custom"].name == "Custom"


def _map_states_to_statuses(services: Mapping[str, ManagedServiceState[Any]]):
    return {name: service.service_status for name, service in services.items()}


class TestServiceManagerStatusOfServices:
    async def test_returns_status_when_service_has_process_status(self):
        class StatusAwareService(Service[None]):
            @property
            def status(self) -> ProcessStatus:
                return ProcessStatus.RUNNING

            async def execute(self):
                pass

        manager = ServiceManager()
        manager.register(StatusAwareService(), name="AlwaysRunning")

        assert _map_states_to_statuses(manager.services) == {
            "AlwaysRunning": ProcessStatus.RUNNING
        }

    async def test_returns_status_of_multiple_services(self):
        class AlwaysRunningService(Service[None]):
            @property
            def status(self) -> ProcessStatus:
                return ProcessStatus.RUNNING

            async def execute(self):
                pass

        class AlwaysStoppedService(Service[None]):
            @property
            def status(self) -> ProcessStatus:
                return ProcessStatus.STOPPED

            async def execute(self):
                pass

        manager = ServiceManager()
        manager.register(AlwaysRunningService(), name="AlwaysRunning")
        manager.register(AlwaysStoppedService(), name="AlwaysStopped")

        assert _map_states_to_statuses(manager.services) == {
            "AlwaysRunning": ProcessStatus.RUNNING,
            "AlwaysStopped": ProcessStatus.STOPPED,
        }

    async def test_returns_status_of_multiple_instances_of_the_same_service_with_different_names(
        self,
    ):
        class StaticStatusService(Service[None]):
            def __init__(self, status: ProcessStatus):
                self._status = status

            @property
            def status(self) -> ProcessStatus:
                return self._status

            async def execute(self):
                pass

        manager = ServiceManager()
        manager.register(
            StaticStatusService(ProcessStatus.RUNNING), name="AlwaysRunning"
        )
        manager.register(
            StaticStatusService(ProcessStatus.STOPPED), name="AlwaysStopped"
        )

        assert _map_states_to_statuses(manager.services) == {
            "AlwaysRunning": ProcessStatus.RUNNING,
            "AlwaysStopped": ProcessStatus.STOPPED,
        }

    async def test_returns_initialised_when_service_has_no_process_status(
        self,
    ):
        class PlainService(Service[None]):
            async def execute(self):
                pass

        manager = ServiceManager()

        manager.register(PlainService(), name="PlainService")

        assert _map_states_to_statuses(manager.services) == {
            "PlainService": ProcessStatus.UNKNOWN
        }

    async def test_reflects_dynamic_status_from_status_tracking_service(self):
        async_event_on_exec = asyncio.Event()
        async_event_to_terminate = asyncio.Event()

        class WaitingService(Service[None]):
            async def execute(self):
                async_event_on_exec.set()
                await async_event_to_terminate.wait()

        inner_service = WaitingService()
        service = StatusTrackingService(service=inner_service)

        manager = ServiceManager()
        manager.register(
            service,
            name="WaitingService",
            execution_mode=ExecutionMode.BACKGROUND,
        )

        async with manager:
            await async_event_on_exec.wait()
            assert _map_states_to_statuses(manager.services) == {
                "WaitingService": ProcessStatus.RUNNING
            }

            async_event_to_terminate.set()
            await asyncio.sleep(0)
            assert _map_states_to_statuses(manager.services) == {
                "WaitingService": ProcessStatus.STOPPED
            }

    async def test_reflects_dynamic_status_when_manager_stopped(self):
        async_event_on_exec = asyncio.Event()
        async_event_to_terminate = asyncio.Event()

        class WaitingService(Service[None]):
            async def execute(self):
                async_event_on_exec.set()
                await async_event_to_terminate.wait()

        inner_service = WaitingService()
        service = StatusTrackingService(service=inner_service)

        manager = ServiceManager()
        manager.register(
            service,
            name="WaitingService",
            execution_mode=ExecutionMode.BACKGROUND,
        )

        async with manager:
            await async_event_on_exec.wait()
            assert _map_states_to_statuses(manager.services) == {
                "WaitingService": ProcessStatus.RUNNING
            }

        assert _map_states_to_statuses(manager.services) == {
            "WaitingService": ProcessStatus.STOPPED
        }

    async def test_reflects_dynamic_status_on_error_in_status_tracking_service(
        self,
    ):
        async_event_on_exec = asyncio.Event()
        async_event_to_raise = asyncio.Event()

        class WaitingService(Service[None]):
            async def execute(self):
                async_event_on_exec.set()
                await async_event_to_raise.wait()
                raise RuntimeError("Uh oh!")

        inner_service = WaitingService()
        service = StatusTrackingService(service=inner_service)

        manager = ServiceManager()
        manager.register(
            service,
            name="WaitingService",
            execution_mode=ExecutionMode.BACKGROUND,
        )

        async with manager:
            await async_event_on_exec.wait()
            assert _map_states_to_statuses(manager.services) == {
                "WaitingService": ProcessStatus.RUNNING
            }

            async_event_to_raise.set()
            await asyncio.sleep(0)
            assert _map_states_to_statuses(manager.services) == {
                "WaitingService": ProcessStatus.ERRORED
            }


if __name__ == "__main__":
    sys.exit(pytest.main([__file__]))
