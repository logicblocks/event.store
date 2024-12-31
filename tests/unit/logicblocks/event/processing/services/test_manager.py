import asyncio
import os
import signal
import sys
import threading
import time
from asyncio import CancelledError
from contextlib import contextmanager

import pytest

from logicblocks.event.processing.services import (
    ExecutionMode,
    IsolationMode,
    Service,
    ServiceManager,
)

# supervision
# schedules
# contexts
# service start and stop?


class TestServiceManagerExecutionModes(object):
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
            execution_mode=ExecutionMode.BACKGROUND,
        )

        times["before"] = time.monotonic_ns()
        tasks = await manager.start()
        times["after"] = time.monotonic_ns()
        control.set()

        await asyncio.gather(*tasks)
        await manager.stop()

        assert times["during"] > times["before"]
        assert times["during"] > times["after"]


class TestServiceManagerIsolationModes(object):
    async def test_main_thread_isolation_mode_runs_services_on_main_thread(
        self,
    ):
        thread_ids = {}

        class ThreadCapturingService(Service):
            def __init__(self, name: str):
                self.name = name

            async def execute(self):
                thread_ids[f"{self.name}:execute"] = threading.get_ident()

        manager = ServiceManager()
        manager.register(
            ThreadCapturingService("service1"),
            isolation_mode=IsolationMode.MAIN_THREAD,
        )
        manager.register(
            ThreadCapturingService("service2"),
            isolation_mode=IsolationMode.MAIN_THREAD,
        )

        thread_ids["main"] = threading.get_ident()
        tasks = await manager.start()

        await asyncio.gather(*tasks)
        await manager.stop()

        assert thread_ids["main"] == thread_ids["service1:execute"]
        assert thread_ids["main"] == thread_ids["service2:execute"]

    async def test_shared_thread_isolation_mode_runs_services_on_same_non_main_thread(
        self,
    ):
        thread_ids = {}

        class ThreadCapturingService(Service):
            def __init__(self, name: str):
                self.name = name

            async def execute(self):
                thread_ids[f"{self.name}:execute"] = threading.get_ident()

        manager = ServiceManager()
        manager.register(
            ThreadCapturingService("service1"),
            isolation_mode=IsolationMode.SHARED_THREAD,
        )
        manager.register(
            ThreadCapturingService("service2"),
            isolation_mode=IsolationMode.SHARED_THREAD,
        )

        thread_ids["main"] = threading.get_ident()
        tasks = await manager.start()

        await asyncio.gather(*tasks)
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
                self.name = name

            async def execute(self):
                thread_ids[f"{self.name}:execute"] = threading.get_ident()

        manager = ServiceManager()
        manager.register(
            ThreadCapturingService("service1"),
            isolation_mode=IsolationMode.DEDICATED_THREAD,
        )
        manager.register(
            ThreadCapturingService("service2"),
            isolation_mode=IsolationMode.DEDICATED_THREAD,
        )

        thread_ids["main"] = threading.get_ident()
        tasks = await manager.start()

        await asyncio.gather(*tasks)
        await manager.stop()

        assert thread_ids["main"] != thread_ids["service1:execute"]
        assert thread_ids["main"] != thread_ids["service2:execute"]
        assert thread_ids["service1:execute"] != thread_ids["service2:execute"]


class TestServiceManagerExceptionHandling(object):
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
            ExceptionalService(), execution_mode=ExecutionMode.BACKGROUND
        )
        manager.register(
            WorkingService(), execution_mode=ExecutionMode.BACKGROUND
        )

        futures = await manager.start()

        await asyncio.gather(*futures, return_exceptions=True)

        await manager.stop()

        assert futures[0].exception() is not None
        assert futures[1].result() == 10

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
            ExceptionalService(), execution_mode=ExecutionMode.FOREGROUND
        )
        manager.register(
            WorkingService(), execution_mode=ExecutionMode.FOREGROUND
        )

        futures = await manager.start()

        await manager.stop()

        assert futures[0].exception() is not None
        assert futures[1].result() == 10

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
            ExceptionalService(), isolation_mode=IsolationMode.SHARED_THREAD
        )
        manager.register(
            WorkingService(), isolation_mode=IsolationMode.SHARED_THREAD
        )

        futures = await manager.start()

        await asyncio.gather(*futures, return_exceptions=True)
        await manager.stop()

        assert futures[0].exception() is not None
        assert futures[1].result() == 10

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
            ExceptionalService(), isolation_mode=IsolationMode.DEDICATED_THREAD
        )
        manager.register(
            WorkingService(), isolation_mode=IsolationMode.DEDICATED_THREAD
        )

        futures = await manager.start()

        await asyncio.gather(*futures, return_exceptions=True)
        await manager.stop()

        assert futures[0].exception() is not None
        assert futures[1].result() == 10


class TestServiceManagerLongRunningServices(object):
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
                self.name = name

            async def execute(self):
                nonlocal iteration_counts
                iteration_counts[self.name] = 0
                while True:
                    iteration_counts[self.name] += 1
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


class TestServiceManagerCancellation(object):
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
                self.name = name

            async def execute(self):
                try:
                    while True:
                        services_running[self.name] = True
                        await asyncio.sleep(0)
                except CancelledError:
                    services_cancelled[self.name] = True
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


class TestServiceManagerSignalHandling(object):
    @pytest.mark.parametrize("sig", [signal.SIGINT, signal.SIGTERM])
    async def test_stops_services_on_all_threads_on_signal(self, sig):
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
                    self.name = name

                async def execute(self):
                    try:
                        while True:
                            services_running[self.name] = True
                            await asyncio.sleep(0)
                    except CancelledError:
                        services_cancelled[self.name] = True
                        raise

            manager = ServiceManager()
            manager.register(
                CancelCapturingService("service1"),
                isolation_mode=IsolationMode.MAIN_THREAD,
            )
            manager.register(
                CancelCapturingService("service2"),
                isolation_mode=IsolationMode.SHARED_THREAD,
            )
            manager.register(
                CancelCapturingService("service3"),
                isolation_mode=IsolationMode.DEDICATED_THREAD,
            )

            futures = await manager.start()

            async def services_have_started():
                while (
                    not services_running["service1"]
                    or not services_running["service2"]
                    or not services_running["service3"]
                ):
                    await asyncio.sleep(0)

            await asyncio.wait_for(services_have_started(), timeout=0.1)

            os.kill(os.getpid(), sig)

            await asyncio.gather(*futures, return_exceptions=True)

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


if __name__ == "__main__":
    sys.exit(pytest.main([__file__]))
