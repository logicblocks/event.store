import asyncio
import warnings
from datetime import timedelta

from logicblocks.event.processing import (
    CallableService,
    PollingService,
    Service,
)


class TestPollingServiceExecute:
    async def test_execute_runs_service_every_interval(self):
        class CallTrackingService(Service[None]):
            def __init__(self):
                self.counter = 0

            async def execute(self) -> None:
                self.counter = self.counter + 1

        inner_service = CallTrackingService()

        service = PollingService(
            service=inner_service,
            poll_interval=timedelta(milliseconds=20),
        )

        task = asyncio.create_task(service.execute())

        await asyncio.sleep(timedelta(milliseconds=50).total_seconds())

        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass

        assert inner_service.counter == 3

    async def test_execute_with_callable_service_every_interval(self):
        counter = 0

        async def count() -> None:
            nonlocal counter
            counter += 1

        service = PollingService(
            service=CallableService(count),
            poll_interval=timedelta(milliseconds=20),
        )

        task = asyncio.create_task(service.execute())

        await asyncio.sleep(timedelta(milliseconds=50).total_seconds())

        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass

        assert counter == 3


class TestPollingServiceWithCallableBackwardsCompatibility:
    async def test_accepts_callable_and_auto_wraps(self):
        counter = 0

        async def count() -> None:
            nonlocal counter
            counter += 1

        service = PollingService(
            service=count,
            poll_interval=timedelta(milliseconds=20),
        )

        task = asyncio.create_task(service.execute())

        await asyncio.sleep(timedelta(milliseconds=50).total_seconds())

        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass

        assert counter == 3

    async def test_writes_deprecation_warning_when_callable_is_used(self):
        async def noop():
            pass

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            PollingService(
                service=noop,
                poll_interval=timedelta(milliseconds=20),
            )

            assert w is not None
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert str(w[0].message) == "Wrap callable in CallableService"

    async def test_accepts_callable_kwarg_for_backwards_compatibility(self):
        counter = 0

        async def count() -> None:
            nonlocal counter
            counter += 1

        service = PollingService(
            callable=count,
            poll_interval=timedelta(milliseconds=20),
        )

        task = asyncio.create_task(service.execute())

        await asyncio.sleep(timedelta(milliseconds=50).total_seconds())

        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass

        assert counter == 3

    async def test_writes_deprecation_warning_when_callable_kwarg_is_used(
        self,
    ):
        async def noop():
            pass

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            PollingService(
                callable=noop,
                poll_interval=timedelta(milliseconds=20),
            )

            assert w is not None
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert str(w[0].message) == "Use 'service' instead of 'callable'"

    async def test_accepts_callable_positional_arg_for_backwards_compatibility(
        self,
    ):
        counter = 0

        async def count() -> None:
            nonlocal counter
            counter += 1

        service = PollingService(
            count,
            poll_interval=timedelta(milliseconds=20),
        )

        task = asyncio.create_task(service.execute())

        await asyncio.sleep(timedelta(milliseconds=50).total_seconds())

        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass

        assert counter == 3

    async def test_writes_deprecation_warning_when_callable_positional_arg_is_used(
        self,
    ):
        async def noop():
            pass

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            PollingService(
                noop,
                poll_interval=timedelta(milliseconds=20),
            )

            assert w is not None
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert str(w[0].message) == "Wrap callable in CallableService"

    async def test_writes_no_warnings_when_service_is_used(self):
        async def noop():
            pass

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            PollingService(
                service=CallableService(noop),
                poll_interval=timedelta(milliseconds=20),
            )

            assert w is not None
            assert len(w) == 0

    async def test_writes_no_warnings_when_service_is_used_positionally(self):
        async def noop():
            pass

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            PollingService(
                CallableService(noop),
                poll_interval=timedelta(milliseconds=20),
            )

            assert w is not None
            assert len(w) == 0
