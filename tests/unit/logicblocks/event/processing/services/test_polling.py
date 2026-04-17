import asyncio
import warnings
from datetime import timedelta

from logicblocks.event.processing import CallableService, PollingService


class TestPollingService:
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

        await asyncio.gather(task, return_exceptions=True)

        assert counter == 3

    async def test_accepts_callable_kwarg_for_backwards_compatibility(self):
        counter = 0

        async def count() -> None:
            nonlocal counter
            counter += 1

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            service = PollingService(
                callable=count,
                poll_interval=timedelta(milliseconds=20),
            )
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert "service" in str(w[0].message)

        task = asyncio.create_task(service.execute())

        await asyncio.sleep(timedelta(milliseconds=50).total_seconds())

        task.cancel()

        await asyncio.gather(task, return_exceptions=True)

        assert counter == 3

    async def test_execute_calls_callable_every_interval(self):
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

        await asyncio.gather(task, return_exceptions=True)

        assert counter == 3
