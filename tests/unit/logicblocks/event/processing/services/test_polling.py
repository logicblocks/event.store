import asyncio
from datetime import timedelta

from logicblocks.event.processing.services import PollingService


class TestPollingService:
    async def test_execute_calls_callable_every_interval(self):
        counter = 0

        async def count() -> None:
            nonlocal counter
            counter += 1

        service = PollingService(
            callable=count, poll_interval=timedelta(milliseconds=20)
        )

        task = asyncio.create_task(service.execute())

        await asyncio.sleep(timedelta(milliseconds=50).total_seconds())

        task.cancel()

        await asyncio.gather(task, return_exceptions=True)

        assert counter == 3
