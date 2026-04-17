import asyncio

import pytest

from logicblocks.event.processing import (
    CallableService,
    ProcessStatus,
    StatusTrackingService,
)


class TestStatusTrackingService:
    async def test_has_initialised_status_before_running(self):
        service = StatusTrackingService(
            service=CallableService(self._noop),
        )

        assert service.status == ProcessStatus.INITIALISED

    async def test_has_stopped_status_after_successful_run(self):
        service = StatusTrackingService(
            service=CallableService(self._noop),
        )

        await service.execute()

        assert service.status == ProcessStatus.STOPPED

    async def test_has_running_status_while_executing(self):
        observed_status = None

        async def capture_status():
            nonlocal observed_status
            observed_status = service.status

        service = StatusTrackingService(
            service=CallableService(capture_status),
        )

        await service.execute()

        assert observed_status == ProcessStatus.RUNNING

    async def test_has_stopped_status_after_cancellation(self):
        async def block_forever():
            await asyncio.sleep(999)

        service = StatusTrackingService(
            service=CallableService(block_forever),
        )

        task = asyncio.create_task(service.execute())
        await asyncio.sleep(0)

        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        assert service.status == ProcessStatus.STOPPED

    async def test_has_errored_status_after_exception(self):
        async def raise_error():
            raise RuntimeError("boom")

        service = StatusTrackingService(
            service=CallableService(raise_error),
        )

        with pytest.raises(RuntimeError):
            await service.execute()

        assert service.status == ProcessStatus.ERRORED

    async def test_returns_inner_service_result(self):
        async def return_value():
            return 42

        service = StatusTrackingService(
            service=CallableService(return_value),
        )

        result = await service.execute()

        assert result == 42

    async def test_delegates_name_to_inner_service(self):
        service = StatusTrackingService(
            service=CallableService(self._noop),
        )

        assert service.name == "_noop"

    async def test_accepts_callable_and_auto_wraps(self):
        async def return_value():
            return 42

        service = StatusTrackingService(service=return_value)

        result = await service.execute()

        assert result == 42

    async def test_auto_wrapped_callable_tracks_status(self):
        async def noop():
            pass

        service = StatusTrackingService(service=noop)

        await service.execute()

        assert service.status == ProcessStatus.STOPPED

    @staticmethod
    async def _noop():
        pass
