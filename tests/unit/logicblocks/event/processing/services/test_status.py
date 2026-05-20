import asyncio

import pytest

from logicblocks.event.processing import (
    CallableService,
    HasProcessStatus,
    ProcessStatus,
    Service,
    StatusTrackingService,
)


class TestStatusTrackingServiceRepr:
    async def test_includes_class_name_and_inner_service_repr(self):
        class InnerService(Service):
            def __repr__(self):
                return "InnerService()"

            async def execute(self):
                pass

        inner = InnerService()
        service = StatusTrackingService(service=inner)

        assert repr(service) == "StatusTrackingService(InnerService())"

    async def test_delegates_to_inner_service_repr(self):
        async def noop():
            pass

        inner = CallableService(noop)
        service = StatusTrackingService(service=inner)

        assert repr(service) == f"StatusTrackingService({inner!r})"


class TestStatusTrackingService:
    async def test_has_initialised_status_before_running(self):
        async def noop():
            pass

        service = StatusTrackingService(
            service=CallableService(noop),
        )

        assert service.status == ProcessStatus.INITIALISED

    async def test_has_stopped_status_after_successful_run(self):
        async def noop():
            pass

        service = StatusTrackingService(
            service=CallableService(noop),
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

    async def test_delegates_to_custom_inner_service(self):
        class CallTrackingService(Service[int]):
            def __init__(self):
                self.counter = 0

            async def execute(self):
                self.counter = self.counter + 1
                return 42

        inner_service = CallTrackingService()

        service = StatusTrackingService(service=inner_service)

        result = await service.execute()

        assert result == 42
        assert inner_service.counter == 1

    async def test_runs_callable_service(self):
        async def return_value():
            return 42

        service = StatusTrackingService(service=CallableService(return_value))

        result = await service.execute()

        assert result == 42

    async def test_callable_service_tracks_status(self):
        async def noop():
            pass

        service = StatusTrackingService(service=CallableService(noop))

        await service.execute()

        assert service.status == ProcessStatus.STOPPED

    async def test_satisfies_has_process_status_protocol(self):
        async def noop():
            pass

        service = StatusTrackingService(service=CallableService(noop))

        assert isinstance(service, HasProcessStatus)

    async def test_plain_service_does_not_satisfy_has_process_status(self):
        class PlainService(Service):
            async def execute(self):
                pass

        assert not isinstance(PlainService(), HasProcessStatus)

    async def test_re_raises_base_exception_with_errored_status(self):
        async def raise_fatal():
            raise KeyboardInterrupt()

        service = StatusTrackingService(service=CallableService(raise_fatal))

        with pytest.raises(KeyboardInterrupt):
            await service.execute()

        assert service.status == ProcessStatus.ERRORED
