import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta
from typing import Protocol, Self
from unittest.mock import Mock

import pytest

from logicblocks.event.processing import (
    EventBroker,
    EventSubscriber,
    Process,
    ProcessStatus,
)
from logicblocks.event.processing.broker.strategies.distributed import (
    DefaultEventSubscriberManager,
    DistributedEventBroker,
    EventSubscriptionCoordinator,
    EventSubscriptionObserver,
)
from logicblocks.event.processing.broker.strategies.distributed.subscribers.manager import (
    EventSubscriberManager,
)
from logicblocks.event.processing.services.error import ContinueErrorHandler, ExitErrorHandler


class StartAndCancelTrackable(Protocol):
    @property
    def start_count(self) -> int:
        raise NotImplementedError

    @property
    def cancel_count(self) -> int:
        raise NotImplementedError


@dataclass(frozen=True)
class Context:
    broker: EventBroker
    event_subscriber_manager: Mock
    event_subscription_coordinator: Mock
    event_subscription_observer: Mock


def make_event_broker():
    event_subscriber_manager = Mock(spec=DefaultEventSubscriberManager)
    event_subscription_coordinator = Mock(spec=EventSubscriptionCoordinator)
    event_subscription_observer = Mock(spec=EventSubscriptionObserver)

    broker = DistributedEventBroker(
        event_subscriber_manager=event_subscriber_manager,
        event_subscription_coordinator=event_subscription_coordinator,
        event_subscription_observer=event_subscription_observer,
    )

    return Context(
        broker=broker,
        event_subscriber_manager=event_subscriber_manager,
        event_subscription_coordinator=event_subscription_coordinator,
        event_subscription_observer=event_subscription_observer,
    )


@asynccontextmanager
async def task_shutdown(task: asyncio.Task):
    try:
        yield
    finally:
        if not task.cancelled():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def status_not_equal_to(
    event_broker: EventBroker, status: ProcessStatus
):
    while True:
        if event_broker.status != status:
            return
        await asyncio.sleep(0)


async def status_equal_to(process: Process, status: ProcessStatus):
    while True:
        if process.status == status:
            return
        await asyncio.sleep(0)


async def cancel_count_is_equal_to(
        trackable: StartAndCancelTrackable, cancel_count: int
):
    while True:
        if trackable.cancel_count == cancel_count:
            return
        await asyncio.sleep(0)


async def start_count_is_equal_to(
        trackable: StartAndCancelTrackable, start_count: int
):
    while True:
        if trackable.start_count == start_count:
            return
        await asyncio.sleep(0)


async def status_eventually_equal_to(process: Process, status: ProcessStatus):
    timeout = timedelta(milliseconds=500)
    await asyncio.wait_for(
        status_equal_to(process, status),
        timeout=timeout.total_seconds(),
    )


async def cancel_count_eventually(
        trackable: StartAndCancelTrackable, cancel_count: int
):
    timeout = timedelta(milliseconds=1000)
    await asyncio.wait_for(
        cancel_count_is_equal_to(trackable, cancel_count),
        timeout=timeout.total_seconds(),
    )


async def start_count_eventually(
        trackable: StartAndCancelTrackable, start_count: int
):
    timeout = timedelta(milliseconds=500)
    await asyncio.wait_for(
        start_count_is_equal_to(trackable, start_count),
        timeout=timeout.total_seconds(),
    )


async def assert_status_eventually(process: Process, status: ProcessStatus):
    try:
        await status_eventually_equal_to(process, status)
    except asyncio.TimeoutError:
        pytest.fail(
            f"Expected status to eventually equal '{status}' "
            f"but timed out waiting."
        )


async def assert_cancel_count_eventually(
        trackable: StartAndCancelTrackable, cancel_count: int
):
    try:
        await cancel_count_eventually(trackable, cancel_count)
    except asyncio.TimeoutError:
        pytest.fail(
            f"Expected to eventually have cancel count {cancel_count} but "
            f"timed out waiting."
        )


async def assert_start_count_eventually(
        trackable: StartAndCancelTrackable, start_count: int
):
    try:
        await start_count_eventually(trackable, start_count)
    except asyncio.TimeoutError:
        pytest.fail(
            f"Expected to eventually have start count {start_count} but "
            f"timed out waiting."
        )


async def assert_task_eventually_done(task: asyncio.Task):
    timeout = timedelta(milliseconds=500)
    try:
        await asyncio.wait_for(task, timeout=timeout.total_seconds())
    except asyncio.TimeoutError:
        pytest.fail(
            "Expected task to eventually be done but timed out waiting."
        )
    except asyncio.CancelledError:
        pytest.fail("Expected task to eventually be done but was cancelled.")


class TestDistributedEventBrokerStatuses:
    async def test_has_initialised_status_before_executing(self):
        context = make_event_broker()
        broker = context.broker

        assert broker.status == ProcessStatus.INITIALISED

    async def test_has_running_status_when_coordinator_and_observer_running(
        self,
    ):
        context = make_event_broker()
        broker = context.broker

        coordinator = context.event_subscription_coordinator
        coordinator.status = ProcessStatus.RUNNING

        observer = context.event_subscription_observer
        observer.status = ProcessStatus.RUNNING

        task = asyncio.create_task(broker.execute())

        async with task_shutdown(task):
            await assert_status_eventually(broker, ProcessStatus.RUNNING)

    async def test_has_starting_status_when_coordinator_is_running_and_observer_is_initialised(
        self,
    ):
        context = make_event_broker()
        broker = context.broker

        coordinator = context.event_subscription_coordinator
        coordinator.status = ProcessStatus.RUNNING

        observer = context.event_subscription_observer
        observer.status = ProcessStatus.INITIALISED

        task = asyncio.create_task(broker.execute())

        async with task_shutdown(task):
            await assert_status_eventually(broker, ProcessStatus.STARTING)

    async def test_has_starting_status_when_coordinator_is_initialised_and_observer_is_running(
        self,
    ):
        context = make_event_broker()
        broker = context.broker

        coordinator = context.event_subscription_coordinator
        coordinator.status = ProcessStatus.RUNNING

        observer = context.event_subscription_observer
        observer.status = ProcessStatus.INITIALISED

        task = asyncio.create_task(broker.execute())

        async with task_shutdown(task):
            await assert_status_eventually(broker, ProcessStatus.STARTING)

    async def test_has_errored_status_when_coordinator_is_errored_and_observer_is_running(
        self,
    ):
        context = make_event_broker()
        broker = context.broker
        coordinator = context.event_subscription_coordinator
        coordinator.status = ProcessStatus.ERRORED

        observer = context.event_subscription_observer
        observer.status = ProcessStatus.INITIALISED

        task = asyncio.create_task(broker.execute())

        async with task_shutdown(task):
            await assert_status_eventually(broker, ProcessStatus.ERRORED)

    async def test_has_errored_status_when_coordinator_is_running_and_observer_is_errored(
        self,
    ):
        context = make_event_broker()
        broker = context.broker
        coordinator = context.event_subscription_coordinator
        coordinator.status = ProcessStatus.ERRORED

        observer = context.event_subscription_observer
        observer.status = ProcessStatus.INITIALISED

        task = asyncio.create_task(broker.execute())

        async with task_shutdown(task):
            await assert_status_eventually(broker, ProcessStatus.ERRORED)

    async def test_has_errored_status_when_coordinator_is_errored_and_observer_is_errored(
        self,
    ):
        context = make_event_broker()
        broker = context.broker
        coordinator = context.event_subscription_coordinator
        coordinator.status = ProcessStatus.ERRORED

        observer = context.event_subscription_observer
        observer.status = ProcessStatus.INITIALISED

        task = asyncio.create_task(broker.execute())

        async with task_shutdown(task):
            await assert_status_eventually(broker, ProcessStatus.ERRORED)


class MockEventSubscriptionCoordinator(EventSubscriptionCoordinator):
    def __init__(self):
        self._status = ProcessStatus.INITIALISED
        self._start_count = 0
        self._cancel_count = 0
        self._exception = None

    @property
    def start_count(self) -> bool:
        return self._start_count

    @property
    def cancel_count(self) -> bool:
        return self._cancel_count

    @property
    def status(self) -> ProcessStatus:
        return self._status

    def raise_on_next_tick(self, exception: BaseException) -> None:
        self._exception = exception

    async def coordinate(self) -> None:
        self._start_count += 1
        self._status = ProcessStatus.STARTING
        try:
            self._status = ProcessStatus.RUNNING
            while True:
                if self._exception is not None:
                    raise self._exception
                await asyncio.sleep(0)
        except asyncio.CancelledError:
            self._cancel_count += 1
            self._status = ProcessStatus.STOPPED
            raise
        except BaseException:
            self._status = ProcessStatus.ERRORED
            raise


class MockEventSubscriptionObserver(EventSubscriptionObserver):
    def __init__(self):
        self._status = ProcessStatus.INITIALISED
        self._start_count = 0
        self._cancel_count = 0
        self._exception = None

    @property
    def start_count(self) -> bool:
        return self._start_count

    @property
    def cancel_count(self) -> bool:
        return self._cancel_count

    @property
    def status(self) -> ProcessStatus:
        return self._status

    def raise_on_next_tick(self, exception: BaseException) -> None:
        self._exception = exception

    async def observe(self) -> None:
        self._start_count += 1
        self._status = ProcessStatus.RUNNING
        try:
            while True:
                if self._exception is not None:
                    raise self._exception
                await asyncio.sleep(0)
        except asyncio.CancelledError:
            self._cancel_count += 1
            self._status = ProcessStatus.STOPPED
            raise
        except BaseException:
            self._status = ProcessStatus.ERRORED
            raise


class MockEventSubscriberManager(EventSubscriberManager):
    def __init__(self):
        self._start_count = 0
        self._cancel_count = 0
        self._exception = None

    @property
    def start_count(self) -> bool:
        return self._start_count

    @property
    def cancel_count(self) -> bool:
        return self._cancel_count

    def raise_on_next_tick(self, exception: BaseException) -> None:
        self._exception = exception

    async def add(self, subscriber: EventSubscriber) -> Self:
        return self

    async def start(self) -> None:
        pass

    async def maintain(self) -> None:
        self._start_count += 1
        try:
            while True:
                if self._exception is not None:
                    raise self._exception
                await asyncio.sleep(0)
        except asyncio.CancelledError:
            self._cancel_count += 1
            raise

    async def stop(self) -> None:
        pass


class TestDistributedEventBrokerErrorHandling:
    async def test_cancels_all_processes_when_broker_is_cancelled(
        self,
    ):
        coordinator = MockEventSubscriptionCoordinator()
        observer = MockEventSubscriptionObserver()
        subscriber_manager = MockEventSubscriberManager()

        broker = DistributedEventBroker(
            event_subscriber_manager=subscriber_manager,
            event_subscription_coordinator=coordinator,
            event_subscription_observer=observer,
        )

        task = asyncio.create_task(broker.execute())

        async with task_shutdown(task):
            await asyncio.gather(
                status_eventually_equal_to(coordinator, ProcessStatus.RUNNING),
                status_eventually_equal_to(observer, ProcessStatus.RUNNING),
            )

            task.cancel()

            await assert_cancel_count_eventually(subscriber_manager, 1)
            await assert_cancel_count_eventually(coordinator, 1)
            await assert_cancel_count_eventually(observer, 1)

            await assert_start_count_eventually(subscriber_manager, 1)
            await assert_start_count_eventually(coordinator, 1)
            await assert_start_count_eventually(observer, 1)

    async def test_cancels_observer_and_subscriber_manager_when_coordinator_raises(
        self,
    ):
        coordinator = MockEventSubscriptionCoordinator()
        observer = MockEventSubscriptionObserver()
        subscriber_manager = MockEventSubscriberManager()

        broker = DistributedEventBroker(
            event_subscriber_manager=subscriber_manager,
            event_subscription_coordinator=coordinator,
            event_subscription_observer=observer,
        )

        task = asyncio.create_task(broker.execute())

        async with task_shutdown(task):
            await asyncio.gather(
                status_eventually_equal_to(coordinator, ProcessStatus.RUNNING),
                status_eventually_equal_to(observer, ProcessStatus.RUNNING),
            )

            coordinator.raise_on_next_tick(BaseException("Oops!"))

            await assert_cancel_count_eventually(subscriber_manager, 1)
            await assert_cancel_count_eventually(observer, 1)
            await assert_cancel_count_eventually(coordinator, 0)

            await assert_start_count_eventually(subscriber_manager, 1)
            await assert_start_count_eventually(coordinator, 1)
            await assert_start_count_eventually(observer, 1)

    async def test_cancels_coordinator_and_subscriber_manager_when_observer_raises(
        self,
    ):
        coordinator = MockEventSubscriptionCoordinator()
        observer = MockEventSubscriptionObserver()
        subscriber_manager = MockEventSubscriberManager()

        broker = DistributedEventBroker(
            event_subscriber_manager=subscriber_manager,
            event_subscription_coordinator=coordinator,
            event_subscription_observer=observer,
        )

        task = asyncio.create_task(broker.execute())

        async with task_shutdown(task):
            await asyncio.gather(
                status_eventually_equal_to(coordinator, ProcessStatus.RUNNING),
                status_eventually_equal_to(observer, ProcessStatus.RUNNING),
            )

            observer.raise_on_next_tick(BaseException("Oops!"))

            await assert_cancel_count_eventually(subscriber_manager, 1)
            await assert_cancel_count_eventually(coordinator, 1)
            await assert_cancel_count_eventually(observer, 0)

            await assert_start_count_eventually(subscriber_manager, 1)
            await assert_start_count_eventually(coordinator, 1)
            await assert_start_count_eventually(observer, 1)

    async def test_cancels_coordinator_and_observer_when_subscriber_manager_raises(
        self,
    ):
        coordinator = MockEventSubscriptionCoordinator()
        observer = MockEventSubscriptionObserver()
        subscriber_manager = MockEventSubscriberManager()

        broker = DistributedEventBroker(
            event_subscriber_manager=subscriber_manager,
            event_subscription_coordinator=coordinator,
            event_subscription_observer=observer,
        )

        task = asyncio.create_task(broker.execute())

        async with task_shutdown(task):
            await asyncio.gather(
                status_eventually_equal_to(coordinator, ProcessStatus.RUNNING),
                status_eventually_equal_to(observer, ProcessStatus.RUNNING),
            )

            subscriber_manager.raise_on_next_tick(BaseException("Oops!"))

            await assert_cancel_count_eventually(subscriber_manager, 0)
            await assert_cancel_count_eventually(observer, 1)
            await assert_cancel_count_eventually(coordinator, 1)

            await assert_start_count_eventually(subscriber_manager, 1)
            await assert_start_count_eventually(coordinator, 1)
            await assert_start_count_eventually(observer, 1)

    async def test_restarts_processes_when_recoverable_exception_is_raised_by_coordinator(
            self
    ):
        coordinator = MockEventSubscriptionCoordinator()
        observer = MockEventSubscriptionObserver()
        subscriber_manager = MockEventSubscriberManager()

        broker = DistributedEventBroker(
            event_subscriber_manager=subscriber_manager,
            event_subscription_coordinator=coordinator,
            event_subscription_observer=observer,
        )

        task = asyncio.create_task(broker.execute())

        async with task_shutdown(task):
            await asyncio.gather(
                status_eventually_equal_to(coordinator, ProcessStatus.RUNNING),
                status_eventually_equal_to(observer, ProcessStatus.RUNNING),
            )

            coordinator.raise_on_next_tick(Exception("Recoverable error"))

            await assert_start_count_eventually(subscriber_manager, 2)
            await assert_start_count_eventually(coordinator, 2)
            await assert_start_count_eventually(observer, 2)

            coordinator.raise_on_next_tick(BaseException("Unrecoverable error"))

            await assert_cancel_count_eventually(coordinator, 0)
            await assert_cancel_count_eventually(observer, 2)
            await assert_cancel_count_eventually(subscriber_manager, 2)

    async def test_restarts_processes_when_recoverable_exception_is_raised_by_observer(
            self
    ):
        coordinator = MockEventSubscriptionCoordinator()
        observer = MockEventSubscriptionObserver()
        subscriber_manager = MockEventSubscriberManager()

        broker = DistributedEventBroker(
            event_subscriber_manager=subscriber_manager,
            event_subscription_coordinator=coordinator,
            event_subscription_observer=observer,
        )

        task = asyncio.create_task(broker.execute())

        async with task_shutdown(task):
            await asyncio.gather(
                status_eventually_equal_to(coordinator, ProcessStatus.RUNNING),
                status_eventually_equal_to(observer, ProcessStatus.RUNNING),
            )

            observer.raise_on_next_tick(Exception("Recoverable error"))

            await assert_start_count_eventually(subscriber_manager, 2)
            await assert_start_count_eventually(coordinator, 2)
            await assert_start_count_eventually(observer, 2)

            observer.raise_on_next_tick(BaseException("Unrecoverable error"))

            await assert_cancel_count_eventually(coordinator, 2)
            await assert_cancel_count_eventually(observer, 0)
            await assert_cancel_count_eventually(subscriber_manager, 2)

    async def test_restarts_processes_when_recoverable_exception_is_raised_by_subscriber_manager(
            self
    ):
        coordinator = MockEventSubscriptionCoordinator()
        observer = MockEventSubscriptionObserver()
        subscriber_manager = MockEventSubscriberManager()

        broker = DistributedEventBroker(
            event_subscriber_manager=subscriber_manager,
            event_subscription_coordinator=coordinator,
            event_subscription_observer=observer,
        )

        task = asyncio.create_task(broker.execute())

        async with task_shutdown(task):
            await asyncio.gather(
                status_eventually_equal_to(coordinator, ProcessStatus.RUNNING),
                status_eventually_equal_to(observer, ProcessStatus.RUNNING),
            )

            subscriber_manager.raise_on_next_tick(
                Exception("Recoverable error")
            )

            await assert_start_count_eventually(subscriber_manager, 2)
            await assert_start_count_eventually(coordinator, 2)
            await assert_start_count_eventually(observer, 2)

            subscriber_manager.raise_on_next_tick(
                BaseException("Unrecoverable error")
            )

            await assert_cancel_count_eventually(coordinator, 2)
            await assert_cancel_count_eventually(observer, 2)
            await assert_cancel_count_eventually(subscriber_manager, 0)

    async def test_uses_specified_error_handler_when_provided(self):
        coordinator = MockEventSubscriptionCoordinator()
        observer = MockEventSubscriptionObserver()
        subscriber_manager = MockEventSubscriberManager()

        broker = DistributedEventBroker(
            event_subscriber_manager=subscriber_manager,
            event_subscription_coordinator=coordinator,
            event_subscription_observer=observer,
            error_handler=ContinueErrorHandler()
        )

        task = asyncio.create_task(broker.execute())

        async with task_shutdown(task):
            await asyncio.gather(
                status_eventually_equal_to(coordinator, ProcessStatus.RUNNING),
                status_eventually_equal_to(observer, ProcessStatus.RUNNING),
            )

            coordinator.raise_on_next_tick(RuntimeError("Oops."))

            await asyncio.sleep(0)

            await assert_task_eventually_done(task)

            await assert_start_count_eventually(subscriber_manager, 1)
            await assert_start_count_eventually(coordinator, 1)
            await assert_start_count_eventually(observer, 1)
