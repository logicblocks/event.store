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


class CancelTrackable(Protocol):
    @property
    def cancelled(self) -> bool:
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


async def cancelled_is_true(cancel_trackable: CancelTrackable):
    while True:
        if cancel_trackable.cancelled:
            return
        await asyncio.sleep(0)


async def status_eventually_equal_to(process: Process, status: ProcessStatus):
    timeout = timedelta(milliseconds=500)
    await asyncio.wait_for(
        status_equal_to(process, status),
        timeout=timeout.total_seconds(),
    )


async def cancelled_eventually(cancel_trackable: CancelTrackable):
    timeout = timedelta(milliseconds=500)
    await asyncio.wait_for(
        cancelled_is_true(cancel_trackable),
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


async def assert_cancelled_eventually(cancel_trackable: CancelTrackable):
    try:
        await cancelled_eventually(cancel_trackable)
    except asyncio.TimeoutError:
        pytest.fail(
            "Expected to eventually be cancelled but timed out waiting."
        )


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
        self._cancelled = False
        self._exception = None

    @property
    def cancelled(self) -> bool:
        return self._cancelled

    @property
    def status(self) -> ProcessStatus:
        return self._status

    def raise_on_next_tick(self, exception: BaseException) -> None:
        self._exception = exception

    async def coordinate(self) -> None:
        self._status = ProcessStatus.STARTING
        try:
            self._status = ProcessStatus.RUNNING
            while True:
                if self._exception is not None:
                    raise self._exception
                await asyncio.sleep(0)
        except asyncio.CancelledError:
            self._cancelled = True
            self._status = ProcessStatus.STOPPED
            raise
        except BaseException:
            self._status = ProcessStatus.ERRORED
            raise


class MockEventSubscriptionObserver(EventSubscriptionObserver):
    def __init__(self):
        self._cancelled = False
        self._status = ProcessStatus.INITIALISED
        self._exception = None

    @property
    def cancelled(self) -> bool:
        return self._cancelled

    @property
    def status(self) -> ProcessStatus:
        return self._status

    def raise_on_next_tick(self, exception: BaseException) -> None:
        self._exception = exception

    async def observe(self) -> None:
        self._status = ProcessStatus.RUNNING
        try:
            while True:
                if self._exception is not None:
                    raise self._exception
                await asyncio.sleep(0)
        except asyncio.CancelledError:
            self._cancelled = True
            self._status = ProcessStatus.STOPPED
            raise
        except BaseException:
            self._status = ProcessStatus.ERRORED
            raise


class MockEventSubscriberManager(EventSubscriberManager):
    def __init__(self):
        self._cancelled = False
        self._exception = None

    @property
    def cancelled(self) -> bool:
        return self._cancelled

    def raise_on_next_tick(self, exception: BaseException) -> None:
        self._exception = exception

    async def add(self, subscriber: EventSubscriber) -> Self:
        return self

    async def start(self) -> None:
        pass

    async def maintain(self) -> None:
        try:
            while True:
                if self._exception is not None:
                    raise self._exception
                await asyncio.sleep(0)
        except asyncio.CancelledError:
            self._cancelled = True
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

            await assert_cancelled_eventually(subscriber_manager)
            await assert_cancelled_eventually(coordinator)
            await assert_cancelled_eventually(observer)

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

            await assert_cancelled_eventually(subscriber_manager)
            await assert_cancelled_eventually(observer)

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

            await assert_cancelled_eventually(subscriber_manager)
            await assert_cancelled_eventually(coordinator)

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

            await assert_cancelled_eventually(observer)
            await assert_cancelled_eventually(coordinator)
