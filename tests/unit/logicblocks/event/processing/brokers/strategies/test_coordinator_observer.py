import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta
from unittest.mock import Mock

import pytest

from logicblocks.event.processing.broker import (
    CoordinatorObserverEventBroker,
    EventBroker,
    EventSubscriberManager,
    EventSubscriptionCoordinator,
    EventSubscriptionObserver,
    ProcessStatus,
)


@dataclass(frozen=True)
class Context:
    broker: EventBroker
    event_subscriber_manager: Mock
    event_subscription_coordinator: Mock
    event_subscription_observer: Mock


def make_event_broker():
    event_subscriber_manager = Mock(spec=EventSubscriberManager)
    event_subscription_coordinator = Mock(spec=EventSubscriptionCoordinator)
    event_subscription_observer = Mock(spec=EventSubscriptionObserver)

    broker = CoordinatorObserverEventBroker(
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


async def status_equal_to(event_broker: EventBroker, status: ProcessStatus):
    while True:
        if event_broker.status == status:
            return
        await asyncio.sleep(0)


async def assert_status_eventually(
    event_broker: EventBroker, status: ProcessStatus
):
    timeout = timedelta(milliseconds=500)
    try:
        await asyncio.wait_for(
            status_equal_to(event_broker, status),
            timeout=timeout.total_seconds(),
        )
    except asyncio.TimeoutError:
        pytest.fail(
            f"Expected status to eventually equal '{status}' "
            f"but timed out after {timeout.total_seconds()} seconds."
        )


class TestCoordinatorObserverEventBroker:
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
