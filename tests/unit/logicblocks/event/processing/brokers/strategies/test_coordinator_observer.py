import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta
from unittest.mock import Mock

import pytest

from logicblocks.event.processing.broker import (
    CoordinatorObserverEventBroker,
    EventBroker,
    EventBrokerStatus,
    EventSubscriberManager,
    EventSubscriptionCoordinator,
    EventSubscriptionCoordinatorStatus,
    EventSubscriptionObserver,
    EventSubscriptionObserverStatus,
    NodeManager,
)
from logicblocks.event.processing.broker.strategies.coordinator_observer import (
    determine_event_broker_status,
)


@dataclass(frozen=True)
class Context:
    broker: EventBroker
    node_manager: Mock
    event_subscriber_manager: Mock
    event_subscription_coordinator: Mock
    event_subscription_observer: Mock


def make_event_broker():
    node_manager = Mock(spec=NodeManager)
    event_subscriber_manager = Mock(spec=EventSubscriberManager)
    event_subscription_coordinator = Mock(spec=EventSubscriptionCoordinator)
    event_subscription_observer = Mock(spec=EventSubscriptionObserver)

    broker = CoordinatorObserverEventBroker(
        node_manager=node_manager,
        event_subscriber_manager=event_subscriber_manager,
        event_subscription_coordinator=event_subscription_coordinator,
        event_subscription_observer=event_subscription_observer,
    )

    return Context(
        broker=broker,
        node_manager=node_manager,
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
    event_broker: EventBroker, status: EventBrokerStatus
):
    while True:
        if event_broker.status != status:
            return
        await asyncio.sleep(0)


async def status_equal_to(
    event_broker: EventBroker, status: EventBrokerStatus
):
    while True:
        if event_broker.status == status:
            return
        await asyncio.sleep(0)


async def assert_status_eventually(
    event_broker: EventBroker, status: EventBrokerStatus
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


@pytest.mark.parametrize(
    "coordinator_status, observer_status, expected_status",
    [
        (
            EventSubscriptionCoordinatorStatus.INITIALISED,
            EventSubscriptionObserverStatus.INITIALISED,
            EventBrokerStatus.INITIALISED,
        ),
        (
            EventSubscriptionCoordinatorStatus.RUNNING,
            EventSubscriptionObserverStatus.RUNNING,
            EventBrokerStatus.RUNNING,
        ),
        (
            EventSubscriptionCoordinatorStatus.STARTING,
            EventSubscriptionObserverStatus.INITIALISED,
            EventBrokerStatus.STARTING,
        ),
        (
            EventSubscriptionCoordinatorStatus.STARTING,
            EventSubscriptionObserverStatus.RUNNING,
            EventBrokerStatus.RUNNING,
        ),
        (
            EventSubscriptionCoordinatorStatus.STARTING,
            EventSubscriptionObserverStatus.STOPPED,
            EventBrokerStatus.STOPPING,
        ),
        (
            EventSubscriptionCoordinatorStatus.STARTING,
            EventSubscriptionObserverStatus.ERRORED,
            EventBrokerStatus.ERRORED,
        ),
        (
            EventSubscriptionCoordinatorStatus.RUNNING,
            EventSubscriptionObserverStatus.INITIALISED,
            EventBrokerStatus.STARTING,
        ),
        (
            EventSubscriptionCoordinatorStatus.INITIALISED,
            EventSubscriptionObserverStatus.RUNNING,
            EventBrokerStatus.STARTING,
        ),
        (
            EventSubscriptionCoordinatorStatus.ERRORED,
            EventSubscriptionObserverStatus.RUNNING,
            EventBrokerStatus.ERRORED,
        ),
        (
            EventSubscriptionCoordinatorStatus.RUNNING,
            EventSubscriptionObserverStatus.ERRORED,
            EventBrokerStatus.ERRORED,
        ),
        (
            EventSubscriptionCoordinatorStatus.ERRORED,
            EventSubscriptionObserverStatus.INITIALISED,
            EventBrokerStatus.ERRORED,
        ),
        (
            EventSubscriptionCoordinatorStatus.INITIALISED,
            EventSubscriptionObserverStatus.ERRORED,
            EventBrokerStatus.ERRORED,
        ),
        (
            EventSubscriptionCoordinatorStatus.STOPPED,
            EventSubscriptionObserverStatus.ERRORED,
            EventBrokerStatus.ERRORED,
        ),
        (
            EventSubscriptionCoordinatorStatus.ERRORED,
            EventSubscriptionObserverStatus.STOPPED,
            EventBrokerStatus.ERRORED,
        ),
        (
            EventSubscriptionCoordinatorStatus.ERRORED,
            EventSubscriptionObserverStatus.ERRORED,
            EventBrokerStatus.ERRORED,
        ),
        (
            EventSubscriptionCoordinatorStatus.STOPPED,
            EventSubscriptionObserverStatus.INITIALISED,
            EventBrokerStatus.STOPPING,
        ),
        (
            EventSubscriptionCoordinatorStatus.INITIALISED,
            EventSubscriptionObserverStatus.STOPPED,
            EventBrokerStatus.STOPPING,
        ),
        (
            EventSubscriptionCoordinatorStatus.STOPPED,
            EventSubscriptionObserverStatus.RUNNING,
            EventBrokerStatus.STOPPING,
        ),
        (
            EventSubscriptionCoordinatorStatus.RUNNING,
            EventSubscriptionObserverStatus.STOPPED,
            EventBrokerStatus.STOPPING,
        ),
        (
            EventSubscriptionCoordinatorStatus.STOPPED,
            EventSubscriptionObserverStatus.STOPPED,
            EventBrokerStatus.STOPPED,
        ),
    ],
)
def test_determine_event_broker_status(
    coordinator_status, observer_status, expected_status
):
    assert (
        determine_event_broker_status(coordinator_status, observer_status)
        == expected_status
    )


class TestCoordinatorObserverEventBroker:
    async def test_has_initialised_status_before_executing(self):
        context = make_event_broker()
        broker = context.broker

        assert broker.status == EventBrokerStatus.INITIALISED

    async def test_has_running_status_when_coordinator_and_observer_running(
        self,
    ):
        context = make_event_broker()
        broker = context.broker

        coordinator = context.event_subscription_coordinator
        coordinator.status = EventSubscriptionCoordinatorStatus.RUNNING

        observer = context.event_subscription_observer
        observer.status = EventSubscriptionObserverStatus.RUNNING

        task = asyncio.create_task(broker.execute())

        async with task_shutdown(task):
            await assert_status_eventually(broker, EventBrokerStatus.RUNNING)

    async def test_has_starting_status_when_coordinator_is_running_and_observer_is_initialised(
        self,
    ):
        context = make_event_broker()
        broker = context.broker

        coordinator = context.event_subscription_coordinator
        coordinator.status = EventSubscriptionCoordinatorStatus.RUNNING

        observer = context.event_subscription_observer
        observer.status = EventSubscriptionObserverStatus.INITIALISED

        task = asyncio.create_task(broker.execute())

        async with task_shutdown(task):
            await assert_status_eventually(broker, EventBrokerStatus.STARTING)

    async def test_has_starting_status_when_coordinator_is_initialised_and_observer_is_running(
        self,
    ):
        context = make_event_broker()
        broker = context.broker

        coordinator = context.event_subscription_coordinator
        coordinator.status = EventSubscriptionCoordinatorStatus.RUNNING

        observer = context.event_subscription_observer
        observer.status = EventSubscriptionObserverStatus.INITIALISED

        task = asyncio.create_task(broker.execute())

        async with task_shutdown(task):
            await assert_status_eventually(broker, EventBrokerStatus.STARTING)

    async def test_has_errored_status_when_coordinator_is_errored_and_observer_is_running(
        self,
    ):
        context = make_event_broker()
        broker = context.broker
        coordinator = context.event_subscription_coordinator
        coordinator.status = EventSubscriptionCoordinatorStatus.ERRORED

        observer = context.event_subscription_observer
        observer.status = EventSubscriptionObserverStatus.INITIALISED

        task = asyncio.create_task(broker.execute())

        async with task_shutdown(task):
            await assert_status_eventually(broker, EventBrokerStatus.ERRORED)

    async def test_has_errored_status_when_coordinator_is_running_and_observer_is_errored(
        self,
    ):
        context = make_event_broker()
        broker = context.broker
        coordinator = context.event_subscription_coordinator
        coordinator.status = EventSubscriptionCoordinatorStatus.ERRORED

        observer = context.event_subscription_observer
        observer.status = EventSubscriptionObserverStatus.INITIALISED

        task = asyncio.create_task(broker.execute())

        async with task_shutdown(task):
            await assert_status_eventually(broker, EventBrokerStatus.ERRORED)

    async def test_has_errored_status_when_coordinator_is_errored_and_observer_is_errored(
        self,
    ):
        context = make_event_broker()
        broker = context.broker
        coordinator = context.event_subscription_coordinator
        coordinator.status = EventSubscriptionCoordinatorStatus.ERRORED

        observer = context.event_subscription_observer
        observer.status = EventSubscriptionObserverStatus.INITIALISED

        task = asyncio.create_task(broker.execute())

        async with task_shutdown(task):
            await assert_status_eventually(broker, EventBrokerStatus.ERRORED)
