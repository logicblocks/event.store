import asyncio
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import timedelta
from types import NoneType
from unittest.mock import Mock

import pytest

from logicblocks.event.processing import (
    ContinueErrorHandler,
    ErrorHandler,
    EventBroker,
    ProcessStatus,
)
from logicblocks.event.processing.broker.strategies.singleton import (
    SingletonEventBroker,
)
from logicblocks.event.processing.broker.subscribers import (
    EventSubscriberStore,
    InMemoryEventSubscriberStore,
)
from logicblocks.event.sources import (
    EventSource,
    EventSourceFactory,
    EventStoreEventSourceFactory,
)
from logicblocks.event.store.adapters import (
    EventStorageAdapter,
    InMemoryEventStorageAdapter,
)
from logicblocks.event.store.store import EventCategory
from logicblocks.event.testing import data
from logicblocks.event.testlogging.logger import CapturingLogger, LogLevel
from logicblocks.event.testsupport import (
    assert_status_eventually,
    assert_task_eventually_done,
    random_capturing_subscriber,
    status_eventually_equal_to,
    task_shutdown,
)
from logicblocks.event.testsupport.subscribers import CapturingEventSubscriber
from logicblocks.event.types import BaseEvent, CategoryIdentifier
from logicblocks.event.types.identifier import EventSourceIdentifier


@dataclass(frozen=True)
class MockedContext:
    node_id: str
    broker: EventBroker
    event_subscriber_store: Mock
    event_source_factory: Mock
    logger: CapturingLogger


@dataclass(frozen=True)
class RealContext:
    broker: EventBroker
    event_subscriber_store: EventSubscriberStore
    event_source_factory: EventSourceFactory
    event_storage_adapter: EventStorageAdapter


def make_event_broker_with_mocked_dependencies(
    error_handler: ErrorHandler[NoneType] | None = None,
    distribution_interval: timedelta = timedelta(milliseconds=10),
):
    node_id = data.random_node_id()

    event_subscriber_store = Mock(spec=EventSubscriberStore)
    event_source_factory = Mock(spec=EventSourceFactory)
    logger = CapturingLogger.create()

    kwargs = {
        "node_id": node_id,
        "event_subscriber_store": event_subscriber_store,
        "event_source_factory": event_source_factory,
        "logger": logger,
        "distribution_interval": distribution_interval,
    }

    if error_handler is not None:
        kwargs["error_handler"] = error_handler

    broker = SingletonEventBroker(**kwargs)

    return MockedContext(
        node_id=node_id,
        broker=broker,
        event_subscriber_store=event_subscriber_store,
        event_source_factory=event_source_factory,
        logger=logger,
    )


def make_event_broker_with_real_dependencies(
    distribution_interval: timedelta | None = None,
):
    node_id = data.random_node_id()

    event_subscriber_store = InMemoryEventSubscriberStore()
    event_storage_adapter = InMemoryEventStorageAdapter()
    event_source_factory = EventStoreEventSourceFactory(
        adapter=event_storage_adapter
    )

    kwargs = {
        "node_id": node_id,
        "event_subscriber_store": event_subscriber_store,
        "event_source_factory": event_source_factory,
    }

    if distribution_interval is not None:
        kwargs["distribution_interval"] = distribution_interval

    broker = SingletonEventBroker(**kwargs)

    return RealContext(
        broker=broker,
        event_subscriber_store=event_subscriber_store,
        event_source_factory=event_source_factory,
        event_storage_adapter=event_storage_adapter,
    )


async def subscriber_has_sources(
    subscriber: CapturingEventSubscriber,
    sources: Sequence[EventSource[EventSourceIdentifier, BaseEvent]],
) -> bool:
    while True:
        if subscriber.sources == sources:
            return True
        await asyncio.sleep(0)


async def assert_sources_eventually(
    subscriber: CapturingEventSubscriber,
    sources: Sequence[EventSource[EventSourceIdentifier, BaseEvent]],
):
    timeout = timedelta(milliseconds=500)
    try:
        await asyncio.wait_for(
            subscriber_has_sources(subscriber, sources),
            timeout=timeout.total_seconds(),
        )
    except asyncio.TimeoutError:
        pytest.fail(
            f"Expected subscriber {subscriber.key} to eventually have "
            f"sources {sources} but timed out waiting."
        )


class TestSingletonEventBrokerStatuses:
    async def test_has_initialised_status_before_executing(self):
        context = make_event_broker_with_mocked_dependencies()
        broker = context.broker

        assert broker.status == ProcessStatus.INITIALISED

    async def test_has_running_status_while_executing(self):
        context = make_event_broker_with_mocked_dependencies()
        broker = context.broker

        task = asyncio.create_task(broker.execute())

        async with task_shutdown(task):
            await assert_status_eventually(broker, ProcessStatus.RUNNING)

    async def test_has_stopped_status_after_cancel(self):
        context = make_event_broker_with_mocked_dependencies()
        broker = context.broker

        async def wait_until_running():
            while True:
                await asyncio.sleep(0)
                status = broker.status
                if status == ProcessStatus.RUNNING:
                    return

        task = asyncio.create_task(broker.execute())

        async with task_shutdown(task):
            await wait_until_running()

            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

            await assert_status_eventually(broker, ProcessStatus.STOPPED)

    async def test_has_errored_status_when_fatal_exception_thrown(self):
        context = make_event_broker_with_mocked_dependencies()
        broker = context.broker
        event_subscriber_store = context.event_subscriber_store

        async def wait_until_running():
            while True:
                await asyncio.sleep(0)
                status = broker.status
                if status == ProcessStatus.RUNNING:
                    return

        task = asyncio.create_task(broker.execute())

        async with task_shutdown(task):
            await wait_until_running()

            event_subscriber_store.list.side_effect = BaseException(
                "Fatal error"
            )

            await asyncio.gather(task, return_exceptions=True)

            await assert_status_eventually(broker, ProcessStatus.ERRORED)


class TestSingletonEventBrokerLogging:
    async def test_logs_when_starting(self):
        context = make_event_broker_with_mocked_dependencies(
            distribution_interval=timedelta(seconds=5),
        )
        node_id = context.node_id
        broker = context.broker
        logger = context.logger

        task = asyncio.create_task(broker.execute())

        async with task_shutdown(task):
            await assert_status_eventually(broker, ProcessStatus.RUNNING)

            startup_event = logger.find_event(
                "event.processing.broker.starting"
            )

            assert startup_event is not None
            assert startup_event.level == LogLevel.INFO
            assert startup_event.is_async is True
            assert startup_event.context == {
                "node": node_id,
                "distribution_interval_seconds": 5.0,
            }

    async def test_logs_when_running(self):
        context = make_event_broker_with_mocked_dependencies(
            distribution_interval=timedelta(seconds=5),
        )
        node_id = context.node_id
        broker = context.broker
        logger = context.logger

        task = asyncio.create_task(broker.execute())

        async with task_shutdown(task):
            await assert_status_eventually(broker, ProcessStatus.RUNNING)

            running_event = logger.find_event(
                "event.processing.broker.running"
            )

            assert running_event is not None
            assert running_event.level == LogLevel.INFO
            assert running_event.is_async is True
            assert running_event.context == {"node": node_id}

    async def test_logs_when_stopped(self):
        context = make_event_broker_with_mocked_dependencies()
        node_id = context.node_id
        broker = context.broker
        logger = context.logger

        async def wait_until_running():
            while True:
                await asyncio.sleep(0)
                status = broker.status
                if status == ProcessStatus.RUNNING:
                    return

        task = asyncio.create_task(broker.execute())

        async with task_shutdown(task):
            await wait_until_running()

            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

            stopped_event = logger.find_event(
                "event.processing.broker.stopped"
            )

            assert stopped_event is not None
            assert stopped_event.level == LogLevel.INFO
            assert stopped_event.is_async is True
            assert stopped_event.context == {"node": node_id}

    async def test_logs_when_errored(self):
        context = make_event_broker_with_mocked_dependencies()
        node_id = context.node_id
        broker = context.broker
        logger = context.logger
        event_subscriber_store = context.event_subscriber_store

        async def wait_until_running():
            while True:
                await asyncio.sleep(0)
                status = broker.status
                if status == ProcessStatus.RUNNING:
                    return

        task = asyncio.create_task(broker.execute())

        async with task_shutdown(task):
            await wait_until_running()

            exception = BaseException("Fatal error")
            event_subscriber_store.list.side_effect = exception

            await asyncio.gather(task, return_exceptions=True)

            failed_event = logger.find_event("event.processing.broker.failed")

            assert failed_event is not None
            assert failed_event.level == LogLevel.ERROR
            assert failed_event.is_async is True
            assert failed_event.context == {"node": node_id}
            assert failed_event.exc_info is not None
            assert failed_event.exc_info[0] is BaseException


class TestSingletonEventBrokerDistribution:
    async def test_distributes_subscriptions_to_its_own_subscribers(self):
        context = make_event_broker_with_real_dependencies()
        broker = context.broker
        event_storage_adapter = context.event_storage_adapter

        subscriber_1_subscription_requests = [
            CategoryIdentifier(
                category=data.random_event_category_name(),
            )
        ]
        subscriber_2_subscription_requests = [
            CategoryIdentifier(
                category=data.random_event_category_name(),
            ),
            CategoryIdentifier(
                category=data.random_event_category_name(),
            ),
        ]

        subscriber_1 = random_capturing_subscriber(
            subscription_requests=subscriber_1_subscription_requests
        )
        subscriber_2 = random_capturing_subscriber(
            subscription_requests=subscriber_2_subscription_requests
        )

        await broker.register(subscriber_1)
        await broker.register(subscriber_2)

        task = asyncio.create_task(broker.execute())

        async with task_shutdown(task):
            await assert_status_eventually(broker, ProcessStatus.RUNNING)

            await assert_sources_eventually(
                subscriber_1,
                [
                    EventCategory(
                        adapter=event_storage_adapter, category=request
                    )
                    for request in subscriber_1_subscription_requests
                ],
            )
            await assert_sources_eventually(
                subscriber_2,
                [
                    EventCategory(
                        adapter=event_storage_adapter, category=request
                    )
                    for request in subscriber_2_subscription_requests
                ],
            )

    async def test_distributes_every_distribution_interval(self):
        context = make_event_broker_with_real_dependencies(
            distribution_interval=timedelta(milliseconds=20)
        )
        broker = context.broker

        subscriber_subscription_requests = [
            CategoryIdentifier(
                category=data.random_event_category_name(),
            )
        ]
        subscriber = random_capturing_subscriber(
            subscription_requests=subscriber_subscription_requests
        )

        await broker.register(subscriber)

        task = asyncio.create_task(broker.execute())

        async with task_shutdown(task):
            await asyncio.sleep(timedelta(milliseconds=50).total_seconds())

            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

            assert subscriber.counts["accept"] >= 2


class TestSingletonEventBrokerErrorHandling:
    async def test_restarts_when_non_fatal_exception_is_raised(
        self,
    ):
        context = make_event_broker_with_mocked_dependencies()
        broker = context.broker
        event_subscriber_store = context.event_subscriber_store

        event_subscriber_store.list.return_value = []

        task = asyncio.create_task(broker.execute())

        async with task_shutdown(task):
            await status_eventually_equal_to(broker, ProcessStatus.RUNNING)

            raised = False

            def raise_exception_once():
                nonlocal raised
                if not raised:
                    raised = True
                    raise RuntimeError("Recoverable error")
                return []

            call_count_before = len(event_subscriber_store.list.mock_calls)

            event_subscriber_store.list.side_effect = raise_exception_once

            # Allow the broker to attempt to recover from the error
            await asyncio.sleep(0.05)

            call_count_after = len(event_subscriber_store.list.mock_calls) - 1

            assert call_count_after > call_count_before

    async def test_uses_specified_error_handler_when_provided(self):
        context = make_event_broker_with_mocked_dependencies(
            error_handler=ContinueErrorHandler(),
        )
        broker = context.broker
        event_subscriber_store = context.event_subscriber_store

        event_subscriber_store.list.return_value = []

        task = asyncio.create_task(broker.execute())

        async with task_shutdown(task):
            await status_eventually_equal_to(broker, ProcessStatus.RUNNING)

            raised = False

            def raise_exception_once():
                nonlocal raised
                if not raised:
                    raised = True
                    raise RuntimeError("Recoverable error")
                return []

            call_count_before = len(event_subscriber_store.list.mock_calls)

            event_subscriber_store.list.side_effect = raise_exception_once

            await assert_task_eventually_done(task)

            call_count_after = len(event_subscriber_store.list.mock_calls) - 1

            assert call_count_after == call_count_before
