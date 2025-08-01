import asyncio
import os
from collections.abc import Sequence
from contextlib import asynccontextmanager
from datetime import timedelta

import pytest
import pytest_asyncio
from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from logicblocks.event.persistence.postgres import ConnectionSettings
from logicblocks.event.processing import (
    EventBroker,
    EventBrokerStorageType,
    EventBrokerType,
    EventProcessor,
    EventSubscriber,
    EventSubscriptionConsumer,
    make_event_broker,
    make_subscriber,
)
from logicblocks.event.processing.broker.strategies.singleton.builder import (
    SingletonEventBrokerSettings,
)
from logicblocks.event.processing.consumers.logger import (
    default_logger as consumer_logger,
)
from logicblocks.event.store import (
    EventStore,
    PostgresEventStorageAdapter,
)
from logicblocks.event.store.state import (
    StoredEventEventConsumerStateConverter,
)
from logicblocks.event.testing import data
from logicblocks.event.testing.builders import NewEventBuilder
from logicblocks.event.testsupport import (
    connection_pool,
    create_table,
    drop_table,
)
from logicblocks.event.types import JsonValue, LogIdentifier, StoredEvent
from logicblocks.event.types.identifier import CategoryIdentifier

connection_settings = ConnectionSettings(
    user="admin",
    password="super-secret",
    host=os.getenv("DB_HOST", "localhost"),
    port=int(os.getenv("DB_PORT", "5432")),
    dbname="some-database",
)


@pytest_asyncio.fixture
async def open_connection_pool():
    async with connection_pool(connection_settings) as pool:
        yield pool


event_broker_settings = SingletonEventBrokerSettings(
    distribution_interval=timedelta(milliseconds=400),
)


class CapturingEventProcessor(EventProcessor):
    def __init__(self):
        self.events = []

    async def process_event(self, event: StoredEvent[str, JsonValue]) -> None:
        self.events.append(event)


def random_category_names(count: int) -> Sequence[str]:
    return [data.random_event_category_name() for _ in range(count)]


async def register_subscribers_on_broker(
    event_broker: EventBroker, subscribers: Sequence[EventSubscriber]
) -> None:
    for subscriber in subscribers:
        await event_broker.register(subscriber)


async def publish_event_per_category(
    event_store: EventStore, categories: Sequence[str]
) -> None:
    for category in categories:
        await event_store.stream(
            category=category, stream=data.random_event_stream_name()
        ).publish(events=[NewEventBuilder().build()])


async def consume_until_event_count(
    event_processor: CapturingEventProcessor,
    subscribers: Sequence[EventSubscriptionConsumer],
    event_count: int,
) -> None:
    while len(event_processor.events) < event_count:
        consume_tasks = [
            asyncio.create_task(subscriber.consume_all())
            for subscriber in subscribers
        ]
        await asyncio.gather(*consume_tasks)
        await asyncio.sleep(timedelta(milliseconds=100).total_seconds())


@asynccontextmanager
async def fail_on_event_processing_timeout():
    try:
        yield
    except asyncio.TimeoutError:
        pytest.fail("Timed out waiting for all events to be processed.")


@asynccontextmanager
async def cleanup(event_broker_task):
    try:
        yield
    finally:
        event_broker_task.cancel()
        await asyncio.gather(event_broker_task, return_exceptions=True)


class TestEventProcessing:
    connection_pool: AsyncConnectionPool[AsyncConnection]

    @pytest_asyncio.fixture(autouse=True)
    async def store_connection_pool(self, open_connection_pool):
        self.connection_pool = open_connection_pool

    @pytest_asyncio.fixture(autouse=True)
    async def reinitialise_storage(self, open_connection_pool):
        await drop_table(open_connection_pool, "events")
        await drop_table(open_connection_pool, "projections")
        await drop_table(open_connection_pool, "subscribers")
        await drop_table(open_connection_pool, "subscriptions")
        await create_table(open_connection_pool, "events")
        await create_table(open_connection_pool, "projections")
        await create_table(open_connection_pool, "subscribers")
        await create_table(open_connection_pool, "subscriptions")

    async def test_consumes_from_category(self):
        subscriber_group_count = 10

        adapter = PostgresEventStorageAdapter(
            connection_source=self.connection_pool
        )
        event_processor = CapturingEventProcessor()
        event_store = EventStore(adapter=adapter)

        categories = random_category_names(count=subscriber_group_count)

        node_id = data.random_node_id()

        subscribers = [
            make_subscriber(
                subscriber_group=f"subscriber-group-for-{category}",
                subscription_request=CategoryIdentifier(category=category),
                subscriber_state_category=event_store.category(
                    category=f"subscriber-state-for-{category}"
                ),
                subscriber_state_converter=StoredEventEventConsumerStateConverter(),
                event_processor=event_processor,
                logger=consumer_logger.bind(node=node_id),
            )
            for category in categories
        ]

        await publish_event_per_category(event_store, categories)

        event_broker = make_event_broker(
            node_id=node_id,
            broker_type=EventBrokerType.Singleton,
            storage_type=EventBrokerStorageType.Postgres,
            settings=SingletonEventBrokerSettings(
                distribution_interval=timedelta(seconds=10)
            ),
            connection_settings=connection_settings,
            connection_pool=self.connection_pool,
            adapter=adapter,
        )

        for subscriber in subscribers:
            await event_broker.register(subscriber)

        event_broker_task = asyncio.create_task(event_broker.execute())

        expected_event_count = subscriber_group_count

        async with (
            cleanup(event_broker_task),
            fail_on_event_processing_timeout(),
        ):
            await asyncio.wait_for(
                consume_until_event_count(
                    event_processor,
                    subscribers,
                    expected_event_count,
                ),
                timeout=timedelta(seconds=20).total_seconds(),
            )

    async def test_consumes_from_log(self):
        subscriber_group_count = 10

        adapter = PostgresEventStorageAdapter(
            connection_source=self.connection_pool
        )
        event_processor = CapturingEventProcessor()
        event_store = EventStore(adapter=adapter)

        categories = random_category_names(count=subscriber_group_count)

        node_id = data.random_node_id()

        subscribers = [
            make_subscriber(
                subscriber_group="subscriber-group-for-log",
                subscription_request=LogIdentifier(),
                subscriber_state_category=event_store.category(
                    category="subscriber-state-for-log"
                ),
                subscriber_state_converter=StoredEventEventConsumerStateConverter(),
                event_processor=event_processor,
                logger=consumer_logger.bind(node=node_id),
            )
        ]

        await publish_event_per_category(event_store, categories)

        event_broker = make_event_broker(
            node_id=node_id,
            broker_type=EventBrokerType.Singleton,
            storage_type=EventBrokerStorageType.Postgres,
            settings=SingletonEventBrokerSettings(
                distribution_interval=timedelta(seconds=10)
            ),
            connection_settings=connection_settings,
            connection_pool=self.connection_pool,
            adapter=adapter,
        )

        for subscriber in subscribers:
            await event_broker.register(subscriber)

        event_broker_task = asyncio.create_task(event_broker.execute())

        expected_event_count = subscriber_group_count

        async with (
            cleanup(event_broker_task),
            fail_on_event_processing_timeout(),
        ):
            await asyncio.wait_for(
                consume_until_event_count(
                    event_processor,
                    subscribers,
                    expected_event_count,
                ),
                timeout=timedelta(seconds=20).total_seconds(),
            )
