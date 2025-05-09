import asyncio
import random
from collections.abc import Mapping, MutableMapping, Sequence
from contextlib import asynccontextmanager
from datetime import timedelta

import pytest
import pytest_asyncio
from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from logicblocks.event.persistence.postgres import ConnectionSettings
from logicblocks.event.processing import (
    EventBroker,
    EventBrokerSettings,
    EventProcessor,
    EventSubscriber,
    EventSubscriptionConsumer,
    ProcessStatus,
    make_postgres_event_broker,
    make_subscriber,
)
from logicblocks.event.processing.broker.logger import (
    default_logger as broker_logger,
)
from logicblocks.event.processing.consumers.logger import (
    default_logger as consumer_logger,
)
from logicblocks.event.store import (
    EventStore,
    PostgresEventStorageAdapter,
)
from logicblocks.event.testing import data
from logicblocks.event.testing.builders import NewEventBuilder
from logicblocks.event.testsupport import (
    connection_pool,
    create_table,
    drop_table,
)
from logicblocks.event.types import JsonValue, StoredEvent
from logicblocks.event.types.identifier import CategoryIdentifier

connection_settings = ConnectionSettings(
    user="admin",
    password="super-secret",
    host="localhost",
    port=5432,
    dbname="some-database",
)
event_broker_settings = EventBrokerSettings(
    subscriber_manager_heartbeat_interval=timedelta(milliseconds=200),
    subscriber_manager_purge_interval=timedelta(seconds=2),
    subscriber_manager_subscriber_max_age=timedelta(seconds=15),
    observer_synchronisation_interval=timedelta(milliseconds=400),
    coordinator_distribution_interval=timedelta(milliseconds=400),
    coordinator_subscriber_max_time_since_last_seen=timedelta(seconds=10),
)


class CapturingEventProcessor(EventProcessor):
    def __init__(self):
        self.events = []

    async def process_event(self, event: StoredEvent[str, JsonValue]) -> None:
        self.events.append(event)


@pytest_asyncio.fixture
async def open_connection_pool():
    async with connection_pool(connection_settings) as pool:
        yield pool


def random_node_ids(count: int) -> Sequence[str]:
    return [data.random_node_id() for _ in range(count)]


def random_category_names(count: int) -> Sequence[str]:
    return [data.random_event_category_name() for _ in range(count)]


def make_subscribers(
    node_ids: Sequence[str],
    categories: Sequence[str],
    event_store: EventStore,
    event_processor: EventProcessor,
) -> Mapping[str, Sequence[EventSubscriptionConsumer]]:
    return {
        node_id: [
            make_subscriber(
                subscriber_group=f"subscriber-group-for-{category}",
                subscription_request=CategoryIdentifier(category=category),
                subscriber_state_category=event_store.category(
                    category=f"subscriber-state-for-{category}"
                ),
                event_processor=event_processor,
                logger=consumer_logger.bind(node=node_id),
            )
            for category in categories
        ]
        for node_id in node_ids
    }


def make_event_broker(
    node_id: str, connection_pool: AsyncConnectionPool[AsyncConnection]
) -> EventBroker:
    return make_postgres_event_broker(
        node_id=node_id,
        connection_settings=connection_settings,
        connection_pool=connection_pool,
        settings=event_broker_settings,
    )


def make_event_brokers(
    node_ids: Sequence[str],
    connection_pool: AsyncConnectionPool[AsyncConnection],
) -> Mapping[str, EventBroker]:
    return {
        node_id: make_event_broker(node_id, connection_pool)
        for node_id in node_ids
    }


async def register_subscribers_on_broker(
    event_broker: EventBroker, subscribers: Sequence[EventSubscriber]
) -> None:
    for subscriber in subscribers:
        await event_broker.register(subscriber)


async def register_subscribers_on_brokers(
    event_brokers: Mapping[str, EventBroker],
    subscribers: Mapping[str, Sequence[EventSubscriber]],
) -> None:
    for node_id, event_broker in event_brokers.items():
        await register_subscribers_on_broker(
            event_broker, subscribers[node_id]
        )


async def start_brokers(
    node_ids: Sequence[str], event_brokers: Mapping[str, EventBroker]
) -> Sequence[asyncio.Task]:
    return [
        asyncio.create_task(event_brokers[node_id].execute())
        for node_id in node_ids
    ]


async def publish_event_per_category(
    event_store: EventStore, categories: Sequence[str]
) -> None:
    for category in categories:
        (
            await event_store.stream(
                category=category, stream=data.random_event_stream_name()
            ).publish(events=[NewEventBuilder().build()])
        )


async def consume_until_event_count(
    event_processor: CapturingEventProcessor,
    subscribers: Mapping[str, Sequence[EventSubscriptionConsumer]],
    event_count: int,
) -> None:
    while len(event_processor.events) < event_count:
        consume_tasks = [
            asyncio.create_task(subscriber.consume_all())
            for _, node_subscribers in subscribers.items()
            for subscriber in node_subscribers
        ]
        await asyncio.gather(*consume_tasks)
        await asyncio.sleep(timedelta(milliseconds=100).total_seconds())


class NodeSet:
    def __init__(
        self,
        connection_pool: AsyncConnectionPool[AsyncConnection],
        event_store: EventStore,
        event_processor: EventProcessor,
        node_count: int,
    ):
        self._connection_pool = connection_pool
        self._event_store = event_store
        self._event_processor = event_processor

        self._node_ids = random_node_ids(node_count)
        self._event_brokers = make_event_brokers(
            self._node_ids, self._connection_pool
        )

        self._event_broker_tasks: Mapping[str, asyncio.Task] = {}
        self._category_names: Sequence[str] = []
        self._subscribers: Mapping[
            str, Sequence[EventSubscriptionConsumer]
        ] = {}
        self._node_statuses: MutableMapping[str, ProcessStatus] = {}

    def schedule_node_start(self, node_id: str) -> asyncio.Task:
        return asyncio.create_task(self._event_brokers[node_id].execute())

    def schedule_node_stop(self, node_id: str) -> asyncio.Task:
        event_broker_task = self._event_broker_tasks[node_id]
        if not event_broker_task.cancelled():
            event_broker_task.cancel()
        return event_broker_task

    async def start_node(self, node_id: str):
        self._node_ids = [*self._node_ids, node_id]

        event_broker = make_event_broker(node_id, self._connection_pool)
        self._event_brokers = {**self._event_brokers, node_id: event_broker}

        subscribers = make_subscribers(
            [node_id],
            self._category_names,
            self._event_store,
            self._event_processor,
        )
        self._subscribers = {**self._subscribers, **subscribers}
        await register_subscribers_on_broker(
            event_broker, subscribers[node_id]
        )

        event_broker_task = self.schedule_node_start(node_id)
        self._event_broker_tasks = {
            **self._event_broker_tasks,
            node_id: event_broker_task,
        }

    async def stop_node(self, node_id: str):
        event_broker = self._event_brokers[node_id]
        event_broker_status = event_broker.status

        self._node_statuses[node_id] = event_broker_status

        self.schedule_node_stop(node_id)

        updated_event_broker_tasks = dict(self._event_broker_tasks)
        stopping_event_broker_task = updated_event_broker_tasks.pop(node_id)
        await asyncio.gather(
            stopping_event_broker_task, return_exceptions=True
        )

        updated_event_brokers = dict(self._event_brokers)
        updated_event_brokers.pop(node_id)

        updated_node_ids = list(self._node_ids)
        updated_node_ids.remove(node_id)

        updated_subscribers = dict(self._subscribers)
        updated_subscribers.pop(node_id)

        self._event_broker_tasks = updated_event_broker_tasks
        self._event_brokers = updated_event_brokers
        self._node_ids = updated_node_ids
        self._subscribers = updated_subscribers

    async def start_nodes(self):
        await broker_logger.ainfo(
            "event.processing.broker.test.starting-nodes",
            node_ids=self._node_ids,
            subscriber_categories=self._category_names,
        )
        self._event_broker_tasks = {
            node_id: self.schedule_node_start(node_id)
            for node_id in self._node_ids
        }

    async def stop_nodes(self):
        for node_id in self._node_ids:
            self.schedule_node_stop(node_id)
        await asyncio.gather(
            *self._event_broker_tasks.values(), return_exceptions=True
        )
        self._event_broker_tasks = {}

    async def register_subscribers_for_categories(
        self, category_names: Sequence[str]
    ):
        self._category_names = category_names

        self._subscribers = make_subscribers(
            self._node_ids,
            self._category_names,
            self._event_store,
            self._event_processor,
        )

        await register_subscribers_on_brokers(
            self._event_brokers, self._subscribers
        )

    async def replace_node(self, node_id: str):
        await self.stop_node(node_id)
        new_node_id = data.random_node_id()
        await self.start_node(new_node_id)

    async def replace_random_node(self):
        await self.replace_node(random.choice(self._node_ids))

    async def replace_all_nodes(self):
        for node_id in self._node_ids:
            await self.replace_node(node_id)

    @property
    def subscribers(self):
        return self._subscribers

    @property
    def node_statuses(self) -> Mapping[str, ProcessStatus]:
        return {
            **self._node_statuses,
            **{
                node_id: self._event_brokers[node_id].status
                for node_id in self._node_ids
            },
        }

    def has_errored_nodes(self):
        return any(
            status == ProcessStatus.ERRORED
            for status in self.node_statuses.values()
        )


@asynccontextmanager
async def node_set_cleanup(node_set: NodeSet):
    try:
        yield
    finally:
        await node_set.stop_nodes()


@asynccontextmanager
async def fail_on_event_processing_timeout():
    try:
        yield
    except asyncio.TimeoutError:
        pytest.fail("Timed out waiting for all events to be processed.")


class TestCoordinatorObserverEventBroker:
    @pytest_asyncio.fixture(autouse=True)
    async def store_connection_pool(self, open_connection_pool):
        self.connection_pool = open_connection_pool

    @pytest_asyncio.fixture(autouse=True)
    async def reinitialise_storage(self, open_connection_pool):
        await drop_table(open_connection_pool, "events")
        await drop_table(open_connection_pool, "subscribers")
        await drop_table(open_connection_pool, "subscriptions")
        await create_table(open_connection_pool, "events")
        await create_table(open_connection_pool, "subscribers")
        await create_table(open_connection_pool, "subscriptions")

    def make_postgres_event_store(self) -> EventStore:
        return EventStore(
            adapter=PostgresEventStorageAdapter(
                connection_source=self.connection_pool
            )
        )

    def make_node_set(
        self,
        event_store: EventStore,
        event_processor: EventProcessor,
        node_count: int,
    ) -> NodeSet:
        return NodeSet(
            connection_pool=self.connection_pool,
            event_store=event_store,
            event_processor=event_processor,
            node_count=node_count,
        )

    async def test_coordinates_many_subscribers_on_single_node(self):
        node_count = 1
        subscriber_group_count = 50

        event_processor = CapturingEventProcessor()
        event_store = self.make_postgres_event_store()
        node_set = self.make_node_set(event_store, event_processor, node_count)

        categories = random_category_names(count=subscriber_group_count)

        await node_set.register_subscribers_for_categories(categories)

        await publish_event_per_category(event_store, categories)

        await node_set.start_nodes()

        expected_event_count = subscriber_group_count

        async with (
            node_set_cleanup(node_set),
            fail_on_event_processing_timeout(),
        ):
            await asyncio.wait_for(
                consume_until_event_count(
                    event_processor,
                    node_set.subscribers,
                    expected_event_count,
                ),
                timeout=timedelta(seconds=60).total_seconds(),
            )

        assert not node_set.has_errored_nodes()

    async def test_coordinates_subscribers_across_many_nodes(self):
        node_count = 3
        subscriber_group_count = 50

        event_processor = CapturingEventProcessor()
        event_store = self.make_postgres_event_store()
        node_set = self.make_node_set(event_store, event_processor, node_count)

        categories = random_category_names(count=subscriber_group_count)

        await node_set.register_subscribers_for_categories(categories)

        await publish_event_per_category(event_store, categories)

        await node_set.start_nodes()

        expected_event_count = subscriber_group_count

        async with (
            node_set_cleanup(node_set),
            fail_on_event_processing_timeout(),
        ):
            await asyncio.wait_for(
                consume_until_event_count(
                    event_processor, node_set.subscribers, expected_event_count
                ),
                timeout=timedelta(seconds=60).total_seconds(),
            )

        assert not node_set.has_errored_nodes()

    async def test_coordinates_subscribers_when_single_node_replaced(self):
        node_count = 1
        subscriber_group_count = 50

        event_processor = CapturingEventProcessor()
        event_store = self.make_postgres_event_store()
        node_set = self.make_node_set(event_store, event_processor, node_count)

        categories = random_category_names(count=subscriber_group_count)

        await node_set.register_subscribers_for_categories(categories)

        await publish_event_per_category(event_store, categories)

        await node_set.start_nodes()

        await asyncio.wait_for(
            consume_until_event_count(
                event_processor, node_set.subscribers, subscriber_group_count
            ),
            timeout=timedelta(seconds=60).total_seconds(),
        )

        await node_set.replace_random_node()

        await publish_event_per_category(event_store, categories)

        expected_event_count = subscriber_group_count * 2

        async with (
            node_set_cleanup(node_set),
            fail_on_event_processing_timeout(),
        ):
            await asyncio.wait_for(
                consume_until_event_count(
                    event_processor, node_set.subscribers, expected_event_count
                ),
                timeout=timedelta(seconds=60).total_seconds(),
            )

        assert not node_set.has_errored_nodes()

    async def test_coordinates_subscribers_when_single_node_among_many_replaced(
        self,
    ):
        node_count = 3
        subscriber_group_count = 50

        event_processor = CapturingEventProcessor()
        event_store = self.make_postgres_event_store()
        node_set = self.make_node_set(event_store, event_processor, node_count)

        categories = random_category_names(count=subscriber_group_count)

        await node_set.register_subscribers_for_categories(categories)

        await publish_event_per_category(event_store, categories)

        await node_set.start_nodes()

        await asyncio.wait_for(
            consume_until_event_count(
                event_processor, node_set.subscribers, subscriber_group_count
            ),
            timeout=timedelta(seconds=60).total_seconds(),
        )

        await node_set.replace_random_node()

        await publish_event_per_category(event_store, categories)

        expected_event_count = subscriber_group_count * 2

        async with (
            node_set_cleanup(node_set),
            fail_on_event_processing_timeout(),
        ):
            await asyncio.wait_for(
                consume_until_event_count(
                    event_processor, node_set.subscribers, expected_event_count
                ),
                timeout=timedelta(seconds=60).total_seconds(),
            )

        assert not node_set.has_errored_nodes()

    async def test_coordinates_subscribers_when_all_nodes_in_many_node_deployment_replaced(
        self,
    ):
        node_count = 3
        subscriber_group_count = 50

        event_processor = CapturingEventProcessor()
        event_store = self.make_postgres_event_store()
        node_set = self.make_node_set(event_store, event_processor, node_count)

        categories = random_category_names(count=subscriber_group_count)

        await node_set.register_subscribers_for_categories(categories)

        await publish_event_per_category(event_store, categories)

        await node_set.start_nodes()

        await asyncio.wait_for(
            consume_until_event_count(
                event_processor, node_set.subscribers, subscriber_group_count
            ),
            timeout=timedelta(seconds=60).total_seconds(),
        )

        await node_set.replace_all_nodes()

        await publish_event_per_category(event_store, categories)

        expected_event_count = subscriber_group_count * 2

        async with (
            node_set_cleanup(node_set),
            fail_on_event_processing_timeout(),
        ):
            await asyncio.wait_for(
                consume_until_event_count(
                    event_processor, node_set.subscribers, expected_event_count
                ),
                timeout=timedelta(seconds=60).total_seconds(),
            )

        assert not node_set.has_errored_nodes()
