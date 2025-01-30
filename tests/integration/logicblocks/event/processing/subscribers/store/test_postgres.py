import os
import sys
from datetime import UTC, datetime, timedelta
from random import shuffle

import pytest
import pytest_asyncio
from psycopg import AsyncConnection, abc, sql
from psycopg.rows import class_row
from psycopg_pool import AsyncConnectionPool

from logicblocks.event.db import PostgresConnectionSettings
from logicblocks.event.processing.broker import (
    EventBroker,
    EventSubscriber,
    EventSubscriberState,
)
from logicblocks.event.processing.broker.subscribers.store.postgres import (
    PostgresEventSubscriberStore,
)
from logicblocks.event.store import EventSource
from logicblocks.event.testing import data
from logicblocks.event.utils.clock import StaticClock

connection_settings = PostgresConnectionSettings(
    user="admin",
    password="super-secret",
    host="localhost",
    port=5432,
    dbname="some-database",
)

project_root = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__), "..", "..", "..", "..", "..", "..", ".."
    )
)


def relative_to_root(*path_parts: str) -> str:
    return os.path.join(project_root, *path_parts)


def create_table_query(table: str) -> abc.Query:
    with open(relative_to_root("sql", "create_subscribers_table.sql")) as f:
        create_table_sql = f.read().replace("subscribers", "{0}")

        return create_table_sql.format(table).encode()


def create_indices_query(table: str) -> abc.Query:
    with open(relative_to_root("sql", "create_subscribers_indices.sql")) as f:
        create_indices_sql = f.read().replace("subscribers", "{0}")

        return create_indices_sql.format(table).encode()


def drop_table_query(table_name: str) -> abc.Query:
    return sql.SQL("DROP TABLE IF EXISTS {0}").format(
        sql.Identifier(table_name)
    )


def truncate_table_query(table_name: str) -> abc.Query:
    return sql.SQL("TRUNCATE {0}").format(sql.Identifier(table_name))


def read_subscriber_states_query(table: str) -> abc.Query:
    return sql.SQL("SELECT * FROM {0} ORDER BY last_seen").format(
        sql.Identifier(table)
    )


async def create_table(
    pool: AsyncConnectionPool[AsyncConnection], table: str
) -> None:
    async with pool.connection() as connection:
        await connection.execute(create_table_query(table))
        await connection.execute(create_indices_query(table))


async def clear_table(
    pool: AsyncConnectionPool[AsyncConnection], table: str
) -> None:
    async with pool.connection() as connection:
        await connection.execute(truncate_table_query(table))


async def drop_table(
    pool: AsyncConnectionPool[AsyncConnection], table: str
) -> None:
    async with pool.connection() as connection:
        await connection.execute(drop_table_query(table))


async def read_subscribers(
    pool: AsyncConnectionPool[AsyncConnection],
    table: str,
) -> list[EventSubscriberState]:
    async with pool.connection() as connection:
        async with connection.cursor(
            row_factory=class_row(EventSubscriberState)
        ) as cursor:
            results = await cursor.execute(read_subscriber_states_query(table))
            return await results.fetchall()


@pytest_asyncio.fixture
async def open_connection_pool():
    conninfo = connection_settings.to_connection_string()
    pool = AsyncConnectionPool[AsyncConnection](conninfo, open=False)

    await pool.open()

    try:
        yield pool
    finally:
        await pool.close()


class CapturingEventSubscriber(EventSubscriber):
    sources: list[EventSource]

    def __init__(self, group: str, id: str):
        self._group = group
        self._id = id

    @property
    def group(self) -> str:
        return self._group

    @property
    def id(self) -> str:
        return self._id

    async def subscribe(self, broker: EventBroker) -> None:
        raise NotImplementedError()

    async def accept(self, source: EventSource) -> None:
        self.sources.append(source)

    async def revoke(self, source: EventSource) -> None:
        self.sources.remove(source)


class TestInMemoryEventSubscriberStore:
    pool: AsyncConnectionPool[AsyncConnection]

    @pytest_asyncio.fixture(autouse=True)
    async def store_connection_pool(self, open_connection_pool):
        self.pool = open_connection_pool

    @pytest_asyncio.fixture(autouse=True)
    async def reinitialise_storage(self, open_connection_pool):
        await drop_table(open_connection_pool, "subscribers")
        await create_table(open_connection_pool, "subscribers")

    async def test_adds_single_subscriber_details(self):
        now = datetime.now(UTC)
        clock = StaticClock(now=now)
        store = PostgresEventSubscriberStore(
            clock=clock, connection_source=self.pool
        )

        subscriber_group = data.random_subscriber_group()
        subscriber_id = data.random_subscriber_id()

        subscriber = CapturingEventSubscriber(
            group=subscriber_group,
            id=subscriber_id,
        )

        await store.add(subscriber)

        states = await store.list()

        assert len(states) == 1
        assert states[0] == EventSubscriberState(
            group=subscriber_group, id=subscriber_id, last_seen=now
        )

    async def test_adds_many_subscriber_details(self):
        now = datetime.now(UTC)
        clock = StaticClock(now=now)
        store = PostgresEventSubscriberStore(
            clock=clock, connection_source=self.pool
        )

        subscriber_1_group = data.random_subscriber_group()
        subscriber_1_id = data.random_subscriber_id()
        subscriber_1 = CapturingEventSubscriber(
            group=subscriber_1_group, id=subscriber_1_id
        )

        subscriber_2_group = data.random_subscriber_group()
        subscriber_2_id = data.random_subscriber_id()
        subscriber_2 = CapturingEventSubscriber(
            group=subscriber_2_group, id=subscriber_2_id
        )

        await store.add(subscriber_1)
        await store.add(subscriber_2)

        states = await store.list()

        assert len(states) == 2
        assert states[0] == EventSubscriberState(
            group=subscriber_1_group, id=subscriber_1_id, last_seen=now
        )
        assert states[1] == EventSubscriberState(
            group=subscriber_2_group, id=subscriber_2_id, last_seen=now
        )

    async def test_adding_already_added_subscriber_updates_last_seen(self):
        time_1 = datetime.now(UTC)
        clock = StaticClock(time_1)
        store = PostgresEventSubscriberStore(
            clock=clock, connection_source=self.pool
        )

        subscriber_group = data.random_subscriber_group()
        subscriber_id = data.random_subscriber_id()

        subscriber = CapturingEventSubscriber(
            group=subscriber_group,
            id=subscriber_id,
        )

        await store.add(subscriber)

        time_2 = time_1 + timedelta(seconds=5)
        clock.set(time_2)

        await store.add(subscriber)

        states = await store.list()

        assert states[0] == EventSubscriberState(
            group=subscriber_group, id=subscriber_id, last_seen=time_2
        )

    async def test_lists_subscribers_by_group(self):
        now = datetime.now(UTC)
        clock = StaticClock(now)
        store = PostgresEventSubscriberStore(
            clock=clock, connection_source=self.pool
        )

        subscriber_group_1 = data.random_subscriber_group()
        subscriber_group_2 = data.random_subscriber_group()

        subscriber_group_1_subscribers = [
            CapturingEventSubscriber(
                group=subscriber_group_1,
                id=str(id),
            )
            for id in range(1, 4)
        ]
        subscriber_group_2_subscribers = [
            CapturingEventSubscriber(
                group=subscriber_group_2,
                id=str(id),
            )
            for id in range(1, 4)
        ]

        subscribers = (
            subscriber_group_1_subscribers + subscriber_group_2_subscribers
        )
        shuffle(subscribers)

        for subscriber in subscribers:
            await store.add(subscriber)

        found_states = await store.list(subscriber_group=subscriber_group_1)

        expected_states = [
            EventSubscriberState(
                group=subscriber_group_1, id=str(id), last_seen=now
            )
            for id in range(1, 4)
        ]

        assert set(found_states) == set(expected_states)

    async def test_lists_subscribers_more_recently_seen_than_max_time(self):
        now = datetime.now(UTC)
        max_age = timedelta(seconds=60)
        older_than_max_age_time = now - timedelta(seconds=90)
        just_newer_than_max_age_time = now - timedelta(
            seconds=59, milliseconds=999
        )
        recent_time = now - timedelta(seconds=5)

        clock = StaticClock(now)
        store = PostgresEventSubscriberStore(
            clock=clock, connection_source=self.pool
        )

        subscriber_group_1 = data.random_subscriber_group()

        clock.set(older_than_max_age_time)

        older_than_max_age_subscribers = [
            CapturingEventSubscriber(
                group=subscriber_group_1,
                id=str(id),
            )
            for id in range(1, 4)
        ]

        for subscriber in older_than_max_age_subscribers:
            await store.add(subscriber)

        clock.set(just_newer_than_max_age_time)

        just_newer_than_max_age_subscribers = [
            CapturingEventSubscriber(
                group=subscriber_group_1,
                id=str(id),
            )
            for id in range(4, 8)
        ]

        for subscriber in just_newer_than_max_age_subscribers:
            await store.add(subscriber)

        clock.set(recent_time)

        recent_subscribers = [
            CapturingEventSubscriber(
                group=subscriber_group_1,
                id=str(id),
            )
            for id in range(8, 12)
        ]

        for subscriber in recent_subscribers:
            await store.add(subscriber)

        clock.set(now)

        found_states = await store.list(max_time_since_last_seen=max_age)

        just_newer_than_max_age_states = [
            EventSubscriberState(
                group=subscriber.group,
                id=subscriber.id,
                last_seen=just_newer_than_max_age_time,
            )
            for subscriber in just_newer_than_max_age_subscribers
        ]

        recent_states = [
            EventSubscriberState(
                group=subscriber.group, id=subscriber.id, last_seen=recent_time
            )
            for subscriber in recent_subscribers
        ]

        expected_states = just_newer_than_max_age_states + recent_states

        assert set(found_states) == set(expected_states)

    async def test_lists_subscribers_by_group_more_recently_seen_than_max_time(
        self,
    ):
        now = datetime.now(UTC)
        max_age = timedelta(seconds=60)
        older_than_max_age_time = now - timedelta(seconds=90)
        newer_than_max_age_time = now - timedelta(seconds=30)

        clock = StaticClock(now)
        store = PostgresEventSubscriberStore(
            clock=clock, connection_source=self.pool
        )

        subscriber_group_1 = data.random_subscriber_group()
        subscriber_group_2 = data.random_subscriber_group()

        clock.set(older_than_max_age_time)

        older_than_max_age_group_1_subscribers = [
            CapturingEventSubscriber(
                group=subscriber_group_1,
                id=str(id),
            )
            for id in range(1, 4)
        ]
        older_than_max_age_group_2_subscribers = [
            CapturingEventSubscriber(
                group=subscriber_group_2,
                id=str(id),
            )
            for id in range(4, 8)
        ]
        older_than_max_age_subscribers = (
            older_than_max_age_group_1_subscribers
            + older_than_max_age_group_2_subscribers
        )

        for subscriber in older_than_max_age_subscribers:
            await store.add(subscriber)

        clock.set(newer_than_max_age_time)

        newer_than_max_age_group_1_subscribers = [
            CapturingEventSubscriber(
                group=subscriber_group_1,
                id=str(id),
            )
            for id in range(8, 12)
        ]
        newer_than_max_age_group_2_subscribers = [
            CapturingEventSubscriber(
                group=subscriber_group_2,
                id=str(id),
            )
            for id in range(12, 16)
        ]
        newer_than_max_age_subscribers = (
            newer_than_max_age_group_1_subscribers
            + newer_than_max_age_group_2_subscribers
        )

        for subscriber in newer_than_max_age_subscribers:
            await store.add(subscriber)

        clock.set(now)

        found_states = await store.list(
            subscriber_group=subscriber_group_1,
            max_time_since_last_seen=max_age,
        )

        expected_states = [
            EventSubscriberState(
                group=subscriber.group,
                id=subscriber.id,
                last_seen=newer_than_max_age_time,
            )
            for subscriber in newer_than_max_age_group_1_subscribers
        ]

        assert set(found_states) == set(expected_states)

    async def test_updates_last_seen_on_heartbeat(self):
        now = datetime.now(UTC)
        previous_last_seen_time = now - timedelta(seconds=10)
        updated_last_seen_time = now

        clock = StaticClock(now)
        store = PostgresEventSubscriberStore(
            clock=clock, connection_source=self.pool
        )

        subscriber_group_1 = data.random_subscriber_group()
        subscriber_id_1 = data.random_subscriber_id()
        subscriber_1 = CapturingEventSubscriber(
            group=subscriber_group_1,
            id=subscriber_id_1,
        )

        subscriber_group_2 = data.random_subscriber_group()
        subscriber_id_2 = data.random_subscriber_id()
        subscriber_2 = CapturingEventSubscriber(
            group=subscriber_group_2,
            id=subscriber_id_2,
        )

        clock.set(previous_last_seen_time)

        await store.add(subscriber_1)
        await store.add(subscriber_2)

        clock.set(now)

        await store.heartbeat(subscriber_1)

        states = await store.list()

        assert len(states) == 2

        subscriber_1_state = next(
            state for state in states if state.id == subscriber_1.id
        )
        subscriber_2_state = next(
            state for state in states if state.id == subscriber_2.id
        )

        assert subscriber_1_state.last_seen == updated_last_seen_time
        assert subscriber_2_state.last_seen == previous_last_seen_time

    async def test_raises_if_heartbeat_called_for_unknown_subscriber(self):
        store = PostgresEventSubscriberStore(connection_source=self.pool)

        subscriber = CapturingEventSubscriber(
            group=data.random_subscriber_group(),
            id=data.random_subscriber_id(),
        )

        with pytest.raises(ValueError):
            await store.heartbeat(subscriber)

    async def test_purges_subscribers_that_have_not_been_seen_for_5_minutes_by_default(
        self,
    ):
        now = datetime.now(UTC)
        five_minutes_ago = now - timedelta(minutes=5)
        just_under_five_minutes_ago = now - timedelta(minutes=4, seconds=59)

        clock = StaticClock(now)
        store = PostgresEventSubscriberStore(
            clock=clock, connection_source=self.pool
        )

        subscriber_group_1 = data.random_subscriber_group()
        subscriber_id_1 = data.random_subscriber_id()
        subscriber_1 = CapturingEventSubscriber(
            group=subscriber_group_1,
            id=subscriber_id_1,
        )

        subscriber_group_2 = data.random_subscriber_group()
        subscriber_id_2 = data.random_subscriber_id()
        subscriber_2 = CapturingEventSubscriber(
            group=subscriber_group_2,
            id=subscriber_id_2,
        )

        clock.set(five_minutes_ago)

        await store.add(subscriber_1)

        clock.set(just_under_five_minutes_ago)

        await store.add(subscriber_2)

        clock.set(now)

        await store.purge()

        states = await store.list()

        assert len(states) == 1
        assert states[0].id == subscriber_2.id

    async def test_purges_subscribers_that_have_not_been_seen_since_specified_max_time(
        self,
    ):
        now = datetime.now(UTC)
        max_age = timedelta(minutes=2)
        two_minutes_ago = now - timedelta(minutes=2)
        just_under_two_minutes_ago = now - timedelta(minutes=1, seconds=59)

        clock = StaticClock(now)
        store = PostgresEventSubscriberStore(
            clock=clock, connection_source=self.pool
        )

        subscriber_group_1 = data.random_subscriber_group()
        subscriber_id_1 = data.random_subscriber_id()
        subscriber_1 = CapturingEventSubscriber(
            group=subscriber_group_1,
            id=subscriber_id_1,
        )

        subscriber_group_2 = data.random_subscriber_group()
        subscriber_id_2 = data.random_subscriber_id()
        subscriber_2 = CapturingEventSubscriber(
            group=subscriber_group_2,
            id=subscriber_id_2,
        )

        clock.set(two_minutes_ago)

        await store.add(subscriber_1)

        clock.set(just_under_two_minutes_ago)

        await store.add(subscriber_2)

        clock.set(now)

        await store.purge()

        states = await store.list(max_time_since_last_seen=max_age)

        assert len(states) == 1
        assert states[0].id == subscriber_2.id


if __name__ == "__main__":
    sys.exit(pytest.main([__file__]))
