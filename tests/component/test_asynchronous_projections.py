import time
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Callable, Mapping, Self

import pytest_asyncio
from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from logicblocks.event.db import PostgresConnectionSettings
from logicblocks.event.processing import (
    EventBrokerSettings,
    EventCount,
    PollingService,
    ProjectionEventProcessor,
    ServiceManager,
    make_postgres_event_broker,
    make_subscriber,
)
from logicblocks.event.projection import (
    PostgresProjectionStorageAdapter,
    ProjectionStore,
    Projector,
)
from logicblocks.event.store import (
    EventStore,
    PostgresEventStorageAdapter,
)
from logicblocks.event.testing import NewEventBuilder, data
from logicblocks.event.testsupport import (
    connection_pool,
    create_table,
    drop_table,
)
from logicblocks.event.types import (
    CategoryIdentifier,
    JsonValue,
    JsonValueConvertible,
    StoredEvent,
    StreamIdentifier,
    default_deserialisation_fallback,
    deserialise_from_json_value,
)

connection_settings = PostgresConnectionSettings(
    user="admin",
    password="super-secret",
    host="localhost",
    port=5432,
    dbname="some-database",
)


@pytest_asyncio.fixture
async def open_connection_pool():
    async with connection_pool(connection_settings) as pool:
        yield pool


class TestAsynchronousProjections:
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

    async def test_projects_from_category(self):
        node_id = data.random_node_id()
        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()
        projection_name = "specific-thing"

        event_store = EventStore(
            adapter=PostgresEventStorageAdapter(
                connection_source=self.connection_pool
            )
        )

        thing_projection_store = ProjectionStore(
            adapter=PostgresProjectionStorageAdapter(
                connection_source=self.connection_pool
            )
        )

        @dataclass
        class Thing(JsonValueConvertible):
            value: int = 5

            @classmethod
            def deserialise(
                cls,
                value: JsonValue,
                fallback: Callable[
                    [Any, JsonValue], Any
                ] = default_deserialisation_fallback,
            ) -> Self:
                if not isinstance(value, Mapping) or not isinstance(
                    value["value"], int
                ):
                    return fallback(cls, value)

                return cls(value=value["value"])

            def serialise(
                self, fallback: Callable[[object], JsonValue]
            ) -> JsonValue:
                return {"value": self.value}

        class ThingProjector(
            Projector[StreamIdentifier, Thing, Mapping[str, Any]]
        ):
            name = projection_name

            def initial_state_factory(self) -> Thing:
                return Thing()

            def initial_metadata_factory(self) -> Mapping[str, Any]:
                return {}

            def id_factory(self, state: Thing, source: StreamIdentifier):
                return source.stream

            @staticmethod
            def thing_got_value(state: Thing, event: StoredEvent) -> Thing:
                payload = deserialise_from_json_value(
                    Mapping[str, Any], event.payload
                )
                state.value = payload["value"]
                return state

        thing_projector = ThingProjector()

        event_processor = ProjectionEventProcessor[Thing, Mapping[str, Any]](
            projector=thing_projector,
            projection_store=thing_projection_store,
            state_type=Thing,
            metadata_type=Mapping[str, Any],
        )

        subscriber_group = data.random_subscriber_group()
        subscriber_state_category = event_store.category(
            category=f"subscriber-{subscriber_group}"
        )

        subscriber = make_subscriber(
            subscriber_group=subscriber_group,
            subscription_request=CategoryIdentifier(category=category_name),
            subscriber_state_category=subscriber_state_category,
            subscriber_state_persistence_interval=EventCount(1),
            event_processor=event_processor,
        )

        event_broker = make_postgres_event_broker(
            node_id=node_id,
            connection_settings=connection_settings,
            connection_pool=self.connection_pool,
            settings=EventBrokerSettings(
                coordinator_distribution_interval=timedelta(milliseconds=100),
                observer_synchronisation_interval=timedelta(milliseconds=100),
            ),
        )

        await event_broker.register(subscriber=subscriber)

        subscriber_service = PollingService(
            callable=subscriber.consume_all,
        )

        service_manager = ServiceManager()
        service_manager.register(event_broker)
        service_manager.register(subscriber_service)

        try:
            await service_manager.start()

            value = data.random_int()

            await event_store.stream(
                category=category_name,
                stream=stream_name,
            ).publish(
                events=[
                    (
                        NewEventBuilder()
                        .with_name("thing-got-value")
                        .with_payload({"value": value})
                        .build()
                    )
                ]
            )

            projected_thing = None

            timeout_start = time.time()
            timeout_seconds = 15
            while time.time() < timeout_start + timeout_seconds:
                projected_thing = await thing_projection_store.locate(
                    source=StreamIdentifier(
                        category=category_name,
                        stream=stream_name,
                    ),
                    name=projection_name,
                    state_type=Thing,
                )
                if projected_thing:
                    break

            assert projected_thing
            assert projected_thing.state.value == value
        finally:
            await service_manager.stop()
