import pytest

from logicblocks.event.db import PostgresConnectionSettings
from logicblocks.event.processing.broker import (
    InMemoryEventStoreEventSourceFactory,
)
from logicblocks.event.processing.broker.sources.factory.store import (
    PostgresEventStoreEventSourceFactory,
)
from logicblocks.event.store import EventCategory, EventStream
from logicblocks.event.store.adapters.in_memory import (
    InMemoryEventStorageAdapter,
)
from logicblocks.event.store.adapters.postgres import (
    PostgresEventStorageAdapter,
)
from logicblocks.event.testing import data
from logicblocks.event.types.identifier import (
    CategoryIdentifier,
    StreamIdentifier,
)

connection_settings = PostgresConnectionSettings(
    host="fake",
    port=1234,
    dbname="db",
    user="user",
    password="supersecret",
)


def make_in_memory_event_storage_adapter() -> InMemoryEventStorageAdapter:
    return InMemoryEventStorageAdapter()


def make_postgres_event_storage_adapter() -> PostgresEventStorageAdapter:
    return PostgresEventStorageAdapter(connection_source=connection_settings)


@pytest.mark.parametrize(
    "adapter_factory,factory_class",
    [
        (
            make_in_memory_event_storage_adapter,
            InMemoryEventStoreEventSourceFactory,
        ),
        (
            make_postgres_event_storage_adapter,
            PostgresEventStoreEventSourceFactory,
        ),
    ],
)
class TestEventStoreEventSourceFactoryDefaultConstructors:
    def test_constructs_event_category(self, adapter_factory, factory_class):
        adapter = adapter_factory()
        factory = factory_class(adapter)

        identifier = CategoryIdentifier(
            category=data.random_event_category_name()
        )

        source = factory.construct(identifier)

        assert source == EventCategory(adapter, identifier)

    def test_constructs_event_stream(self, adapter_factory, factory_class):
        adapter = adapter_factory()
        factory = factory_class(adapter)

        identifier = StreamIdentifier(
            category=data.random_event_category_name(),
            stream=data.random_event_stream_name(),
        )

        source = factory.construct(identifier)

        assert source == EventStream(adapter, identifier)
