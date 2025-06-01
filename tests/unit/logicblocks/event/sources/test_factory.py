import pytest

from logicblocks.event.persistence.postgres import ConnectionSettings
from logicblocks.event.sources import (
    InMemoryEventStoreEventSourceFactory,
    PostgresEventStoreEventSourceFactory,
)
from logicblocks.event.store import (
    EventCategory,
    EventStream,
    InMemoryEventStorageAdapter,
    PostgresEventStorageAdapter,
)
from logicblocks.event.testing import data
from logicblocks.event.types import (
    CategoryIdentifier,
    StreamIdentifier,
)

connection_settings = ConnectionSettings(
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
