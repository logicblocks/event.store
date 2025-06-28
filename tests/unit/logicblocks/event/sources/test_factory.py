import pytest

from logicblocks.event.persistence.postgres import ConnectionSettings
from logicblocks.event.sources import (
    ConstrainedEventSource,
    EventStoreEventSourceFactory,
)
from logicblocks.event.store import (
    EventCategory,
    EventLog,
    EventStream,
    InMemoryEventStorageAdapter,
    PostgresEventStorageAdapter,
)
from logicblocks.event.store.constraints import StreamNamePrefixConstraint
from logicblocks.event.testing import data
from logicblocks.event.types import (
    CategoryIdentifier,
    CategoryPartitionIdentifier,
    LogPartitionIdentifier,
    PartitionIdentifier,
    StreamIdentifier,
    StreamNamePrefixPartitionIdentifier,
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
    "adapter_factory",
    [
        make_in_memory_event_storage_adapter,
        make_postgres_event_storage_adapter,
    ],
)
class TestEventStoreEventSourceFactoryDefaultConstructors:
    def test_constructs_event_category(self, adapter_factory):
        adapter = adapter_factory()
        factory = EventStoreEventSourceFactory(adapter)

        identifier = CategoryIdentifier(
            category=data.random_event_category_name()
        )

        source = factory.construct(identifier)

        assert source == EventCategory(adapter, identifier)

    def test_constructs_event_stream(self, adapter_factory):
        adapter = adapter_factory()
        factory = EventStoreEventSourceFactory(adapter)

        identifier = StreamIdentifier(
            category=data.random_event_category_name(),
            stream=data.random_event_stream_name(),
        )

        source = factory.construct(identifier)

        assert source == EventStream(adapter, identifier)

    def test_constructs_category_partition_with_stream_name_prefix(
        self, adapter_factory
    ):
        adapter = adapter_factory()
        factory = EventStoreEventSourceFactory(adapter)

        partition_id = StreamNamePrefixPartitionIdentifier(value="abc")
        identifier = CategoryPartitionIdentifier(
            category=data.random_event_category_name(), partition=partition_id
        )

        actual_source = factory.construct(identifier)

        unconstrained_source = EventCategory(
            adapter, CategoryIdentifier(category=identifier.category)
        )
        expected_source = ConstrainedEventSource(
            identifier=identifier,
            delegate=unconstrained_source,
            constraints={StreamNamePrefixConstraint(prefix="abc")},
        )

        assert actual_source == expected_source

    def test_constructs_log_partition_with_stream_name_prefix(
        self, adapter_factory
    ):
        adapter = adapter_factory()
        factory = EventStoreEventSourceFactory(adapter)

        partition_id = StreamNamePrefixPartitionIdentifier(value="123")
        identifier = LogPartitionIdentifier(partition=partition_id)

        actual_source = factory.construct(identifier)

        unconstrained_source = EventLog(adapter)
        expected_source = ConstrainedEventSource(
            identifier=identifier,
            delegate=unconstrained_source,
            constraints={StreamNamePrefixConstraint(prefix="123")},
        )

        assert actual_source == expected_source

    def test_constructs_different_category_partitions_with_different_prefixes(
        self, adapter_factory
    ):
        adapter = adapter_factory()
        factory = EventStoreEventSourceFactory(adapter)

        category_name = data.random_event_category_name()

        partition_1 = StreamNamePrefixPartitionIdentifier(value="a")
        identifier_1 = CategoryPartitionIdentifier(
            category=category_name, partition=partition_1
        )

        partition_2 = StreamNamePrefixPartitionIdentifier(value="b")
        identifier_2 = CategoryPartitionIdentifier(
            category=category_name, partition=partition_2
        )

        actual_source_1 = factory.construct(identifier_1)
        actual_source_2 = factory.construct(identifier_2)

        unconstrained_source = EventCategory(
            adapter, CategoryIdentifier(category=category_name)
        )
        expected_source_1 = ConstrainedEventSource(
            identifier=identifier_1,
            delegate=unconstrained_source,
            constraints={StreamNamePrefixConstraint(prefix="a")},
        )
        expected_source_2 = ConstrainedEventSource(
            identifier=identifier_2,
            delegate=unconstrained_source,
            constraints={StreamNamePrefixConstraint(prefix="b")},
        )

        assert (actual_source_1, actual_source_2) == (
            expected_source_1,
            expected_source_2,
        )

    def test_raises_error_for_unsupported_category_partition_type(
        self, adapter_factory
    ):
        adapter = adapter_factory()
        factory = EventStoreEventSourceFactory(adapter)

        class UnsupportedPartitionIdentifier(PartitionIdentifier):
            def serialise(self, fallback=None):
                return {"type": "unsupported"}

        unsupported_partition_identifier = UnsupportedPartitionIdentifier()

        identifier = CategoryPartitionIdentifier(
            category=data.random_event_category_name(),
            partition=unsupported_partition_identifier,
        )

        with pytest.raises(ValueError):
            factory.construct(identifier)

    def test_raises_error_for_unsupported_log_partition_type(
        self, adapter_factory
    ):
        adapter = adapter_factory()
        factory = EventStoreEventSourceFactory(adapter)

        class UnsupportedPartitionIdentifier(PartitionIdentifier):
            def serialise(self, fallback=None):
                return {"type": "unsupported"}

        unsupported_partition_identifier = UnsupportedPartitionIdentifier()

        identifier = LogPartitionIdentifier(
            partition=unsupported_partition_identifier
        )

        with pytest.raises(ValueError):
            factory.construct(identifier)
