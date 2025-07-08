from datetime import timedelta
from uuid import uuid4

from logicblocks.event.processing import (
    DistributedEventBroker,
    DistributedEventBrokerSettings,
    EventBrokerStorageType,
    EventBrokerType,
    SingletonEventBroker,
    SingletonEventBrokerSettings,
    make_event_broker,
)
from logicblocks.event.store.adapters import InMemoryEventStorageAdapter


class TestFactoryForSingletonEventBroker:
    def test_makes_singleton_in_memory_broker(self):
        broker = make_event_broker(
            node_id=str(uuid4()),
            broker_type=EventBrokerType.Singleton,
            storage_type=EventBrokerStorageType.InMemory,
            settings=SingletonEventBrokerSettings(
                distribution_interval=timedelta(seconds=10)
            ),
            adapter=InMemoryEventStorageAdapter(),
        )
        assert broker is not None
        assert isinstance(broker, SingletonEventBroker)


class TestFactoryForDistributedEventBroker:
    def test_makes_distributed_in_memory_broker(self):
        broker = make_event_broker(
            node_id=str(uuid4()),
            broker_type=EventBrokerType.Distributed,
            storage_type=EventBrokerStorageType.InMemory,
            settings=DistributedEventBrokerSettings(
                coordinator_distribution_interval=timedelta(seconds=10),
                subscriber_manager_heartbeat_interval=timedelta(seconds=5),
            ),
            adapter=InMemoryEventStorageAdapter(),
        )
        assert broker is not None
        assert isinstance(broker, DistributedEventBroker)
