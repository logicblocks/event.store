from datetime import timedelta
from uuid import uuid4

from logicblocks.event.processing import (
    BrokerType,
    SingletonEventBroker,
    SingletonEventBrokerSettings,
    StorageType,
    make_event_broker,
)
from logicblocks.event.store.adapters import InMemoryEventStorageAdapter


class TestEventBrokerFactories:
    def test_make_event_broker_makes_singleton_in_memory_broker(self):
        broker = make_event_broker(
            node_id=str(uuid4()),
            broker_type=BrokerType.Singleton,
            storage_type=StorageType.InMemory,
            settings=SingletonEventBrokerSettings(
                distribution_interval=timedelta(seconds=10)
            ),
            adapter=InMemoryEventStorageAdapter(),
        )
        assert broker is not None
        assert isinstance(broker, SingletonEventBroker)
