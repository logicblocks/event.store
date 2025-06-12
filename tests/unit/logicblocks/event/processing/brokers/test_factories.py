from datetime import timedelta
from uuid import uuid4

from logicblocks.event.processing import (
    EventBrokerStorageType,
    EventBrokerType,
    SingletonEventBroker,
    SingletonEventBrokerSettings,
    make_event_broker,
)
from logicblocks.event.processing.broker.factories import (
    SingletonEventBrokerTypeType,
)
from logicblocks.event.store.adapters import InMemoryEventStorageAdapter


def do_stuff(broker_type: SingletonEventBrokerTypeType):
    return make_event_broker(
        node_id=str(uuid4()),
        broker_type=broker_type,
        storage_type=EventBrokerStorageType.InMemory,
        settings=SingletonEventBrokerSettings(
            distribution_interval=timedelta(seconds=10)
        ),
        adapter=InMemoryEventStorageAdapter(),
    )


class TestEventBrokerFactories:
    def test_make_event_broker_makes_singleton_in_memory_broker(self):
        broker = do_stuff(EventBrokerType.Singleton)
        assert broker is not None
        assert isinstance(broker, SingletonEventBroker)
