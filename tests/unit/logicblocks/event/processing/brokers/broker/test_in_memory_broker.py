from typing import assert_type

from logicblocks.event.processing.broker import (
    EventBroker,
    EventBrokerSettings,
    make_in_memory_event_broker,
)
from logicblocks.event.store import EventStore, InMemoryEventStorageAdapter


class TestMakeInMemoryBroker:
    def test_make_in_memory_broker_uses_provided_adapter(self):
        adapter = InMemoryEventStorageAdapter()
        broker = make_in_memory_event_broker(
            node_id="test.node",
            settings=EventBrokerSettings(),
            adapter=adapter,
        )

        assert_type(broker, EventBroker)

    def test_make_in_memory_broker_uses_adapter_from_store(self):
        adapter = InMemoryEventStorageAdapter()
        store = EventStore(adapter=adapter)

        broker = make_in_memory_event_broker(
            node_id="test.node",
            settings=EventBrokerSettings(),
            store=store,
        )

        assert_type(broker, EventBroker)
