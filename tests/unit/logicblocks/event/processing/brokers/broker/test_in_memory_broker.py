from typing import assert_type

from logicblocks.event.processing.broker import (
    EventBroker,
    EventBrokerSettings,
    make_in_memory_event_broker,
)
from logicblocks.event.store import EventStore, InMemoryEventStorageAdapter


class TestMakeInMemoryBroker:
    def test_make_in_memory_broker_uses_provided_adaptor(self):
        adaptor = InMemoryEventStorageAdapter()
        broker = make_in_memory_event_broker(
            node_id="test.node",
            settings=EventBrokerSettings(),
            adaptor=adaptor,
        )

        assert_type(broker, EventBroker)

    def test_make_in_memory_broker_uses_adaptor_from_store(self):
        adaptor = InMemoryEventStorageAdapter()
        store = EventStore(adapter=adaptor)

        broker = make_in_memory_event_broker(
            node_id="test.node",
            settings=EventBrokerSettings(),
            store=store,
        )

        assert_type(broker, EventBroker)
