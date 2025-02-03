from logicblocks.event.processing.broker import (
    EventSubscriberStateStore,
    InMemoryEventSubscriberStateStore,
)
from logicblocks.event.testcases.processing.subscribers.stores.state import (
    EventSubscriberStateStoreCases,
)
from logicblocks.event.utils.clock import Clock


class TestInMemoryEventSubscriberStateStore(EventSubscriberStateStoreCases):
    def construct_store(
        self, node_id: str, clock: Clock
    ) -> EventSubscriberStateStore:
        return InMemoryEventSubscriberStateStore(node_id=node_id, clock=clock)
