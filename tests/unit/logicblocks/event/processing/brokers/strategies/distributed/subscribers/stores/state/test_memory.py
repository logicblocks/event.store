from logicblocks.event.processing.broker.strategies.distributed import (
    EventSubscriberStateStore,
    InMemoryEventSubscriberStateStore,
)
from logicblocks.event.testcases import (
    EventSubscriberStateStoreCases,
)
from logicblocks.event.utils.clock import Clock


class TestInMemoryEventSubscriberStateStore(EventSubscriberStateStoreCases):
    def construct_store(
        self, node_id: str, clock: Clock
    ) -> EventSubscriberStateStore:
        return InMemoryEventSubscriberStateStore(node_id=node_id, clock=clock)
