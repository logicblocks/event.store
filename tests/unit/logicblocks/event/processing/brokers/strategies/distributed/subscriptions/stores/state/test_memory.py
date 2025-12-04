from logicblocks.event.testcases import (
    EventSubscriptionStateStoreCases,
)

from logicblocks.event.processing.broker.strategies.distributed import (
    EventSubscriptionStateStore,
    InMemoryEventSubscriptionStateStore,
)


class TestInMemoryEventSubscriptionStateStore(
    EventSubscriptionStateStoreCases
):
    def construct_store(self, node_id: str) -> EventSubscriptionStateStore:
        return InMemoryEventSubscriptionStateStore(node_id=node_id)
