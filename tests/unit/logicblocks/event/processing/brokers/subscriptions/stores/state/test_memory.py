from logicblocks.event.processing import (
    EventSubscriptionStateStore,
    InMemoryEventSubscriptionStateStore,
)
from logicblocks.event.testcases import (
    EventSubscriptionStateStoreCases,
)


class TestInMemoryEventSubscriptionStateStore(
    EventSubscriptionStateStoreCases
):
    def construct_store(self, node_id: str) -> EventSubscriptionStateStore:
        return InMemoryEventSubscriptionStateStore(node_id=node_id)
