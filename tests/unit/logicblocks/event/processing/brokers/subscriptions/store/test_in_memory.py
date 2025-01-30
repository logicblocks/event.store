from logicblocks.event.processing.broker import (
    EventSubscriptionStore,
    InMemoryEventSubscriptionStore,
)
from logicblocks.event.testcases.processing.subscriptions.store import (
    BaseTestEventSubscriptionStore,
)


class TestInMemoryEventSubscriptionStore(BaseTestEventSubscriptionStore):
    def construct_store(self) -> EventSubscriptionStore:
        return InMemoryEventSubscriptionStore()

    pass
