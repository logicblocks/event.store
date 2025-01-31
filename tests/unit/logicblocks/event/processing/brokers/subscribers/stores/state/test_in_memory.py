from logicblocks.event.processing.broker import (
    EventSubscriberStateStore,
    InMemoryEventSubscriberStateStore,
)
from logicblocks.event.testcases.processing.subscribers.stores.state import (
    EventSubscriberStateStoreCases,
)


class TestInMemoryEventSubscriberStateStore(EventSubscriberStateStoreCases):
    def construct_store(self, clock) -> EventSubscriberStateStore:
        return InMemoryEventSubscriberStateStore(clock)
