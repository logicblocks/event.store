from logicblocks.event.processing.broker import (
    EventSubscriberStore,
    InMemoryEventSubscriberStore,
)
from logicblocks.event.testcases.processing.subscribers.store import (
    BaseTestSubscriberStore,
)


class TestInMemoryEventSubscriberStore(BaseTestSubscriberStore):
    def construct_store(self, clock) -> EventSubscriberStore:
        return InMemoryEventSubscriberStore(clock)
