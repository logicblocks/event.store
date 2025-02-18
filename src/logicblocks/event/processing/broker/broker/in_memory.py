from ....store import InMemoryEventStorageAdapter
from ..locks import InMemoryLockManager
from ..nodes import InMemoryNodeStateStore
from ..sources import (
    InMemoryEventStoreEventSourceFactory,
)
from ..subscribers import (
    InMemoryEventSubscriberStateStore,
)
from ..subscriptions import InMemoryEventSubscriptionStateStore
from .base import EventBroker
from .broker_builder import (
    EventBrokerBuilder,
    EventBrokerDependencies,
    EventBrokerSettings,
)


class InMemoryEventBrokerBuilder(EventBrokerBuilder):
    def dependencies(self) -> EventBrokerDependencies:
        return EventBrokerDependencies(
            node_state_store=InMemoryNodeStateStore(),
            event_subscriber_state_store=InMemoryEventSubscriberStateStore(
                node_id=self.node_id,
            ),
            event_subscription_state_store=InMemoryEventSubscriptionStateStore(
                node_id=self.node_id
            ),
            lock_manager=InMemoryLockManager(),
            event_source_factory=InMemoryEventStoreEventSourceFactory(
                adapter=InMemoryEventStorageAdapter()
            ),
        )


def make_in_memory_event_broker(
    node_id: str,
    settings: EventBrokerSettings,
) -> EventBroker:
    return InMemoryEventBrokerBuilder(node_id).prepare().build(settings)
