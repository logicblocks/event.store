from logicblocks.event.store import InMemoryEventStorageAdapter

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


class InMemoryEventBrokerBuilder(
    EventBrokerBuilder[(InMemoryEventStorageAdapter,)]
):
    def dependencies(
        self, adapter: InMemoryEventStorageAdapter
    ) -> EventBrokerDependencies:
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
                adapter=adapter
            ),
        )


def make_in_memory_event_broker(
    node_id: str,
    settings: EventBrokerSettings,
    adapter: InMemoryEventStorageAdapter,
) -> EventBroker:
    return InMemoryEventBrokerBuilder(node_id).prepare(adapter).build(settings)
