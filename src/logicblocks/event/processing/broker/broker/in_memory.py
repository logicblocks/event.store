from typing import overload

from ....store import EventStore, InMemoryEventStorageAdapter
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


@overload
def make_in_memory_event_broker(
    node_id: str,
    settings: EventBrokerSettings,
    *,
    store: EventStore[InMemoryEventStorageAdapter],
) -> EventBroker:
    pass


@overload
def make_in_memory_event_broker(
    node_id: str,
    settings: EventBrokerSettings,
    *,
    adapter: InMemoryEventStorageAdapter,
) -> EventBroker:
    pass


def make_in_memory_event_broker(
    node_id: str,
    settings: EventBrokerSettings,
    *,
    store: EventStore[InMemoryEventStorageAdapter] | None = None,
    adapter: InMemoryEventStorageAdapter | None = None,
) -> EventBroker:
    if adapter is None:
        if store is None:
            raise ValueError("Either store or adapter must be provided")
        adapter = store.adapter

    return InMemoryEventBrokerBuilder(node_id).prepare(adapter).build(settings)
