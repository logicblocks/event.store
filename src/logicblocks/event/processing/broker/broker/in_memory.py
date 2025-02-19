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
        self, adaptor: InMemoryEventStorageAdapter
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
                adapter=adaptor
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
    adaptor: InMemoryEventStorageAdapter,
) -> EventBroker:
    pass


def make_in_memory_event_broker(
    node_id: str,
    settings: EventBrokerSettings,
    *,
    store: EventStore[InMemoryEventStorageAdapter] | None = None,
    adaptor: InMemoryEventStorageAdapter | None = None,
) -> EventBroker:
    if adaptor is None:
        if store is None:
            raise ValueError("Either store or adaptor must be provided")
        adaptor = store.adapter

    return InMemoryEventBrokerBuilder(node_id).prepare(adaptor).build(settings)
