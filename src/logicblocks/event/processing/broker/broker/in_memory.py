from datetime import timedelta

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
from .broker import EventBroker
from .broker_builder import EventBrokerBuilder, PrepareResults


class _InMemoryEventBrokerBuilder(EventBrokerBuilder):
    def _prepare(self) -> PrepareResults:
        return PrepareResults(
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
    node_manager_heartbeat_interval: timedelta = timedelta(seconds=10),
    node_manager_purge_interval: timedelta = timedelta(minutes=1),
    node_manager_node_max_age: timedelta = timedelta(minutes=10),
    subscriber_manager_heartbeat_interval: timedelta = timedelta(seconds=10),
    subscriber_manager_purge_interval: timedelta = timedelta(minutes=1),
    subscriber_manager_subscriber_max_age: timedelta = timedelta(minutes=10),
    coordinator_subscriber_max_time_since_last_seen: timedelta = timedelta(
        seconds=60
    ),
    coordinator_distribution_interval: timedelta = timedelta(seconds=20),
    observer_synchronisation_interval: timedelta = timedelta(seconds=20),
) -> EventBroker:
    return (
        _InMemoryEventBrokerBuilder(node_id)
        .prepare()
        .build(
            node_manager_heartbeat_interval=node_manager_heartbeat_interval,
            node_manager_purge_interval=node_manager_purge_interval,
            node_manager_node_max_age=node_manager_node_max_age,
            subscriber_manager_heartbeat_interval=subscriber_manager_heartbeat_interval,
            subscriber_manager_purge_interval=subscriber_manager_purge_interval,
            subscriber_manager_subscriber_max_age=subscriber_manager_subscriber_max_age,
            coordinator_subscriber_max_time_since_last_seen=coordinator_subscriber_max_time_since_last_seen,
            coordinator_distribution_interval=coordinator_distribution_interval,
            observer_synchronisation_interval=observer_synchronisation_interval,
        )
    )
