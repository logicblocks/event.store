from typing import Self

from . import LockManager
from .subscribers import EventSubscriberStore
from .subscriptions import EventSubscriptionStore
from .types import EventSubscriptionSources


class PostgresEventSubscriptionCoordinator:
    def __init__(
        self,
        lock_manager: LockManager,
        subscriber_store: EventSubscriberStore,
        subscription_store: EventSubscriptionStore,
    ):
        self.lock_manager = lock_manager
        self.subscriber_store = subscriber_store
        self.subscription_store = subscription_store
        self.subscription_sources: list[EventSubscriptionSources] = []

    def register(self, sources: EventSubscriptionSources) -> Self:
        self.subscription_sources.append(sources)
        return self

    async def coordinate(self) -> None:
        # with lock
        #   every distribute interval:
        #     list subscribers
        #     list existing subscriptions
        #     reconcile subscriptions and remove/add in transaction
        #   every rebalance interval:
        #     list subscribers
        #     list existing subscriptions
        #     rebalance subscriptions and add/update/remove in transaction
        # do we remove allocations and then wait??
        pass


# class PostgresEventBroker(EventBroker, Service):
#     def __init__(self):
#         self.consumers: list[EventSubscriber] = []
#
#     async def register(self, subscriber: EventSubscriber) -> None:
#         pass
#
#     def execute(self):
#         while True:
#             # try to become leader
#             # register and allocate work
#             # ---
#             # provide consumers with their event sources
#             # revoke them when no longer allowed
#             pass
