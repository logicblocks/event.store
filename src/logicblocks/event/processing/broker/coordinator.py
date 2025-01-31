import itertools
import operator
from collections.abc import Sequence
from datetime import timedelta

from .locks import LockManager
from .sources import EventSubscriptionSourcesStore
from .subscribers import EventSubscriberStore
from .subscriptions import (
    EventSubscriptionChange,
    EventSubscriptionChangeType,
    EventSubscriptionKey,
    EventSubscriptionState,
    EventSubscriptionStore,
)
from .types import EventSubscriberKey


def chunk[T](values: Sequence[T], chunks: int) -> Sequence[Sequence[T]]:
    return [values[i::chunks] for i in range(chunks)]


class EventSubscriptionCoordinator:
    def __init__(
        self,
        lock_manager: LockManager,
        subscriber_store: EventSubscriberStore,
        subscription_store: EventSubscriptionStore,
        subscription_sources_store: EventSubscriptionSourcesStore,
        subscriber_max_time_since_last_seen: timedelta = timedelta(seconds=60),
    ):
        self.lock_manager = lock_manager
        self.subscriber_store = subscriber_store
        self.subscription_store = subscription_store
        self.subscription_sources_store = subscription_sources_store

        self.subscriber_max_time_since_last_seen = (
            subscriber_max_time_since_last_seen
        )

    async def coordinate(self) -> None:
        async with self.lock_manager.wait_for_lock("coordinator"):
            pass
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

    async def distribute(self) -> None:
        subscribers = await self.subscriber_store.list(
            max_time_since_last_seen=self.subscriber_max_time_since_last_seen
        )
        subscribers = sorted(subscribers, key=operator.attrgetter("group"))
        subscriber_map = {
            subscriber.key: subscriber for subscriber in subscribers
        }
        subscriber_groups = itertools.groupby(
            subscribers, operator.attrgetter("group")
        )

        subscriptions = await self.subscription_store.list()
        subscription_map = {
            subscription.key: subscription for subscription in subscriptions
        }

        subscription_sources = await self.subscription_sources_store.list()
        subscription_sources_map = {
            subscription_source.subscriber_group: subscription_source
            for subscription_source in subscription_sources
        }

        subscriber_groups_with_instances = {
            subscriber.group for subscriber in subscribers
        }
        subscriber_groups_with_subscriptions = {
            subscription.group for subscription in subscriptions
        }
        removed_subscriber_groups = (
            subscriber_groups_with_subscriptions
            - subscriber_groups_with_instances
        )

        changes: list[EventSubscriptionChange] = []

        for subscription in subscriptions:
            if (
                EventSubscriberKey(subscription.group, subscription.id)
                not in subscriber_map
            ):
                changes.append(
                    EventSubscriptionChange(
                        type=EventSubscriptionChangeType.REMOVE,
                        state=subscription,
                    )
                )

        for subscriber_group, subscribers in subscriber_groups:
            subscribers = list(subscribers)
            subscriber_group_subscriptions = [
                subscription_map[key]
                for subscriber in subscribers
                if (
                    key := EventSubscriptionKey(
                        group=subscriber_group, id=subscriber.id
                    )
                )
                and key in subscription_map
            ]

            subscription_source = subscription_sources_map[subscriber_group]
            known_event_sources = subscription_source.event_sources
            allocated_event_sources = [
                event_source
                for subscription in subscriber_group_subscriptions
                for event_source in subscription.event_sources
                if EventSubscriberKey(subscription.group, subscription.id)
                in subscriber_map
            ]
            removed_event_sources = [
                event_source
                for event_source in allocated_event_sources
                if event_source not in known_event_sources
            ]
            new_event_sources = list(
                set(known_event_sources) - set(allocated_event_sources)
            )

            new_event_source_chunks = chunk(
                new_event_sources, len(subscribers)
            )

            for index, subscriber in enumerate(subscribers):
                subscription = subscription_map.get(
                    EventSubscriptionKey(
                        group=subscriber_group,
                        id=subscriber.id,
                    ),
                    None,
                )
                if subscription is None:
                    changes.append(
                        EventSubscriptionChange(
                            type=EventSubscriptionChangeType.ADD,
                            state=EventSubscriptionState(
                                group=subscriber_group,
                                id=subscriber.id,
                                event_sources=new_event_source_chunks[index],
                            ),
                        )
                    )
                else:
                    remaining_event_sources = set(
                        subscription.event_sources
                    ) - set(removed_event_sources)
                    new_event_sources = new_event_source_chunks[index]
                    changes.append(
                        EventSubscriptionChange(
                            type=EventSubscriptionChangeType.REPLACE,
                            state=EventSubscriptionState(
                                group=subscriber_group,
                                id=subscriber.id,
                                event_sources=[
                                    *remaining_event_sources,
                                    *new_event_sources,
                                ],
                            ),
                        )
                    )

        for subscriber_group in removed_subscriber_groups:
            await self.subscription_sources_store.remove(subscriber_group)

        await self.subscription_store.apply(changes=changes)
