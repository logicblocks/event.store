import asyncio
import itertools
import operator
from collections.abc import Sequence
from datetime import timedelta
from enum import StrEnum
from typing import Any

from structlog.types import FilteringBoundLogger

from logicblocks.event.types import str_serialisation_fallback

from .locks import LockManager
from .logger import default_logger
from .sources import (
    EventSubscriptionSourceMapping,
    EventSubscriptionSourceMappingStore,
)
from .subscribers import EventSubscriberState, EventSubscriberStateStore
from .subscriptions import (
    EventSubscriptionState,
    EventSubscriptionStateChange,
    EventSubscriptionStateChangeType,
    EventSubscriptionStateStore,
)


def chunk[T](values: Sequence[T], chunks: int) -> Sequence[Sequence[T]]:
    return [values[i::chunks] for i in range(chunks)]


def class_fullname(klass: type[Any]):
    module = klass.__module__
    if module == "builtins":
        return klass.__qualname__
    return module + "." + klass.__qualname__


def log_event_name(event: str) -> str:
    return f"event.processing.broker.coordinator.{event}"


def subscription_status(
    subscriptions: Sequence[EventSubscriptionState],
) -> dict[str, Any]:
    existing: dict[str, Any] = {}
    for subscription in subscriptions:
        if existing.get(subscription.group, None) is None:
            existing[subscription.group] = {}

        existing[subscription.group][subscription.id] = {
            "sources": [
                event_source.serialise(fallback=str_serialisation_fallback)
                for event_source in subscription.event_sources
            ]
        }
    return existing


def subscriber_group_status(
    subscribers: Sequence[EventSubscriberState],
    subscription_source_mappings: Sequence[EventSubscriptionSourceMapping],
) -> dict[str, Any]:
    latest: dict[str, Any] = {}
    for subscriber in subscribers:
        if latest.get(subscriber.group, None) is None:
            latest[subscriber.group] = {}
        if latest[subscriber.group].get("subscribers", None) is None:
            latest[subscriber.group]["subscribers"] = []
        latest[subscriber.group]["subscribers"].append(subscriber.id)

    for mapping in subscription_source_mappings:
        if latest.get(mapping.subscriber_group, None) is None:
            latest[mapping.subscriber_group] = {}
        latest[mapping.subscriber_group]["sources"] = [
            event_source.serialise(fallback=str_serialisation_fallback)
            for event_source in mapping.event_sources
        ]
    return latest


def subscription_change_summary(
    changes: Sequence[EventSubscriptionStateChange],
) -> dict[str, Any]:
    return {
        "additions": len(
            [
                change
                for change in changes
                if change.type == EventSubscriptionStateChangeType.ADD
            ]
        ),
        "removals": len(
            [
                change
                for change in changes
                if change.type == EventSubscriptionStateChangeType.REMOVE
            ]
        ),
        "replacements": len(
            [
                change
                for change in changes
                if change.type == EventSubscriptionStateChangeType.REPLACE
            ]
        ),
    }


class EventSubscriptionCoordinatorStatus(StrEnum):
    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    ERRORED = "errored"


class EventSubscriptionCoordinator:
    def __init__(
        self,
        node_id: str,
        lock_manager: LockManager,
        subscriber_state_store: EventSubscriberStateStore,
        subscription_state_store: EventSubscriptionStateStore,
        subscription_source_mapping_store: EventSubscriptionSourceMappingStore,
        logger: FilteringBoundLogger = default_logger,
        subscriber_max_time_since_last_seen: timedelta = timedelta(seconds=60),
        distribution_interval: timedelta = timedelta(seconds=20),
    ):
        self._node_id = node_id

        self._lock_manager = lock_manager
        self._logger = logger.bind(node=node_id)
        self._subscriber_state_store = subscriber_state_store
        self._subscription_state_store = subscription_state_store
        self._subscription_source_mapping_store = (
            subscription_source_mapping_store
        )

        self._subscriber_max_time_since_last_seen = (
            subscriber_max_time_since_last_seen
        )
        self._distribution_interval = distribution_interval
        self._status = EventSubscriptionCoordinatorStatus.STOPPED

    @property
    def status(self) -> EventSubscriptionCoordinatorStatus:
        return self._status

    async def coordinate(self) -> None:
        distribution_interval_seconds = (
            self._distribution_interval.total_seconds()
        )
        subscriber_max_last_seen_time = (
            self._subscriber_max_time_since_last_seen.total_seconds()
        )

        await self._logger.ainfo(
            log_event_name("starting"),
            distribution_interval_seconds=distribution_interval_seconds,
            subscriber_max_time_since_last_seen_seconds=subscriber_max_last_seen_time,
        )
        self._status = EventSubscriptionCoordinatorStatus.STARTING

        try:
            async with self._lock_manager.wait_for_lock(LOCK_NAME):
                await self._logger.ainfo(log_event_name("running"))
                self._status = EventSubscriptionCoordinatorStatus.RUNNING
                while True:
                    await self.distribute()
                    await asyncio.sleep(distribution_interval_seconds)
        except asyncio.CancelledError:
            self._status = EventSubscriptionCoordinatorStatus.STOPPED
            await self._logger.ainfo(log_event_name("stopped"))
            raise
        except BaseException:
            self._status = EventSubscriptionCoordinatorStatus.ERRORED
            await self._logger.aexception(log_event_name("failed"))
            raise

    async def distribute(self) -> None:
        await self._logger.adebug(log_event_name("distribution.starting"))

        subscriptions = await self._subscription_state_store.list()
        subscription_map = {
            subscription.key: subscription for subscription in subscriptions
        }

        await self._logger.adebug(
            log_event_name("distribution.existing-status"),
            subscriber_groups=subscription_status(subscriptions),
        )

        subscribers = await self._subscriber_state_store.list(
            max_time_since_last_seen=self._subscriber_max_time_since_last_seen
        )
        subscribers = sorted(subscribers, key=operator.attrgetter("group"))
        subscriber_map = {
            subscriber.key: subscriber for subscriber in subscribers
        }
        subscriber_group_subscribers = itertools.groupby(
            subscribers, operator.attrgetter("group")
        )

        subscription_source_mappings = (
            await self._subscription_source_mapping_store.list()
        )
        subscription_source_mappings_map = {
            subscription_source_mapping.subscriber_group: subscription_source_mapping
            for subscription_source_mapping in subscription_source_mappings
        }

        await self._logger.adebug(
            log_event_name("distribution.latest-status"),
            subscriber_groups=subscriber_group_status(
                subscribers, subscription_source_mappings
            ),
        )

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

        changes: list[EventSubscriptionStateChange] = []

        for subscription in subscriptions:
            if subscription.subscriber_key not in subscriber_map:
                changes.append(
                    EventSubscriptionStateChange(
                        type=EventSubscriptionStateChangeType.REMOVE,
                        subscription=subscription,
                    )
                )

        for subscriber_group, subscribers in subscriber_group_subscribers:
            subscribers = list(subscribers)
            subscriber_group_subscriptions = [
                subscription_map[subscriber.subscription_key]
                for subscriber in subscribers
                if subscriber.subscription_key in subscription_map
            ]

            subscription_source_mapping = subscription_source_mappings_map[
                subscriber_group
            ]
            known_event_sources = subscription_source_mapping.event_sources
            allocated_event_sources = [
                event_source
                for subscription in subscriber_group_subscriptions
                for event_source in subscription.event_sources
                if subscription.subscriber_key in subscriber_map
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
                    subscriber.subscription_key, None
                )
                if subscription is None:
                    changes.append(
                        EventSubscriptionStateChange(
                            type=EventSubscriptionStateChangeType.ADD,
                            subscription=EventSubscriptionState(
                                group=subscriber_group,
                                id=subscriber.id,
                                node_id=subscriber.node_id,
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
                        EventSubscriptionStateChange(
                            type=EventSubscriptionStateChangeType.REPLACE,
                            subscription=EventSubscriptionState(
                                group=subscriber_group,
                                id=subscriber.id,
                                node_id=subscriber.node_id,
                                event_sources=[
                                    *remaining_event_sources,
                                    *new_event_sources,
                                ],
                            ),
                        )
                    )

        for subscriber_group in removed_subscriber_groups:
            await self._subscription_source_mapping_store.remove(
                subscriber_group
            )

        await self._subscription_state_store.apply(changes=changes)

        subscriptions = await self._subscription_state_store.list()

        await self._logger.adebug(
            log_event_name("distribution.updated-status"),
            subscriber_groups=subscription_status(subscriptions),
        )

        await self._logger.adebug(
            log_event_name("distribution.complete"),
            subscription_changes=subscription_change_summary(changes),
        )


LOCK_NAME = class_fullname(EventSubscriptionCoordinator)
