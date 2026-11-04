import asyncio
from abc import ABC, abstractmethod
from collections.abc import Sequence
from datetime import timedelta

from structlog.types import FilteringBoundLogger

from logicblocks.event.sources import EventSourceFactory
from logicblocks.event.types import Event

from ....process import Process, ProcessStatus
from ...logger import default_logger
from ...subscribers import EventSubscriberStore
from .difference import EventSubscriptionDifference
from .subscriptions import EventSubscriptionState, EventSubscriptionStateStore


def log_event_name(event: str) -> str:
    return f"event.processing.broker.observer.{event}"


class EventSubscriptionObserver[E: Event](Process, ABC):
    """Applies stored subscriptions to the subscribers on this node."""

    @abstractmethod
    async def observe(self) -> None:
        """Synchronise subscriptions to subscribers until cancelled."""
        raise NotImplementedError


class DefaultEventSubscriptionObserver[E: Event](EventSubscriptionObserver[E]):
    """Periodically synchronises stored subscriptions to local subscribers.

    Args:
        node_id: The ID of the node on which the observer runs.
        subscriber_store: The store of this node's subscribers.
        subscription_state_store: The shared store of subscription state.
        event_source_factory: The factory used to construct event sources
            from allocated identifiers.
        subscription_difference: Calculates changes between subscription
            snapshots.
        logger: The logger to use.
        synchronisation_interval: The time to wait between synchronisations.
    """

    _existing_subscriptions: Sequence[EventSubscriptionState]

    def __init__(
        self,
        node_id: str,
        subscriber_store: EventSubscriberStore[E],
        subscription_state_store: EventSubscriptionStateStore,
        event_source_factory: EventSourceFactory[E],
        subscription_difference: EventSubscriptionDifference = EventSubscriptionDifference(),
        logger: FilteringBoundLogger = default_logger,
        synchronisation_interval: timedelta = timedelta(seconds=20),
    ):
        self._subscriber_store = subscriber_store
        self._subscription_state_store = subscription_state_store
        self._subscription_difference = subscription_difference
        self._event_source_factory = event_source_factory
        self._logger = logger.bind(node=node_id)

        self._existing_subscriptions = []

        self._synchronisation_interval = synchronisation_interval
        self._status = ProcessStatus.INITIALISED

    @property
    def status(self) -> ProcessStatus:
        return self._status

    async def observe(self):
        await self._logger.ainfo(
            log_event_name("starting"),
            synchronisation_interval_seconds=self._synchronisation_interval.total_seconds(),
        )
        self._status = ProcessStatus.RUNNING
        try:
            while True:
                await self.synchronise()
                await asyncio.sleep(
                    self._synchronisation_interval.total_seconds()
                )
        except asyncio.CancelledError:
            self._status = ProcessStatus.STOPPED
            await self._logger.ainfo(log_event_name("stopped"))
            raise
        except BaseException:
            self._status = ProcessStatus.ERRORED
            await self._logger.aexception(log_event_name("failed"))
            raise

    async def synchronise(self):
        """Apply changes in subscriptions since the last synchronisation.

        Revoked event sources are withdrawn before newly allocated ones are
        accepted. Changes for subscribers not on this node are ignored.
        """
        await self._logger.adebug(log_event_name("synchronisation.starting"))

        existing = self._existing_subscriptions
        updated = await self._subscription_state_store.list()

        changeset = self._subscription_difference.diff(existing, updated)

        for revocation in changeset.revocations:
            subscriber = await self._subscriber_store.get(
                revocation.subscriber_key
            )
            if subscriber is not None:
                event_source = self._event_source_factory.construct(
                    revocation.event_source
                )
                await subscriber.withdraw(event_source)

        for allocation in changeset.allocations:
            subscriber = await self._subscriber_store.get(
                allocation.subscriber_key
            )
            if subscriber is not None:
                event_source = self._event_source_factory.construct(
                    allocation.event_source
                )
                await subscriber.accept(event_source)

        self._existing_subscriptions = updated

        await self._logger.adebug(
            log_event_name("synchronisation.complete"),
            changes={
                "allocation_count": len(changeset.allocations),
                "revocation_count": len(changeset.revocations),
            },
        )
