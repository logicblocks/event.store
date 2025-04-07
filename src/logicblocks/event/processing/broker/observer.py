import asyncio
from collections.abc import Sequence
from datetime import timedelta

from structlog.types import FilteringBoundLogger

from .difference import EventSubscriptionDifference
from .logger import default_logger
from .process import Process, ProcessStatus
from .sources import EventSourceFactory
from .subscribers import EventSubscriberStore
from .subscriptions import EventSubscriptionState, EventSubscriptionStateStore


def log_event_name(event: str) -> str:
    return f"event.processing.broker.observer.{event}"


class EventSubscriptionObserver(Process):
    _existing_subscriptions: Sequence[EventSubscriptionState]

    def __init__(
        self,
        node_id: str,
        subscriber_store: EventSubscriberStore,
        subscription_state_store: EventSubscriptionStateStore,
        event_source_factory: EventSourceFactory,
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
