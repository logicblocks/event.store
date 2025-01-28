import asyncio

from logicblocks.event.processing.broker import (
    EventBroker,
    InMemoryEventSubscriberStore,
    InMemoryEventSubscriptionStore,
    InMemoryLockManager,
    PostgresEventSubscriptionCoordinator,
)
from logicblocks.event.processing.broker.types import (
    EventSubscriber,
    EventSubscriptionSources,
)
from logicblocks.event.store import EventSource
from logicblocks.event.testing import data
from logicblocks.event.types import CategoryIdentifier


class CapturingEventSubscriber(EventSubscriber):
    sources: list[EventSource]

    def __init__(self, name: str, id: str):
        self._id = id
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    @property
    def id(self) -> str:
        return self._id

    async def subscribe(self, broker: EventBroker) -> None:
        raise NotImplementedError()

    async def accept(self, source: EventSource) -> None:
        self.sources.append(source)

    async def revoke(self, source: EventSource) -> None:
        self.sources.remove(source)


class TestPostgresEventSubscriptionCoordinator:
    async def test_distributes_source_for_single_subscriber_single_instance(
        self,
    ):
        subscriber_name = "thing-projector"
        subscriber_id = data.random_subscriber_id()

        subscriber = CapturingEventSubscriber(
            name=subscriber_name, id=subscriber_id
        )

        subscriber_store = InMemoryEventSubscriberStore()
        subscription_store = InMemoryEventSubscriptionStore()
        lock_manager = InMemoryLockManager()

        await subscriber_store.add(subscriber)

        coordinator = PostgresEventSubscriptionCoordinator(
            lock_manager=lock_manager,
            subscriber_store=subscriber_store,
            subscription_store=subscription_store,
        )
        coordinator.register(
            EventSubscriptionSources(
                subscriber_name="thing-projector",
                event_sources=[CategoryIdentifier(category="things")],
            )
        )

        asyncio.create_task(coordinator.coordinate())
