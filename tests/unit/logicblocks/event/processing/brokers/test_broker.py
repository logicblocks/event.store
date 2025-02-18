import asyncio
from dataclasses import dataclass
from datetime import timedelta
from typing import Self, Sequence

from logicblocks.event.processing.broker import (
    CoordinatorObserverEventBroker,
    EventBroker,
    EventSubscriber,
    EventSubscriberHealth,
    EventSubscriberManager,
    EventSubscriptionCoordinator,
    EventSubscriptionDifference,
    EventSubscriptionObserver,
    InMemoryEventStoreEventSourceFactory,
    InMemoryEventSubscriberStateStore,
    InMemoryEventSubscriberStore,
    InMemoryEventSubscriptionSourceMappingStore,
    InMemoryEventSubscriptionStateStore,
    InMemoryLockManager,
    InMemoryNodeStateStore,
    NodeManager,
)
from logicblocks.event.store import EventSource
from logicblocks.event.store.adapters import InMemoryEventStorageAdapter
from logicblocks.event.testing import data
from logicblocks.event.types import EventSourceIdentifier


@dataclass(frozen=True)
class DummyEventSubscriber(EventSubscriber):
    _group: str
    _id: str

    @property
    def group(self) -> str:
        return self._group

    @property
    def id(self) -> str:
        return self._id

    @property
    def sequences(self) -> Sequence[EventSourceIdentifier]:
        return []

    def health(self) -> EventSubscriberHealth:
        return EventSubscriberHealth.HEALTHY

    async def accept(self, source: EventSource) -> None:
        pass

    async def withdraw(self, source: EventSource) -> None:
        pass


class DummyNodeManager(NodeManager):
    running: bool = False

    async def execute(self):
        self.running = True
        while True:
            await asyncio.sleep(0)


class DummyEventSubscriberManager(EventSubscriberManager):
    running: bool = False
    subscribers: list[EventSubscriber] = []

    async def add(self, subscriber: EventSubscriber) -> Self:
        self.subscribers.append(subscriber)
        return self

    async def execute(self):
        self.running = True
        while True:
            await asyncio.sleep(0)


class DummyEventSubscriptionCoordinator(EventSubscriptionCoordinator):
    running: bool = False

    async def coordinate(self) -> None:
        self.running = True
        while True:
            await asyncio.sleep(0)


class DummyEventSubscriptionObserver(EventSubscriptionObserver):
    running: bool = False

    async def observe(self):
        self.running = True
        while True:
            await asyncio.sleep(0)


@dataclass(frozen=True)
class Context:
    broker: EventBroker
    node_manager: DummyNodeManager
    event_subscriber_manager: DummyEventSubscriberManager
    event_subscription_coordinator: DummyEventSubscriptionCoordinator
    event_subscription_observer: DummyEventSubscriptionObserver


def make_event_broker():
    node_id = data.random_node_id()

    subscriber_store = InMemoryEventSubscriberStore()
    subscriber_state_store = InMemoryEventSubscriberStateStore(node_id=node_id)
    subscription_state_store = InMemoryEventSubscriptionStateStore(
        node_id=node_id
    )
    subscription_source_mapping_store = (
        InMemoryEventSubscriptionSourceMappingStore()
    )

    node_manager = DummyNodeManager(
        node_id=node_id, node_state_store=InMemoryNodeStateStore()
    )
    event_subscriber_manager = DummyEventSubscriberManager(
        node_id=node_id,
        subscriber_store=subscriber_store,
        subscriber_state_store=subscriber_state_store,
        subscription_source_mapping_store=subscription_source_mapping_store,
    )
    event_subscription_coordinator = DummyEventSubscriptionCoordinator(
        node_id=node_id,
        lock_manager=InMemoryLockManager(),
        subscriber_state_store=subscriber_state_store,
        subscription_state_store=subscription_state_store,
        subscription_source_mapping_store=subscription_source_mapping_store,
    )
    event_subscription_observer = DummyEventSubscriptionObserver(
        node_id=node_id,
        subscriber_store=subscriber_store,
        subscription_state_store=subscription_state_store,
        subscription_difference=EventSubscriptionDifference(),
        event_source_factory=InMemoryEventStoreEventSourceFactory(
            adapter=InMemoryEventStorageAdapter()
        ),
    )

    broker = CoordinatorObserverEventBroker(
        node_manager=node_manager,
        event_subscriber_manager=event_subscriber_manager,
        event_subscription_coordinator=event_subscription_coordinator,
        event_subscription_observer=event_subscription_observer,
    )

    return Context(
        broker=broker,
        node_manager=node_manager,
        event_subscriber_manager=event_subscriber_manager,
        event_subscription_coordinator=event_subscription_coordinator,
        event_subscription_observer=event_subscription_observer,
    )


class TestCoordinatorObserverEventBroker:
    async def test_register_adds_subscriber_to_manager(self):
        context = make_event_broker()
        broker = context.broker
        event_subscriber_manager = context.event_subscriber_manager

        subscriber_1 = DummyEventSubscriber(
            data.random_subscriber_group(), data.random_subscriber_id()
        )
        subscriber_2 = DummyEventSubscriber(
            data.random_subscriber_group(), data.random_subscriber_id()
        )

        await broker.register(subscriber_1)
        await broker.register(subscriber_2)

        assert event_subscriber_manager.subscribers == [
            subscriber_1,
            subscriber_2,
        ]

    async def test_execute_runs_all_subprocesses(self):
        context = make_event_broker()
        broker = context.broker
        node_manager = context.node_manager
        event_subscriber_manager = context.event_subscriber_manager
        event_subscription_coordinator = context.event_subscription_coordinator
        event_subscription_observer = context.event_subscription_observer

        task = asyncio.create_task(broker.execute())

        async def wait_until_running():
            while True:
                if (
                    node_manager.running
                    and event_subscriber_manager.running
                    and event_subscription_coordinator.running
                    and event_subscription_observer.running
                ):
                    return
                else:
                    await asyncio.sleep(0)

        await asyncio.wait_for(
            wait_until_running(),
            timeout=timedelta(milliseconds=100).total_seconds(),
        )

        task.cancel()

        await asyncio.gather(task, return_exceptions=True)
