import asyncio
from collections import defaultdict
from datetime import timedelta

from logicblocks.event.processing.broker import (
    EventSubscriber,
    EventSubscriberHealth,
    EventSubscriberKey,
    EventSubscriberManager,
    InMemoryEventSubscriberStateStore,
    InMemoryEventSubscriberStore,
)
from logicblocks.event.store import EventSource
from logicblocks.event.testing import data


class CapturingEventSubscriber(EventSubscriber):
    sources: list[EventSource]
    counts: dict[str, int]

    def __init__(self, group: str, id: str, health: EventSubscriberHealth):
        self.sources = []
        self.counts = defaultdict(lambda: 0)
        self._group = group
        self._id = id
        self._health = health

    @property
    def group(self) -> str:
        return self._group

    @property
    def id(self) -> str:
        return self._id

    def health(self) -> EventSubscriberHealth:
        self.counts["health"] += 1
        return self._health

    async def accept(self, source: EventSource) -> None:
        self.sources.append(source)

    async def withdraw(self, source: EventSource) -> None:
        self.sources.remove(source)


class CountingEventSubscriberStateStore(InMemoryEventSubscriberStateStore):
    counts: dict[str, int]

    def __init__(self, node_id: str):
        super().__init__(node_id)
        self.counts = defaultdict(lambda: 0)

    async def heartbeat(self, subscriber: EventSubscriberKey) -> None:
        self.counts[f"heartbeat:{subscriber.group}/{subscriber.id}"] += 1
        return await super().heartbeat(subscriber)

    async def purge(
        self, max_time_since_last_seen: timedelta = timedelta(minutes=5)
    ) -> None:
        max_age_str = str(int(max_time_since_last_seen.total_seconds()))
        self.counts[f"purge:{max_age_str}"] += 1
        return await super().purge(max_time_since_last_seen)


class TestEventSubscriberManager:
    async def test_add_adds_subscriber_to_store(self):
        node_id = data.random_node_id()
        subscriber_store = InMemoryEventSubscriberStore()
        subscriber_state_store = InMemoryEventSubscriberStateStore(node_id)

        manager = EventSubscriberManager(
            node_id=node_id,
            subscriber_store=subscriber_store,
            subscriber_state_store=subscriber_state_store,
        )

        subscriber_1 = CapturingEventSubscriber(
            group=data.random_subscriber_group(),
            id=data.random_subscriber_id(),
            health=EventSubscriberHealth.HEALTHY,
        )
        subscriber_2 = CapturingEventSubscriber(
            group=data.random_subscriber_group(),
            id=data.random_subscriber_id(),
            health=EventSubscriberHealth.HEALTHY,
        )

        await manager.add(subscriber_1)
        await manager.add(subscriber_2)

        subscribers = await subscriber_store.list()

        assert subscriber_1 in subscribers
        assert subscriber_2 in subscribers

    async def test_execute_adds_subscriber_state_to_store_when_started(self):
        node_id = data.random_node_id()
        subscriber_store = InMemoryEventSubscriberStore()
        subscriber_state_store = InMemoryEventSubscriberStateStore(node_id)

        manager = EventSubscriberManager(
            node_id=node_id,
            subscriber_store=subscriber_store,
            subscriber_state_store=subscriber_state_store,
        )

        subscriber_1 = CapturingEventSubscriber(
            group=data.random_subscriber_group(),
            id=data.random_subscriber_id(),
            health=EventSubscriberHealth.HEALTHY,
        )
        subscriber_2 = CapturingEventSubscriber(
            group=data.random_subscriber_group(),
            id=data.random_subscriber_id(),
            health=EventSubscriberHealth.HEALTHY,
        )

        await manager.add(subscriber_1)
        await manager.add(subscriber_2)

        task = asyncio.create_task(manager.execute())

        try:

            async def get_subscriber_states():
                while True:
                    states = await subscriber_state_store.list()
                    if len(states) == 2:
                        return states
                    else:
                        await asyncio.sleep(0)

            states = await asyncio.wait_for(
                get_subscriber_states(),
                timeout=timedelta(milliseconds=100).total_seconds(),
            )

            assert set(state.key for state in states) == {
                EventSubscriberKey(subscriber_1.group, subscriber_1.id),
                EventSubscriberKey(subscriber_2.group, subscriber_2.id),
            }
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_execute_removes_subscriber_state_from_store_when_interrupted(
        self,
    ):
        node_id = data.random_node_id()
        subscriber_store = InMemoryEventSubscriberStore()
        subscriber_state_store = InMemoryEventSubscriberStateStore(node_id)

        manager = EventSubscriberManager(
            node_id=node_id,
            subscriber_store=subscriber_store,
            subscriber_state_store=subscriber_state_store,
        )

        subscriber_1 = CapturingEventSubscriber(
            group=data.random_subscriber_group(),
            id=data.random_subscriber_id(),
            health=EventSubscriberHealth.HEALTHY,
        )
        subscriber_2 = CapturingEventSubscriber(
            group=data.random_subscriber_group(),
            id=data.random_subscriber_id(),
            health=EventSubscriberHealth.HEALTHY,
        )

        await manager.add(subscriber_1)
        await manager.add(subscriber_2)

        task = asyncio.create_task(manager.execute())

        try:

            async def get_subscriber_states():
                while True:
                    states = await subscriber_state_store.list()
                    if len(states) == 2:
                        return states
                    else:
                        await asyncio.sleep(0)

            await asyncio.wait_for(
                get_subscriber_states(),
                timeout=timedelta(milliseconds=100).total_seconds(),
            )

            task.cancel()

            await asyncio.gather(task, return_exceptions=True)

            states = await subscriber_state_store.list()

            assert len(states) == 0
        finally:
            if not task.cancelled():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    async def test_execute_heartbeats_healthy_subscribers_every_interval(self):
        node_id = data.random_node_id()
        subscriber_store = InMemoryEventSubscriberStore()
        subscriber_state_store = CountingEventSubscriberStateStore(node_id)

        manager = EventSubscriberManager(
            node_id=node_id,
            subscriber_store=subscriber_store,
            subscriber_state_store=subscriber_state_store,
            heartbeat_interval=timedelta(milliseconds=20),
        )

        subscriber_1 = CapturingEventSubscriber(
            group=data.random_subscriber_group(),
            id=data.random_subscriber_id(),
            health=EventSubscriberHealth.HEALTHY,
        )
        subscriber_2 = CapturingEventSubscriber(
            group=data.random_subscriber_group(),
            id=data.random_subscriber_id(),
            health=EventSubscriberHealth.HEALTHY,
        )

        await manager.add(subscriber_1)
        await manager.add(subscriber_2)

        task = asyncio.create_task(manager.execute())

        await asyncio.sleep(timedelta(milliseconds=50).total_seconds())

        task.cancel()

        await asyncio.gather(task, return_exceptions=True)

        subscriber_1_count_key = (
            f"heartbeat:{subscriber_1.group}/{subscriber_1.id}"
        )
        subscriber_2_count_key = (
            f"heartbeat:{subscriber_2.group}/{subscriber_2.id}"
        )

        subscriber_1_heartbeat_count = subscriber_state_store.counts[
            subscriber_1_count_key
        ]
        subscriber_2_heartbeat_count = subscriber_state_store.counts[
            subscriber_2_count_key
        ]
        subscriber_1_health_check_count = subscriber_1.counts["health"]
        subscriber_2_health_check_count = subscriber_2.counts["health"]

        assert 2 <= subscriber_1_heartbeat_count <= 3
        assert 2 <= subscriber_2_heartbeat_count <= 3
        assert 2 <= subscriber_1_health_check_count <= 3
        assert 2 <= subscriber_2_health_check_count <= 3

    async def test_execute_does_not_heartbeat_unhealthy_subscribers_each_interval(
        self,
    ):
        node_id = data.random_node_id()
        subscriber_store = InMemoryEventSubscriberStore()
        subscriber_state_store = CountingEventSubscriberStateStore(node_id)

        manager = EventSubscriberManager(
            node_id=node_id,
            subscriber_store=subscriber_store,
            subscriber_state_store=subscriber_state_store,
            heartbeat_interval=timedelta(milliseconds=20),
        )

        subscriber_1 = CapturingEventSubscriber(
            group=data.random_subscriber_group(),
            id=data.random_subscriber_id(),
            health=EventSubscriberHealth.UNHEALTHY,
        )
        subscriber_2 = CapturingEventSubscriber(
            group=data.random_subscriber_group(),
            id=data.random_subscriber_id(),
            health=EventSubscriberHealth.UNHEALTHY,
        )

        await manager.add(subscriber_1)
        await manager.add(subscriber_2)

        task = asyncio.create_task(manager.execute())

        await asyncio.sleep(timedelta(milliseconds=50).total_seconds())

        task.cancel()

        await asyncio.gather(task, return_exceptions=True)

        subscriber_1_count_key = (
            f"heartbeat:{subscriber_1.group}/{subscriber_1.id}"
        )
        subscriber_2_count_key = (
            f"heartbeat:{subscriber_2.group}/{subscriber_2.id}"
        )

        subscriber_1_heartbeat_count = subscriber_state_store.counts[
            subscriber_1_count_key
        ]
        subscriber_2_heartbeat_count = subscriber_state_store.counts[
            subscriber_2_count_key
        ]
        subscriber_1_health_check_count = subscriber_1.counts["health"]
        subscriber_2_health_check_count = subscriber_2.counts["health"]

        assert subscriber_1_heartbeat_count == 0
        assert subscriber_2_heartbeat_count == 0
        assert 2 <= subscriber_1_health_check_count <= 3
        assert 2 <= subscriber_2_health_check_count <= 3

    async def test_execute_purges_state_store_every_purge_interval(self):
        node_id = data.random_node_id()
        subscriber_store = InMemoryEventSubscriberStore()
        subscriber_state_store = CountingEventSubscriberStateStore(node_id)

        manager = EventSubscriberManager(
            node_id=node_id,
            subscriber_store=subscriber_store,
            subscriber_state_store=subscriber_state_store,
            purge_interval=timedelta(milliseconds=20),
            subscriber_max_age=timedelta(minutes=3),
        )

        task = asyncio.create_task(manager.execute())

        await asyncio.sleep(timedelta(milliseconds=50).total_seconds())

        task.cancel()

        await asyncio.gather(task, return_exceptions=True)

        purge_count = subscriber_state_store.counts["purge:180"]

        assert 2 <= purge_count <= 3
