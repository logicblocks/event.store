import asyncio
from collections import defaultdict
from collections.abc import Sequence
from datetime import timedelta

from pytest_unordered import unordered

from logicblocks.event.processing.broker import (
    EventSubscriber,
    EventSubscriberHealth,
    EventSubscriberKey,
    EventSubscriberManager,
    InMemoryEventSubscriberStateStore,
    InMemoryEventSubscriberStore,
    InMemoryEventSubscriptionSourceMappingStore,
)
from logicblocks.event.processing.broker.sources.stores.mappings.base import (
    EventSubscriptionSourceMapping,
)
from logicblocks.event.store import EventSource
from logicblocks.event.testing import data
from logicblocks.event.testlogging.logger import CapturingLogger, LogLevel
from logicblocks.event.types import CategoryIdentifier, EventSourceIdentifier


class CapturingEventSubscriber(EventSubscriber):
    sources: list[EventSource]
    counts: dict[str, int]

    def __init__(
        self,
        group: str,
        id: str,
        sequences: Sequence[EventSourceIdentifier],
        health: EventSubscriberHealth = EventSubscriberHealth.HEALTHY,
    ):
        self.sources = []
        self.counts = defaultdict(lambda: 0)
        self._group = group
        self._id = id
        self._sequences = sequences
        self._health = health

    @property
    def group(self) -> str:
        return self._group

    @property
    def id(self) -> str:
        return self._id

    @property
    def subscription_requests(self) -> Sequence[EventSourceIdentifier]:
        return self._sequences

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
        subscription_source_mapping_store = (
            InMemoryEventSubscriptionSourceMappingStore()
        )

        manager = EventSubscriberManager(
            node_id=node_id,
            subscriber_store=subscriber_store,
            subscriber_state_store=subscriber_state_store,
            subscription_source_mapping_store=subscription_source_mapping_store,
        )

        subscriber_1 = CapturingEventSubscriber(
            group=data.random_subscriber_group(),
            id=data.random_subscriber_id(),
            sequences=[CategoryIdentifier(data.random_event_category_name())],
            health=EventSubscriberHealth.HEALTHY,
        )
        subscriber_2 = CapturingEventSubscriber(
            group=data.random_subscriber_group(),
            id=data.random_subscriber_id(),
            sequences=[CategoryIdentifier(data.random_event_category_name())],
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
        subscription_source_mapping_store = (
            InMemoryEventSubscriptionSourceMappingStore()
        )

        manager = EventSubscriberManager(
            node_id=node_id,
            subscriber_store=subscriber_store,
            subscriber_state_store=subscriber_state_store,
            subscription_source_mapping_store=subscription_source_mapping_store,
        )

        subscriber_1 = CapturingEventSubscriber(
            group=data.random_subscriber_group(),
            id=data.random_subscriber_id(),
            sequences=[CategoryIdentifier(data.random_event_category_name())],
            health=EventSubscriberHealth.HEALTHY,
        )
        subscriber_2 = CapturingEventSubscriber(
            group=data.random_subscriber_group(),
            id=data.random_subscriber_id(),
            sequences=[CategoryIdentifier(data.random_event_category_name())],
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

    async def test_execute_adds_subscription_mapping_to_store_when_started(
        self,
    ):
        node_id = data.random_node_id()
        subscriber_store = InMemoryEventSubscriberStore()
        subscriber_state_store = InMemoryEventSubscriberStateStore(node_id)
        subscription_source_mapping_store = (
            InMemoryEventSubscriptionSourceMappingStore()
        )

        manager = EventSubscriberManager(
            node_id=node_id,
            subscriber_store=subscriber_store,
            subscriber_state_store=subscriber_state_store,
            subscription_source_mapping_store=subscription_source_mapping_store,
        )

        subscriber_1_sequences = [
            CategoryIdentifier(data.random_event_category_name()),
            CategoryIdentifier(data.random_event_category_name()),
        ]
        subscriber_2_sequences = [
            CategoryIdentifier(data.random_event_category_name())
        ]

        subscriber_group_1 = data.random_subscriber_group()
        subscriber_group_2 = data.random_subscriber_group()

        subscriber_1 = CapturingEventSubscriber(
            group=subscriber_group_1,
            id=data.random_subscriber_id(),
            sequences=subscriber_1_sequences,
            health=EventSubscriberHealth.HEALTHY,
        )
        subscriber_2 = CapturingEventSubscriber(
            group=subscriber_group_2,
            id=data.random_subscriber_id(),
            sequences=subscriber_2_sequences,
            health=EventSubscriberHealth.HEALTHY,
        )

        await manager.add(subscriber_1)
        await manager.add(subscriber_2)

        task = asyncio.create_task(manager.execute())

        try:

            async def get_subscription_source_mappings():
                while True:
                    mappings = await subscription_source_mapping_store.list()
                    if len(mappings) == 2:
                        return mappings
                    else:
                        await asyncio.sleep(0)

            mappings = await asyncio.wait_for(
                get_subscription_source_mappings(),
                timeout=timedelta(milliseconds=100).total_seconds(),
            )

            assert set(mappings) == {
                EventSubscriptionSourceMapping(
                    subscriber_group=subscriber_group_1,
                    event_sources=tuple(subscriber_1_sequences),
                ),
                EventSubscriptionSourceMapping(
                    subscriber_group=subscriber_group_2,
                    event_sources=tuple(subscriber_2_sequences),
                ),
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
        subscription_source_mapping_store = (
            InMemoryEventSubscriptionSourceMappingStore()
        )

        manager = EventSubscriberManager(
            node_id=node_id,
            subscriber_store=subscriber_store,
            subscriber_state_store=subscriber_state_store,
            subscription_source_mapping_store=subscription_source_mapping_store,
        )

        subscriber_1 = CapturingEventSubscriber(
            group=data.random_subscriber_group(),
            id=data.random_subscriber_id(),
            sequences=[CategoryIdentifier(data.random_event_category_name())],
            health=EventSubscriberHealth.HEALTHY,
        )
        subscriber_2 = CapturingEventSubscriber(
            group=data.random_subscriber_group(),
            id=data.random_subscriber_id(),
            sequences=[CategoryIdentifier(data.random_event_category_name())],
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

    async def test_execute_removes_subscription_source_mappings_from_store_when_interrupted(
        self,
    ):
        node_id = data.random_node_id()
        subscriber_store = InMemoryEventSubscriberStore()
        subscriber_state_store = InMemoryEventSubscriberStateStore(node_id)
        subscription_source_mapping_store = (
            InMemoryEventSubscriptionSourceMappingStore()
        )

        manager = EventSubscriberManager(
            node_id=node_id,
            subscriber_store=subscriber_store,
            subscriber_state_store=subscriber_state_store,
            subscription_source_mapping_store=subscription_source_mapping_store,
        )

        subscriber_1_sequences = [
            CategoryIdentifier(data.random_event_category_name()),
            CategoryIdentifier(data.random_event_category_name()),
        ]
        subscriber_2_sequences = [
            CategoryIdentifier(data.random_event_category_name())
        ]

        subscriber_group_1 = data.random_subscriber_group()
        subscriber_group_2 = data.random_subscriber_group()

        subscriber_1 = CapturingEventSubscriber(
            group=subscriber_group_1,
            id=data.random_subscriber_id(),
            sequences=subscriber_1_sequences,
            health=EventSubscriberHealth.HEALTHY,
        )
        subscriber_2 = CapturingEventSubscriber(
            group=subscriber_group_2,
            id=data.random_subscriber_id(),
            sequences=subscriber_2_sequences,
            health=EventSubscriberHealth.HEALTHY,
        )

        await manager.add(subscriber_1)
        await manager.add(subscriber_2)

        task = asyncio.create_task(manager.execute())

        try:

            async def get_subscription_source_mappings():
                while True:
                    mappings = await subscription_source_mapping_store.list()
                    if len(mappings) == 2:
                        return mappings
                    else:
                        await asyncio.sleep(0)

            await asyncio.wait_for(
                get_subscription_source_mappings(),
                timeout=timedelta(milliseconds=100).total_seconds(),
            )

            task.cancel()

            await asyncio.gather(task, return_exceptions=True)

            mappings = await subscription_source_mapping_store.list()

            assert len(mappings) == 0
        finally:
            if not task.cancelled():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    async def test_execute_heartbeats_healthy_subscribers_every_interval(self):
        node_id = data.random_node_id()
        subscriber_store = InMemoryEventSubscriberStore()
        subscriber_state_store = CountingEventSubscriberStateStore(node_id)
        subscription_source_mapping_store = (
            InMemoryEventSubscriptionSourceMappingStore()
        )

        manager = EventSubscriberManager(
            node_id=node_id,
            subscriber_store=subscriber_store,
            subscriber_state_store=subscriber_state_store,
            heartbeat_interval=timedelta(milliseconds=20),
            subscription_source_mapping_store=subscription_source_mapping_store,
        )

        subscriber_1 = CapturingEventSubscriber(
            group=data.random_subscriber_group(),
            id=data.random_subscriber_id(),
            sequences=[CategoryIdentifier(data.random_event_category_name())],
            health=EventSubscriberHealth.HEALTHY,
        )
        subscriber_2 = CapturingEventSubscriber(
            group=data.random_subscriber_group(),
            id=data.random_subscriber_id(),
            sequences=[CategoryIdentifier(data.random_event_category_name())],
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
        subscription_source_mapping_store = (
            InMemoryEventSubscriptionSourceMappingStore()
        )

        manager = EventSubscriberManager(
            node_id=node_id,
            subscriber_store=subscriber_store,
            subscriber_state_store=subscriber_state_store,
            heartbeat_interval=timedelta(milliseconds=20),
            subscription_source_mapping_store=subscription_source_mapping_store,
        )

        subscriber_1 = CapturingEventSubscriber(
            group=data.random_subscriber_group(),
            id=data.random_subscriber_id(),
            sequences=[CategoryIdentifier(data.random_event_category_name())],
            health=EventSubscriberHealth.UNHEALTHY,
        )
        subscriber_2 = CapturingEventSubscriber(
            group=data.random_subscriber_group(),
            id=data.random_subscriber_id(),
            sequences=[CategoryIdentifier(data.random_event_category_name())],
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
        subscription_source_mapping_store = (
            InMemoryEventSubscriptionSourceMappingStore()
        )

        manager = EventSubscriberManager(
            node_id=node_id,
            subscriber_store=subscriber_store,
            subscriber_state_store=subscriber_state_store,
            purge_interval=timedelta(milliseconds=20),
            subscriber_max_age=timedelta(minutes=3),
            subscription_source_mapping_store=subscription_source_mapping_store,
        )

        task = asyncio.create_task(manager.execute())

        await asyncio.sleep(timedelta(milliseconds=50).total_seconds())

        task.cancel()

        await asyncio.gather(task, return_exceptions=True)

        purge_count = subscriber_state_store.counts["purge:180"]

        assert 2 <= purge_count <= 3

    async def test_execute_logs_on_startup(self):
        logger = CapturingLogger.create()

        node_id = data.random_node_id()
        subscriber_store = InMemoryEventSubscriberStore()
        subscriber_state_store = CountingEventSubscriberStateStore(node_id)
        subscription_source_mapping_store = (
            InMemoryEventSubscriptionSourceMappingStore()
        )

        manager = EventSubscriberManager(
            node_id=node_id,
            subscriber_store=subscriber_store,
            subscriber_state_store=subscriber_state_store,
            subscription_source_mapping_store=subscription_source_mapping_store,
            logger=logger,
            heartbeat_interval=timedelta(seconds=5),
            purge_interval=timedelta(seconds=30),
            subscriber_max_age=timedelta(minutes=3),
        )

        subscriber = CapturingEventSubscriber(
            group=data.random_subscriber_group(),
            id=data.random_subscriber_id(),
            sequences=[CategoryIdentifier(data.random_event_category_name())],
        )

        await manager.add(subscriber)

        task = asyncio.create_task(manager.execute())

        try:

            async def startup_complete():
                while True:
                    if len(await subscriber_state_store.list()) == 0:
                        await asyncio.sleep(0)
                    else:
                        return

            await asyncio.wait_for(
                startup_complete(),
                timeout=timedelta(milliseconds=100).total_seconds(),
            )

            startup_event = logger.find_event(
                "event.processing.broker.subscriber-manager.starting"
            )

            assert startup_event is not None
            assert startup_event.level == LogLevel.INFO
            assert startup_event.is_async is True
            assert startup_event.context == {
                "node": node_id,
                "subscribers": [subscriber.key.dict()],
                "heartbeat_interval_seconds": 5.0,
                "purge_interval_seconds": 30.0,
                "subscriber_max_age_seconds": 180.0,
            }
        finally:
            if not task.cancelled():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    async def test_execute_logs_on_shutdown(self):
        logger = CapturingLogger.create()

        node_id = data.random_node_id()
        subscriber_store = InMemoryEventSubscriberStore()
        subscriber_state_store = CountingEventSubscriberStateStore(node_id)
        subscription_source_mapping_store = (
            InMemoryEventSubscriptionSourceMappingStore()
        )

        manager = EventSubscriberManager(
            node_id=node_id,
            subscriber_store=subscriber_store,
            subscriber_state_store=subscriber_state_store,
            subscription_source_mapping_store=subscription_source_mapping_store,
            logger=logger,
            heartbeat_interval=timedelta(seconds=5),
            purge_interval=timedelta(seconds=30),
            subscriber_max_age=timedelta(minutes=3),
        )

        subscriber = CapturingEventSubscriber(
            group=data.random_subscriber_group(),
            id=data.random_subscriber_id(),
            sequences=[CategoryIdentifier(data.random_event_category_name())],
        )

        await manager.add(subscriber)

        task = asyncio.create_task(manager.execute())

        try:

            async def startup_complete():
                while True:
                    if len(await subscriber_state_store.list()) == 0:
                        await asyncio.sleep(0)
                    else:
                        return

            await asyncio.wait_for(
                startup_complete(),
                timeout=timedelta(milliseconds=100).total_seconds(),
            )

            task.cancel()

            await asyncio.gather(task, return_exceptions=True)

            shutdown_event = logger.find_event(
                "event.processing.broker.subscriber-manager.stopped"
            )

            assert shutdown_event is not None
            assert shutdown_event.level == LogLevel.INFO
            assert shutdown_event.is_async is True
            assert shutdown_event.context == {
                "node": node_id,
                "subscribers": [subscriber.key.dict()],
            }
        finally:
            if not task.cancelled():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    async def test_execute_logs_when_registering_subscribers(self):
        logger = CapturingLogger.create()

        node_id = data.random_node_id()
        subscriber_store = InMemoryEventSubscriberStore()
        subscriber_state_store = InMemoryEventSubscriberStateStore(node_id)
        subscription_source_mapping_store = (
            InMemoryEventSubscriptionSourceMappingStore()
        )

        manager = EventSubscriberManager(
            node_id=node_id,
            subscriber_store=subscriber_store,
            subscriber_state_store=subscriber_state_store,
            subscription_source_mapping_store=subscription_source_mapping_store,
            logger=logger,
        )

        subscriber_1_group = data.random_subscriber_group()
        subscriber_1_id = data.random_subscriber_id()
        subscriber_1_category_name = data.random_event_category_name()
        subscriber_1_sequences = [
            CategoryIdentifier(subscriber_1_category_name)
        ]

        subscriber_1 = CapturingEventSubscriber(
            group=subscriber_1_group,
            id=subscriber_1_id,
            sequences=subscriber_1_sequences,
            health=EventSubscriberHealth.HEALTHY,
        )

        subscriber_2_group = data.random_subscriber_group()
        subscriber_2_id = data.random_subscriber_id()
        subscriber_2_category_name = data.random_event_category_name()
        subscriber_2_sequences = [
            CategoryIdentifier(subscriber_2_category_name)
        ]

        subscriber_2 = CapturingEventSubscriber(
            group=subscriber_2_group,
            id=subscriber_2_id,
            sequences=subscriber_2_sequences,
            health=EventSubscriberHealth.HEALTHY,
        )

        await manager.add(subscriber_1)
        await manager.add(subscriber_2)

        task = asyncio.create_task(manager.execute())

        try:

            async def subscribers_added():
                while True:
                    states = await subscriber_state_store.list()
                    if len(states) == 2:
                        return states
                    else:
                        await asyncio.sleep(0)

            await asyncio.wait_for(
                subscribers_added(),
                timeout=timedelta(milliseconds=100).total_seconds(),
            )

            register_log_events = logger.find_events(
                "event.processing.broker.subscriber-manager."
                "registering-subscriber"
            )

            assert len(register_log_events) == 2

            assert register_log_events[0].level == LogLevel.INFO
            assert register_log_events[0].is_async is True

            assert register_log_events[1].level == LogLevel.INFO
            assert register_log_events[1].is_async is True

            assert [
                register_log_events[0].context,
                register_log_events[1].context,
            ] == unordered(
                [
                    {
                        "node": node_id,
                        "subscriber": {
                            "group": subscriber_1_group,
                            "id": subscriber_1_id,
                            "sequences": [
                                {
                                    "type": "category",
                                    "category": subscriber_1_category_name,
                                }
                            ],
                        },
                    },
                    {
                        "node": node_id,
                        "subscriber": {
                            "group": subscriber_2_group,
                            "id": subscriber_2_id,
                            "sequences": [
                                {
                                    "type": "category",
                                    "category": subscriber_2_category_name,
                                }
                            ],
                        },
                    },
                ]
            )
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_execute_logs_when_unregistering_subscribers(
        self,
    ):
        logger = CapturingLogger.create()

        node_id = data.random_node_id()
        subscriber_store = InMemoryEventSubscriberStore()
        subscriber_state_store = InMemoryEventSubscriberStateStore(node_id)
        subscription_source_mapping_store = (
            InMemoryEventSubscriptionSourceMappingStore()
        )

        manager = EventSubscriberManager(
            node_id=node_id,
            subscriber_store=subscriber_store,
            subscriber_state_store=subscriber_state_store,
            subscription_source_mapping_store=subscription_source_mapping_store,
            logger=logger,
        )

        subscriber_1_group = data.random_subscriber_group()
        subscriber_1_id = data.random_subscriber_id()
        subscriber_1_sequences = [
            CategoryIdentifier(data.random_event_category_name())
        ]

        subscriber_1 = CapturingEventSubscriber(
            group=subscriber_1_group,
            id=subscriber_1_id,
            sequences=subscriber_1_sequences,
            health=EventSubscriberHealth.HEALTHY,
        )

        subscriber_2_group = data.random_subscriber_group()
        subscriber_2_id = data.random_subscriber_id()
        subscriber_2_sequences = [
            CategoryIdentifier(data.random_event_category_name())
        ]

        subscriber_2 = CapturingEventSubscriber(
            group=subscriber_2_group,
            id=subscriber_2_id,
            sequences=subscriber_2_sequences,
            health=EventSubscriberHealth.HEALTHY,
        )

        await manager.add(subscriber_1)
        await manager.add(subscriber_2)

        task = asyncio.create_task(manager.execute())

        try:

            async def subscribers_added():
                while True:
                    states = await subscriber_state_store.list()
                    if len(states) == 2:
                        return states
                    else:
                        await asyncio.sleep(0)

            await asyncio.wait_for(
                subscribers_added(),
                timeout=timedelta(milliseconds=100).total_seconds(),
            )

            task.cancel()

            await asyncio.gather(task, return_exceptions=True)

            unregister_log_events = logger.find_events(
                "event.processing.broker.subscriber-manager."
                "unregistering-subscriber"
            )

            assert len(unregister_log_events) == 2

            assert unregister_log_events[0].level == LogLevel.INFO
            assert unregister_log_events[0].is_async is True

            assert unregister_log_events[1].level == LogLevel.INFO
            assert unregister_log_events[1].is_async is True

            assert [
                unregister_log_events[0].context,
                unregister_log_events[1].context,
            ] == unordered(
                [
                    {
                        "node": node_id,
                        "subscriber": {
                            "group": subscriber_1_group,
                            "id": subscriber_1_id,
                        },
                    },
                    {
                        "node": node_id,
                        "subscriber": {
                            "group": subscriber_2_group,
                            "id": subscriber_2_id,
                        },
                    },
                ]
            )
        finally:
            if not task.cancelled():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    async def test_execute_logs_on_purge(self):
        logger = CapturingLogger.create()

        node_id = data.random_node_id()
        subscriber_store = InMemoryEventSubscriberStore()
        subscriber_state_store = CountingEventSubscriberStateStore(node_id)
        subscription_source_mapping_store = (
            InMemoryEventSubscriptionSourceMappingStore()
        )

        manager = EventSubscriberManager(
            node_id=node_id,
            subscriber_store=subscriber_store,
            subscriber_state_store=subscriber_state_store,
            subscription_source_mapping_store=subscription_source_mapping_store,
            logger=logger,
            purge_interval=timedelta(milliseconds=20),
            subscriber_max_age=timedelta(minutes=3),
        )

        task = asyncio.create_task(manager.execute())

        await asyncio.sleep(timedelta(milliseconds=50).total_seconds())

        task.cancel()

        await asyncio.gather(task, return_exceptions=True)

        purge_log_events = logger.find_events(
            "event.processing.broker.subscriber-manager.purging-subscribers"
        )

        assert len(purge_log_events) > 1
        assert purge_log_events[0].level == LogLevel.DEBUG
        assert purge_log_events[0].is_async is True
        assert purge_log_events[0].context == {
            "node": node_id,
            "subscriber_max_age_seconds": 180.0,
        }

    async def test_execute_logs_on_heartbeat(self):
        logger = CapturingLogger.create()

        node_id = data.random_node_id()
        subscriber_store = InMemoryEventSubscriberStore()
        subscriber_state_store = CountingEventSubscriberStateStore(node_id)
        subscription_source_mapping_store = (
            InMemoryEventSubscriptionSourceMappingStore()
        )

        manager = EventSubscriberManager(
            node_id=node_id,
            subscriber_store=subscriber_store,
            subscriber_state_store=subscriber_state_store,
            subscription_source_mapping_store=subscription_source_mapping_store,
            logger=logger,
            heartbeat_interval=timedelta(milliseconds=20),
        )

        subscriber_1 = CapturingEventSubscriber(
            group=data.random_subscriber_group(),
            id=data.random_subscriber_id(),
            sequences=[CategoryIdentifier(data.random_event_category_name())],
            health=EventSubscriberHealth.HEALTHY,
        )
        subscriber_2 = CapturingEventSubscriber(
            group=data.random_subscriber_group(),
            id=data.random_subscriber_id(),
            sequences=[CategoryIdentifier(data.random_event_category_name())],
            health=EventSubscriberHealth.HEALTHY,
        )

        await manager.add(subscriber_1)
        await manager.add(subscriber_2)

        task = asyncio.create_task(manager.execute())

        await asyncio.sleep(timedelta(milliseconds=50).total_seconds())

        task.cancel()

        await asyncio.gather(task, return_exceptions=True)

        heartbeat_log_events = logger.find_events(
            "event.processing.broker.subscriber-manager.sending-heartbeats"
        )

        assert len(heartbeat_log_events) > 1
        assert heartbeat_log_events[0].level == LogLevel.DEBUG
        assert heartbeat_log_events[0].is_async is True
        assert heartbeat_log_events[0].context == {
            "node": node_id,
            "subscribers": unordered(
                [
                    subscriber_1.key.dict(),
                    subscriber_2.key.dict(),
                ]
            ),
        }

    async def test_execute_logs_healthy_subscribers_on_heartbeat(self):
        logger = CapturingLogger.create()

        node_id = data.random_node_id()
        subscriber_store = InMemoryEventSubscriberStore()
        subscriber_state_store = CountingEventSubscriberStateStore(node_id)
        subscription_source_mapping_store = (
            InMemoryEventSubscriptionSourceMappingStore()
        )

        manager = EventSubscriberManager(
            node_id=node_id,
            subscriber_store=subscriber_store,
            subscriber_state_store=subscriber_state_store,
            subscription_source_mapping_store=subscription_source_mapping_store,
            logger=logger,
            heartbeat_interval=timedelta(milliseconds=20),
        )

        subscriber_1 = CapturingEventSubscriber(
            group=data.random_subscriber_group(),
            id=data.random_subscriber_id(),
            sequences=[CategoryIdentifier(data.random_event_category_name())],
            health=EventSubscriberHealth.HEALTHY,
        )
        subscriber_2 = CapturingEventSubscriber(
            group=data.random_subscriber_group(),
            id=data.random_subscriber_id(),
            sequences=[CategoryIdentifier(data.random_event_category_name())],
            health=EventSubscriberHealth.HEALTHY,
        )

        await manager.add(subscriber_1)
        await manager.add(subscriber_2)

        task = asyncio.create_task(manager.execute())

        await asyncio.sleep(timedelta(milliseconds=50).total_seconds())

        task.cancel()

        await asyncio.gather(task, return_exceptions=True)

        subscriber_1_log_events = logger.find_events(
            "event.processing.broker.subscriber-manager.subscriber-healthy",
            lambda log_event: (
                log_event.context["subscriber"]["id"] == subscriber_1.id
            ),
        )
        subscriber_2_log_events = logger.find_events(
            "event.processing.broker.subscriber-manager.subscriber-healthy",
            lambda log_event: (
                log_event.context["subscriber"]["id"] == subscriber_2.id
            ),
        )

        assert len(subscriber_1_log_events) > 1
        assert subscriber_1_log_events[0].level == LogLevel.DEBUG
        assert subscriber_1_log_events[0].is_async is True
        assert subscriber_1_log_events[0].context == {
            "node": node_id,
            "subscriber": {"group": subscriber_1.group, "id": subscriber_1.id},
        }

        assert len(subscriber_2_log_events) > 1
        assert subscriber_2_log_events[0].level == LogLevel.DEBUG
        assert subscriber_2_log_events[0].is_async is True
        assert subscriber_2_log_events[0].context == {
            "node": node_id,
            "subscriber": {"group": subscriber_2.group, "id": subscriber_2.id},
        }

    async def test_execute_logs_unhealthy_subscribers_on_heartbeat(self):
        logger = CapturingLogger.create()

        node_id = data.random_node_id()
        subscriber_store = InMemoryEventSubscriberStore()
        subscriber_state_store = CountingEventSubscriberStateStore(node_id)
        subscription_source_mapping_store = (
            InMemoryEventSubscriptionSourceMappingStore()
        )

        manager = EventSubscriberManager(
            node_id=node_id,
            subscriber_store=subscriber_store,
            subscriber_state_store=subscriber_state_store,
            subscription_source_mapping_store=subscription_source_mapping_store,
            logger=logger,
            heartbeat_interval=timedelta(milliseconds=20),
        )

        subscriber_1 = CapturingEventSubscriber(
            group=data.random_subscriber_group(),
            id=data.random_subscriber_id(),
            sequences=[CategoryIdentifier(data.random_event_category_name())],
            health=EventSubscriberHealth.UNHEALTHY,
        )
        subscriber_2 = CapturingEventSubscriber(
            group=data.random_subscriber_group(),
            id=data.random_subscriber_id(),
            sequences=[CategoryIdentifier(data.random_event_category_name())],
            health=EventSubscriberHealth.UNHEALTHY,
        )

        await manager.add(subscriber_1)
        await manager.add(subscriber_2)

        task = asyncio.create_task(manager.execute())

        await asyncio.sleep(timedelta(milliseconds=50).total_seconds())

        task.cancel()

        await asyncio.gather(task, return_exceptions=True)

        subscriber_1_log_events = logger.find_events(
            "event.processing.broker.subscriber-manager.subscriber-unhealthy",
            lambda log_event: (
                log_event.context["subscriber"]["id"] == subscriber_1.id
            ),
        )
        subscriber_2_log_events = logger.find_events(
            "event.processing.broker.subscriber-manager.subscriber-unhealthy",
            lambda log_event: (
                log_event.context["subscriber"]["id"] == subscriber_2.id
            ),
        )

        assert len(subscriber_1_log_events) > 1
        assert subscriber_1_log_events[0].level == LogLevel.ERROR
        assert subscriber_1_log_events[0].is_async is True
        assert subscriber_1_log_events[0].context == {
            "node": node_id,
            "subscriber": {"group": subscriber_1.group, "id": subscriber_1.id},
        }

        assert len(subscriber_2_log_events) > 1
        assert subscriber_2_log_events[0].level == LogLevel.ERROR
        assert subscriber_2_log_events[0].is_async is True
        assert subscriber_2_log_events[0].context == {
            "node": node_id,
            "subscriber": {"group": subscriber_2.group, "id": subscriber_2.id},
        }
