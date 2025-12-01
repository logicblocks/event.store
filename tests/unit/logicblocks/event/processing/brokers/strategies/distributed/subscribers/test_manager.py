import asyncio
from collections import defaultdict
from datetime import timedelta

from logicblocks.event.testlogging.logger import CapturingLogger, LogLevel
from logicblocks.event.testsupport import CapturingEventSubscriber
from pytest_unordered import unordered

from logicblocks.event.processing import EventSubscriber
from logicblocks.event.processing.broker.strategies.distributed import (
    DefaultEventSubscriberManager,
    InMemoryEventSubscriberStateStore,
)
from logicblocks.event.processing.broker.subscribers import (
    InMemoryEventSubscriberStore,
)
from logicblocks.event.processing.broker.types import (
    EventSubscriberHealth,
    EventSubscriberKey,
)
from logicblocks.event.testing import data
from logicblocks.event.types import CategoryIdentifier


class CountingEventSubscriberStateStore(InMemoryEventSubscriberStateStore):
    counts: dict[str, int]

    def __init__(self, node_id: str):
        super().__init__(node_id)
        self.counts = defaultdict(lambda: 0)

    async def heartbeat(self, subscriber: EventSubscriber) -> None:
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

        manager = DefaultEventSubscriberManager(
            node_id=node_id,
            subscriber_store=subscriber_store,
            subscriber_state_store=subscriber_state_store,
        )

        subscriber_1 = CapturingEventSubscriber(
            group=data.random_subscriber_group(),
            id=data.random_subscriber_id(),
            subscription_requests=[
                CategoryIdentifier(data.random_event_category_name())
            ],
            health=EventSubscriberHealth.HEALTHY,
        )
        subscriber_2 = CapturingEventSubscriber(
            group=data.random_subscriber_group(),
            id=data.random_subscriber_id(),
            subscription_requests=[
                CategoryIdentifier(data.random_event_category_name())
            ],
            health=EventSubscriberHealth.HEALTHY,
        )

        await manager.add(subscriber_1)
        await manager.add(subscriber_2)

        subscribers = await subscriber_store.list()

        assert subscriber_1 in subscribers
        assert subscriber_2 in subscribers

    async def test_start_logs_starting(self):
        logger = CapturingLogger.create()

        node_id = data.random_node_id()
        subscriber_store = InMemoryEventSubscriberStore()
        subscriber_state_store = CountingEventSubscriberStateStore(node_id)

        manager = DefaultEventSubscriberManager(
            node_id=node_id,
            subscriber_store=subscriber_store,
            subscriber_state_store=subscriber_state_store,
            logger=logger,
            heartbeat_interval=timedelta(seconds=5),
            purge_interval=timedelta(seconds=30),
            subscriber_max_age=timedelta(minutes=3),
        )

        subscriber = CapturingEventSubscriber(
            group=data.random_subscriber_group(),
            id=data.random_subscriber_id(),
            subscription_requests=[
                CategoryIdentifier(data.random_event_category_name())
            ],
        )

        await manager.add(subscriber)

        await manager.start()

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

    async def test_start_registers_subscriber_states(self):
        node_id = data.random_node_id()
        subscriber_store = InMemoryEventSubscriberStore()
        subscriber_state_store = InMemoryEventSubscriberStateStore(node_id)

        manager = DefaultEventSubscriberManager(
            node_id=node_id,
            subscriber_store=subscriber_store,
            subscriber_state_store=subscriber_state_store,
        )

        subscriber_1 = CapturingEventSubscriber(
            group=data.random_subscriber_group(),
            id=data.random_subscriber_id(),
            subscription_requests=[
                CategoryIdentifier(data.random_event_category_name())
            ],
            health=EventSubscriberHealth.HEALTHY,
        )
        subscriber_2 = CapturingEventSubscriber(
            group=data.random_subscriber_group(),
            id=data.random_subscriber_id(),
            subscription_requests=[
                CategoryIdentifier(data.random_event_category_name())
            ],
            health=EventSubscriberHealth.HEALTHY,
        )

        await manager.add(subscriber_1)
        await manager.add(subscriber_2)

        await manager.start()

        states = await subscriber_state_store.list()
        assert set(state.key for state in states) == {
            EventSubscriberKey(subscriber_1.group, subscriber_1.id),
            EventSubscriberKey(subscriber_2.group, subscriber_2.id),
        }

    async def test_start_logs_registering_subscribers(self):
        logger = CapturingLogger.create()

        node_id = data.random_node_id()
        subscriber_store = InMemoryEventSubscriberStore()
        subscriber_state_store = InMemoryEventSubscriberStateStore(node_id)

        manager = DefaultEventSubscriberManager(
            node_id=node_id,
            subscriber_store=subscriber_store,
            subscriber_state_store=subscriber_state_store,
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
            subscription_requests=subscriber_1_sequences,
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
            subscription_requests=subscriber_2_sequences,
            health=EventSubscriberHealth.HEALTHY,
        )

        await manager.add(subscriber_1)
        await manager.add(subscriber_2)

        await manager.start()

        register_log_events = logger.find_events(
            "event.processing.broker.subscriber-manager.registering-subscriber"
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
                        "subscription_requests": [
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
                        "subscription_requests": [
                            {
                                "type": "category",
                                "category": subscriber_2_category_name,
                            }
                        ],
                    },
                },
            ]
        )

    async def test_stop_logs_shutdown(self):
        logger = CapturingLogger.create()

        node_id = data.random_node_id()
        subscriber_store = InMemoryEventSubscriberStore()
        subscriber_state_store = CountingEventSubscriberStateStore(node_id)

        manager = DefaultEventSubscriberManager(
            node_id=node_id,
            subscriber_store=subscriber_store,
            subscriber_state_store=subscriber_state_store,
            logger=logger,
            heartbeat_interval=timedelta(seconds=5),
            purge_interval=timedelta(seconds=30),
            subscriber_max_age=timedelta(minutes=3),
        )

        subscriber = CapturingEventSubscriber(
            group=data.random_subscriber_group(),
            id=data.random_subscriber_id(),
            subscription_requests=[
                CategoryIdentifier(data.random_event_category_name())
            ],
        )

        await manager.add(subscriber)

        await subscriber_state_store.add(subscriber)

        await manager.stop()

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

    async def test_stop_removes_subscriber_states(
        self,
    ):
        node_id = data.random_node_id()
        subscriber_store = InMemoryEventSubscriberStore()
        subscriber_state_store = InMemoryEventSubscriberStateStore(node_id)

        manager = DefaultEventSubscriberManager(
            node_id=node_id,
            subscriber_store=subscriber_store,
            subscriber_state_store=subscriber_state_store,
        )

        subscriber_1 = CapturingEventSubscriber(
            group=data.random_subscriber_group(),
            id=data.random_subscriber_id(),
            subscription_requests=[
                CategoryIdentifier(data.random_event_category_name())
            ],
            health=EventSubscriberHealth.HEALTHY,
        )
        subscriber_2 = CapturingEventSubscriber(
            group=data.random_subscriber_group(),
            id=data.random_subscriber_id(),
            subscription_requests=[
                CategoryIdentifier(data.random_event_category_name())
            ],
            health=EventSubscriberHealth.HEALTHY,
        )

        await manager.add(subscriber_1)
        await manager.add(subscriber_2)

        await subscriber_state_store.add(subscriber_1)
        await subscriber_state_store.add(subscriber_2)

        await manager.stop()

        states = await subscriber_state_store.list()

        assert len(states) == 0

    async def test_stop_logs_unregistering_subscribers(
        self,
    ):
        logger = CapturingLogger.create()

        node_id = data.random_node_id()
        subscriber_store = InMemoryEventSubscriberStore()
        subscriber_state_store = InMemoryEventSubscriberStateStore(node_id)

        manager = DefaultEventSubscriberManager(
            node_id=node_id,
            subscriber_store=subscriber_store,
            subscriber_state_store=subscriber_state_store,
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
            subscription_requests=subscriber_1_sequences,
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
            subscription_requests=subscriber_2_sequences,
            health=EventSubscriberHealth.HEALTHY,
        )

        await manager.add(subscriber_1)
        await manager.add(subscriber_2)

        await subscriber_state_store.add(subscriber_1)
        await subscriber_state_store.add(subscriber_2)

        await manager.stop()

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

    async def test_maintain_heartbeats_healthy_subscribers_every_interval(
        self,
    ):
        node_id = data.random_node_id()
        subscriber_store = InMemoryEventSubscriberStore()
        subscriber_state_store = CountingEventSubscriberStateStore(node_id)

        manager = DefaultEventSubscriberManager(
            node_id=node_id,
            subscriber_store=subscriber_store,
            subscriber_state_store=subscriber_state_store,
            heartbeat_interval=timedelta(milliseconds=20),
        )

        subscriber_1 = CapturingEventSubscriber(
            group=data.random_subscriber_group(),
            id=data.random_subscriber_id(),
            subscription_requests=[
                CategoryIdentifier(data.random_event_category_name())
            ],
            health=EventSubscriberHealth.HEALTHY,
        )
        subscriber_2 = CapturingEventSubscriber(
            group=data.random_subscriber_group(),
            id=data.random_subscriber_id(),
            subscription_requests=[
                CategoryIdentifier(data.random_event_category_name())
            ],
            health=EventSubscriberHealth.HEALTHY,
        )

        await manager.add(subscriber_1)
        await manager.add(subscriber_2)

        await subscriber_state_store.add(subscriber_1)
        await subscriber_state_store.add(subscriber_2)

        task = asyncio.create_task(manager.maintain())

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

    async def test_maintain_does_not_heartbeat_unhealthy_subscribers_each_interval(
        self,
    ):
        node_id = data.random_node_id()
        subscriber_store = InMemoryEventSubscriberStore()
        subscriber_state_store = CountingEventSubscriberStateStore(node_id)

        manager = DefaultEventSubscriberManager(
            node_id=node_id,
            subscriber_store=subscriber_store,
            subscriber_state_store=subscriber_state_store,
            heartbeat_interval=timedelta(milliseconds=20),
        )

        subscriber_1 = CapturingEventSubscriber(
            group=data.random_subscriber_group(),
            id=data.random_subscriber_id(),
            subscription_requests=[
                CategoryIdentifier(data.random_event_category_name())
            ],
            health=EventSubscriberHealth.UNHEALTHY,
        )
        subscriber_2 = CapturingEventSubscriber(
            group=data.random_subscriber_group(),
            id=data.random_subscriber_id(),
            subscription_requests=[
                CategoryIdentifier(data.random_event_category_name())
            ],
            health=EventSubscriberHealth.UNHEALTHY,
        )

        await manager.add(subscriber_1)
        await manager.add(subscriber_2)

        await subscriber_state_store.add(subscriber_1)
        await subscriber_state_store.add(subscriber_2)

        task = asyncio.create_task(manager.maintain())

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

    async def test_maintain_purges_state_store_every_purge_interval(self):
        node_id = data.random_node_id()
        subscriber_store = InMemoryEventSubscriberStore()
        subscriber_state_store = CountingEventSubscriberStateStore(node_id)

        manager = DefaultEventSubscriberManager(
            node_id=node_id,
            subscriber_store=subscriber_store,
            subscriber_state_store=subscriber_state_store,
            purge_interval=timedelta(milliseconds=20),
            subscriber_max_age=timedelta(minutes=3),
        )

        task = asyncio.create_task(manager.maintain())

        await asyncio.sleep(timedelta(milliseconds=50).total_seconds())

        task.cancel()

        await asyncio.gather(task, return_exceptions=True)

        purge_count = subscriber_state_store.counts["purge:180"]

        assert 2 <= purge_count <= 3

    async def test_maintain_logs_on_purge(self):
        logger = CapturingLogger.create()

        node_id = data.random_node_id()
        subscriber_store = InMemoryEventSubscriberStore()
        subscriber_state_store = CountingEventSubscriberStateStore(node_id)

        manager = DefaultEventSubscriberManager(
            node_id=node_id,
            subscriber_store=subscriber_store,
            subscriber_state_store=subscriber_state_store,
            logger=logger,
            purge_interval=timedelta(milliseconds=20),
            subscriber_max_age=timedelta(minutes=3),
        )

        task = asyncio.create_task(manager.maintain())

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

    async def test_maintain_logs_on_heartbeat(self):
        logger = CapturingLogger.create()

        node_id = data.random_node_id()
        subscriber_store = InMemoryEventSubscriberStore()
        subscriber_state_store = CountingEventSubscriberStateStore(node_id)

        manager = DefaultEventSubscriberManager(
            node_id=node_id,
            subscriber_store=subscriber_store,
            subscriber_state_store=subscriber_state_store,
            logger=logger,
            heartbeat_interval=timedelta(milliseconds=20),
        )

        subscriber_1 = CapturingEventSubscriber(
            group=data.random_subscriber_group(),
            id=data.random_subscriber_id(),
            subscription_requests=[
                CategoryIdentifier(data.random_event_category_name())
            ],
            health=EventSubscriberHealth.HEALTHY,
        )
        subscriber_2 = CapturingEventSubscriber(
            group=data.random_subscriber_group(),
            id=data.random_subscriber_id(),
            subscription_requests=[
                CategoryIdentifier(data.random_event_category_name())
            ],
            health=EventSubscriberHealth.HEALTHY,
        )

        await manager.add(subscriber_1)
        await manager.add(subscriber_2)

        await subscriber_state_store.add(subscriber_1)
        await subscriber_state_store.add(subscriber_2)

        task = asyncio.create_task(manager.maintain())

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

    async def test_maintain_logs_healthy_subscribers_on_heartbeat(self):
        logger = CapturingLogger.create()

        node_id = data.random_node_id()
        subscriber_store = InMemoryEventSubscriberStore()
        subscriber_state_store = CountingEventSubscriberStateStore(node_id)

        manager = DefaultEventSubscriberManager(
            node_id=node_id,
            subscriber_store=subscriber_store,
            subscriber_state_store=subscriber_state_store,
            logger=logger,
            heartbeat_interval=timedelta(milliseconds=20),
        )

        subscriber_1 = CapturingEventSubscriber(
            group=data.random_subscriber_group(),
            id=data.random_subscriber_id(),
            subscription_requests=[
                CategoryIdentifier(data.random_event_category_name())
            ],
            health=EventSubscriberHealth.HEALTHY,
        )
        subscriber_2 = CapturingEventSubscriber(
            group=data.random_subscriber_group(),
            id=data.random_subscriber_id(),
            subscription_requests=[
                CategoryIdentifier(data.random_event_category_name())
            ],
            health=EventSubscriberHealth.HEALTHY,
        )

        await manager.add(subscriber_1)
        await manager.add(subscriber_2)

        await subscriber_state_store.add(subscriber_1)
        await subscriber_state_store.add(subscriber_2)

        task = asyncio.create_task(manager.maintain())

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

    async def test_maintain_logs_unhealthy_subscribers_on_heartbeat(self):
        logger = CapturingLogger.create()

        node_id = data.random_node_id()
        subscriber_store = InMemoryEventSubscriberStore()
        subscriber_state_store = CountingEventSubscriberStateStore(node_id)

        manager = DefaultEventSubscriberManager(
            node_id=node_id,
            subscriber_store=subscriber_store,
            subscriber_state_store=subscriber_state_store,
            logger=logger,
            heartbeat_interval=timedelta(milliseconds=20),
        )

        subscriber_1 = CapturingEventSubscriber(
            group=data.random_subscriber_group(),
            id=data.random_subscriber_id(),
            subscription_requests=[
                CategoryIdentifier(data.random_event_category_name())
            ],
            health=EventSubscriberHealth.UNHEALTHY,
        )
        subscriber_2 = CapturingEventSubscriber(
            group=data.random_subscriber_group(),
            id=data.random_subscriber_id(),
            subscription_requests=[
                CategoryIdentifier(data.random_event_category_name())
            ],
            health=EventSubscriberHealth.UNHEALTHY,
        )

        await manager.add(subscriber_1)
        await manager.add(subscriber_2)

        await subscriber_state_store.add(subscriber_1)
        await subscriber_state_store.add(subscriber_2)

        task = asyncio.create_task(manager.maintain())

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
