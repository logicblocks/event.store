from pytest_unordered import unordered

from logicblocks.event.processing.broker import EventBroker, EventSubscriber
from logicblocks.event.processing.consumers import EventSubscriptionConsumer
from logicblocks.event.processing.consumers.types import EventConsumer
from logicblocks.event.store import EventSource, EventStore
from logicblocks.event.store.adapters import InMemoryEventStorageAdapter
from logicblocks.event.testing import data
from logicblocks.event.testlogging import CapturingLogger
from logicblocks.event.testlogging.logger import LogLevel
from logicblocks.event.types.identifier import CategoryIdentifier


class CapturingEventConsumer(EventConsumer):
    def __init__(self, source: EventSource):
        self.source = source
        self.invoke_count = 0

    async def consume_all(self) -> None:
        self.invoke_count += 1


class CapturingEventConsumerFactory:
    def __init__(self) -> None:
        self.consumers: list[CapturingEventConsumer] = []

    def factory(self, source: EventSource) -> CapturingEventConsumer:
        consumer = CapturingEventConsumer(source)
        self.consumers.append(consumer)
        return consumer


class CapturingEventBroker(EventBroker):
    def __init__(self):
        super().__init__()
        self.consumers = []

    async def register(self, subscriber: EventSubscriber) -> None:
        self.consumers.append(subscriber)

    async def execute(self) -> None:
        pass


class TestEventSubscriptionConsumer:
    async def test_exposes_required_properties(self):
        delegate_factory = CapturingEventConsumerFactory()

        category_name_1 = data.random_event_category_name()
        category_name_2 = data.random_event_category_name()
        sequence_1 = CategoryIdentifier(category=category_name_1)
        sequence_2 = CategoryIdentifier(category=category_name_2)

        subscriber_group = data.random_subscriber_group()
        subscriber_id = data.random_subscriber_id()

        sequences = [sequence_1, sequence_2]

        subscription_consumer = EventSubscriptionConsumer(
            group=subscriber_group,
            id=subscriber_id,
            sequences=sequences,
            delegate_factory=delegate_factory.factory,
        )

        assert subscription_consumer.group == subscriber_group
        assert subscription_consumer.id == subscriber_id
        assert subscription_consumer.identifiers == sequences

    async def test_consumes_from_received_source_on_consume_all(self):
        delegate_factory = CapturingEventConsumerFactory()
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())

        category_name = data.random_event_category_name()
        sequence = CategoryIdentifier(category=category_name)

        consumer = EventSubscriptionConsumer(
            group=data.random_subscriber_group(),
            id=data.random_subscriber_id(),
            sequences=[sequence],
            delegate_factory=delegate_factory.factory,
        )

        category_source = event_store.category(category=category_name)

        await consumer.accept(category_source)
        await consumer.consume_all()

        consumers = delegate_factory.consumers
        assert len(consumers) == 1

        consumer = consumers[0]
        assert consumer.source == category_source
        assert consumer.invoke_count == 1

    async def test_consumes_from_all_received_sources_on_consume_all(self):
        delegate_factory = CapturingEventConsumerFactory()
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())

        category_name = data.random_event_category_name()
        sequence = CategoryIdentifier(category=category_name)

        consumer = EventSubscriptionConsumer(
            group=data.random_subscriber_group(),
            id=data.random_subscriber_id(),
            sequences=[sequence],
            delegate_factory=delegate_factory.factory,
        )

        stream_1_source = event_store.stream(
            category=category_name, stream=data.random_event_stream_name()
        )
        stream_2_source = event_store.stream(
            category=category_name, stream=data.random_event_stream_name()
        )

        await consumer.accept(stream_1_source)
        await consumer.accept(stream_2_source)

        await consumer.consume_all()

        consumers = delegate_factory.consumers
        assert len(consumers) == 2

        stream_1_consumer = next(
            consumer
            for consumer in consumers
            if consumer.source == stream_1_source
        )
        stream_2_consumer = next(
            consumer
            for consumer in consumers
            if consumer.source == stream_2_source
        )

        assert stream_1_consumer.invoke_count == 1
        assert stream_2_consumer.invoke_count == 1

    async def test_stops_consuming_from_revoked_source_on_consume_all(self):
        delegate_factory = CapturingEventConsumerFactory()
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())

        category_name = data.random_event_category_name()
        sequence = CategoryIdentifier(category=category_name)

        consumer = EventSubscriptionConsumer(
            group=data.random_subscriber_group(),
            id=data.random_subscriber_id(),
            sequences=[sequence],
            delegate_factory=delegate_factory.factory,
        )

        stream_1_source = event_store.stream(
            category=category_name, stream=data.random_event_stream_name()
        )
        stream_2_source = event_store.stream(
            category=category_name, stream=data.random_event_stream_name()
        )

        await consumer.accept(stream_1_source)
        await consumer.accept(stream_2_source)

        await consumer.consume_all()

        await consumer.withdraw(stream_1_source)

        await consumer.consume_all()

        consumers = delegate_factory.consumers
        assert len(consumers) == 2

        stream_1_consumer = next(
            consumer
            for consumer in consumers
            if consumer.source == stream_1_source
        )
        stream_2_consumer = next(
            consumer
            for consumer in consumers
            if consumer.source == stream_2_source
        )

        assert stream_1_consumer.invoke_count == 1
        assert stream_2_consumer.invoke_count == 2

    async def test_logs_when_accepting_source(self):
        logger = CapturingLogger.create()
        delegate_factory = CapturingEventConsumerFactory()
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())

        category_name = data.random_event_category_name()
        sequence = CategoryIdentifier(category=category_name)

        subscriber_group = data.random_subscriber_group()
        subscriber_id = data.random_subscriber_id()

        consumer = EventSubscriptionConsumer(
            group=subscriber_group,
            id=subscriber_id,
            sequences=[sequence],
            delegate_factory=delegate_factory.factory,
            logger=logger,
        )

        stream_1_name = data.random_event_stream_name()
        stream_2_name = data.random_event_stream_name()

        stream_1_source = event_store.stream(
            category=category_name, stream=stream_1_name
        )
        stream_2_source = event_store.stream(
            category=category_name, stream=stream_2_name
        )

        await consumer.accept(stream_1_source)
        await consumer.accept(stream_2_source)

        log_events = logger.events
        accept_log_events = [
            log_event
            for log_event in log_events
            if log_event.event
            == "event.consumer.subscription.accepting-source"
        ]

        assert len(accept_log_events) == 2
        assert accept_log_events[0].level == LogLevel.INFO
        assert accept_log_events[0].is_async is True
        assert accept_log_events[0].context == {
            "subscriber": {"group": subscriber_group, "id": subscriber_id},
            "source": {
                "type": "stream",
                "category": category_name,
                "stream": stream_1_name,
            },
        }
        assert accept_log_events[1].level == LogLevel.INFO
        assert accept_log_events[1].is_async is True
        assert accept_log_events[1].context == {
            "subscriber": {"group": subscriber_group, "id": subscriber_id},
            "source": {
                "type": "stream",
                "category": category_name,
                "stream": stream_2_name,
            },
        }

    async def test_logs_when_withdrawing_source(self):
        logger = CapturingLogger.create()
        delegate_factory = CapturingEventConsumerFactory()
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())

        category_name = data.random_event_category_name()
        sequence = CategoryIdentifier(category=category_name)

        subscriber_group = data.random_subscriber_group()
        subscriber_id = data.random_subscriber_id()

        consumer = EventSubscriptionConsumer(
            group=subscriber_group,
            id=subscriber_id,
            sequences=[sequence],
            delegate_factory=delegate_factory.factory,
            logger=logger,
        )

        stream_1_name = data.random_event_stream_name()
        stream_2_name = data.random_event_stream_name()

        stream_1_source = event_store.stream(
            category=category_name, stream=stream_1_name
        )
        stream_2_source = event_store.stream(
            category=category_name, stream=stream_2_name
        )

        await consumer.accept(stream_1_source)
        await consumer.accept(stream_2_source)

        await consumer.withdraw(stream_1_source)
        await consumer.withdraw(stream_2_source)

        accept_log_events = logger.find_events(
            "event.consumer.subscription.withdrawing-source"
        )

        assert len(accept_log_events) == 2
        assert accept_log_events[0].level == LogLevel.INFO
        assert accept_log_events[0].is_async is True
        assert accept_log_events[0].context == {
            "subscriber": {"group": subscriber_group, "id": subscriber_id},
            "source": {
                "type": "stream",
                "category": category_name,
                "stream": stream_1_name,
            },
        }
        assert accept_log_events[1].level == LogLevel.INFO
        assert accept_log_events[1].is_async is True
        assert accept_log_events[1].context == {
            "subscriber": {"group": subscriber_group, "id": subscriber_id},
            "source": {
                "type": "stream",
                "category": category_name,
                "stream": stream_2_name,
            },
        }

    async def test_logs_when_starting_and_completing_consume(self):
        logger = CapturingLogger.create()
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        delegate_factory = CapturingEventConsumerFactory()

        subscriber_group = data.random_subscriber_group()
        subscriber_id = data.random_subscriber_id()

        consumer = EventSubscriptionConsumer(
            group=subscriber_group,
            id=subscriber_id,
            sequences=[
                CategoryIdentifier(category=data.random_event_category_name())
            ],
            delegate_factory=delegate_factory.factory,
            logger=logger,
        )

        category_name = data.random_event_category_name()

        stream_1_source = event_store.stream(
            category=category_name, stream=data.random_event_stream_name()
        )
        stream_2_source = event_store.stream(
            category=category_name, stream=data.random_event_stream_name()
        )

        await consumer.accept(stream_1_source)
        await consumer.accept(stream_2_source)

        await consumer.consume_all()

        startup_event = logger.find_event(
            "event.consumer.subscription.starting-consume"
        )

        assert startup_event is not None
        assert startup_event.level == LogLevel.DEBUG
        assert startup_event.is_async is True
        assert startup_event.context == {
            "subscriber": {"group": subscriber_group, "id": subscriber_id},
            "sources": unordered(
                [
                    stream_1_source.identifier.dict(),
                    stream_2_source.identifier.dict(),
                ]
            ),
        }

        complete_event = logger.find_event(
            "event.consumer.subscription.completed-consume"
        )

        assert complete_event is not None
        assert complete_event.level == LogLevel.DEBUG
        assert complete_event.is_async is True
        assert complete_event.context == {
            "subscriber": {"group": subscriber_group, "id": subscriber_id},
            "sources": unordered(
                [
                    stream_1_source.identifier.dict(),
                    stream_2_source.identifier.dict(),
                ]
            ),
        }

    async def test_logs_when_consuming_from_each_source(self):
        logger = CapturingLogger.create()

        delegate_factory = CapturingEventConsumerFactory()
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())

        category_name = data.random_event_category_name()
        sequence = CategoryIdentifier(category=category_name)

        subscriber_group = data.random_subscriber_group()
        subscriber_id = data.random_subscriber_id()

        consumer = EventSubscriptionConsumer(
            group=subscriber_group,
            id=subscriber_id,
            sequences=[sequence],
            delegate_factory=delegate_factory.factory,
            logger=logger,
        )

        stream_1_name = data.random_event_stream_name()
        stream_2_name = data.random_event_stream_name()

        stream_1_source = event_store.stream(
            category=category_name, stream=stream_1_name
        )
        stream_2_source = event_store.stream(
            category=category_name, stream=stream_2_name
        )

        await consumer.accept(stream_1_source)
        await consumer.accept(stream_2_source)

        await consumer.consume_all()

        consume_events = logger.find_events(
            "event.consumer.subscription.consuming-source"
        )

        assert len(consume_events) == 2
        assert consume_events[0].level == LogLevel.DEBUG
        assert consume_events[0].is_async is True

        assert consume_events[1].level == LogLevel.DEBUG
        assert consume_events[1].is_async is True

        assert [
            consume_events[0].context,
            consume_events[1].context,
        ] == unordered(
            [
                {
                    "subscriber": {
                        "group": subscriber_group,
                        "id": subscriber_id,
                    },
                    "source": {
                        "type": "stream",
                        "category": category_name,
                        "stream": stream_1_name,
                    },
                },
                {
                    "subscriber": {
                        "group": subscriber_group,
                        "id": subscriber_id,
                    },
                    "source": {
                        "type": "stream",
                        "category": category_name,
                        "stream": stream_2_name,
                    },
                },
            ]
        )
