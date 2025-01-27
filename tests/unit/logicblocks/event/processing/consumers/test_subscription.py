from logicblocks.event.processing.consumers import EventSubscriptionConsumer
from logicblocks.event.processing.consumers.types import (
    EventSubscriber,
    EventBroker,
    EventConsumer
)
from logicblocks.event.store import EventStore, EventSource
from logicblocks.event.store.adapters import InMemoryEventStorageAdapter
from logicblocks.event.testing import data
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
        self.consumers = []

    async def register(self, subscriber: EventSubscriber) -> None:
        self.consumers.append(subscriber)


class TestEventSubscriptionConsumer:
    async def test_registers_subscription_with_broker_on_subscribe(self):
        broker = CapturingEventBroker()
        delegate_factory = CapturingEventConsumerFactory()

        category_name = data.random_event_category_name()
        sequence = CategoryIdentifier(category=category_name)

        consumer = EventSubscriptionConsumer(
            sequence=sequence,
            delegate_factory=delegate_factory.factory,
        )

        await consumer.subscribe(broker=broker)

        assert consumer in broker.consumers

    async def test_consumes_from_received_source_on_consume_all(self):
        delegate_factory = CapturingEventConsumerFactory()
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())

        category_name = data.random_event_category_name()
        sequence = CategoryIdentifier(category=category_name)

        consumer = EventSubscriptionConsumer(
            sequence=sequence,
            delegate_factory=delegate_factory.factory,
        )

        category_source = event_store.category(category=category_name)

        await consumer.receive(category_source)
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
            sequence=sequence,
            delegate_factory=delegate_factory.factory,
        )

        stream_1_source = event_store.stream(
            category=category_name, stream=data.random_event_stream_name()
        )
        stream_2_source = event_store.stream(
            category=category_name, stream=data.random_event_stream_name()
        )

        await consumer.receive(stream_1_source)
        await consumer.receive(stream_2_source)

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
            sequence=sequence,
            delegate_factory=delegate_factory.factory,
        )

        stream_1_source = event_store.stream(
            category=category_name, stream=data.random_event_stream_name()
        )
        stream_2_source = event_store.stream(
            category=category_name, stream=data.random_event_stream_name()
        )

        await consumer.receive(stream_1_source)
        await consumer.receive(stream_2_source)

        await consumer.consume_all()

        await consumer.revoke(stream_1_source)

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
