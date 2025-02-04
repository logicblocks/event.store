from logicblocks.event.processing.broker import EventBroker, EventSubscriber
from logicblocks.event.processing.consumers import EventSubscriptionConsumer
from logicblocks.event.processing.consumers.types import EventConsumer
from logicblocks.event.store import EventSource, EventStore
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
        super().__init__()
        self.consumers = []

    async def register(self, subscriber: EventSubscriber) -> None:
        self.consumers.append(subscriber)

    async def execute(self) -> None:
        pass


class TestEventSubscriptionConsumer:
    async def test_consumes_from_received_source_on_consume_all(self):
        delegate_factory = CapturingEventConsumerFactory()
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())

        category_name = data.random_event_category_name()
        sequence = CategoryIdentifier(category=category_name)

        consumer = EventSubscriptionConsumer(
            group=data.random_subscriber_group(),
            id=data.random_subscriber_id(),
            sequence=sequence,
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
            sequence=sequence,
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
            sequence=sequence,
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
