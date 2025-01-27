from logicblocks.event.processing.consumers import (
    EventConsumerStateStore,
    EventCount,
    EventProcessor,
    EventSourceConsumer,
)
from logicblocks.event.store.adapters.in_memory import (
    InMemoryEventStorageAdapter,
)
from logicblocks.event.store.store import EventStore
from logicblocks.event.testing import NewEventBuilder, data
from logicblocks.event.types import StoredEvent


class CapturingEventProcessor(EventProcessor):
    def __init__(self):
        self.processed_events = []

    async def process_event(self, event: StoredEvent):
        self.processed_events.append(event)


class TestEventSourceConsumer:
    async def test_consumes_all_events_on_first_consume(self):
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_category = event_store.category(
            category=data.random_event_category_name()
        )
        state_store = EventConsumerStateStore(category=state_category)

        category_name = data.random_event_category_name()
        stream_1_name = data.random_event_stream_name()
        stream_2_name = data.random_event_stream_name()

        source = event_store.category(category=category_name)

        processor = CapturingEventProcessor()

        consumer = EventSourceConsumer(
            source=source,
            processor=processor,
            state_store=state_store,
        )

        stream_1 = event_store.stream(
            category=category_name, stream=stream_1_name
        )
        stream_2 = event_store.stream(
            category=category_name, stream=stream_2_name
        )

        publish_1_events = await stream_1.publish(
            events=[NewEventBuilder().build()]
        )
        publish_2_events = await stream_2.publish(
            events=[NewEventBuilder().build()]
        )

        await consumer.consume_all()

        assert processor.processed_events == [
            *publish_1_events,
            *publish_2_events,
        ]

    async def test_consumes_only_new_events_on_subsequent_consumes(self):
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_category = event_store.category(
            category=data.random_event_category_name()
        )
        state_store = EventConsumerStateStore(category=state_category)

        category_name = data.random_event_category_name()
        stream_1_name = data.random_event_stream_name()
        stream_2_name = data.random_event_stream_name()

        source = event_store.category(category=category_name)

        processor = CapturingEventProcessor()

        consumer = EventSourceConsumer(
            source=source,
            processor=processor,
            state_store=state_store,
        )

        stream_1 = event_store.stream(
            category=category_name, stream=stream_1_name
        )
        stream_2 = event_store.stream(
            category=category_name, stream=stream_2_name
        )

        publish_1_events = await stream_1.publish(
            events=[NewEventBuilder().build()]
        )
        publish_2_events = await stream_2.publish(
            events=[NewEventBuilder().build()]
        )

        await consumer.consume_all()

        publish_3_events = await stream_1.publish(
            events=[NewEventBuilder().build()]
        )

        await consumer.consume_all()

        assert processor.processed_events == [
            *publish_1_events,
            *publish_2_events,
            *publish_3_events,
        ]

    async def test_doesnt_reprocess_already_processed_events_on_restart(self):
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_category = event_store.category(
            category=data.random_event_category_name()
        )

        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()

        source = event_store.category(category=category_name)

        processor = CapturingEventProcessor()

        state_store = EventConsumerStateStore(
            category=state_category, persistence_interval=EventCount(5)
        )

        consumer = EventSourceConsumer(
            source=source,
            processor=processor,
            state_store=state_store,
        )

        published_events = await event_store.stream(
            category=category_name, stream=stream_name
        ).publish(
            events=[
                NewEventBuilder().build(),
                NewEventBuilder().build(),
            ]
        )

        await consumer.consume_all()

        state_store = EventConsumerStateStore(
            category=state_category, persistence_interval=EventCount(5)
        )
        consumer = EventSourceConsumer(
            source=source,
            processor=processor,
            state_store=state_store,
        )

        await consumer.consume_all()

        assert processor.processed_events == [
            *published_events,
        ]
