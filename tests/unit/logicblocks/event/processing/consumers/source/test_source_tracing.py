from logicblocks.event.processing import (
    EventConsumerStateStore,
    EventProcessor,
    EventSourceConsumer,
)
from logicblocks.event.processing.consumers import (
    StoredEventEventConsumerStateConverter,
)
from logicblocks.event.store import (
    EventStore,
    InMemoryEventStorageAdapter,
)
from logicblocks.event.store.tracing import maybe_get_tracing_metadata
from logicblocks.event.testing import NewEventBuilder, data
from logicblocks.event.types import StoredEvent


class PublishingEventProcessor(EventProcessor):
    def __init__(self, target_stream):
        self._target_stream = target_stream
        self.published_events: list[StoredEvent] = []

    async def process_event(self, event: StoredEvent):
        stored = await self._target_stream.publish(
            events=[NewEventBuilder().with_name("reaction-event").build()]
        )
        self.published_events.extend(stored)


class TestEventSourceConsumerCausationChain:
    async def test_events_written_during_processing_include_causation_event_id(
        self,
    ):
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_category = event_store.category(
            category=data.random_event_category_name()
        )
        state_store = EventConsumerStateStore(
            category=state_category,
            converter=StoredEventEventConsumerStateConverter(),
        )

        source_category = data.random_event_category_name()
        source_stream_name = data.random_event_stream_name()
        target_category = data.random_event_category_name()
        target_stream_name = data.random_event_stream_name()

        source = event_store.category(category=source_category)
        source_stream = event_store.stream(
            category=source_category, stream=source_stream_name
        )
        target_stream = event_store.stream(
            category=target_category, stream=target_stream_name
        )

        processor = PublishingEventProcessor(target_stream=target_stream)

        consumer = EventSourceConsumer(
            source=source,
            processor=processor,
            state_store=state_store,
        )

        published_source_events = await source_stream.publish(
            events=[NewEventBuilder().build()]
        )
        source_event = published_source_events[0]

        await consumer.consume_all()

        reaction_event = processor.published_events[0]
        causation_event_id = reaction_event.metadata["tracing"][
            "causation_event_id"
        ]
        assert causation_event_id == source_event.id

    async def test_events_written_during_processing_have_trace_id(self):
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_category = event_store.category(
            category=data.random_event_category_name()
        )
        state_store = EventConsumerStateStore(
            category=state_category,
            converter=StoredEventEventConsumerStateConverter(),
        )

        source_category = data.random_event_category_name()
        source_stream_name = data.random_event_stream_name()
        target_category = data.random_event_category_name()
        target_stream_name = data.random_event_stream_name()

        source = event_store.category(category=source_category)
        source_stream = event_store.stream(
            category=source_category, stream=source_stream_name
        )
        target_stream = event_store.stream(
            category=target_category, stream=target_stream_name
        )

        processor = PublishingEventProcessor(target_stream=target_stream)

        consumer = EventSourceConsumer(
            source=source,
            processor=processor,
            state_store=state_store,
        )

        await source_stream.publish(events=[NewEventBuilder().build()])
        await consumer.consume_all()

        reaction_event = processor.published_events[0]
        trace_id = reaction_event.metadata["tracing"]["trace_id"]
        assert isinstance(trace_id, str)
        assert len(trace_id) > 0

    async def test_each_source_event_produces_independent_trace_ids(self):
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_category = event_store.category(
            category=data.random_event_category_name()
        )
        state_store = EventConsumerStateStore(
            category=state_category,
            converter=StoredEventEventConsumerStateConverter(),
        )

        source_category = data.random_event_category_name()
        source_stream_name = data.random_event_stream_name()
        target_category = data.random_event_category_name()
        target_stream_name = data.random_event_stream_name()

        source = event_store.category(category=source_category)
        source_stream = event_store.stream(
            category=source_category, stream=source_stream_name
        )
        target_stream = event_store.stream(
            category=target_category, stream=target_stream_name
        )

        processor = PublishingEventProcessor(target_stream=target_stream)

        consumer = EventSourceConsumer(
            source=source,
            processor=processor,
            state_store=state_store,
        )

        await source_stream.publish(
            events=[
                NewEventBuilder().build(),
                NewEventBuilder().build(),
            ]
        )
        await consumer.consume_all()

        trace_id_1 = processor.published_events[0].metadata["tracing"][
            "trace_id"
        ]
        trace_id_2 = processor.published_events[1].metadata["tracing"][
            "trace_id"
        ]
        assert trace_id_1 != trace_id_2

    async def test_causation_id_matches_each_respective_source_event(self):
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_category = event_store.category(
            category=data.random_event_category_name()
        )
        state_store = EventConsumerStateStore(
            category=state_category,
            converter=StoredEventEventConsumerStateConverter(),
        )

        source_category = data.random_event_category_name()
        source_stream_name = data.random_event_stream_name()
        target_category = data.random_event_category_name()
        target_stream_name = data.random_event_stream_name()

        source = event_store.category(category=source_category)
        source_stream = event_store.stream(
            category=source_category, stream=source_stream_name
        )
        target_stream = event_store.stream(
            category=target_category, stream=target_stream_name
        )

        processor = PublishingEventProcessor(target_stream=target_stream)

        consumer = EventSourceConsumer(
            source=source,
            processor=processor,
            state_store=state_store,
        )

        source_events = await source_stream.publish(
            events=[
                NewEventBuilder().build(),
                NewEventBuilder().build(),
            ]
        )
        await consumer.consume_all()

        assert (
            processor.published_events[0].metadata["tracing"][
                "causation_event_id"
            ]
            == source_events[0].id
        )
        assert (
            processor.published_events[1].metadata["tracing"][
                "causation_event_id"
            ]
            == source_events[1].id
        )

    async def test_tracing_context_is_not_active_after_consumption(self):
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_category = event_store.category(
            category=data.random_event_category_name()
        )
        state_store = EventConsumerStateStore(
            category=state_category,
            converter=StoredEventEventConsumerStateConverter(),
        )

        source_category = data.random_event_category_name()
        source_stream_name = data.random_event_stream_name()
        target_category = data.random_event_category_name()
        target_stream_name = data.random_event_stream_name()

        source = event_store.category(category=source_category)
        source_stream = event_store.stream(
            category=source_category, stream=source_stream_name
        )
        target_stream = event_store.stream(
            category=target_category, stream=target_stream_name
        )

        processor = PublishingEventProcessor(target_stream=target_stream)

        consumer = EventSourceConsumer(
            source=source,
            processor=processor,
            state_store=state_store,
        )

        await source_stream.publish(events=[NewEventBuilder().build()])
        await consumer.consume_all()

        assert maybe_get_tracing_metadata() is None
