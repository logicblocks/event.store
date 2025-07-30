from logicblocks.event.processing import (
    EventConsumerStateStore,
    EventCount,
    EventSourceConsumer,
)
from logicblocks.event.processing.consumers import (
    EventIterator,
    EventProcessorManager,
    ManagedEventIteratorProcessor,
)
from logicblocks.event.store import (
    EventStore,
    InMemoryEventStorageAdapter,
)
from logicblocks.event.testing import NewEventBuilder, data
from logicblocks.event.testlogging import CapturingLogger
from logicblocks.event.testlogging.logger import LogLevel
from logicblocks.event.types import Event


class CapturingEventProcessor[E: Event](ManagedEventIteratorProcessor[E]):
    def __init__(self):
        self.processed_events = []
        self.manager: EventProcessorManager[E]

    async def process(
        self, events: EventIterator[E], manager: EventProcessorManager[E]
    ) -> None:
        self.processed_events = self.processed_events + [
            event async for event in events
        ]
        self.manager = manager


class ThrowingEventProcessor[E: Event](ManagedEventIteratorProcessor[E]):
    def __init__(
        self, error: BaseException | type[BaseException] = RuntimeError
    ):
        self._error = error

    async def process(
        self, events: EventIterator[E], manager: EventProcessorManager[E]
    ) -> None:
        raise self._error


class TestEventSourceConsumerWithAutoCommitProcessor:
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

    async def test_consumes_from_start_on_subsequent_consumes_without_acknowledge(
        self,
    ):
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
            *publish_1_events,
            *publish_2_events,
            *publish_3_events,
        ]

    async def test_consumes_only_new_events_on_subsequent_consumes_on_acknowledge(
        self,
    ):
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
        processor.manager.acknowledge(
            list(publish_1_events) + list(publish_2_events)
        )

        publish_3_events = await stream_1.publish(
            events=[NewEventBuilder().build()]
        )

        await consumer.consume_all()

        assert processor.processed_events == [
            *publish_1_events,
            *publish_2_events,
            *publish_3_events,
        ]

    async def test_does_reprocess_already_processed_events_on_restart_without_acknowledge_and_commit(
        self,
    ):
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_category = event_store.category(
            category=data.random_event_category_name()
        )

        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()

        source = event_store.category(category=category_name)

        processor = CapturingEventProcessor()

        state_store = EventConsumerStateStore(
            category=state_category, persistence_interval=EventCount(2)
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
            *published_events,
        ]

    async def test_does_reprocess_already_processed_events_on_restart_without_acknowledge(
        self,
    ):
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_category = event_store.category(
            category=data.random_event_category_name()
        )

        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()

        source = event_store.category(category=category_name)

        processor = CapturingEventProcessor()

        state_store = EventConsumerStateStore(
            category=state_category, persistence_interval=EventCount(2)
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
        await processor.manager.commit(force=True)

        assert processor.processed_events == [
            *published_events,
            *published_events,
        ]

    async def test_does_reprocess_already_processed_events_on_restart_without_commit(
        self,
    ):
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_category = event_store.category(
            category=data.random_event_category_name()
        )

        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()

        source = event_store.category(category=category_name)

        processor = CapturingEventProcessor()

        state_store = EventConsumerStateStore(
            category=state_category, persistence_interval=EventCount(2)
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
        processor.manager.acknowledge(published_events)

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
            *published_events,
        ]

    async def test_doesnt_reprocess_already_processed_events_on_restart_on_acknowledge_and_commit(
        self,
    ):
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_category = event_store.category(
            category=data.random_event_category_name()
        )

        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()

        source = event_store.category(category=category_name)

        processor = CapturingEventProcessor()

        state_store = EventConsumerStateStore(
            category=state_category, persistence_interval=EventCount(2)
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
        processor.manager.acknowledge(published_events)
        await processor.manager.commit(force=True)

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

    async def test_logs_when_consume_all_starting(self):
        logger = CapturingLogger.create()

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
            logger=logger,
        )

        stream_1 = event_store.stream(
            category=category_name, stream=stream_1_name
        )
        stream_2 = event_store.stream(
            category=category_name, stream=stream_2_name
        )

        await stream_1.publish(events=[NewEventBuilder().build()])
        stream_2_publish_1 = await stream_2.publish(
            events=[NewEventBuilder().build()]
        )

        await consumer.consume_all()
        processor.manager.acknowledge(stream_2_publish_1)
        await processor.manager.commit(force=True)

        await stream_1.publish(events=[NewEventBuilder().build()])

        await consumer.consume_all()

        startup_log_events = logger.find_events(
            "event.consumer.source.starting-consume"
        )
        assert len(startup_log_events) == 2

        assert startup_log_events[0].level == LogLevel.DEBUG
        assert startup_log_events[0].is_async is True
        assert startup_log_events[0].context == {
            "source": {"type": "category", "category": category_name},
            "last_sequence_number": None,
        }

        assert startup_log_events[1].level == LogLevel.DEBUG
        assert startup_log_events[1].is_async is True
        assert startup_log_events[1].context == {
            "source": {"type": "category", "category": category_name},
            "last_sequence_number": stream_2_publish_1[-1].sequence_number,
        }

    async def test_logs_when_consume_all_complete(self):
        logger = CapturingLogger.create()

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
            logger=logger,
        )

        stream_1 = event_store.stream(
            category=category_name, stream=stream_1_name
        )
        stream_2 = event_store.stream(
            category=category_name, stream=stream_2_name
        )

        stream_1_publish = await stream_1.publish(
            events=[NewEventBuilder().build()]
        )
        stream_2_publish = await stream_2.publish(
            events=[NewEventBuilder().build()]
        )

        await consumer.consume_all()
        processor.manager.acknowledge(
            list(stream_1_publish) + list(stream_2_publish)
        )
        await processor.manager.commit(force=True)

        complete_log_events = logger.find_events(
            "event.consumer.source.completed-consume"
        )

        assert len(complete_log_events) == 1

        assert complete_log_events[0].level == LogLevel.DEBUG
        assert complete_log_events[0].is_async is True
        assert complete_log_events[0].context == {
            "source": {"type": "category", "category": category_name},
            "consumed_count": 2,
            "processed_count": 0,
        }

    async def test_logs_details_of_each_consumed_event(self):
        logger = CapturingLogger.create()

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
            logger=logger,
        )

        stream_1 = event_store.stream(
            category=category_name, stream=stream_1_name
        )
        stream_2 = event_store.stream(
            category=category_name, stream=stream_2_name
        )

        stored_events_1 = await stream_1.publish(
            events=[NewEventBuilder().build()]
        )
        stored_events_2 = await stream_2.publish(
            events=[NewEventBuilder().build()]
        )

        await consumer.consume_all()

        consuming_log_events = logger.find_events(
            "event.consumer.source.consuming-event"
        )

        assert len(consuming_log_events) == 2

        assert consuming_log_events[0].level == LogLevel.DEBUG
        assert consuming_log_events[0].is_async is True
        assert consuming_log_events[0].context == {
            "source": {"type": "category", "category": category_name},
            "envelope": stored_events_1[0].summarise(),
        }

        assert consuming_log_events[1].level == LogLevel.DEBUG
        assert consuming_log_events[1].is_async is True
        assert consuming_log_events[1].context == {
            "source": {"type": "category", "category": category_name},
            "envelope": stored_events_2[0].summarise(),
        }
