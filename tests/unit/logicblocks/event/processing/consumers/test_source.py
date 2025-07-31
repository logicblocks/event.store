from logicblocks.event.processing import (
    EventConsumerStateStore,
    EventCount,
    EventProcessor,
    EventSourceConsumer,
)
from logicblocks.event.sources.constraints import SequenceNumberAfterConstraint
from logicblocks.event.store import (
    EventStore,
    InMemoryEventStorageAdapter,
)
from logicblocks.event.store.state import (
    StoredEventEventConsumerStateConverter,
)
from logicblocks.event.testing import NewEventBuilder, data
from logicblocks.event.testlogging import CapturingLogger
from logicblocks.event.testlogging.logger import LogLevel
from logicblocks.event.types import StoredEvent


class CapturingEventProcessor(EventProcessor):
    def __init__(self):
        self.processed_events = []

    async def process_event(self, event: StoredEvent):
        self.processed_events.append(event)


class ThrowingEventProcessor(EventProcessor):
    def __init__(
        self, error: BaseException | type[BaseException] = RuntimeError
    ):
        self._error = error

    async def process_event(self, event: StoredEvent):
        raise self._error


class TestEventSourceConsumer:
    async def test_consumes_all_events_on_first_consume(self):
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_category = event_store.category(
            category=data.random_event_category_name()
        )
        state_store = EventConsumerStateStore(
            category=state_category,
            converter=StoredEventEventConsumerStateConverter(),
        )

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
        state_store = EventConsumerStateStore(
            category=state_category,
            converter=StoredEventEventConsumerStateConverter(),
        )

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

    async def test_doesnt_reprocess_already_processed_events_on_restart_when_save_state_after_consumption_true(
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
            category=state_category,
            converter=StoredEventEventConsumerStateConverter(),
            persistence_interval=EventCount(5),
        )

        consumer = EventSourceConsumer(
            source=source,
            processor=processor,
            state_store=state_store,
            save_state_after_consumption=True,
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
            category=state_category,
            converter=StoredEventEventConsumerStateConverter(),
            persistence_interval=EventCount(5),
        )
        consumer = EventSourceConsumer(
            source=source,
            processor=processor,
            state_store=state_store,
            save_state_after_consumption=True,
        )

        await consumer.consume_all()

        assert processor.processed_events == [
            *published_events,
        ]

    async def test_reprocess_already_processed_events_on_restart_when_save_state_after_consumption_false(
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
            category=state_category,
            converter=StoredEventEventConsumerStateConverter(),
            persistence_interval=EventCount(5),
        )

        consumer = EventSourceConsumer(
            source=source,
            processor=processor,
            state_store=state_store,
            save_state_after_consumption=False,
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
            category=state_category,
            converter=StoredEventEventConsumerStateConverter(),
            persistence_interval=EventCount(5),
        )
        consumer = EventSourceConsumer(
            source=source,
            processor=processor,
            state_store=state_store,
            save_state_after_consumption=False,
        )

        await consumer.consume_all()

        assert processor.processed_events == [
            *published_events,
            *published_events,
        ]

    async def test_does_not_reprocess_already_processed_events_on_restart_when_save_state_after_consumption_false_after_event_count(
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
            category=state_category,
            converter=StoredEventEventConsumerStateConverter(),
            persistence_interval=EventCount(5),
        )

        consumer = EventSourceConsumer(
            source=source,
            processor=processor,
            state_store=state_store,
            save_state_after_consumption=False,
        )

        published_events = await event_store.stream(
            category=category_name, stream=stream_name
        ).publish(
            events=[
                NewEventBuilder().build(),
                NewEventBuilder().build(),
                NewEventBuilder().build(),
                NewEventBuilder().build(),
                NewEventBuilder().build(),
                NewEventBuilder().build(),
            ]
        )

        await consumer.consume_all()

        state_store = EventConsumerStateStore(
            category=state_category,
            converter=StoredEventEventConsumerStateConverter(),
            persistence_interval=EventCount(5),
        )
        consumer = EventSourceConsumer(
            source=source,
            processor=processor,
            state_store=state_store,
            save_state_after_consumption=False,
        )

        await consumer.consume_all()

        assert processor.processed_events == [
            *published_events,
            *published_events[5:],
        ]

    async def test_logs_when_consume_all_starting(self):
        logger = CapturingLogger.create()

        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_category = event_store.category(
            category=data.random_event_category_name()
        )
        state_store = EventConsumerStateStore(
            category=state_category,
            converter=StoredEventEventConsumerStateConverter(),
        )

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
            "constraint": None,
        }

        assert startup_log_events[1].level == LogLevel.DEBUG
        assert startup_log_events[1].is_async is True
        assert startup_log_events[1].context == {
            "source": {"type": "category", "category": category_name},
            "constraint": SequenceNumberAfterConstraint(
                stream_2_publish_1[-1].sequence_number
            ),
        }

    async def test_logs_when_consume_all_complete(self):
        logger = CapturingLogger.create()

        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_category = event_store.category(
            category=data.random_event_category_name()
        )
        state_store = EventConsumerStateStore(
            category=state_category,
            converter=StoredEventEventConsumerStateConverter(),
        )

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
        await stream_2.publish(events=[NewEventBuilder().build()])

        await consumer.consume_all()

        complete_log_events = logger.find_events(
            "event.consumer.source.completed-consume"
        )

        assert len(complete_log_events) == 1

        assert complete_log_events[0].level == LogLevel.DEBUG
        assert complete_log_events[0].is_async is True
        assert complete_log_events[0].context == {
            "source": {"type": "category", "category": category_name},
            "consumed_count": 2,
        }

    async def test_logs_details_of_each_consumed_event(self):
        logger = CapturingLogger.create()

        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_category = event_store.category(
            category=data.random_event_category_name()
        )
        state_store = EventConsumerStateStore(
            category=state_category,
            converter=StoredEventEventConsumerStateConverter(),
        )

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

    async def test_logs_error_during_event_processing(self):
        logger = CapturingLogger.create()

        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_category = event_store.category(
            category=data.random_event_category_name()
        )
        state_store = EventConsumerStateStore(
            category=state_category,
            converter=StoredEventEventConsumerStateConverter(),
        )

        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()

        source = event_store.category(category=category_name)

        error = RuntimeError("Oops")
        processor = ThrowingEventProcessor(error=error)

        consumer = EventSourceConsumer(
            source=source,
            processor=processor,
            state_store=state_store,
            logger=logger,
        )

        stream = event_store.stream(category=category_name, stream=stream_name)

        stored_events = await stream.publish(
            events=[NewEventBuilder().build()]
        )

        try:
            await consumer.consume_all()
        except BaseException:
            pass

        error_log_events = logger.find_events(
            "event.consumer.source.processor-failed"
        )

        assert len(error_log_events) == 1

        assert error_log_events[0].level == LogLevel.ERROR
        assert error_log_events[0].is_async is True
        assert error_log_events[0].context == {
            "source": {"type": "category", "category": category_name},
            "envelope": stored_events[0].summarise(),
        }
        assert error_log_events[0].exc_info is not None
        assert error_log_events[0].exc_info[1] == error
