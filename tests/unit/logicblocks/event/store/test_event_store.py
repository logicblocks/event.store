import sys
from datetime import datetime

import pytest

from logicblocks.event.store import EventStore, conditions, constraints
from logicblocks.event.store.adapters import InMemoryEventStorageAdapter
from logicblocks.event.store.exceptions import UnmetWriteConditionError
from logicblocks.event.testing import NewEventBuilder, StoredEventBuilder, data
from logicblocks.event.testlogging import CapturingLogger
from logicblocks.event.testlogging.logger import LogLevel
from logicblocks.event.types import (
    CategoryIdentifier,
    NewEvent,
    StoredEvent,
    StreamIdentifier,
)


class TestStreamIdentifier:
    def test_exposes_identifier(self):
        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()

        adapter = InMemoryEventStorageAdapter()
        store = EventStore(adapter=adapter)

        stream = store.stream(category=category_name, stream=stream_name)

        assert stream.identifier == StreamIdentifier(
            category=category_name, stream=stream_name
        )


class TestStreamBasics:
    async def test_exposes_stream_identifier(self):
        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()

        store = EventStore(adapter=InMemoryEventStorageAdapter())
        stream = store.stream(category=category_name, stream=stream_name)

        assert stream.identifier == StreamIdentifier(
            category=category_name, stream=stream_name
        )

    async def test_has_no_events_in_stream_initially(self):
        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()

        store = EventStore(adapter=InMemoryEventStorageAdapter())
        stream = store.stream(category=category_name, stream=stream_name)

        events = await stream.read()

        assert events == []

    async def test_reads_single_published_event(self):
        now = datetime.now()
        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()
        event_name = data.random_event_name()
        payload = data.random_event_payload()

        store = EventStore(adapter=InMemoryEventStorageAdapter())
        stream = store.stream(category=category_name, stream=stream_name)

        stored_events = await stream.publish(
            events=[
                NewEvent(
                    name=event_name,
                    payload=payload,
                    observed_at=now,
                    occurred_at=now,
                )
            ],
        )

        read_events = await stream.read()
        expected_events = [
            StoredEvent(
                id=stored_events[0].id,
                name=event_name,
                category=category_name,
                stream=stream_name,
                payload=payload,
                position=0,
                sequence_number=stored_events[0].sequence_number,
                occurred_at=now,
                observed_at=now,
            )
        ]

        assert read_events == expected_events

    async def test_reads_multiple_published_events(self):
        now = datetime.now()
        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()
        new_events = [
            NewEventBuilder()
            .with_observed_at(now)
            .with_occurred_at(now)
            .build()
            for _ in range(10)
        ]

        store = EventStore(adapter=InMemoryEventStorageAdapter())
        stream = store.stream(category=category_name, stream=stream_name)

        stored_events = await stream.publish(events=new_events)

        read_events = await stream.read()
        expected_events = [
            StoredEvent(
                id=stored_events[position].id,
                name=event.name,
                category=category_name,
                stream=stream_name,
                payload=event.payload,
                position=position,
                sequence_number=stored_events[position].sequence_number,
                occurred_at=event.occurred_at,
                observed_at=event.observed_at,
            )
            for event, position in zip(new_events, range(len(new_events)))
        ]

        assert read_events == expected_events

    async def test_reads_events_in_multiple_streams_in_same_category(self):
        category_name = data.random_event_category_name()
        stream_1_name = data.random_event_stream_name()
        stream_2_name = data.random_event_stream_name()

        stream_1_new_events = [NewEventBuilder().build() for _ in range(5)]
        stream_2_new_events = [NewEventBuilder().build() for _ in range(5)]

        store = EventStore(adapter=InMemoryEventStorageAdapter())
        stream_1 = store.stream(category=category_name, stream=stream_1_name)
        stream_2 = store.stream(category=category_name, stream=stream_2_name)

        await stream_1.publish(events=stream_1_new_events)
        await stream_2.publish(events=stream_2_new_events)

        stream_1_events = await stream_1.read()
        stream_2_events = await stream_2.read()

        actual_streams = {
            "stream_1": [
                (event.name, event.stream, event.category)
                for event in stream_1_events
            ],
            "stream_2": [
                (event.name, event.stream, event.category)
                for event in stream_2_events
            ],
        }
        expected_streams = {
            "stream_1": [
                (event.name, stream_1_name, category_name)
                for event in stream_1_new_events
            ],
            "stream_2": [
                (event.name, stream_2_name, category_name)
                for event in stream_2_new_events
            ],
        }

        assert actual_streams == expected_streams

    async def test_reads_events_in_streams_in_different_categories(self):
        category_1_name = data.random_event_category_name()
        category_2_name = data.random_event_category_name()
        category_1_stream_name = data.random_event_stream_name()
        category_2_stream_name = data.random_event_stream_name()

        category_1_stream_new_events = [
            NewEventBuilder().build() for _ in range(5)
        ]
        category_2_stream_new_events = [
            NewEventBuilder().build() for _ in range(5)
        ]

        store = EventStore(adapter=InMemoryEventStorageAdapter())
        category_1_stream = store.stream(
            category=category_1_name, stream=category_1_stream_name
        )
        category_2_stream = store.stream(
            category=category_2_name, stream=category_2_stream_name
        )

        await category_1_stream.publish(events=category_1_stream_new_events)
        await category_2_stream.publish(events=category_2_stream_new_events)

        category_1_stream_events = await category_1_stream.read()
        category_2_stream_events = await category_2_stream.read()

        actual_streams = {
            "stream_1": [
                (event.name, event.stream, event.category)
                for event in category_1_stream_events
            ],
            "stream_2": [
                (event.name, event.stream, event.category)
                for event in category_2_stream_events
            ],
        }
        expected_streams = {
            "stream_1": [
                (event.name, category_1_stream_name, category_1_name)
                for event in category_1_stream_new_events
            ],
            "stream_2": [
                (event.name, category_2_stream_name, category_2_name)
                for event in category_2_stream_new_events
            ],
        }

        assert actual_streams == expected_streams


class TestStreamRead:
    async def test_reads_all_events_in_stream_by_default(self):
        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()
        new_events = [NewEventBuilder().build() for _ in range(10)]

        store = EventStore(adapter=InMemoryEventStorageAdapter())
        stream = store.stream(category=category_name, stream=stream_name)

        await stream.publish(events=new_events)

        read_events = await stream.read()

        assert len(read_events) == 10

    async def test_reads_events_in_stream_after_sequence_number(self):
        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()
        new_events = [NewEventBuilder().build() for _ in range(10)]

        store = EventStore(adapter=InMemoryEventStorageAdapter())
        stream = store.stream(category=category_name, stream=stream_name)

        stored_events = await stream.publish(events=new_events)

        sequence_number = stored_events[4].sequence_number

        read_events = await stream.read(
            constraints={constraints.sequence_number_after(sequence_number)}
        )

        assert read_events == stored_events[5:]


class TestStreamLatest:
    async def test_reads_latest_event_in_stream_if_present(self):
        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()
        new_events = [NewEventBuilder().build() for _ in range(3)]

        store = EventStore(adapter=InMemoryEventStorageAdapter())
        stream = store.stream(category=category_name, stream=stream_name)

        stored_events = await stream.publish(events=new_events)

        latest_event = await stream.latest()

        assert latest_event == stored_events[-1]

    async def test_returns_none_if_stream_is_empty(self):
        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()

        store = EventStore(adapter=InMemoryEventStorageAdapter())
        stream = store.stream(category=category_name, stream=stream_name)

        latest_event = await stream.latest()

        assert latest_event is None


class TestStreamIteration:
    async def test_can_treat_stream_as_iterable_over_all_events(self):
        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()
        new_events = [NewEventBuilder().build() for _ in range(10)]

        store = EventStore(adapter=InMemoryEventStorageAdapter())
        stream = store.stream(category=category_name, stream=stream_name)

        await stream.publish(events=new_events)

        new_event_keys = [
            (event.name, stream_name, category_name) for event in new_events
        ]
        stream_event_keys = [
            (event.name, event.stream, event.category)
            async for event in stream
        ]

        assert stream_event_keys == new_event_keys

    async def test_iterates_over_all_events_in_stream_by_default(self):
        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()
        new_events = [NewEventBuilder().build() for _ in range(10)]

        store = EventStore(adapter=InMemoryEventStorageAdapter())
        stream = store.stream(category=category_name, stream=stream_name)

        await stream.publish(events=new_events)

        new_event_keys = [
            (event.name, stream_name, category_name) for event in new_events
        ]
        stream_event_keys = [
            (event.name, event.stream, event.category)
            async for event in stream.iterate()
        ]

        assert stream_event_keys == new_event_keys

    async def test_iterates_over_events_after_sequence_number(self):
        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()
        new_events = [NewEventBuilder().build() for _ in range(10)]

        store = EventStore(adapter=InMemoryEventStorageAdapter())
        stream = store.stream(category=category_name, stream=stream_name)

        stored_events = await stream.publish(events=new_events)

        sequence_number = stored_events[4].sequence_number

        new_event_keys = [
            (event.name, stream_name, category_name)
            for event in new_events[5:]
        ]
        stream_event_keys = [
            (event.name, event.stream, event.category)
            async for event in stream.iterate(
                constraints={
                    constraints.sequence_number_after(sequence_number)
                }
            )
        ]

        assert stream_event_keys == new_event_keys


class TestStreamPublishing:
    async def test_publishes_if_stream_position_matches_position_condition(
        self,
    ):
        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()

        store = EventStore(adapter=InMemoryEventStorageAdapter())
        stream = store.stream(category=category_name, stream=stream_name)

        await stream.publish(
            events=[NewEventBuilder().build() for _ in range(10)]
        )

        new_event = NewEventBuilder().build()

        stored_events = await stream.publish(
            events=[new_event], conditions={conditions.position_is(9)}
        )

        found_events = await stream.read()

        assert found_events[-1] == stored_events[-1]

    async def test_raises_if_stream_position_beyond_position_condition(self):
        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()

        store = EventStore(adapter=InMemoryEventStorageAdapter())
        stream = store.stream(category=category_name, stream=stream_name)

        await stream.publish(
            events=[NewEventBuilder().build() for _ in range(10)]
        )

        new_event = NewEventBuilder().build()

        with pytest.raises(UnmetWriteConditionError):
            await stream.publish(
                events=[new_event], conditions={conditions.position_is(5)}
            )

    async def test_raises_if_stream_position_before_condition_position(self):
        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()

        store = EventStore(adapter=InMemoryEventStorageAdapter())
        stream = store.stream(category=category_name, stream=stream_name)

        await stream.publish(
            events=[NewEventBuilder().build() for _ in range(5)]
        )

        new_event = NewEventBuilder().build()

        with pytest.raises(UnmetWriteConditionError):
            await stream.publish(
                events=[new_event], conditions={conditions.position_is(10)}
            )

    async def test_publishes_if_stream_empty_and_empty_condition_specified(
        self,
    ):
        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()

        store = EventStore(adapter=InMemoryEventStorageAdapter())
        stream = store.stream(category=category_name, stream=stream_name)

        new_event = NewEventBuilder().build()

        stored_events = await stream.publish(
            events=[new_event], conditions={conditions.stream_is_empty()}
        )

        found_events = await stream.read()

        assert found_events[-1] == stored_events[-1]

    async def test_raises_if_stream_not_empty_and_empty_condition_specified(
        self,
    ):
        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()

        store = EventStore(adapter=InMemoryEventStorageAdapter())
        stream = store.stream(category=category_name, stream=stream_name)

        await stream.publish(
            events=[NewEventBuilder().build() for _ in range(5)]
        )

        new_event = NewEventBuilder().build()

        with pytest.raises(UnmetWriteConditionError):
            await stream.publish(
                events=[new_event], conditions={conditions.stream_is_empty()}
            )


class TestStreamLogging:
    async def test_logs_on_publish(self):
        logger = CapturingLogger.create()

        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()

        store = EventStore(
            adapter=InMemoryEventStorageAdapter(), logger=logger
        )
        stream = store.stream(category=category_name, stream=stream_name)

        await stream.publish(
            events=[
                NewEventBuilder().with_name("first-thing-happened").build(),
                NewEventBuilder().with_name("second-thing-happened").build(),
            ]
        )

        log_event = logger.find_event("event.stream.publishing")

        assert log_event is not None
        assert log_event.level == LogLevel.INFO
        assert log_event.is_async is True
        assert log_event.context == {
            "category": category_name,
            "stream": stream_name,
            "event_count": 2,
            "event_names": ["first-thing-happened", "second-thing-happened"],
            "conditions": [],
        }

    async def test_logs_on_latest(self):
        logger = CapturingLogger.create()

        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()

        store = EventStore(
            adapter=InMemoryEventStorageAdapter(), logger=logger
        )
        stream = store.stream(category=category_name, stream=stream_name)

        await stream.publish(
            events=[
                NewEventBuilder().with_name("first-thing-happened").build(),
                NewEventBuilder().with_name("second-thing-happened").build(),
            ]
        )

        await stream.latest()

        log_event = logger.find_event("event.stream.reading-latest")

        assert log_event is not None
        assert log_event.level == LogLevel.INFO
        assert log_event.is_async is True
        assert log_event.context == {
            "category": category_name,
            "stream": stream_name,
        }

    async def test_logs_on_iterating(self):
        logger = CapturingLogger.create()

        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()

        store = EventStore(
            adapter=InMemoryEventStorageAdapter(), logger=logger
        )
        stream = store.stream(category=category_name, stream=stream_name)

        await stream.publish(
            events=[
                NewEventBuilder().with_name("first-thing-happened").build(),
                NewEventBuilder().with_name("second-thing-happened").build(),
            ]
        )

        stream.iterate()

        log_event = logger.find_event("event.stream.iterating")

        assert log_event is not None
        assert log_event.level == LogLevel.INFO
        assert log_event.is_async is False
        assert log_event.context == {
            "category": category_name,
            "stream": stream_name,
            "constraints": [],
        }


class TestCategoryIdentifier:
    def test_exposes_identifier(self):
        category_name = data.random_event_category_name()

        adapter = InMemoryEventStorageAdapter()
        store = EventStore(adapter=adapter)

        category = store.category(category=category_name)

        assert category.identifier == CategoryIdentifier(
            category=category_name
        )


class TestCategoryBasics:
    async def test_has_no_events_in_category_initially(self):
        category_name = data.random_event_category_name()

        store = EventStore(adapter=InMemoryEventStorageAdapter())
        category = store.category(category=category_name)

        events = await category.read()

        assert events == []

    async def test_reads_single_published_event_for_single_stream_in_category(
        self,
    ):
        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()
        new_event = NewEventBuilder().build()

        store = EventStore(adapter=InMemoryEventStorageAdapter())
        category = store.category(category=category_name)
        stream = category.stream(stream=stream_name)

        stored_events = await stream.publish(events=[new_event])

        read_events = await category.read()
        expected_events = [
            StoredEvent(
                id=stored_events[0].id,
                name=new_event.name,
                category=category_name,
                stream=stream_name,
                payload=new_event.payload,
                position=0,
                sequence_number=stored_events[0].sequence_number,
                occurred_at=new_event.occurred_at,
                observed_at=new_event.observed_at,
            )
        ]

        assert read_events == expected_events

    async def test_reads_multiple_published_events_for_single_stream_in_category(
        self,
    ):
        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()
        new_events = [NewEventBuilder().build() for _ in range(10)]

        store = EventStore(adapter=InMemoryEventStorageAdapter())
        category = store.category(category=category_name)
        stream = category.stream(stream=stream_name)

        stored_events = await stream.publish(events=new_events)

        read_events = await category.read()
        expected_events = [
            StoredEvent(
                id=stored_events[position].id,
                name=event.name,
                category=category_name,
                stream=stream_name,
                payload=event.payload,
                position=position,
                sequence_number=stored_events[position].sequence_number,
                occurred_at=event.occurred_at,
                observed_at=event.observed_at,
            )
            for event, position in zip(new_events, range(len(new_events)))
        ]

        assert read_events == expected_events

    async def test_reads_for_different_streams_in_category_in_publish_order(
        self,
    ):
        category_name = data.random_event_category_name()
        stream_1_name = data.random_event_stream_name()
        stream_2_name = data.random_event_stream_name()

        stream_1_new_event_1 = NewEventBuilder().build()
        stream_1_new_event_2 = NewEventBuilder().build()
        stream_2_new_event_1 = NewEventBuilder().build()
        stream_2_new_event_2 = NewEventBuilder().build()

        store = EventStore(adapter=InMemoryEventStorageAdapter())
        category = store.category(category=category_name)
        stream_1 = category.stream(stream=stream_1_name)
        stream_2 = category.stream(stream=stream_2_name)

        stream_1_stored_events_1 = await stream_1.publish(
            events=[stream_1_new_event_1]
        )
        stream_2_stored_events_1 = await stream_2.publish(
            events=[stream_2_new_event_1]
        )
        stream_2_stored_events_2 = await stream_2.publish(
            events=[stream_2_new_event_2]
        )
        stream_1_stored_events_2 = await stream_1.publish(
            events=[stream_1_new_event_2]
        )

        read_events = await category.read()

        stream_1_expected_stored_event_builder = (
            StoredEventBuilder()
            .with_category(category_name)
            .with_stream(stream_1_name)
        )
        stream_2_expected_stored_event_builder = (
            StoredEventBuilder()
            .with_category(category_name)
            .with_stream(stream_2_name)
        )

        stream_1_expected_stored_events_1 = (
            stream_1_expected_stored_event_builder.from_new_event(
                stream_1_new_event_1
            )
            .with_id(stream_1_stored_events_1[0].id)
            .with_sequence_number(stream_1_stored_events_1[0].sequence_number)
            .with_position(0)
            .build()
        )
        stream_2_expected_stored_events_1 = (
            stream_2_expected_stored_event_builder.from_new_event(
                stream_2_new_event_1
            )
            .with_id(stream_2_stored_events_1[0].id)
            .with_sequence_number(stream_2_stored_events_1[0].sequence_number)
            .with_position(0)
            .build()
        )
        stream_2_expected_stored_events_2 = (
            stream_2_expected_stored_event_builder.from_new_event(
                stream_2_new_event_2
            )
            .with_id(stream_2_stored_events_2[0].id)
            .with_sequence_number(stream_2_stored_events_2[0].sequence_number)
            .with_position(1)
            .build()
        )
        stream_1_expected_stored_events_2 = (
            stream_1_expected_stored_event_builder.from_new_event(
                stream_1_new_event_2
            )
            .with_id(stream_1_stored_events_2[0].id)
            .with_sequence_number(stream_1_stored_events_2[0].sequence_number)
            .with_position(1)
            .build()
        )

        expected_events = [
            stream_1_expected_stored_events_1,
            stream_2_expected_stored_events_1,
            stream_2_expected_stored_events_2,
            stream_1_expected_stored_events_2,
        ]

        assert read_events == expected_events

    async def test_ignores_events_in_other_categories(self):
        category = data.random_event_category_name()
        stream = data.random_event_stream_name()
        new_event = NewEventBuilder().build()

        store = EventStore(adapter=InMemoryEventStorageAdapter())
        stored_events = await store.stream(
            category=category, stream=stream
        ).publish(events=[new_event])
        await store.stream(
            category=(data.random_event_category_name()),
            stream=(data.random_event_stream_name()),
        ).publish(events=[(NewEventBuilder().build())])

        category_to_read = store.category(category=category)

        read_events = await category_to_read.read()
        expected_events = [
            StoredEventBuilder()
            .from_new_event(new_event)
            .with_id(stored_events[0].id)
            .with_category(category)
            .with_stream(stream)
            .with_sequence_number(0)
            .with_position(0)
            .build()
        ]

        assert read_events == expected_events


class TestCategoryRead:
    async def test_reads_all_events_in_category_by_default(self):
        category_name = data.random_event_category_name()
        stream_1_name = data.random_event_stream_name()
        stream_2_name = data.random_event_stream_name()

        stream_1_new_events = [NewEventBuilder().build() for _ in range(10)]
        stream_2_new_events = [NewEventBuilder().build() for _ in range(10)]

        store = EventStore(adapter=InMemoryEventStorageAdapter())

        category = store.category(category=category_name)

        stored_events_1 = await category.stream(stream=stream_1_name).publish(
            events=stream_1_new_events
        )
        stored_events_2 = await category.stream(stream=stream_2_name).publish(
            events=stream_2_new_events
        )

        expected_events = list(stored_events_1) + list(stored_events_2)
        read_events = await category.read()

        assert read_events == expected_events

    async def test_reads_events_in_category_after_sequence_number(self):
        category_name = data.random_event_category_name()
        stream_1_name = data.random_event_stream_name()
        stream_2_name = data.random_event_stream_name()

        stream_1_new_events = [NewEventBuilder().build() for _ in range(10)]
        stream_2_new_events = [NewEventBuilder().build() for _ in range(10)]

        store = EventStore(adapter=InMemoryEventStorageAdapter())

        category = store.category(category=category_name)

        stored_events_1 = await category.stream(stream=stream_1_name).publish(
            events=stream_1_new_events
        )
        stored_events_2 = await category.stream(stream=stream_2_name).publish(
            events=stream_2_new_events
        )

        sequence_number = stored_events_1[4].sequence_number

        expected_events = list(stored_events_1[5:]) + list(stored_events_2)
        read_events = await category.read(
            constraints={constraints.sequence_number_after(sequence_number)}
        )

        assert read_events == expected_events


class TestCategoryLatest:
    async def test_reads_latest_event_in_category_if_present(self):
        category_name = data.random_event_category_name()
        stream_1_name = data.random_event_stream_name()
        stream_2_name = data.random_event_stream_name()

        stream_1_new_events = [NewEventBuilder().build() for _ in range(10)]
        stream_2_new_events = [NewEventBuilder().build() for _ in range(10)]

        store = EventStore(adapter=InMemoryEventStorageAdapter())

        category = store.category(category=category_name)

        await category.stream(stream=stream_1_name).publish(
            events=stream_1_new_events
        )
        stored_events_2 = await category.stream(stream=stream_2_name).publish(
            events=stream_2_new_events
        )

        latest_event = await category.latest()

        assert latest_event == stored_events_2[-1]

    async def test_returns_none_if_category_is_empty(self):
        category_name = data.random_event_category_name()

        store = EventStore(adapter=InMemoryEventStorageAdapter())
        category = store.category(category=category_name)

        latest_event = await category.latest()

        assert latest_event is None


class TestCategoryIteration:
    async def test_can_treat_category_as_iterable_over_all_events(self):
        category_name = data.random_event_category_name()
        stream_1_name = data.random_event_stream_name()
        stream_2_name = data.random_event_stream_name()

        stream_1_new_events = [NewEventBuilder().build() for _ in range(10)]
        stream_2_new_events = [NewEventBuilder().build() for _ in range(10)]

        store = EventStore(adapter=InMemoryEventStorageAdapter())
        category = store.category(category=category_name)

        await category.stream(stream=stream_1_name).publish(
            events=stream_1_new_events
        )
        await category.stream(stream=stream_2_name).publish(
            events=stream_2_new_events
        )

        new_event_keys = [
            (event.name, stream_1_name, category_name)
            for event in stream_1_new_events
        ] + [
            (event.name, stream_2_name, category_name)
            for event in stream_2_new_events
        ]
        stream_event_keys = [
            (event.name, event.stream, event.category)
            async for event in category
        ]

        assert stream_event_keys == new_event_keys

    async def test_iterates_over_all_events_in_category_by_default(self):
        category_name = data.random_event_category_name()
        stream_1_name = data.random_event_stream_name()
        stream_2_name = data.random_event_stream_name()

        stream_1_new_events = [NewEventBuilder().build() for _ in range(10)]
        stream_2_new_events = [NewEventBuilder().build() for _ in range(10)]

        store = EventStore(adapter=InMemoryEventStorageAdapter())
        category = store.category(category=category_name)

        await category.stream(stream=stream_1_name).publish(
            events=stream_1_new_events
        )
        await category.stream(stream=stream_2_name).publish(
            events=stream_2_new_events
        )

        new_event_keys = [
            (event.name, stream_1_name, category_name)
            for event in stream_1_new_events
        ] + [
            (event.name, stream_2_name, category_name)
            for event in stream_2_new_events
        ]
        stream_event_keys = [
            (event.name, event.stream, event.category)
            async for event in category.iterate()
        ]

        assert stream_event_keys == new_event_keys

    async def test_iterates_over_events_after_sequence_number(self):
        category_name = data.random_event_category_name()
        stream_1_name = data.random_event_stream_name()
        stream_2_name = data.random_event_stream_name()

        stream_1_new_events = [NewEventBuilder().build() for _ in range(10)]
        stream_2_new_events = [NewEventBuilder().build() for _ in range(10)]

        store = EventStore(adapter=InMemoryEventStorageAdapter())
        category = store.category(category=category_name)

        stream_1_stored_events = await category.stream(
            stream=stream_1_name
        ).publish(events=stream_1_new_events)
        (
            await category.stream(stream=stream_2_name).publish(
                events=stream_2_new_events
            )
        )

        sequence_number = stream_1_stored_events[4].sequence_number

        new_event_keys = [
            (event.name, stream_1_name, category_name)
            for event in stream_1_new_events[5:]
        ] + [
            (event.name, stream_2_name, category_name)
            for event in stream_2_new_events
        ]
        stream_event_keys = [
            (event.name, event.stream, event.category)
            async for event in category.iterate(
                constraints={
                    constraints.sequence_number_after(sequence_number)
                }
            )
        ]

        assert stream_event_keys == new_event_keys


class TestCategoryLogging:
    async def test_logs_on_latest(self):
        logger = CapturingLogger.create()

        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()

        store = EventStore(
            adapter=InMemoryEventStorageAdapter(), logger=logger
        )
        category = store.category(category=category_name)

        await category.stream(stream=stream_name).publish(
            events=[
                NewEventBuilder().with_name("first-thing-happened").build(),
                NewEventBuilder().with_name("second-thing-happened").build(),
            ]
        )

        await category.latest()

        log_event = logger.find_event("event.category.reading-latest")

        assert log_event is not None
        assert log_event.level == LogLevel.INFO
        assert log_event.is_async is True
        assert log_event.context == {
            "category": category_name,
        }

    async def test_logs_on_iterating(self):
        logger = CapturingLogger.create()

        category_name = data.random_event_category_name()
        stream_1_name = data.random_event_stream_name()
        stream_2_name = data.random_event_stream_name()

        store = EventStore(
            adapter=InMemoryEventStorageAdapter(), logger=logger
        )
        category = store.category(category=category_name)
        stream_1 = category.stream(stream=stream_1_name)
        stream_2 = category.stream(stream=stream_2_name)

        await stream_1.publish(
            events=[
                NewEventBuilder().with_name("first-thing-happened").build(),
                NewEventBuilder().with_name("second-thing-happened").build(),
            ]
        )
        await stream_2.publish(
            events=[
                NewEventBuilder().with_name("third-thing-happened").build(),
                NewEventBuilder().with_name("fourth-thing-happened").build(),
            ]
        )

        category.iterate()

        log_event = logger.find_event("event.category.iterating")

        assert log_event is not None
        assert log_event.level == LogLevel.INFO
        assert log_event.is_async is False
        assert log_event.context == {
            "category": category_name,
            "constraints": [],
        }


if __name__ == "__main__":
    sys.exit(pytest.main([__file__]))
