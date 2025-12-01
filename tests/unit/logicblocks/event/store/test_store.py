import sys
from collections.abc import Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, Self

import pytest
from logicblocks.event.testlogging import CapturingLogger
from logicblocks.event.testlogging.logger import LogLevel

from logicblocks.event.sources import constraints
from logicblocks.event.store import (
    EventStore,
    conditions,
)
from logicblocks.event.store.adapters import InMemoryEventStorageAdapter
from logicblocks.event.store.exceptions import UnmetWriteConditionError
from logicblocks.event.store.types import stream_publish_definition
from logicblocks.event.testing import NewEventBuilder, StoredEventBuilder, data
from logicblocks.event.types import (
    CategoryIdentifier,
    JsonValue,
    JsonValueConvertible,
    LogIdentifier,
    NewEvent,
    StoredEvent,
    StreamIdentifier,
    default_deserialisation_fallback,
    str_serialisation_fallback,
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


@dataclass(frozen=True)
class Payload1(JsonValueConvertible):
    value: int

    @classmethod
    def deserialise(
        cls,
        value: JsonValue,
        fallback: Callable[
            [Any, JsonValue], Any
        ] = default_deserialisation_fallback,
    ) -> Self:
        if not isinstance(value, Mapping) or not isinstance(
            value["value"], int
        ):
            return fallback(cls, value)

        return cls(value=value["value"])

    def serialise(self, fallback: Callable[[object], JsonValue]) -> JsonValue:
        return {"value": self.value}


@dataclass(frozen=True)
class Payload2(JsonValueConvertible):
    value: str

    @classmethod
    def deserialise(
        cls,
        value: JsonValue,
        fallback: Callable[
            [Any, JsonValue], Any
        ] = default_deserialisation_fallback,
    ) -> Self:
        if not isinstance(value, Mapping) or not isinstance(
            value["value"], str
        ):
            return fallback(cls, value)

        return cls(value=value["value"])

    def serialise(self, fallback: Callable[[object], JsonValue]) -> JsonValue:
        return {"value": self.value}


class TestStreamPublishing:
    async def test_publishes_if_event_payload_type_is_persistable_type(self):
        now = datetime.now()
        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()

        store = EventStore(adapter=InMemoryEventStorageAdapter())
        stream = store.stream(category=category_name, stream=stream_name)

        new_event_1 = (
            NewEventBuilder[str, Payload1]()
            .with_payload(Payload1(value=45))
            .with_occurred_at(now)
            .with_observed_at(now)
            .build()
        )
        new_event_2 = (
            NewEventBuilder[str, Payload2]()
            .with_payload(Payload2(value="hello"))
            .with_occurred_at(now)
            .with_observed_at(now)
            .build()
        )

        stored_events = await stream.publish(events=[new_event_1, new_event_2])

        read_events = await stream.read()
        expected_events = [
            StoredEvent(
                id=stored_events[0].id,
                name=new_event_1.name,
                category=category_name,
                stream=stream_name,
                payload={"value": 45},
                position=0,
                sequence_number=stored_events[0].sequence_number,
                occurred_at=now,
                observed_at=now,
            ),
            StoredEvent(
                id=stored_events[1].id,
                name=new_event_2.name,
                category=category_name,
                stream=stream_name,
                payload={"value": "hello"},
                position=1,
                sequence_number=stored_events[1].sequence_number,
                occurred_at=now,
                observed_at=now,
            ),
        ]

        assert read_events == expected_events

    async def test_publishes_if_event_name_type_is_string_literal(self):
        now = datetime.now()
        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()

        store = EventStore(adapter=InMemoryEventStorageAdapter())
        stream = store.stream(category=category_name, stream=stream_name)

        new_event_1 = (
            NewEventBuilder[Literal["thing-1-happened", "thing-2-happened"]]()
            .with_name("thing-1-happened")
            .with_occurred_at(now)
            .with_observed_at(now)
            .build()
        )
        new_event_2 = (
            NewEventBuilder[Literal["thing-1-happened", "thing-2-happened"]]()
            .with_name("thing-2-happened")
            .with_occurred_at(now)
            .with_observed_at(now)
            .build()
        )

        stored_events = await stream.publish(events=[new_event_1, new_event_2])

        read_events = await stream.read()
        expected_events = [
            StoredEvent(
                id=stored_events[0].id,
                name=new_event_1.name,
                category=category_name,
                stream=stream_name,
                payload=new_event_1.payload,
                position=0,
                sequence_number=stored_events[0].sequence_number,
                occurred_at=now,
                observed_at=now,
            ),
            StoredEvent(
                id=stored_events[1].id,
                name=new_event_2.name,
                category=category_name,
                stream=stream_name,
                payload=new_event_2.payload,
                position=1,
                sequence_number=stored_events[1].sequence_number,
                occurred_at=now,
                observed_at=now,
            ),
        ]

        assert read_events == expected_events

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
            events=[new_event], condition=conditions.position_is(9)
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
                events=[new_event], condition=conditions.position_is(5)
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
                events=[new_event], condition=conditions.position_is(10)
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
            events=[new_event], condition=conditions.stream_is_empty()
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
                events=[new_event], condition=conditions.stream_is_empty()
            )


class TestStreamLogging:
    async def test_logs_event_and_conditions_pre_publish(self):
        logger = CapturingLogger.create()

        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()

        store = EventStore(
            adapter=InMemoryEventStorageAdapter(), logger=logger
        )
        stream = store.stream(category=category_name, stream=stream_name)

        new_events = [
            NewEventBuilder().with_name("first-thing-happened").build(),
            NewEventBuilder().with_name("second-thing-happened").build(),
        ]
        await stream.publish(
            events=new_events, condition=conditions.stream_is_empty()
        )

        log_event = logger.find_event("event.stream.publishing")

        assert log_event is not None
        assert log_event.level == LogLevel.DEBUG
        assert log_event.is_async is True
        assert log_event.context == {
            "category": category_name,
            "stream": stream_name,
            "events": [
                new_event.serialise(fallback=str_serialisation_fallback)
                for new_event in new_events
            ],
            "conditions": conditions.stream_is_empty(),
        }

    async def test_logs_envelope_post_publish_when_not_debug_and_successful(
        self,
    ):
        logger = CapturingLogger.create(log_level=LogLevel.INFO)

        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()

        store = EventStore(
            adapter=InMemoryEventStorageAdapter(), logger=logger
        )
        stream = store.stream(category=category_name, stream=stream_name)

        stored_events = await stream.publish(
            events=[
                NewEventBuilder().with_name("first-thing-happened").build(),
                NewEventBuilder().with_name("second-thing-happened").build(),
            ]
        )

        log_event = logger.find_event("event.stream.published")

        assert log_event is not None
        assert log_event.level == LogLevel.INFO
        assert log_event.is_async is True
        assert log_event.context == {
            "category": category_name,
            "stream": stream_name,
            "events": [
                stored_event.summarise() for stored_event in stored_events
            ],
        }

    async def test_logs_event_post_publish_when_debug_and_successful(self):
        logger = CapturingLogger.create(log_level=LogLevel.DEBUG)

        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()

        store = EventStore(
            adapter=InMemoryEventStorageAdapter(), logger=logger
        )
        stream = store.stream(category=category_name, stream=stream_name)

        stored_events = await stream.publish(
            events=[
                NewEventBuilder().with_name("first-thing-happened").build(),
                NewEventBuilder().with_name("second-thing-happened").build(),
            ]
        )

        log_event = logger.find_event("event.stream.published")

        assert log_event is not None
        assert log_event.level == LogLevel.INFO
        assert log_event.is_async is True
        assert log_event.context == {
            "category": category_name,
            "stream": stream_name,
            "events": [
                stored_event.serialise(fallback=str_serialisation_fallback)
                for stored_event in stored_events
            ],
        }

    async def test_logs_when_publish_unsuccessful(self):
        logger = CapturingLogger.create(log_level=LogLevel.DEBUG)

        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()

        store = EventStore(
            adapter=InMemoryEventStorageAdapter(), logger=logger
        )
        stream = store.stream(category=category_name, stream=stream_name)

        new_event_1 = NewEventBuilder().with_name("a-thing-happened").build()
        await stream.publish(events=[new_event_1])

        new_event_2 = NewEventBuilder().with_name("b-thing-happened").build()

        with suppress(UnmetWriteConditionError):
            await stream.publish(
                events=[new_event_2], condition=conditions.stream_is_empty()
            )

        log_event = logger.find_event("event.stream.publish-failed")

        assert log_event is not None
        assert log_event.level == LogLevel.WARNING
        assert log_event.is_async is True
        assert log_event.context == {
            "category": category_name,
            "stream": stream_name,
            "events": [new_event_2.summarise()],
            "reason": repr(UnmetWriteConditionError("stream is not empty")),
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
        assert log_event.level == LogLevel.DEBUG
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
        assert log_event.level == LogLevel.DEBUG
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


class TestCategoryPublish:
    async def test_publishes_events_to_multiple_streams_in_category(self):
        category_name = data.random_event_category_name()
        stream_1_name = data.random_event_stream_name()
        stream_2_name = data.random_event_stream_name()

        stream_1_events = [NewEventBuilder().build() for _ in range(2)]
        stream_2_events = [NewEventBuilder().build() for _ in range(3)]

        store = EventStore(adapter=InMemoryEventStorageAdapter())
        category = store.category(category=category_name)

        streams = {
            stream_1_name: stream_publish_definition(
                events=stream_1_events,
                condition=conditions.stream_is_empty(),
            ),
            stream_2_name: stream_publish_definition(events=stream_2_events),
        }

        stored_events = await category.publish(streams=streams)

        stream_1 = category.stream(stream=stream_1_name)
        stream_2 = category.stream(stream=stream_2_name)

        stream_1_read_events = await stream_1.read()
        stream_2_read_events = await stream_2.read()

        expected_stream_1_events = [
            StoredEvent(
                id=stored_events[stream_1_name][i].id,
                name=event.name,
                category=category_name,
                stream=stream_1_name,
                payload=event.payload,
                position=i,
                sequence_number=stored_events[stream_1_name][
                    i
                ].sequence_number,
                occurred_at=event.occurred_at,
                observed_at=event.observed_at,
            )
            for i, event in enumerate(stream_1_events)
        ]

        expected_stream_2_events = [
            StoredEvent(
                id=stored_events[stream_2_name][i].id,
                name=event.name,
                category=category_name,
                stream=stream_2_name,
                payload=event.payload,
                position=i,
                sequence_number=stored_events[stream_2_name][
                    i
                ].sequence_number,
                occurred_at=event.occurred_at,
                observed_at=event.observed_at,
            )
            for i, event in enumerate(stream_2_events)
        ]

        assert stream_1_read_events == expected_stream_1_events
        assert stream_2_read_events == expected_stream_2_events


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
        assert log_event.level == LogLevel.DEBUG
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
        assert log_event.level == LogLevel.DEBUG
        assert log_event.is_async is False
        assert log_event.context == {
            "category": category_name,
            "constraints": [],
        }


class TestLogIdentifier:
    def test_exposes_identifier(self):
        store = EventStore(adapter=InMemoryEventStorageAdapter())

        log = store.log()

        assert log.identifier == LogIdentifier()


class TestLogBasics:
    async def test_has_no_events_in_log_initially(self):
        store = EventStore(adapter=InMemoryEventStorageAdapter())
        log = store.log()

        events = await log.read()

        assert events == []

    async def test_reads_single_published_event(self):
        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()
        new_event = NewEventBuilder().build()
        adapter = InMemoryEventStorageAdapter()

        store = EventStore(adapter=adapter)
        category = store.category(category=category_name)
        stream = category.stream(stream=stream_name)

        stored_events = await stream.publish(events=[new_event])

        log = store.log()

        read_log = await log.read()
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

        assert read_log == expected_events

    async def test_reads_multiple_published_events_in_same_category(self):
        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()
        new_events = [NewEventBuilder().build() for _ in range(10)]
        adapter = InMemoryEventStorageAdapter()

        store = EventStore(adapter=adapter)
        category = store.category(category=category_name)
        stream = category.stream(stream=stream_name)

        stored_events = await stream.publish(events=new_events)

        log = store.log()
        read_log = await log.read()

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

        assert read_log == expected_events

    async def test_reads_events_across_all_streams_in_a_category(self):
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

        log = store.log()
        read_log = await log.read()

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

        assert read_log == expected_events

    async def test_reads_events_across_different_categories(self):
        category_1_name = data.random_event_category_name()
        category_2_name = data.random_event_category_name()
        category_1_stream_name = data.random_event_stream_name()
        category_2_stream_name = data.random_event_stream_name()

        category_1_stream_new_event_1 = NewEventBuilder().build()
        category_1_stream_new_event_2 = NewEventBuilder().build()
        category_2_stream_new_event_1 = NewEventBuilder().build()
        category_2_stream_new_event_2 = NewEventBuilder().build()

        store = EventStore(adapter=InMemoryEventStorageAdapter())
        category_1 = store.category(category=category_1_name)
        category_2 = store.category(category=category_2_name)
        stream_1 = category_1.stream(stream=category_1_stream_name)
        stream_2 = category_2.stream(stream=category_2_stream_name)

        category_1_stream_stored_events_1 = await stream_1.publish(
            events=[category_1_stream_new_event_1]
        )
        category_2_stream_stored_events_1 = await stream_2.publish(
            events=[category_2_stream_new_event_1]
        )
        category_2_stream_stored_events_2 = await stream_2.publish(
            events=[category_2_stream_new_event_2]
        )
        category_1_stream_stored_events_2 = await stream_1.publish(
            events=[category_1_stream_new_event_2]
        )

        log = store.log()
        read_log = await log.read()

        category_1_stream_expected_stored_event_builder = (
            StoredEventBuilder()
            .with_category(category_1_name)
            .with_stream(category_1_stream_name)
        )
        category_2_stream_expected_stored_event_builder = (
            StoredEventBuilder()
            .with_category(category_2_name)
            .with_stream(category_2_stream_name)
        )

        category_1_stream_expected_stored_events_1 = (
            category_1_stream_expected_stored_event_builder.from_new_event(
                category_1_stream_new_event_1
            )
            .with_id(category_1_stream_stored_events_1[0].id)
            .with_sequence_number(
                category_1_stream_stored_events_1[0].sequence_number
            )
            .with_position(0)
            .build()
        )
        category_2_stream_expected_stored_events_1 = (
            category_2_stream_expected_stored_event_builder.from_new_event(
                category_2_stream_new_event_1
            )
            .with_id(category_2_stream_stored_events_1[0].id)
            .with_sequence_number(
                category_2_stream_stored_events_1[0].sequence_number
            )
            .with_position(0)
            .build()
        )
        category_2_stream_expected_stored_events_2 = (
            category_2_stream_expected_stored_event_builder.from_new_event(
                category_2_stream_new_event_2
            )
            .with_id(category_2_stream_stored_events_2[0].id)
            .with_sequence_number(
                category_2_stream_stored_events_2[0].sequence_number
            )
            .with_position(1)
            .build()
        )
        category_1_stream_expected_stored_events_2 = (
            category_1_stream_expected_stored_event_builder.from_new_event(
                category_1_stream_new_event_2
            )
            .with_id(category_1_stream_stored_events_2[0].id)
            .with_sequence_number(
                category_1_stream_stored_events_2[0].sequence_number
            )
            .with_position(1)
            .build()
        )

        expected_events = [
            category_1_stream_expected_stored_events_1,
            category_2_stream_expected_stored_events_1,
            category_2_stream_expected_stored_events_2,
            category_1_stream_expected_stored_events_2,
        ]

        assert read_log == expected_events


class TestLogRead:
    async def test_reads_events_in_log_after_sequence_number(self):
        category_1_name = data.random_event_category_name()
        category_2_name = data.random_event_category_name()
        stream_1_name = data.random_event_stream_name()
        stream_2_name = data.random_event_stream_name()

        stream_1_new_events = [NewEventBuilder().build() for _ in range(10)]
        stream_2_new_events = [NewEventBuilder().build() for _ in range(10)]

        store = EventStore(adapter=InMemoryEventStorageAdapter())

        category_1 = store.category(category=category_1_name)
        category_2 = store.category(category=category_2_name)

        stored_events_1 = await category_1.stream(
            stream=stream_1_name
        ).publish(events=stream_1_new_events)
        stored_events_2 = await category_2.stream(
            stream=stream_2_name
        ).publish(events=stream_2_new_events)

        sequence_number = stored_events_1[4].sequence_number

        expected_events = list(stored_events_1[5:]) + list(stored_events_2)
        log = store.log()
        log_events = await log.read(
            constraints={constraints.sequence_number_after(sequence_number)}
        )

        assert log_events == expected_events


class TestLogLatest:
    async def test_reads_latest_event_in_log_if_present(self):
        category_1_name = data.random_event_category_name()
        category_2_name = data.random_event_category_name()
        stream_1_name = data.random_event_stream_name()
        stream_2_name = data.random_event_stream_name()

        stream_1_new_events = [NewEventBuilder().build() for _ in range(10)]
        stream_2_new_events = [NewEventBuilder().build() for _ in range(10)]

        store = EventStore(adapter=InMemoryEventStorageAdapter())

        category_1 = store.category(category=category_1_name)
        category_2 = store.category(category=category_2_name)

        await category_1.stream(stream=stream_1_name).publish(
            events=stream_1_new_events
        )
        stored_events_2 = await category_2.stream(
            stream=stream_2_name
        ).publish(events=stream_2_new_events)

        log = store.log()
        latest_event = await log.latest()

        assert latest_event == stored_events_2[-1]

    async def test_returns_none_if_log_is_empty(self):
        store = EventStore(adapter=InMemoryEventStorageAdapter())

        log = store.log()
        latest_event = await log.latest()

        assert latest_event is None


class TestLogIteration:
    async def test_can_treat_log_as_iterable_over_all_events(self):
        category_1_name = data.random_event_category_name()
        category_2_name = data.random_event_category_name()
        stream_1_name = data.random_event_stream_name()
        stream_2_name = data.random_event_stream_name()

        stream_1_new_events = [NewEventBuilder().build() for _ in range(10)]
        stream_2_new_events = [NewEventBuilder().build() for _ in range(10)]

        store = EventStore(adapter=InMemoryEventStorageAdapter())
        category_1 = store.category(category=category_1_name)
        category_2 = store.category(category=category_2_name)

        await category_1.stream(stream=stream_1_name).publish(
            events=stream_1_new_events
        )
        await category_2.stream(stream=stream_2_name).publish(
            events=stream_2_new_events
        )

        new_event_keys = [
            (event.name, stream_1_name, category_1_name)
            for event in stream_1_new_events
        ] + [
            (event.name, stream_2_name, category_2_name)
            for event in stream_2_new_events
        ]

        log = store.log()

        stream_event_keys = [
            (event.name, event.stream, event.category) async for event in log
        ]

        assert stream_event_keys == new_event_keys

    async def test_iterates_over_events_after_sequence_number(self):
        category_1_name = data.random_event_category_name()
        category_2_name = data.random_event_category_name()
        stream_1_name = data.random_event_stream_name()
        stream_2_name = data.random_event_stream_name()

        stream_1_new_events = [NewEventBuilder().build() for _ in range(10)]
        stream_2_new_events = [NewEventBuilder().build() for _ in range(10)]

        store = EventStore(adapter=InMemoryEventStorageAdapter())
        category_1 = store.category(category=category_1_name)
        category_2 = store.category(category=category_2_name)

        stream_1_stored_events = await category_1.stream(
            stream=stream_1_name
        ).publish(events=stream_1_new_events)
        (
            await category_2.stream(stream=stream_2_name).publish(
                events=stream_2_new_events
            )
        )

        sequence_number = stream_1_stored_events[4].sequence_number

        new_event_keys = [
            (event.name, stream_1_name, category_1_name)
            for event in stream_1_new_events[5:]
        ] + [
            (event.name, stream_2_name, category_2_name)
            for event in stream_2_new_events
        ]

        log = store.log()

        stream_event_keys = [
            (event.name, event.stream, event.category)
            async for event in log.iterate(
                constraints={
                    constraints.sequence_number_after(sequence_number)
                }
            )
        ]

        assert stream_event_keys == new_event_keys


class TestLogLogging:
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

        log = store.log()
        await log.latest()

        log_event = logger.find_event("event.log.reading-latest")

        assert log_event is not None
        assert log_event.level == LogLevel.DEBUG
        assert log_event.is_async is True
        assert log_event.context == {}

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

        log = store.log()
        log.iterate()

        log_event = logger.find_event("event.log.iterating")

        assert log_event is not None
        assert log_event.level == LogLevel.DEBUG
        assert log_event.is_async is False
        assert log_event.context == {
            "constraints": [],
        }


if __name__ == "__main__":
    sys.exit(pytest.main([__file__]))
