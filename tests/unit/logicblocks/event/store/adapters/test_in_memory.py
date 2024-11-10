import sys
import pytest

from logicblocks.event.store import conditions
from logicblocks.event.store.exceptions import UnmetWriteConditionError
from logicblocks.event.testing import NewEventBuilder
from logicblocks.event.testing.data import (
    random_event_category_name,
    random_event_stream_name,
)
from logicblocks.event.types import StoredEvent
from logicblocks.event.store.adapters import InMemoryStorageAdapter


class TestSave(object):
    def test_stores_single_event_for_later_retrieval(self):
        adapter = InMemoryStorageAdapter()

        event_category = random_event_category_name()
        event_stream = random_event_stream_name()

        new_event = NewEventBuilder().build()

        stored_events = adapter.save(
            category=event_category, stream=event_stream, events=[new_event]
        )
        stored_event = stored_events[0]

        actual_events = list(
            adapter.scan_stream(category=event_category, stream=event_stream)
        )
        expected_events = [
            StoredEvent(
                id=stored_event.id,
                name=new_event.name,
                category=event_category,
                stream=event_stream,
                position=0,
                sequence_number=stored_event.sequence_number,
                payload=new_event.payload,
                observed_at=new_event.observed_at,
                occurred_at=new_event.occurred_at,
            )
        ]

        assert actual_events == expected_events

    def test_stores_multiple_events_in_same_stream(self):
        adapter = InMemoryStorageAdapter()

        event_category = random_event_category_name()
        event_stream = random_event_stream_name()
        new_event_1 = NewEventBuilder().build()
        new_event_2 = NewEventBuilder().build()

        stored_events = adapter.save(
            category=event_category,
            stream=event_stream,
            events=[new_event_1, new_event_2],
        )
        stored_event_1 = stored_events[0]
        stored_event_2 = stored_events[1]

        actual_records = list(
            adapter.scan_stream(category=event_category, stream=event_stream)
        )
        expected_records = [
            StoredEvent(
                id=stored_event_1.id,
                name=new_event_1.name,
                category=event_category,
                stream=event_stream,
                position=0,
                sequence_number=stored_event_1.sequence_number,
                payload=new_event_1.payload,
                observed_at=new_event_1.observed_at,
                occurred_at=new_event_1.occurred_at,
            ),
            StoredEvent(
                id=stored_event_2.id,
                name=new_event_2.name,
                category=event_category,
                stream=event_stream,
                position=1,
                sequence_number=stored_event_2.sequence_number,
                payload=new_event_2.payload,
                observed_at=new_event_2.observed_at,
                occurred_at=new_event_2.occurred_at,
            ),
        ]

        assert actual_records == expected_records

    def test_stores_multiple_events_in_sequential_saves(self):
        adapter = InMemoryStorageAdapter()

        event_category = random_event_category_name()
        event_stream = random_event_stream_name()

        new_event_1 = NewEventBuilder().build()
        new_event_2 = NewEventBuilder().build()

        stored_events_1 = adapter.save(
            category=event_category,
            stream=event_stream,
            events=[new_event_1],
        )
        stored_event_1 = stored_events_1[0]

        stored_events_2 = adapter.save(
            category=event_category,
            stream=event_stream,
            events=[new_event_2],
        )
        stored_event_2 = stored_events_2[0]

        actual_records = list(
            adapter.scan_stream(category=event_category, stream=event_stream)
        )
        expected_records = [
            StoredEvent(
                id=stored_event_1.id,
                name=new_event_1.name,
                category=event_category,
                stream=event_stream,
                position=0,
                sequence_number=stored_event_1.sequence_number,
                payload=new_event_1.payload,
                observed_at=new_event_1.observed_at,
                occurred_at=new_event_1.occurred_at,
            ),
            StoredEvent(
                id=stored_event_2.id,
                name=new_event_2.name,
                category=event_category,
                stream=event_stream,
                position=1,
                sequence_number=stored_event_2.sequence_number,
                payload=new_event_2.payload,
                observed_at=new_event_2.observed_at,
                occurred_at=new_event_2.occurred_at,
            ),
        ]

        assert actual_records == expected_records


class TestWriteConditions(object):
    def test_writes_when_empty_stream_condition_and_stream_empty(self):
        adapter = InMemoryStorageAdapter()

        event_category = random_event_category_name()
        event_stream = random_event_stream_name()

        new_event = NewEventBuilder().build()

        stored_events = adapter.save(
            category=event_category,
            stream=event_stream,
            events=[new_event],
            conditions={conditions.stream_is_empty()},
        )
        stored_event = stored_events[0]

        actual_records = list(
            adapter.scan_stream(category=event_category, stream=event_stream)
        )
        expected_records = [
            StoredEvent(
                id=stored_event.id,
                name=new_event.name,
                category=event_category,
                stream=event_stream,
                position=0,
                sequence_number=stored_event.sequence_number,
                payload=new_event.payload,
                observed_at=new_event.observed_at,
                occurred_at=new_event.occurred_at,
            )
        ]

        assert actual_records == expected_records

    def test_writes_when_empty_stream_condition_and_category_not_empty(self):
        adapter = InMemoryStorageAdapter()

        event_category = random_event_category_name()
        event_stream_1 = random_event_stream_name()
        event_stream_2 = random_event_stream_name()

        new_event_1 = NewEventBuilder().build()
        new_event_2 = NewEventBuilder().build()

        adapter.save(
            category=event_category,
            stream=event_stream_1,
            events=[new_event_1],
        )

        stored_events = adapter.save(
            category=event_category,
            stream=event_stream_2,
            events=[new_event_2],
            conditions={conditions.stream_is_empty()},
        )
        stored_event = stored_events[0]

        actual_records = list(
            adapter.scan_stream(category=event_category, stream=event_stream_2)
        )
        expected_records = [
            StoredEvent(
                id=stored_event.id,
                name=new_event_2.name,
                category=event_category,
                stream=event_stream_2,
                position=0,
                sequence_number=stored_event.sequence_number,
                payload=new_event_2.payload,
                observed_at=new_event_2.observed_at,
                occurred_at=new_event_2.occurred_at,
            )
        ]

        assert actual_records == expected_records

    def test_writes_when_empty_stream_condition_and_log_not_empty(self):
        adapter = InMemoryStorageAdapter()

        event_category_1 = random_event_category_name()
        event_category_2 = random_event_category_name()
        event_stream_1 = random_event_stream_name()
        event_stream_2 = random_event_stream_name()

        new_event_1 = NewEventBuilder().build()
        new_event_2 = NewEventBuilder().build()

        adapter.save(
            category=event_category_1,
            stream=event_stream_1,
            events=[new_event_1],
        )

        stored_events = adapter.save(
            category=event_category_2,
            stream=event_stream_2,
            events=[new_event_2],
            conditions={conditions.stream_is_empty()},
        )
        stored_event = stored_events[0]

        actual_records = list(
            adapter.scan_stream(
                category=event_category_2, stream=event_stream_2
            )
        )
        expected_records = [
            StoredEvent(
                id=stored_event.id,
                name=new_event_2.name,
                category=event_category_2,
                stream=event_stream_2,
                position=0,
                sequence_number=stored_event.sequence_number,
                payload=new_event_2.payload,
                observed_at=new_event_2.observed_at,
                occurred_at=new_event_2.occurred_at,
            )
        ]

        assert actual_records == expected_records

    def test_raises_when_empty_stream_condition_and_stream_not_empty(self):
        adapter = InMemoryStorageAdapter()

        event_category = random_event_category_name()
        event_stream = random_event_stream_name()

        adapter.save(
            category=event_category,
            stream=event_stream,
            events=[NewEventBuilder().build()],
        )

        with pytest.raises(UnmetWriteConditionError):
            adapter.save(
                category=event_category,
                stream=event_stream,
                events=[NewEventBuilder().build()],
                conditions={conditions.stream_is_empty()},
            )


if __name__ == "__main__":
    sys.exit(pytest.main([__file__]))
