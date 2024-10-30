import sys
import pytest

from datetime import datetime

from logicblocks.event.types import NewEvent, StoredEvent
from logicblocks.event.store.adapters import InMemoryStorageAdapter


class TestInMemoryStorageAdapter:
    def test_stores_single_event_for_later_retrieval(self):
        adapter = InMemoryStorageAdapter()

        event_name = "something-happened"
        event_category = "some-category"
        event_stream = "some-stream"
        event_payload = {"foo": "bar"}
        event_observed_at = datetime.now()
        event_occurred_at = datetime.now()

        new_event = NewEvent(
            name=event_name,
            payload=event_payload,
            observed_at=event_observed_at,
            occurred_at=event_occurred_at,
        )

        stored_events = adapter.save(
            category=event_category, stream=event_stream, events=[new_event]
        )
        stored_event = stored_events[0]

        found_events = list(
            adapter.scan_stream(category=event_category, stream=event_stream)
        )
        expected_events = [
            StoredEvent(
                id=stored_event.id,
                name=event_name,
                stream=event_stream,
                category=event_category,
                payload=event_payload,
                position=0,
                sequence_number=stored_event.sequence_number,
                observed_at=event_observed_at,
                occurred_at=event_occurred_at,
            )
        ]

        assert found_events == expected_events


if __name__ == "__main__":
    sys.exit(pytest.main([__file__]))
