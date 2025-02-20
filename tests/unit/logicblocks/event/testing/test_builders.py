import sys
from datetime import UTC, datetime

import pytest

from logicblocks.event.testing import (
    MappingProjectionBuilder,
    NewEventBuilder,
    StoredEventBuilder,
)
from logicblocks.event.types import StreamIdentifier


class TestNewEventBuilder:
    def test_builds_new_event_with_defaults(self):
        builder = NewEventBuilder()

        event = builder.build()

        assert event.name is not None
        assert event.payload is not None
        assert event.observed_at is not None
        assert event.occurred_at is not None

    def test_randomises_event_name(self):
        names = [NewEventBuilder().build().name for _ in range(100)]

        assert len(set(names)) == 100

    def test_randomises_event_payload(self):
        payload_keys = [
            map(str, NewEventBuilder().build().payload.keys())
            for _ in range(100)
        ]

        assert len(set(payload_keys)) == 100

    def test_builds_new_event_with_specified_name(self):
        builder = NewEventBuilder().with_name("event-name")

        event = builder.build()

        assert event.name == "event-name"

    def test_builds_new_event_with_specified_payload(self):
        builder = NewEventBuilder().with_payload({"key": "value"})

        event = builder.build()

        assert event.payload == {"key": "value"}

    def test_builds_new_event_with_specified_occurred_at(self):
        occurred_at = datetime.now(UTC)
        builder = NewEventBuilder().with_occurred_at(occurred_at)

        event = builder.build()

        assert event.occurred_at == occurred_at


class TestStoredEventBuilder:
    def test_builds_stored_event_with_defaults(self):
        builder = StoredEventBuilder()

        event = builder.build()

        assert event.id is not None
        assert event.name is not None
        assert event.stream is not None
        assert event.category is not None
        assert event.position is not None
        assert event.sequence_number is not None
        assert event.payload is not None
        assert event.occurred_at is not None
        assert event.observed_at is not None

    def test_randomises_event_name(self):
        names = [StoredEventBuilder().build().name for _ in range(100)]

        assert len(set(names)) == 100

    def test_randomises_event_stream(self):
        streams = [StoredEventBuilder().build().stream for _ in range(100)]

        assert len(set(streams)) == 100

    def test_randomises_event_category(self):
        categories = [
            StoredEventBuilder().build().category for _ in range(100)
        ]

        assert len(set(categories)) == 100

    def test_randomises_event_position(self):
        positions = [StoredEventBuilder().build().position for _ in range(100)]

        assert len(set(positions)) == 100

    def test_randomises_event_sequence_number(self):
        positions = [StoredEventBuilder().build().position for _ in range(100)]

        assert len(set(positions)) == 100

    def test_randomises_event_payload(self):
        payload_keys = [
            map(str, StoredEventBuilder().build().payload.keys())
            for _ in range(100)
        ]

        assert len(set(payload_keys)) == 100

    def test_builds_stored_event_with_specified_name(self):
        builder = StoredEventBuilder().with_name("event-name")

        event = builder.build()

        assert event.name == "event-name"

    def test_builds_stored_event_with_specified_stream(self):
        builder = StoredEventBuilder().with_stream("event-stream")

        event = builder.build()

        assert event.stream == "event-stream"

    def test_builds_stored_event_with_specified_category(self):
        builder = StoredEventBuilder().with_category("event-category")

        event = builder.build()

        assert event.category == "event-category"

    def test_builds_stored_event_with_specified_position(self):
        builder = StoredEventBuilder().with_position(42)

        event = builder.build()

        assert event.position == 42

    def test_builds_stored_event_with_specified_sequence_number(self):
        builder = StoredEventBuilder().with_sequence_number(42)

        event = builder.build()

        assert event.sequence_number == 42

    def test_builds_stored_event_with_specified_payload(self):
        builder = StoredEventBuilder().with_payload({"key": "value"})

        event = builder.build()

        assert event.payload == {"key": "value"}

    def test_builds_stored_event_with_specified_occurred_at(self):
        occurred_at = datetime.now(UTC)
        builder = StoredEventBuilder().with_occurred_at(occurred_at)

        event = builder.build()

        assert event.occurred_at == occurred_at

    def test_builds_stored_event_with_specified_observed_at(self):
        observed_at = datetime.now(UTC)
        builder = StoredEventBuilder().with_observed_at(observed_at)

        event = builder.build()

        assert event.observed_at == observed_at

    def test_builds_stored_event_from_new_event(self):
        new_event = NewEventBuilder().build()

        builder = StoredEventBuilder().from_new_event(new_event)

        event = builder.build()

        assert event.name == new_event.name
        assert event.payload == new_event.payload
        assert event.occurred_at == new_event.occurred_at
        assert event.observed_at == new_event.observed_at


class TestMappingProjectionBuilder:
    def test_builds_projection_with_defaults(self):
        builder = MappingProjectionBuilder()

        projection = builder.build()

        assert projection.id is not None
        assert projection.name is not None
        assert projection.source is not None

    def test_randomises_projection_id(self):
        ids = [MappingProjectionBuilder().build().id for _ in range(100)]

        assert len(set(ids)) == 100

    def test_randomises_projection_name(self):
        names = [MappingProjectionBuilder().build().name for _ in range(100)]

        assert len(set(names)) == 100

    def test_randomises_projection_state(self):
        state_keys = [
            map(str, MappingProjectionBuilder().build().state.keys())
            for _ in range(100)
        ]

        assert len(set(state_keys)) == 100

    def test_randomises_projection_source(self):
        sources = [
            MappingProjectionBuilder().build().source for _ in range(100)
        ]

        assert len(set(sources)) == 100

    def test_builds_projection_with_specified_id(self):
        builder = MappingProjectionBuilder().with_id("projection-id")

        projection = builder.build()

        assert projection.id == "projection-id"

    def test_builds_projection_with_specified_name(self):
        builder = MappingProjectionBuilder().with_name("projection-name")

        projection = builder.build()

        assert projection.name == "projection-name"

    def test_builds_projection_with_specified_state(self):
        builder = MappingProjectionBuilder().with_state({"key": "value"})

        projection = builder.build()

        assert projection.state == {"key": "value"}

    def test_builds_projection_with_specified_source(self):
        builder = MappingProjectionBuilder().with_source(
            StreamIdentifier(category="category", stream="stream")
        )

        projection = builder.build()

        assert projection.source == StreamIdentifier(
            category="category", stream="stream"
        )


if __name__ == "__main__":
    sys.exit(pytest.main([__file__]))
