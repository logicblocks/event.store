from logicblocks.event.store import conditions
from logicblocks.event.store.types import (
    resolve_batch_metadata,
    stream_publish_definition,
)
from logicblocks.event.testing import NewEventBuilder
from logicblocks.event.types import NewEvent


def new_event_without_metadata(name: str = "event") -> NewEvent:
    return NewEvent(name=name, payload={"value": name}, metadata=None)


class TestResolveBatchMetadata:
    def test_returns_events_unchanged_when_batch_metadata_is_none(self):
        events = [
            new_event_without_metadata("event-1"),
            NewEventBuilder().with_metadata({"a": "b"}).build(),
        ]

        resolved = resolve_batch_metadata(events, None)

        assert resolved == events

    def test_fills_none_event_with_batch_metadata_preserving_other_fields(
        self,
    ):
        batch = {"actor": "svc"}
        event = new_event_without_metadata()

        resolved = resolve_batch_metadata([event], batch)

        expected = NewEvent(
            name=event.name,
            payload=event.payload,
            metadata=batch,
            observed_at=event.observed_at,
            occurred_at=event.occurred_at,
        )
        assert resolved == [expected]

    def test_leaves_event_with_own_metadata_untouched(self):
        own = {"actor": "own"}
        event = NewEventBuilder().with_metadata(own).build()

        resolved = resolve_batch_metadata([event], {"actor": "svc"})

        assert resolved == [event]

    def test_fills_only_none_events_in_mixed_batch(self):
        batch = {"actor": "svc"}
        none_event = new_event_without_metadata()
        own_event = NewEventBuilder().with_metadata({"actor": "own"}).build()

        resolved = resolve_batch_metadata([none_event, own_event], batch)

        expected_filled = NewEvent(
            name=none_event.name,
            payload=none_event.payload,
            metadata=batch,
            observed_at=none_event.observed_at,
            occurred_at=none_event.occurred_at,
        )
        assert resolved == [expected_filled, own_event]

    def test_returns_empty_list_for_empty_events(self):
        assert resolve_batch_metadata([], {"actor": "svc"}) == []

    def test_fills_none_event_with_empty_mapping_batch_metadata(self):
        none_event = new_event_without_metadata()

        resolved = resolve_batch_metadata([none_event], {})

        expected = NewEvent(
            name=none_event.name,
            payload=none_event.payload,
            metadata={},
            observed_at=none_event.observed_at,
            occurred_at=none_event.occurred_at,
        )
        assert resolved == [expected]


class TestStreamPublishDefinition:
    def test_creates_definition_with_events_only(self):
        events = [
            NewEventBuilder().with_name("event-1").build(),
            NewEventBuilder().with_name("event-2").build(),
        ]

        definition = stream_publish_definition(events=events)

        assert definition == {"events": events}

    def test_creates_definition_with_events_and_condition(self):
        events = [
            NewEventBuilder().with_name("event-1").build(),
            NewEventBuilder().with_name("event-2").build(),
        ]
        condition = conditions.stream_is_empty()

        definition = stream_publish_definition(
            events=events, condition=condition
        )

        assert definition == {"events": events, "condition": condition}

    def test_omits_condition_when_none(self):
        events = [
            NewEventBuilder().with_name("event-1").build(),
        ]

        definition = stream_publish_definition(events=events, condition=None)

        assert definition == {"events": events}
        assert "condition" not in definition

    def test_omits_metadata_when_not_provided(self):
        events = [
            NewEventBuilder().with_name("event-1").build(),
        ]

        definition = stream_publish_definition(events=events)

        assert "metadata" not in definition

    def test_creates_definition_with_events_and_metadata(self):
        events = [
            NewEventBuilder().with_name("event-1").build(),
        ]

        definition = stream_publish_definition(
            events=events, metadata={"a": "b"}
        )

        assert definition == {"events": events, "metadata": {"a": "b"}}

    def test_creates_definition_with_events_condition_and_metadata(self):
        events = [
            NewEventBuilder().with_name("event-1").build(),
        ]
        condition = conditions.stream_is_empty()

        definition = stream_publish_definition(
            events=events, condition=condition, metadata={"a": "b"}
        )

        assert definition == {
            "events": events,
            "condition": condition,
            "metadata": {"a": "b"},
        }
