from logicblocks.event.store import conditions
from logicblocks.event.store.types import stream_publish_definition
from logicblocks.event.testing import NewEventBuilder


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
