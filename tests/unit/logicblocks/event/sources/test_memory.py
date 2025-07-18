from logicblocks.event.sources import InMemoryEventSource, constraints
from logicblocks.event.testing.builders import StoredEventBuilder
from logicblocks.event.types import CategoryIdentifier, StoredEvent


class TestInMemoryEventSource:
    async def test_identifier_exposes_provided_identifier(self):
        identifier = CategoryIdentifier(category="test")
        source = InMemoryEventSource[CategoryIdentifier, StoredEvent](
            events=[], identifier=identifier
        )

        assert source.identifier == identifier

    async def test_latest_fetches_latest_event_from_provided_events(self):
        event_1 = StoredEventBuilder().build()
        event_2 = StoredEventBuilder().build()
        event_3 = StoredEventBuilder().build()

        source = InMemoryEventSource[CategoryIdentifier, StoredEvent](
            events=[event_1, event_2, event_3],
            identifier=CategoryIdentifier(category="test"),
        )

        assert await source.latest() == event_3

    async def test_latest_returns_none_when_no_provided_events(self):
        source = InMemoryEventSource[CategoryIdentifier, StoredEvent](
            events=[], identifier=CategoryIdentifier(category="test")
        )

        assert await source.latest() is None

    async def test_iterate_yields_provided_events(self):
        event_1 = StoredEventBuilder().build()
        event_2 = StoredEventBuilder().build()
        event_3 = StoredEventBuilder().build()

        source = InMemoryEventSource[CategoryIdentifier, StoredEvent](
            events=[event_1, event_2, event_3],
            identifier=CategoryIdentifier(category="test"),
        )

        events = [event async for event in source.iterate()]

        assert events == [event_1, event_2, event_3]

    async def test_iterate_yields_provided_events_with_constraints(self):
        event_1 = StoredEventBuilder().with_sequence_number(0).build()
        event_2 = StoredEventBuilder().with_sequence_number(1).build()
        event_3 = StoredEventBuilder().with_sequence_number(2).build()
        event_4 = StoredEventBuilder().with_sequence_number(3).build()

        source = InMemoryEventSource[CategoryIdentifier, StoredEvent](
            events=[event_1, event_2, event_3, event_4],
            identifier=CategoryIdentifier(category="test"),
        )

        constraint = constraints.sequence_number_after(1)
        events = [
            event async for event in source.iterate(constraints={constraint})
        ]

        assert events == [event_3, event_4]

    async def test_iterate_yields_nothing_when_no_provided_events(self):
        source = InMemoryEventSource[CategoryIdentifier, StoredEvent](
            events=[], identifier=CategoryIdentifier(category="test")
        )

        events = [event async for event in source.iterate()]

        assert events == []

    def test_is_equal_when_has_same_events_and_identifier(self):
        event_1 = StoredEventBuilder().build()
        event_2 = StoredEventBuilder().build()
        event_3 = StoredEventBuilder().build()

        source_1 = InMemoryEventSource[CategoryIdentifier, StoredEvent](
            events=[event_1, event_2, event_3],
            identifier=CategoryIdentifier(category="test"),
        )

        source_2 = InMemoryEventSource[CategoryIdentifier, StoredEvent](
            events=[event_1, event_2, event_3],
            identifier=CategoryIdentifier(category="test"),
        )

        assert source_1 == source_2

    def test_is_not_equal_when_has_same_events_but_different_identifier(self):
        event_1 = StoredEventBuilder().build()
        event_2 = StoredEventBuilder().build()
        event_3 = StoredEventBuilder().build()

        identifier_1 = CategoryIdentifier(category="test-1")
        identifier_2 = CategoryIdentifier(category="test-2")

        source_1 = InMemoryEventSource[CategoryIdentifier, StoredEvent](
            events=[event_1, event_2, event_3], identifier=identifier_1
        )

        source_2 = InMemoryEventSource[CategoryIdentifier, StoredEvent](
            events=[event_1, event_2, event_3], identifier=identifier_2
        )

        assert source_1 != source_2

    def test_is_not_equal_when_has_different_events_but_same_identifier(self):
        event_1 = StoredEventBuilder().build()
        event_2 = StoredEventBuilder().build()
        event_3 = StoredEventBuilder().build()

        source_1 = InMemoryEventSource[CategoryIdentifier, StoredEvent](
            events=[event_1, event_2],
            identifier=CategoryIdentifier(category="test"),
        )

        source_2 = InMemoryEventSource[CategoryIdentifier, StoredEvent](
            events=[event_2, event_3],
            identifier=CategoryIdentifier(category="test"),
        )

        assert source_1 != source_2
