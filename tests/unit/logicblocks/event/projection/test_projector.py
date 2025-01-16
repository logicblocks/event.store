import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import reduce

import pytest

from logicblocks.event.projection import (
    MissingHandlerBehaviour,
    MissingProjectionHandlerError,
    Projector,
)
from logicblocks.event.store import EventStore
from logicblocks.event.store.adapters import InMemoryEventStorageAdapter
from logicblocks.event.testing import NewEventBuilder, data
from logicblocks.event.testing.builders import StoredEventBuilder
from logicblocks.event.types import (
    EventSequenceIdentifier,
    Projection,
    StoredEvent,
    StreamIdentifier,
)

generic_event = (
    StoredEventBuilder()
    .with_category(data.random_event_category_name())
    .with_stream(data.random_event_stream_name())
    .with_payload({})
)


@dataclass
class Aggregate:
    def __init__(
        self,
        something_occurred_at: datetime | None = None,
        something_else_occurred_at: datetime | None = None,
    ):
        self.something_occurred_at = something_occurred_at
        self.something_else_occurred_at = something_else_occurred_at


class AggregateProjector(Projector[Aggregate]):
    def initial_state_factory(self) -> Aggregate:
        return Aggregate()

    def id_factory(
        self, state: Aggregate, coordinates: EventSequenceIdentifier
    ) -> str:
        match coordinates:
            case StreamIdentifier(stream=stream):
                return stream
            case _:
                raise ValueError("Unexpected coordinates.")

    @staticmethod
    def something_occurred(state: Aggregate, event: StoredEvent) -> Aggregate:
        return Aggregate(
            something_occurred_at=event.occurred_at,
            something_else_occurred_at=state.something_else_occurred_at,
        )

    @staticmethod
    def something_else_occurred(
        state: Aggregate, event: StoredEvent
    ) -> Aggregate:
        return Aggregate(
            something_occurred_at=state.something_occurred_at,
            something_else_occurred_at=event.occurred_at,
        )


class TestProjectorEventApplication:
    def test_applies_one_event_when_handler_available(self):
        occurred_at = datetime.now(UTC)
        position = 0

        stored_event = (
            generic_event.with_name("something-occurred")
            .with_position(position)
            .with_occurred_at(occurred_at)
            .build()
        )

        projector = AggregateProjector()

        actual_state = projector.apply(event=stored_event)
        expected_state = Aggregate(something_occurred_at=occurred_at)

        assert expected_state == actual_state

    def test_applies_many_events_when_handlers_available(self):
        something_occurred_at = datetime.now(UTC)
        something_stored_event = (
            generic_event.with_name("something-occurred")
            .with_position(1)
            .with_occurred_at(something_occurred_at)
            .build()
        )

        something_else_occurred_at = datetime.now(UTC)
        second_position = 2
        something_else_stored_event = (
            generic_event.with_name("something-else-occurred")
            .with_position(second_position)
            .with_occurred_at(something_else_occurred_at)
            .build()
        )

        projector = AggregateProjector()

        actual_state = reduce(
            lambda state, event: projector.apply(state=state, event=event),
            [something_stored_event, something_else_stored_event],
            projector.initial_state_factory(),
        )
        expected_state = Aggregate(
            something_occurred_at=something_occurred_at,
            something_else_occurred_at=something_else_occurred_at,
        )

        assert expected_state == actual_state

    def test_allows_camel_case_event_names(self):
        occurred_at = datetime.now(UTC)
        position = 0

        stored_event = (
            generic_event.with_name("SomethingOccurred")
            .with_position(position)
            .with_occurred_at(occurred_at)
            .build()
        )

        projector = AggregateProjector()

        actual_state = projector.apply(event=stored_event)
        expected_state = Aggregate(something_occurred_at=occurred_at)

        assert expected_state == actual_state

    def test_allows_lower_camel_case_event_names(self):
        occurred_at = datetime.now(UTC)
        position = 0

        stored_event = (
            generic_event.with_name("somethingOccurred")
            .with_position(position)
            .with_occurred_at(occurred_at)
            .build()
        )

        projector = AggregateProjector()

        actual_state = projector.apply(event=stored_event)
        expected_state = Aggregate(something_occurred_at=occurred_at)

        assert expected_state == actual_state

    def test_allows_snake_case_event_names(self):
        occurred_at = datetime.now(UTC)
        position = 0

        stored_event = (
            generic_event.with_name("something_occurred")
            .with_position(position)
            .with_occurred_at(occurred_at)
            .build()
        )

        projector = AggregateProjector()

        actual_state = projector.apply(event=stored_event)
        expected_state = Aggregate(something_occurred_at=occurred_at)

        assert expected_state == actual_state

    def test_allows_space_separated_event_names(self):
        occurred_at = datetime.now(UTC)
        position = 0

        stored_event = (
            generic_event.with_name("something occurred")
            .with_position(position)
            .with_occurred_at(occurred_at)
            .build()
        )

        projector = AggregateProjector()

        actual_state = projector.apply(event=stored_event)
        expected_state = Aggregate(something_occurred_at=occurred_at)

        assert expected_state == actual_state

    def test_allows_allows_capitalised_event_names(self):
        occurred_at = datetime.now(UTC)
        position = 0

        stored_event = (
            generic_event.with_name("SOMETHING-OCCURRED")
            .with_position(position)
            .with_occurred_at(occurred_at)
            .build()
        )

        projector = AggregateProjector()

        actual_state = projector.apply(event=stored_event)
        expected_state = Aggregate(something_occurred_at=occurred_at)

        assert expected_state == actual_state

    def test_allows_special_characters_in_event_names(self):
        occurred_at = datetime.now(UTC)
        position = 0

        stored_event = (
            generic_event.with_name("something$occurred")
            .with_position(position)
            .with_occurred_at(occurred_at)
            .build()
        )

        projector = AggregateProjector()

        actual_state = projector.apply(event=stored_event)
        expected_state = Aggregate(something_occurred_at=occurred_at)

        assert expected_state == actual_state

    def test_raises_on_missing_handler_by_default(self):
        stored_event = generic_event.with_name(
            "something-weird-occurred"
        ).build()

        class StrictAggregateProjector(AggregateProjector):
            pass

        projector = StrictAggregateProjector()

        with pytest.raises(MissingProjectionHandlerError):
            projector.apply(event=stored_event)

    def test_ignores_event_on_missing_handler_when_requested(self):
        stored_event = generic_event.with_name(
            "something-weird-occurred"
        ).build()

        class LenientAggregateProjector(AggregateProjector):
            missing_handler_behaviour = MissingHandlerBehaviour.IGNORE

        projector = LenientAggregateProjector()

        actual_state = projector.apply(event=stored_event, state=Aggregate())
        expected_state = Aggregate()

        assert expected_state == actual_state


class TestProjectorProjection:
    async def test_projects_from_event_source(self):
        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()

        store = EventStore(adapter=InMemoryEventStorageAdapter())
        stream = store.stream(category=category_name, stream=stream_name)

        something_occurred_at = datetime.now(UTC)
        something_else_occurred_at = datetime.now(UTC)

        new_events = [
            (
                NewEventBuilder()
                .with_name("something-occurred")
                .with_occurred_at(something_occurred_at)
                .build()
            ),
            (
                NewEventBuilder()
                .with_name("something-else-occurred")
                .with_occurred_at(something_else_occurred_at)
                .build()
            ),
        ]

        await stream.publish(events=new_events)

        projector = AggregateProjector()

        actual_projection = await projector.project(source=stream)
        expected_projection = Projection[Aggregate](
            id=stream_name,
            state=Aggregate(
                something_occurred_at=something_occurred_at,
                something_else_occurred_at=something_else_occurred_at,
            ),
            version=2,
            source=StreamIdentifier(
                category=category_name, stream=stream_name
            ),
            name="aggregate",
        )

        assert expected_projection == actual_projection

    async def test_uses_defined_type_when_property_overridden(self):
        @dataclass
        class Thing:
            value: int = 5

        class CustomTypeProjector(Projector[Thing]):
            name = "specific-thing"

            def initial_state_factory(self) -> Thing:
                return Thing()

            def id_factory(
                self, state: Thing, coordinates: EventSequenceIdentifier
            ):
                match coordinates:
                    case StreamIdentifier(stream=stream):
                        return stream
                    case _:
                        raise ValueError("Unexpected coordinates.")

            @staticmethod
            def thing_got_value(state: Thing, event: StoredEvent) -> Thing:
                state.value = event.payload["value"]
                return state

        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()

        store = EventStore(adapter=InMemoryEventStorageAdapter())
        stream = store.stream(category=category_name, stream=stream_name)

        new_events = [
            (
                NewEventBuilder()
                .with_name("thing-got-value")
                .with_payload({"value": 42})
                .build()
            )
        ]

        await stream.publish(events=new_events)

        projector = CustomTypeProjector()

        actual_projection = await projector.project(source=stream)
        expected_projection = Projection[Thing](
            id=stream_name,
            state=Thing(value=42),
            version=1,
            source=stream.identifier,
            name="specific-thing",
        )

        assert expected_projection == actual_projection


if __name__ == "__main__":
    sys.exit(pytest.main([__file__]))
