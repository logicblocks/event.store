import sys
import pytest

from datetime import datetime, UTC

from logicblocks.event.testing.builders import StoredEventBuilder
from logicblocks.event.projection import Projector
from logicblocks.event.types import Projection

from logicblocks.event.testing.data import random_int

generic_event = (
    StoredEventBuilder()
    .with_category("category")
    .with_stream("stream")
    .with_payload({})
)


class MyTestProjector(Projector):
    def something_occurred(self, state, event):
        return {**state, "something_occurred_at": event.occurred_at}

    def something_else_occurred(self, state, event):
        return {**state, "something_else_occurred_at": event.occurred_at}


class TestProjector(object):
    def test_projection_with_one_event(self):
        occurred_at = datetime.now(UTC)
        position = random_int()
        event = (
            generic_event.with_name("something_occurred")
            .with_position(position)
            .with_occurred_at(occurred_at)
            .build()
        )
        projector = MyTestProjector()

        actual_projection = projector.project({}, [event])
        expected_projection = Projection(
            state={"something_occurred_at": occurred_at}, position=position
        )

        assert expected_projection == actual_projection

    def test_projection_with_two_events(self):
        something_occurred_at = datetime.now(UTC)
        something_event = (
            generic_event.with_name("something_occurred")
            .with_position(1)
            .with_occurred_at(something_occurred_at)
            .build()
        )

        something_else_occurred_at = datetime.now(UTC)
        second_position = 2
        something_else_event = (
            generic_event.with_name("something_else_occurred")
            .with_position(second_position)
            .with_occurred_at(something_else_occurred_at)
            .build()
        )

        projector = MyTestProjector()

        expected_projection = Projection(
            state={
                "something_occurred_at": something_occurred_at,
                "something_else_occurred_at": something_else_occurred_at,
            },
            position=second_position,
        )

        actual_projection = projector.project(
            {}, [something_event, something_else_event]
        )

        assert expected_projection == actual_projection

    def test_projection_can_overwrite_fields(self):
        something_event = (
            generic_event.with_name("something_occurred")
            .with_position(1)
            .with_occurred_at(datetime.now(UTC))
            .build()
        )

        second_position = 2
        something_occurred_at = datetime.now(UTC)
        something_event_new = (
            generic_event.with_name("something_occurred")
            .with_position(second_position)
            .with_occurred_at(something_occurred_at)
            .build()
        )

        projector = MyTestProjector()

        expected_projection = Projection(
            state={
                "something_occurred_at": something_occurred_at,
            },
            position=second_position,
        )

        actual_projection = projector.project(
            {}, [something_event, something_event_new]
        )

        assert expected_projection == actual_projection


if __name__ == "__main__":
    sys.exit(pytest.main([__file__]))
