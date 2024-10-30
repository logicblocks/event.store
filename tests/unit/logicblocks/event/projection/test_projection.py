import sys
import pytest

from datetime import datetime, UTC

from logicblocks.event.testing.builders import StoredEventBuilder
from logicblocks.event.projection import Projector
from logicblocks.event.types import Projection

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
        event = (
            generic_event.with_name("something_occurred")
            .with_occurred_at(occurred_at)
            .build()
        )
        projector = MyTestProjector()

        actual_projection = projector.project({}, [event])
        expected_projection = Projection(
            state={"something_occurred_at": occurred_at}
        )

        assert expected_projection == actual_projection

    def test_projection_with_two_events(self):
        something_occurred_at = datetime.now(UTC)
        something_event = (
            generic_event.with_name("something_occurred")
            .with_occurred_at(something_occurred_at)
            .build()
        )

        something_else_occurred_at = datetime.now(UTC)
        something_else_event = (
            generic_event.with_name("something_else_occurred")
            .with_occurred_at(something_else_occurred_at)
            .build()
        )

        projector = MyTestProjector()

        expected_projection = Projection(
            state={
                "something_occurred_at": something_occurred_at,
                "something_else_occurred_at": something_else_occurred_at,
            }
        )

        actual_projection = projector.project(
            {}, [something_event, something_else_event]
        )

        assert expected_projection == actual_projection

    def test_projection_can_overwrite_fields(self):
        something_event = (
            generic_event.with_name("something_occurred")
            .with_occurred_at(datetime.now(UTC))
            .build()
        )

        something_occurred_at = datetime.now(UTC)
        something_event_new = (
            generic_event.with_name("something_occurred")
            .with_occurred_at(something_occurred_at)
            .build()
        )

        projector = MyTestProjector()

        expected_projection = Projection(
            state={
                "something_occurred_at": something_occurred_at,
            }
        )

        actual_projection = projector.project(
            {}, [something_event, something_event_new]
        )

        assert expected_projection == actual_projection


if __name__ == "__main__":
    sys.exit(pytest.main([__file__]))
