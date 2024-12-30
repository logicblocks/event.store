import sys
from datetime import UTC, datetime
from typing import Any, Callable, Mapping

import pytest

from logicblocks.event.projection import Projector
from logicblocks.event.projection.exceptions import MissingHandlerError
from logicblocks.event.testing.builders import StoredEventBuilder
from logicblocks.event.testing.data import random_int
from logicblocks.event.types import Projection, StoredEvent

generic_event = (
    StoredEventBuilder()
    .with_category("category")
    .with_stream("stream")
    .with_payload({})
)

handlers: Mapping[
    str, Callable[[Mapping[str, Any], StoredEvent], Mapping[str, Any]]
] = {
    "something-occurred": lambda state, event: {
        **state,
        "something_occurred_at": event.occurred_at,
    },
    "something-else-occurred": lambda state, event: {
        **state,
        "something_else_occurred_at": event.occurred_at,
    },
}


class TestProjector(object):
    def test_projection_with_one_event(self):
        occurred_at = datetime.now(UTC)
        position = random_int()
        event = (
            generic_event.with_name("something-occurred")
            .with_position(position)
            .with_occurred_at(occurred_at)
            .build()
        )
        projector = Projector(handlers=handlers)

        actual_projection = projector.project({}, [event])
        expected_projection = Projection(
            state={"something_occurred_at": occurred_at}, position=position
        )

        assert expected_projection == actual_projection

    def test_projection_with_two_events(self):
        something_occurred_at = datetime.now(UTC)
        something_event = (
            generic_event.with_name("something-occurred")
            .with_position(1)
            .with_occurred_at(something_occurred_at)
            .build()
        )

        something_else_occurred_at = datetime.now(UTC)
        second_position = 2
        something_else_event = (
            generic_event.with_name("something-else-occurred")
            .with_position(second_position)
            .with_occurred_at(something_else_occurred_at)
            .build()
        )

        projector = Projector(handlers=handlers)

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
            generic_event.with_name("something-occurred")
            .with_position(1)
            .with_occurred_at(datetime.now(UTC))
            .build()
        )

        second_position = 2
        something_occurred_at = datetime.now(UTC)
        something_event_new = (
            generic_event.with_name("something-occurred")
            .with_position(second_position)
            .with_occurred_at(something_occurred_at)
            .build()
        )

        projector = Projector(handlers=handlers)

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

    def test_raises_on_missing_handler(self):
        event = generic_event.with_name("something-occurred").build()
        projector = Projector(handlers={})

        with pytest.raises(MissingHandlerError):
            projector.project({}, [event])


if __name__ == "__main__":
    sys.exit(pytest.main([__file__]))
