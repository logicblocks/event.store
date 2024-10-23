import unittest

from datetime import datetime, UTC

from logicblocks.event.testing.builders import StoredEventBuilder
from logicblocks.event.projection import Projection

generic_event = (
    StoredEventBuilder()
    .with_category("category")
    .with_stream("stream")
    .with_payload({})
)


class MyTestProjection(Projection):
    def something_occurred(self, state, event):
        return {**state, "something_occurred_at": event.occurred_at}

    def something_else_occurred(self, state, event):
        return {**state, "something_else_occurred_at": event.occurred_at}


class TestProjection(unittest.TestCase):
    def test_projection_with_one_event(self):
        occurred_at = datetime.now(UTC)
        event = (
            generic_event.with_name("something_occurred")
            .with_occurred_at(occurred_at)
            .build()
        )
        projection = MyTestProjection()

        expected_projection = {"something_occurred_at": occurred_at}

        self.assertEqual(
            expected_projection, projection.project({}, [event]).projection
        )

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

        projection = MyTestProjection()

        expected_projection = {
            "something_occurred_at": something_occurred_at,
            "something_else_occurred_at": something_else_occurred_at,
        }

        self.assertEqual(
            expected_projection,
            projection.project(
                {}, [something_event, something_else_event]
            ).projection,
        )


if __name__ == "__main__":
    unittest.main()
