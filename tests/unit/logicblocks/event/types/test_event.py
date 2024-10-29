import sys
import pytest

from collections import namedtuple

from datetime import datetime, tzinfo, UTC, timedelta

from logicblocks.event.types import NewEvent, StoredEvent
from logicblocks.event.utils import StaticClock


class VerifyingStaticClock(StaticClock):
    def __init__(self, now: datetime):
        super().__init__(now)
        self._expected_timezone: tzinfo | None = None

    def expect_timezone(self, tz: tzinfo):
        self._expected_timezone = tz
        return self

    def now(self, tz: tzinfo | None = None) -> datetime:
        if self._expected_timezone is not None:
            assert tz == self._expected_timezone
        return super().now(tz)


class TestNewEvent(object):
    def test_uses_occurred_at_when_provided(self):
        occurred_at = datetime.now(UTC)
        event = NewEvent(
            name="something-happened",
            payload={"foo": "bar"},
            occurred_at=occurred_at,
        )

        assert event.occurred_at == occurred_at

    def test_uses_observed_at_when_provided(self):
        observed_at = datetime.now(UTC)
        event = NewEvent(
            name="something-happened",
            payload={"foo": "bar"},
            observed_at=observed_at,
        )

        assert event.observed_at, observed_at

    def test_defaults_occurred_at_to_observed_at(self):
        observed_at = datetime.now(UTC)
        event = NewEvent(
            name="something-happened",
            payload={"foo": "bar"},
            observed_at=observed_at,
        )

        assert event.occurred_at == observed_at

    def test_defaults_observed_at_to_now_in_utc(self):
        now = datetime.now(UTC)
        clock = VerifyingStaticClock(now).expect_timezone(UTC)
        event = NewEvent(
            name="something-happened", payload={"foo": "bar"}, clock=clock
        )

        assert event.observed_at == now

    def test_uses_same_observed_at_and_occurred_at_if_neither_provided(self):
        now = datetime.now(UTC)
        clock = VerifyingStaticClock(now).expect_timezone(UTC)
        event = NewEvent(
            name="something-happened", payload={"foo": "bar"}, clock=clock
        )

        assert event.observed_at == now and event.occurred_at == now

    def test_includes_all_attributes_in_representation(self):
        now = datetime.now(UTC)
        event = NewEvent(
            name="something-happened",
            payload={"foo": "bar"},
            observed_at=now,
            occurred_at=now,
        )

        assert str(event) == (
            "NewEvent("
            "name=something-happened, "
            "payload={'foo': 'bar'}, "
            f"observed_at={now}, "
            f"occurred_at={now})"
        )

    def test_is_equal_when_all_attributes_equal_and_same_type(self):
        now = datetime.now(UTC)
        event1 = NewEvent(
            name="something-happened",
            payload={"foo": "bar"},
            observed_at=now,
            occurred_at=now,
        )
        event2 = NewEvent(
            name="something-happened",
            payload={"foo": "bar"},
            observed_at=now,
            occurred_at=now,
        )

        assert event1 == event2

    def test_is_not_equal_when_event_name_different(self):
        now = datetime.now(UTC)
        event1 = NewEvent(
            name="first-thing-happened",
            payload={"foo": "bar"},
            observed_at=now,
            occurred_at=now,
        )
        event2 = NewEvent(
            name="another-thing-happened",
            payload={"foo": "bar"},
            observed_at=now,
            occurred_at=now,
        )

        assert not event1 == event2

    def test_is_not_equal_when_event_payload_different(self):
        now = datetime.now(UTC)
        event1 = NewEvent(
            name="something-happened",
            payload={"foo": "bar"},
            observed_at=now,
            occurred_at=now,
        )
        event2 = NewEvent(
            name="something-happened",
            payload={"baz": "qux"},
            observed_at=now,
            occurred_at=now,
        )

        assert not event1 == event2

    def test_is_not_equal_when_event_observed_at_different(self):
        now = datetime.now(UTC)
        past = now - timedelta(days=1)
        event1 = NewEvent(
            name="something-happened",
            payload={"foo": "bar"},
            observed_at=now,
            occurred_at=now,
        )
        event2 = NewEvent(
            name="something-happened",
            payload={"foo": "bar"},
            observed_at=past,
            occurred_at=now,
        )

        assert not event1 == event2

    def test_is_not_equal_when_event_occurred_at_different(self):
        now = datetime.now(UTC)
        past = now - timedelta(days=1)
        event1 = NewEvent(
            name="something-happened",
            payload={"foo": "bar"},
            observed_at=now,
            occurred_at=now,
        )
        event2 = NewEvent(
            name="something-happened",
            payload={"foo": "bar"},
            observed_at=now,
            occurred_at=past,
        )

        assert not event1 == event2

    def test_is_not_equal_when_different_type(self):
        now = datetime.now(UTC)
        event1 = NewEvent(
            name="something-happened",
            payload={"foo": "bar"},
            observed_at=now,
            occurred_at=now,
        )
        event2 = StoredEvent(
            id="some-id",
            name="something-happened",
            stream="some-stream",
            category="some-category",
            position=0,
            payload={"foo": "bar"},
            observed_at=now,
            occurred_at=now,
        )

        assert not event1 == event2

    def test_has_same_hashcode_when_same_attributes(self):
        now = datetime.now(UTC)
        event1 = NewEvent(
            name="something-happened",
            payload={"foo": {"bar": "baz"}},
            observed_at=now,
            occurred_at=now,
        )
        event2 = NewEvent(
            name="something-happened",
            payload={"foo": {"bar": "baz"}},
            observed_at=now,
            occurred_at=now,
        )

        assert hash(event1) == hash(event2)

    def test_has_different_hashcode_when_different_event_name(self):
        now = datetime.now(UTC)
        event1 = NewEvent(
            name="first-thing-happened",
            payload={"foo": "bar"},
            observed_at=now,
            occurred_at=now,
        )
        event2 = NewEvent(
            name="another-thing-happened",
            payload={"foo": "bar"},
            observed_at=now,
            occurred_at=now,
        )

        assert not hash(event1) == hash(event2)

    def test_has_different_hashcode_when_different_event_payload(self):
        now = datetime.now(UTC)
        event1 = NewEvent(
            name="something-happened",
            payload={"foo": "bar"},
            observed_at=now,
            occurred_at=now,
        )
        event2 = NewEvent(
            name="something-happened",
            payload={"baz": "qux"},
            observed_at=now,
            occurred_at=now,
        )

        assert not hash(event1) == hash(event2)

    def test_has_different_hashcode_when_different_event_observed_at(self):
        now = datetime.now(UTC)
        past = now - timedelta(days=1)
        event1 = NewEvent(
            name="something-happened",
            payload={"foo": "bar"},
            observed_at=now,
            occurred_at=now,
        )
        event2 = NewEvent(
            name="something-happened",
            payload={"baz": "qux"},
            observed_at=past,
            occurred_at=now,
        )

        assert not hash(event1) == hash(event2)

    def test_has_different_hashcode_when_different_event_occurred_at(self):
        now = datetime.now(UTC)
        past = now - timedelta(days=1)
        event1 = NewEvent(
            name="something-happened",
            payload={"foo": "bar"},
            observed_at=now,
            occurred_at=now,
        )
        event2 = NewEvent(
            name="something-happened",
            payload={"baz": "qux"},
            observed_at=now,
            occurred_at=past,
        )

        assert not hash(event1) == hash(event2)


class TestStoredEvent(object):
    def test_includes_all_attributes_in_representation(self):
        now = datetime.now(UTC)
        stored_event = StoredEvent(
            id="some-id",
            name="something-happened",
            stream="some-stream",
            category="some-category",
            position=0,
            payload={"foo": "bar"},
            observed_at=now,
            occurred_at=now,
        )

        assert str(stored_event) == (
            "StoredEvent("
            "id=some-id, "
            "name=something-happened, "
            "stream=some-stream, "
            "category=some-category, "
            "position=0, "
            "payload={'foo': 'bar'}, "
            f"observed_at={now}, "
            f"occurred_at={now})"
        )

    def test_is_equal_when_all_attributes_equal_and_same_type(self):
        now = datetime.now(UTC)
        event1 = StoredEvent(
            id="some-id",
            name="something-happened",
            stream="some-stream",
            category="some-category",
            position=0,
            payload={"foo": "bar"},
            observed_at=now,
            occurred_at=now,
        )
        event2 = StoredEvent(
            id="some-id",
            name="something-happened",
            stream="some-stream",
            category="some-category",
            position=0,
            payload={"foo": "bar"},
            observed_at=now,
            occurred_at=now,
        )

        assert event1 == event2

    def test_is_not_equal_when_event_id_different(self):
        now = datetime.now(UTC)
        event1 = StoredEvent(
            id="first-id",
            name="something-happened",
            stream="some-stream",
            category="some-category",
            position=0,
            payload={"foo": "bar"},
            observed_at=now,
            occurred_at=now,
        )
        event2 = StoredEvent(
            id="second-id",
            name="something-happened",
            stream="some-stream",
            category="some-category",
            position=0,
            payload={"foo": "bar"},
            observed_at=now,
            occurred_at=now,
        )

        assert not event1 == event2

    def test_is_not_equal_when_event_name_different(self):
        now = datetime.now(UTC)
        event1 = StoredEvent(
            id="some-id",
            name="first-thing-happened",
            stream="some-stream",
            category="some-category",
            position=0,
            payload={"foo": "bar"},
            observed_at=now,
            occurred_at=now,
        )
        event2 = StoredEvent(
            id="some-id",
            name="another-thing-happened",
            stream="some-stream",
            category="some-category",
            position=0,
            payload={"foo": "bar"},
            observed_at=now,
            occurred_at=now,
        )

        assert not event1 == event2

    def test_is_not_equal_when_event_payload_different(self):
        now = datetime.now(UTC)
        event1 = StoredEvent(
            id="some-id",
            name="something-happened",
            stream="some-stream",
            category="some-category",
            position=0,
            payload={"foo": "bar"},
            observed_at=now,
            occurred_at=now,
        )
        event2 = StoredEvent(
            id="some-id",
            name="something-happened",
            stream="some-stream",
            category="some-category",
            position=0,
            payload={"baz": "qux"},
            observed_at=now,
            occurred_at=now,
        )

        assert not event1 == event2

    def test_is_not_equal_when_event_observed_at_different(self):
        now = datetime.now(UTC)
        past = now - timedelta(days=1)
        event1 = StoredEvent(
            id="some-id",
            name="something-happened",
            stream="some-stream",
            category="some-category",
            position=0,
            payload={"foo": "bar"},
            observed_at=now,
            occurred_at=now,
        )
        event2 = StoredEvent(
            id="some-id",
            name="something-happened",
            stream="some-stream",
            category="some-category",
            position=0,
            payload={"foo": "bar"},
            observed_at=past,
            occurred_at=now,
        )

        assert not event1 == event2

    def test_is_not_equal_when_event_occurred_at_different(self):
        now = datetime.now(UTC)
        past = now - timedelta(days=1)
        event1 = StoredEvent(
            id="some-id",
            name="something-happened",
            stream="some-stream",
            category="some-category",
            position=0,
            payload={"foo": "bar"},
            observed_at=now,
            occurred_at=now,
        )
        event2 = StoredEvent(
            id="some-id",
            name="something-happened",
            stream="some-stream",
            category="some-category",
            position=0,
            payload={"foo": "bar"},
            observed_at=now,
            occurred_at=past,
        )

        assert not event1 == event2

    def test_is_not_equal_when_different_type(self):
        OtherStoredEvent = namedtuple(
            "OtherStoredEvent",
            [
                "id",
                "name",
                "stream",
                "category",
                "position",
                "payload",
                "observed_at",
                "occurred_at",
            ],
        )

        now = datetime.now(UTC)
        event1 = StoredEvent(
            id="some-id",
            name="something-happened",
            stream="some-stream",
            category="some-category",
            position=0,
            payload={"foo": "bar"},
            observed_at=now,
            occurred_at=now,
        )
        event2 = OtherStoredEvent(
            id="some-id",
            name="something-happened",
            stream="some-stream",
            category="some-category",
            position=0,
            payload={"foo": "bar"},
            observed_at=now,
            occurred_at=now,
        )

        assert not event1 == event2

    def test_has_same_hashcode_when_same_attributes(self):
        now = datetime.now(UTC)
        event1 = StoredEvent(
            id="some-id",
            name="something-happened",
            stream="some-stream",
            category="some-category",
            position=0,
            payload={"foo": "bar"},
            observed_at=now,
            occurred_at=now,
        )
        event2 = StoredEvent(
            id="some-id",
            name="something-happened",
            stream="some-stream",
            category="some-category",
            position=0,
            payload={"foo": "bar"},
            observed_at=now,
            occurred_at=now,
        )

        assert hash(event1) == hash(event2)

    def test_has_different_hashcode_when_different_event_name(self):
        now = datetime.now(UTC)
        event1 = StoredEvent(
            id="some-id",
            name="first-thing-happened",
            stream="some-stream",
            category="some-category",
            position=0,
            payload={"foo": "bar"},
            observed_at=now,
            occurred_at=now,
        )
        event2 = StoredEvent(
            id="some-id",
            name="something-happened",
            stream="some-stream",
            category="some-category",
            position=0,
            payload={"foo": "bar"},
            observed_at=now,
            occurred_at=now,
        )

        assert not hash(event1) == hash(event2)

    def test_has_different_hashcode_when_different_id(self):
        now = datetime.now(UTC)
        event1 = StoredEvent(
            id="first-id",
            name="something-happened",
            stream="some-stream",
            category="some-category",
            position=0,
            payload={"foo": "bar"},
            observed_at=now,
            occurred_at=now,
        )
        event2 = StoredEvent(
            id="second-id",
            name="something-happened",
            stream="some-stream",
            category="some-category",
            position=0,
            payload={"foo": "bar"},
            observed_at=now,
            occurred_at=now,
        )

        assert not hash(event1) == hash(event2)

    def test_has_different_hashcode_when_different_event_stream(self):
        now = datetime.now(UTC)
        event1 = StoredEvent(
            id="some-id",
            name="something-happened",
            stream="first-stream",
            category="some-category",
            position=0,
            payload={"foo": "bar"},
            observed_at=now,
            occurred_at=now,
        )
        event2 = StoredEvent(
            id="some-id",
            name="something-happened",
            stream="second-stream",
            category="some-category",
            position=0,
            payload={"foo": "bar"},
            observed_at=now,
            occurred_at=now,
        )

        assert not hash(event1) == hash(event2)

    def test_has_different_hashcode_when_different_event_category(self):
        now = datetime.now(UTC)
        event1 = StoredEvent(
            id="some-id",
            name="something-happened",
            stream="some-stream",
            category="first-category",
            position=0,
            payload={"foo": "bar"},
            observed_at=now,
            occurred_at=now,
        )
        event2 = StoredEvent(
            id="some-id",
            name="something-happened",
            stream="some-stream",
            category="second-category",
            position=0,
            payload={"foo": "bar"},
            observed_at=now,
            occurred_at=now,
        )

        assert not hash(event1) == hash(event2)

    def test_has_different_hashcode_when_different_event_position(self):
        now = datetime.now(UTC)
        event1 = StoredEvent(
            id="some-id",
            name="something-happened",
            stream="some-stream",
            category="some-category",
            position=0,
            payload={"foo": "bar"},
            observed_at=now,
            occurred_at=now,
        )
        event2 = StoredEvent(
            id="some-id",
            name="something-happened",
            stream="some-stream",
            category="some-category",
            position=1,
            payload={"foo": "bar"},
            observed_at=now,
            occurred_at=now,
        )

        assert not hash(event1) == hash(event2)

    def test_has_different_hashcode_when_different_event_payload(self):
        now = datetime.now(UTC)
        event1 = StoredEvent(
            id="some-id",
            name="something-happened",
            stream="some-stream",
            category="some-category",
            position=0,
            payload={"foo": "bar"},
            observed_at=now,
            occurred_at=now,
        )
        event2 = StoredEvent(
            id="some-id",
            name="something-happened",
            stream="some-stream",
            category="some-category",
            position=0,
            payload={"baz": "qux"},
            observed_at=now,
            occurred_at=now,
        )

        assert not hash(event1) == hash(event2)

    def test_has_different_hashcode_when_different_event_observed_at(self):
        now = datetime.now(UTC)
        past = now - timedelta(days=1)
        event1 = StoredEvent(
            id="some-id",
            name="something-happened",
            stream="some-stream",
            category="some-category",
            position=0,
            payload={"foo": "bar"},
            observed_at=now,
            occurred_at=now,
        )
        event2 = StoredEvent(
            id="some-id",
            name="something-happened",
            stream="some-stream",
            category="some-category",
            position=0,
            payload={"baz": "qux"},
            observed_at=past,
            occurred_at=now,
        )

        assert not hash(event1) == hash(event2)

    def test_has_different_hashcode_when_different_event_occurred_at(self):
        now = datetime.now(UTC)
        past = now - timedelta(days=1)
        event1 = StoredEvent(
            id="some-id",
            name="something-happened",
            stream="some-stream",
            category="some-category",
            position=0,
            payload={"foo": "bar"},
            observed_at=now,
            occurred_at=now,
        )
        event2 = StoredEvent(
            id="some-id",
            name="something-happened",
            stream="some-stream",
            category="some-category",
            position=0,
            payload={"baz": "qux"},
            observed_at=now,
            occurred_at=past,
        )

        assert not hash(event1) == hash(event2)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__]))
