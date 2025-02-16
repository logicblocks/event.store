import sys
from typing import Hashable

import pytest

from logicblocks.event.types import identifier


class TestLogIdentifier:
    def test_returns_json(self):
        assert identifier.LogIdentifier().json() == '{"type": "log"}'

    def test_returns_representation(self):
        assert repr(identifier.LogIdentifier()) == "LogIdentifier()"

    def test_hashes_identifier_using_representation(self):
        assert isinstance(identifier.LogIdentifier(), Hashable)
        assert hash(identifier.LogIdentifier()) == hash(
            repr(identifier.LogIdentifier())
        )


class TestCategoryIdentifier:
    def test_returns_json(self):
        assert (
            identifier.CategoryIdentifier(category="test").json()
            == '{"type": "category", "category": "test"}'
        )

    def test_returns_representation(self):
        assert (
            repr(identifier.CategoryIdentifier(category="test"))
            == "CategoryIdentifier(category='test')"
        )

    def test_hashes_identifier_using_representation(self):
        assert isinstance(identifier.CategoryIdentifier("test"), Hashable)
        assert hash(identifier.CategoryIdentifier(category="test")) == hash(
            repr(identifier.CategoryIdentifier(category="test"))
        )

    def test_exposes_category(self):
        assert (
            identifier.CategoryIdentifier(category="test").category == "test"
        )


class TestStreamIdentifier:
    def test_returns_json(self):
        assert (
            identifier.StreamIdentifier(
                category="test", stream="stream"
            ).json()
            == '{"type": "stream", "category": "test", "stream": "stream"}'
        )

    def test_returns_representation(self):
        assert (
            repr(identifier.StreamIdentifier(category="test", stream="stream"))
            == "StreamIdentifier(category='test',stream='stream')"
        )

    def test_hashes_identifier_using_representation(self):
        assert isinstance(
            identifier.StreamIdentifier(category="test", stream="stream"),
            Hashable,
        )
        assert hash(
            identifier.StreamIdentifier(category="test", stream="stream")
        ) == hash(
            repr(identifier.StreamIdentifier(category="test", stream="stream"))
        )

    def test_exposes_category(self):
        assert (
            identifier.StreamIdentifier(
                category="test", stream="stream"
            ).category
            == "test"
        )

    def test_exposes_stream(self):
        assert (
            identifier.StreamIdentifier(
                category="test", stream="stream"
            ).stream
            == "stream"
        )


class TestTargetFunction:
    def test_returns_log_identifier_when_category_and_stream_not_provided(
        self,
    ):
        assert identifier.target() == identifier.LogIdentifier()

    def test_returns_log_identifier_when_category_and_stream_none(self):
        assert (
            identifier.target(category=None, stream=None)
            == identifier.LogIdentifier()
        )

    def test_returns_log_identifier_when_category_none_and_stream_not_provided(
        self,
    ):
        assert identifier.target(category=None) == identifier.LogIdentifier()

    def test_returns_log_identifier_when_category_not_provided_and_stream_none(
        self,
    ):
        assert identifier.target(stream=None) == identifier.LogIdentifier()

    def test_raises_when_category_not_provided_and_stream_provided(self):
        with pytest.raises(ValueError):
            identifier.target(stream="stream")

    def test_raises_when_category_none_and_stream_provided(self):
        with pytest.raises(ValueError):
            identifier.target(category=None, stream="stream")

    def test_returns_category_identifier_when_category_provided_and_stream_not_provided(
        self,
    ):
        assert identifier.target(
            category="test"
        ) == identifier.CategoryIdentifier(category="test")

    def test_returns_category_identifier_when_category_provided_and_stream_none(
        self,
    ):
        assert identifier.target(
            category="test", stream=None
        ) == identifier.CategoryIdentifier(category="test")

    def test_returns_stream_identifier_when_category_and_stream_provided(self):
        assert identifier.target(
            category="test", stream="stream"
        ) == identifier.StreamIdentifier(category="test", stream="stream")


class TestIdentifierFunction:
    def test_returns_log_identifier_for_log_identifier_dict(self):
        assert (
            identifier.event_sequence_identifier({"type": "log"})
            == identifier.LogIdentifier()
        )

    def test_returns_category_identifier_for_category_identifier_dict(self):
        assert identifier.event_sequence_identifier(
            {"type": "category", "category": "test"}
        ) == identifier.CategoryIdentifier(category="test")

    def test_returns_stream_identifier_for_stream_identifier_dict(self):
        assert identifier.event_sequence_identifier(
            {"type": "stream", "category": "test", "stream": "stream"}
        ) == identifier.StreamIdentifier(category="test", stream="stream")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__]))
