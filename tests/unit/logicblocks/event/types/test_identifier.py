import sys
from typing import Hashable

import pytest

from logicblocks.event.types import identifier


class TestLogIdentifier(object):
    def test_returns_json_representation(self):
        assert identifier.Log().json() == '{"type": "log"}'

    def test_returns_debug_representation(self):
        assert repr(identifier.Log()) == "identifier.Log()"

    def test_hashes_identifier_using_json_representation(self):
        assert isinstance(identifier.Log(), Hashable)
        assert hash(identifier.Log()) == hash(identifier.Log().json())


class TestCategoryIdentifier(object):
    def test_returns_json_representation(self):
        assert (
            identifier.Category(category="test").json()
            == '{"type": "category", "category": "test"}'
        )

    def test_returns_debug_representation(self):
        assert (
            repr(identifier.Category(category="test"))
            == "identifier.Category(category=test)"
        )

    def test_hashes_identifier_using_json_representation(self):
        assert isinstance(identifier.Category("test"), Hashable)
        assert hash(identifier.Category(category="test")) == hash(
            identifier.Category(category="test").json()
        )

    def test_exposes_category(self):
        assert identifier.Category(category="test").category == "test"


class TestStreamIdentifier(object):
    def test_returns_json_representation(self):
        assert (
            identifier.Stream(category="test", stream="stream").json()
            == '{"type": "stream", "category": "test", "stream": "stream"}'
        )

    def test_returns_debug_representation(self):
        assert (
            repr(identifier.Stream(category="test", stream="stream"))
            == "identifier.Stream(category=test,stream=stream)"
        )

    def test_hashes_identifier_using_json_representation(self):
        assert isinstance(
            identifier.Stream(category="test", stream="stream"), Hashable
        )
        assert hash(
            identifier.Stream(category="test", stream="stream")
        ) == hash(identifier.Stream(category="test", stream="stream").json())

    def test_exposes_category(self):
        assert (
            identifier.Stream(category="test", stream="stream").category
            == "test"
        )

    def test_exposes_stream(self):
        assert (
            identifier.Stream(category="test", stream="stream").stream
            == "stream"
        )


class TestTargetFunction(object):
    def test_returns_log_identifier_when_category_and_stream_not_provided(
        self,
    ):
        assert identifier.target() == identifier.Log()

    def test_returns_log_identifier_when_category_and_stream_none(self):
        assert (
            identifier.target(category=None, stream=None) == identifier.Log()
        )

    def test_returns_log_identifier_when_category_none_and_stream_not_provided(
        self,
    ):
        assert identifier.target(category=None) == identifier.Log()

    def test_returns_log_identifier_when_category_not_provided_and_stream_none(
        self,
    ):
        assert identifier.target(stream=None) == identifier.Log()

    def test_raises_when_category_not_provided_and_stream_provided(self):
        with pytest.raises(ValueError):
            identifier.target(stream="stream")

    def test_raises_when_category_none_and_stream_provided(self):
        with pytest.raises(ValueError):
            identifier.target(category=None, stream="stream")

    def test_returns_category_identifier_when_category_provided_and_stream_not_provided(
        self,
    ):
        assert identifier.target(category="test") == identifier.Category(
            category="test"
        )

    def test_returns_category_identifier_when_category_provided_and_stream_none(
        self,
    ):
        assert identifier.target(
            category="test", stream=None
        ) == identifier.Category(category="test")

    def test_returns_stream_identifier_when_category_and_stream_provided(self):
        assert identifier.target(
            category="test", stream="stream"
        ) == identifier.Stream(category="test", stream="stream")


if __name__ == "__main__":
    sys.exit(pytest.main([__file__]))
