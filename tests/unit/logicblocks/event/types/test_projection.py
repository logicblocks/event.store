import sys
from collections.abc import Hashable

import pytest

from logicblocks.event.types import Projection, identifier


class TestProjection:
    def test_projection_returns_json_representation(self):
        projection = Projection(
            id="a",
            state={"a": 1},
            version=1,
            source=identifier.StreamIdentifier(category="test", stream="a"),
            name="thing",
        )

        actual = projection.json()
        expected = (
            "{"
            '"id": "a", '
            '"name": "thing", '
            '"state": {"a": 1}, '
            '"version": 1, '
            '"source": {"type": "stream", "category": "test", "stream": "a"}'
            "}"
        )
        assert actual == expected

    def test_projection_returns_debug_representation(self):
        projection = Projection(
            id="a",
            state={"a": 1},
            version=1,
            source=identifier.StreamIdentifier(category="test", stream="a"),
            name="thing",
        )

        assert (
            repr(projection) == "Projection("
            "id='a',"
            "name='thing',"
            "state={'a': 1},"
            "version=1,"
            "source=StreamIdentifier(category='test',stream='a')"
            ")"
        )

    def test_hashes_projection_using_json_representation(self):
        projection = Projection(
            id="a",
            name="thing",
            state={"a": 1},
            version=1,
            source=identifier.StreamIdentifier(category="test", stream="a"),
        )

        assert isinstance(projection, Hashable)
        assert hash(projection) == hash(projection.json())

    def test_exposes_id(self):
        projection = Projection(
            id="a",
            name="thing",
            state={"a": 1},
            version=1,
            source=identifier.StreamIdentifier(category="test", stream="a"),
        )
        assert projection.id == "a"

    def test_exposes_name(self):
        projection = Projection(
            id="a",
            name="thing",
            state={"a": 1},
            version=1,
            source=identifier.StreamIdentifier(category="test", stream="a"),
        )
        assert projection.name == "thing"

    def test_exposes_state(self):
        projection = Projection(
            id="a",
            name="thing",
            state={"a": 1},
            version=1,
            source=identifier.StreamIdentifier(category="test", stream="a"),
        )
        assert projection.state == {"a": 1}

    def test_exposes_version(self):
        projection = Projection(
            id="a",
            name="thing",
            state={"a": 1},
            version=1,
            source=identifier.StreamIdentifier(category="test", stream="a"),
        )
        assert projection.version == 1

    def test_exposes_source(self):
        source = identifier.StreamIdentifier(category="test", stream="a")
        projection = Projection(
            id="a",
            name="thing",
            state={"a": 1},
            version=1,
            source=source,
        )
        assert projection.source == source


if __name__ == "__main__":
    sys.exit(pytest.main([__file__]))
