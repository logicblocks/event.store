import sys
from collections.abc import Hashable, Mapping
from dataclasses import dataclass
from typing import Any

import pytest

from logicblocks.event.types import Projection, identifier


@dataclass
class Thing:
    value: int = 5


class TestProjection:
    def test_projection_returns_envelope(self):
        projection = Projection(
            id="a",
            state={"a": 1},
            version=1,
            source=identifier.StreamIdentifier(category="test", stream="a"),
            name="thing",
        )

        actual = projection.envelope()
        expected = {
            "id": "a",
            "name": "thing",
            "version": 1,
            "source": {"type": "stream", "category": "test", "stream": "a"},
        }
        assert actual == expected

    def test_projection_returns_dict_for_dict_state(self):
        projection = Projection(
            id="a",
            state={"a": 1},
            version=1,
            source=identifier.StreamIdentifier(category="test", stream="a"),
            name="thing",
        )

        actual = projection.dict()
        expected = {
            "id": "a",
            "name": "thing",
            "state": {"a": 1},
            "version": 1,
            "source": {"type": "stream", "category": "test", "stream": "a"},
        }
        assert actual == expected

    def test_projection_returns_dict_for_generic_state(self):
        projection = Projection[Thing](
            id="a",
            state=Thing(),
            version=1,
            source=identifier.StreamIdentifier(category="test", stream="a"),
            name="thing",
        )

        actual = projection.dict()
        expected = {
            "id": "a",
            "name": "thing",
            "state": {"value": 5},
            "version": 1,
            "source": {"type": "stream", "category": "test", "stream": "a"},
        }
        assert actual == expected

    def test_projection_returns_dict_for_generic_state_custom_converter(self):
        projection = Projection[Thing](
            id="a",
            state=Thing(),
            version=1,
            source=identifier.StreamIdentifier(category="test", stream="a"),
            name="thing",
        )

        def converter(value: Thing) -> Mapping[str, Any]:
            return {"val": value.value + 1}

        actual = projection.dict(converter)
        expected = {
            "id": "a",
            "name": "thing",
            "state": {"val": 6},
            "version": 1,
            "source": {"type": "stream", "category": "test", "stream": "a"},
        }
        assert actual == expected

    def test_projection_returns_json_for_dict_state(self):
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

    def test_projection_returns_json_for_generic_state(self):
        projection = Projection[Thing](
            id="a",
            state=Thing(),
            version=1,
            source=identifier.StreamIdentifier(category="test", stream="a"),
            name="thing",
        )

        actual = projection.json()
        expected = (
            "{"
            '"id": "a", '
            '"name": "thing", '
            '"state": {"value": 5}, '
            '"version": 1, '
            '"source": {"type": "stream", "category": "test", "stream": "a"}'
            "}"
        )
        assert actual == expected

    def test_projection_returns_json_for_generic_state_custom_converter(self):
        projection = Projection[Thing](
            id="a",
            state=Thing(),
            version=1,
            source=identifier.StreamIdentifier(category="test", stream="a"),
            name="thing",
        )

        def converter(value: Thing) -> Mapping[str, Any]:
            return {"val": value.value + 1}

        actual = projection.json(converter)
        expected = (
            "{"
            '"id": "a", '
            '"name": "thing", '
            '"state": {"val": 6}, '
            '"version": 1, '
            '"source": {"type": "stream", "category": "test", "stream": "a"}'
            "}"
        )
        assert actual == expected

    def test_projection_returns_representation(self):
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

    def test_hashes_projection_using_representation(self):
        projection = Projection(
            id="a",
            name="thing",
            state={"a": 1},
            version=1,
            source=identifier.StreamIdentifier(category="test", stream="a"),
        )

        assert isinstance(projection, Hashable)
        assert hash(projection) == hash(repr(projection))

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
