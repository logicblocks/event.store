import sys
from collections.abc import Hashable, Mapping
from dataclasses import dataclass
from typing import Any, Self

import pytest

from logicblocks.event.types import Projection, identifier


@dataclass
class Thing:
    value: int = 5

    @classmethod
    def deserialise(cls, value: Mapping[str, Any]) -> Self:
        return cls(value=int(value["value"]))

    def serialise(self) -> Mapping[str, Any]:
        return {"value": self.value}


@dataclass
class ThingMeta:
    updated_at: str

    @classmethod
    def deserialise(cls, value: Mapping[str, Any]) -> Self:
        return cls(updated_at=str(value["updated_at"]))

    def serialise(self) -> Mapping[str, Any]:
        return {"updated_at": self.updated_at}


class TestProjection:
    def test_projection_returns_envelope(self):
        projection = Projection(
            id="a",
            name="thing",
            source=identifier.StreamIdentifier(category="test", stream="a"),
            state={"a": 1},
            metadata={"updated_at": "2024-01-01T00:00:00Z"},
        )

        actual = projection.envelope()
        expected = {
            "id": "a",
            "name": "thing",
            "source": {"type": "stream", "category": "test", "stream": "a"},
        }
        assert actual == expected

    def test_projection_returns_dict_for_dict_state_and_metadata(self):
        projection = Projection(
            id="a",
            name="thing",
            source=identifier.StreamIdentifier(category="test", stream="a"),
            state={"a": 1},
            metadata={"updated_at": "2024-01-01T00:00:00Z"},
        )

        actual = projection.dict()
        expected = {
            "id": "a",
            "name": "thing",
            "source": {"type": "stream", "category": "test", "stream": "a"},
            "state": {"a": 1},
            "metadata": {"updated_at": "2024-01-01T00:00:00Z"},
        }
        assert actual == expected

    def test_projection_returns_dict_for_generic_state_and_metadata(self):
        projection = Projection[Thing, ThingMeta](
            id="a",
            state=Thing(),
            source=identifier.StreamIdentifier(category="test", stream="a"),
            name="thing",
            metadata=ThingMeta(updated_at="2024-01-01T00:00:00Z"),
        )

        actual = projection.dict()
        expected = {
            "id": "a",
            "name": "thing",
            "source": {"type": "stream", "category": "test", "stream": "a"},
            "state": {"value": 5},
            "metadata": {"updated_at": "2024-01-01T00:00:00Z"},
        }
        assert actual == expected

    def test_projection_returns_json_for_dict_state_and_metadata(self):
        projection = Projection(
            id="a",
            name="thing",
            source=identifier.StreamIdentifier(category="test", stream="a"),
            state={"a": 1},
            metadata={"updated_at": "2024-01-01T00:00:00Z"},
        )

        actual = projection.json()
        expected = (
            "{"
            '"id": "a", '
            '"name": "thing", '
            '"source": {"type": "stream", "category": "test", "stream": "a"}, '
            '"state": {"a": 1}, '
            '"metadata": {"updated_at": "2024-01-01T00:00:00Z"}'
            "}"
        )
        assert actual == expected

    def test_projection_returns_json_for_generic_state_and_metadata(self):
        projection = Projection[Thing, ThingMeta](
            id="a",
            name="thing",
            source=identifier.StreamIdentifier(category="test", stream="a"),
            state=Thing(),
            metadata=ThingMeta(updated_at="2024-01-01T00:00:00Z"),
        )

        actual = projection.json()
        expected = (
            "{"
            '"id": "a", '
            '"name": "thing", '
            '"source": {"type": "stream", "category": "test", "stream": "a"}, '
            '"state": {"value": 5}, '
            '"metadata": {"updated_at": "2024-01-01T00:00:00Z"}'
            "}"
        )
        assert actual == expected

    def test_projection_returns_representation(self):
        projection = Projection(
            id="a",
            name="thing",
            source=identifier.StreamIdentifier(category="test", stream="a"),
            state={"a": 1},
            metadata={"updated_at": "2024-01-01T00:00:00Z"},
        )

        assert (
            repr(projection) == "Projection("
            "id='a',"
            "name='thing',"
            "source=StreamIdentifier(category='test',stream='a'),"
            "state={'a': 1},"
            "metadata={'updated_at': '2024-01-01T00:00:00Z'}"
            ")"
        )

    def test_hashes_projection_using_representation(self):
        projection = Projection(
            id="a",
            name="thing",
            source=identifier.StreamIdentifier(category="test", stream="a"),
            state={"a": 1},
            metadata={"thing": "value"},
        )

        assert isinstance(projection, Hashable)
        assert hash(projection) == hash(repr(projection))

    def test_exposes_id(self):
        projection = Projection(
            id="a",
            name="thing",
            source=identifier.StreamIdentifier(category="test", stream="a"),
            state={"a": 1},
            metadata={},
        )
        assert projection.id == "a"

    def test_exposes_name(self):
        projection = Projection(
            id="a",
            name="thing",
            source=identifier.StreamIdentifier(category="test", stream="a"),
            state={"a": 1},
            metadata={},
        )
        assert projection.name == "thing"

    def test_exposes_source(self):
        source = identifier.StreamIdentifier(category="test", stream="a")
        projection = Projection(
            id="a", name="thing", state={"a": 1}, source=source, metadata={}
        )
        assert projection.source == source

    def test_exposes_state(self):
        projection = Projection(
            id="a",
            name="thing",
            source=identifier.StreamIdentifier(category="test", stream="a"),
            state={"a": 1},
            metadata={},
        )
        assert projection.state == {"a": 1}

    def test_exposes_metadata(self):
        metadata = {"important": "label"}
        projection = Projection(
            id="a",
            name="thing",
            state={"a": 1},
            source=identifier.StreamIdentifier(category="test", stream="a"),
            metadata=metadata,
        )
        assert projection.metadata == metadata


if __name__ == "__main__":
    sys.exit(pytest.main([__file__]))
