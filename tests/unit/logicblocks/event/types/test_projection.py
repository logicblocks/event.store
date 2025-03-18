import sys
from collections.abc import Callable, Hashable
from dataclasses import dataclass
from typing import Any, Self

import pytest

from logicblocks.event.types import (
    JsonValue,
    Projection,
    default_serialisation_fallback,
    identifier,
)
from logicblocks.event.types.json import JsonValueConvertible, is_json_object
from tests.component.test_asynchronous_projections import (
    default_deserialisation_fallback,
)


@dataclass
class ConvertibleThing(JsonValueConvertible):
    value: int = 5

    @classmethod
    def deserialise(
        cls,
        value: JsonValue,
        fallback: Callable[
            [Any, JsonValue], Any
        ] = default_deserialisation_fallback,
    ) -> Self:
        if (
            not is_json_object(value)
            or "value" not in value
            or not isinstance(value["value"], int)
        ):
            return fallback(cls, value)

        return cls(value=int(value["value"]))

    def serialise(
        self,
        fallback: Callable[
            [object], JsonValue
        ] = default_serialisation_fallback,
    ) -> JsonValue:
        return {"value": self.value}


@dataclass
class ConvertibleThingMeta(JsonValueConvertible):
    updated_at: str

    @classmethod
    def deserialise(
        cls,
        value: JsonValue,
        fallback: Callable[
            [Any, JsonValue], Any
        ] = default_deserialisation_fallback,
    ) -> Self:
        if (
            not is_json_object(value)
            or "updated_at" not in value
            or not isinstance(value["updated_at"], str)
        ):
            return fallback(cls, value)

        return cls(updated_at=str(value["updated_at"]))

    def serialise(
        self,
        fallback: Callable[
            [object], JsonValue
        ] = default_serialisation_fallback,
    ) -> JsonValue:
        return {"updated_at": self.updated_at}


class TestProjection:
    def test_returns_summary(self):
        projection = Projection(
            id="a",
            name="thing",
            source=identifier.StreamIdentifier(category="test", stream="a"),
            state={"a": 1},
            metadata={"updated_at": "2024-01-01T00:00:00Z"},
        )

        actual = projection.summarise()
        expected = {
            "id": "a",
            "name": "thing",
            "source": {"type": "stream", "category": "test", "stream": "a"},
        }
        assert actual == expected

    def test_can_serialise_for_dict_state_and_metadata(self):
        projection = Projection(
            id="a",
            name="thing",
            source=identifier.StreamIdentifier(category="test", stream="a"),
            state={"a": 1},
            metadata={"updated_at": "2024-01-01T00:00:00Z"},
        )

        actual = projection.serialise()
        expected = {
            "id": "a",
            "name": "thing",
            "source": {"type": "stream", "category": "test", "stream": "a"},
            "state": {"a": 1},
            "metadata": {"updated_at": "2024-01-01T00:00:00Z"},
        }
        assert actual == expected

    def test_can_serialise_for_convertible_state_and_metadata(self):
        projection = Projection[ConvertibleThing, ConvertibleThingMeta](
            id="a",
            state=ConvertibleThing(),
            source=identifier.StreamIdentifier(category="test", stream="a"),
            name="thing",
            metadata=ConvertibleThingMeta(updated_at="2024-01-01T00:00:00Z"),
        )

        actual = projection.serialise()
        expected = {
            "id": "a",
            "name": "thing",
            "source": {"type": "stream", "category": "test", "stream": "a"},
            "state": {"value": 5},
            "metadata": {"updated_at": "2024-01-01T00:00:00Z"},
        }
        assert actual == expected

    def test_returns_representation(self):
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

    def test_hashes_using_representation(self):
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
