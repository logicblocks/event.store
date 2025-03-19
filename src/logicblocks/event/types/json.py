from collections.abc import Callable, Mapping
from typing import Any, Protocol, Self, Sequence, TypeGuard, runtime_checkable

type JsonObject = Mapping[str, JsonValue]
type JsonArray = Sequence[JsonValue]
type JsonPrimitive = str | int | float | bool | None
type JsonValue = JsonObject | JsonArray | JsonPrimitive

JsonValueType = JsonValue.__value__


@runtime_checkable
class JsonValueSerialisable(Protocol):
    def serialise(self, fallback: Callable[[object], JsonValue]) -> JsonValue:
        raise NotImplementedError


@runtime_checkable
class JsonValueDeserialisable(Protocol):
    @classmethod
    def deserialise(
        cls, value: JsonValue, fallback: Callable[[type[Any], JsonValue], Any]
    ) -> Self:
        raise NotImplementedError


@runtime_checkable
class JsonValueConvertible(
    JsonValueSerialisable, JsonValueDeserialisable, Protocol
):
    pass


def is_json_object(value: Any) -> TypeGuard[JsonObject]:
    match value:
        case {**kvs}:
            return all(
                isinstance(key, str) and is_json_value(value)
                for key, value in kvs.items()
            )
        case _:
            return False


def is_json_array(value: Any) -> TypeGuard[JsonArray]:
    match value:
        case [*items]:
            return all(is_json_value(item) for item in items)
        case _:
            return False


def is_json_primitive(value: Any) -> TypeGuard[JsonPrimitive]:
    match value:
        case str() | int() | float() | bool() | None:
            return True
        case _:
            return False


def is_json_value(value: Any) -> TypeGuard[JsonValue]:
    match value:
        case str() | int() | float() | bool() | None:
            return True
        case [*items]:
            return all(is_json_value(item) for item in items)
        case {**kvs}:
            return all(
                isinstance(key, str) and is_json_value(value)
                for key, value in kvs.items()
            )
        case _:
            return False
