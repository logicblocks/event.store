from collections.abc import Callable, Mapping
from typing import Any, Protocol, Self, Sequence, TypeGuard, runtime_checkable

type JsonObject = Mapping[str, JsonValue]
"""A JSON object: a mapping from string keys to JSON values."""

type JsonArray = Sequence[JsonValue]
"""A JSON array: a sequence of JSON values."""

type JsonPrimitive = str | int | float | bool | None
"""A JSON primitive: a string, number, boolean or `None`."""

type JsonValue = JsonObject | JsonArray | JsonPrimitive
"""Any JSON value: an object, array or primitive."""

JsonValueType = JsonValue.__value__
"""The runtime value of the `JsonValue` alias.

Pass this where a type object is expected, for example as the `state_type`
when loading a projection, to request an untyped JSON value.
"""


@runtime_checkable
class JsonValueSerialisable(Protocol):
    """Protocol for objects that can serialise themselves to a JSON value."""

    def serialise(self, fallback: Callable[[object], JsonValue]) -> JsonValue:
        """Serialise the object to a JSON value.

        Args:
            fallback: Called to serialise any nested value that is neither a
                JSON value nor `JsonValueSerialisable`.

        Returns:
            the serialised JSON value.
        """
        raise NotImplementedError


@runtime_checkable
class JsonValueDeserialisable(Protocol):
    """Protocol for classes that can deserialise instances from JSON."""

    @classmethod
    def deserialise(
        cls, value: JsonValue, fallback: Callable[[type[Any], JsonValue], Any]
    ) -> Self:
        """Deserialise an instance from a JSON value.

        Args:
            value: The JSON value to deserialise.
            fallback: Called with a target type and value to deserialise
                any nested value that cannot otherwise be deserialised.

        Returns:
            the deserialised instance.
        """
        raise NotImplementedError


@runtime_checkable
class JsonValueConvertible(
    JsonValueSerialisable, JsonValueDeserialisable, Protocol
):
    """Protocol for types that are JSON serialisable and deserialisable."""

    pass


def is_json_object(value: Any) -> TypeGuard[JsonObject]:
    """Check whether a value is a JSON object.

    Args:
        value: The value to check.

    Returns:
        `True` if the value is a mapping with string keys whose values are
            all JSON values, checked recursively.
    """
    match value:
        case {**kvs}:
            return all(
                isinstance(key, str) and is_json_value(value)
                for key, value in kvs.items()
            )
        case _:
            return False


def is_json_array(value: Any) -> TypeGuard[JsonArray]:
    """Check whether a value is a JSON array.

    Args:
        value: The value to check.

    Returns:
        `True` if the value is a sequence, other than a string or bytes,
            whose items are all JSON values, checked recursively.
    """
    match value:
        case [*items]:
            return all(is_json_value(item) for item in items)
        case _:
            return False


def is_json_primitive(value: Any) -> TypeGuard[JsonPrimitive]:
    """Check whether a value is a JSON primitive.

    Args:
        value: The value to check.

    Returns:
        `True` if the value is a string, number, boolean or `None`.
    """
    match value:
        case str() | int() | float() | bool() | None:
            return True
        case _:
            return False


def is_json_value(value: Any) -> TypeGuard[JsonValue]:
    """Check whether a value is a JSON value.

    Args:
        value: The value to check.

    Returns:
        `True` if the value is a JSON primitive, array or object, checked
            recursively.
    """
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
