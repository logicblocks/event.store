from collections.abc import Callable, Mapping, Sequence
from inspect import isclass
from types import NoneType
from typing import Any, cast, get_args, get_origin

from .json import (
    JsonValue,
    JsonValueConvertible,
    JsonValueDeserialisable,
    JsonValueSerialisable,
    JsonValueType,
    is_json_array,
    is_json_object,
    is_json_value,
)
from .string import (
    StringConvertible,
    StringSerialisable,
)

type JsonPersistable = JsonValueConvertible | JsonValue
type JsonLoggable = JsonValueSerialisable | JsonValue

type StringPersistable = StringConvertible | str
type StringLoggable = StringSerialisable | str


def raising_serialisation_fallback(value: object) -> Any:
    raise ValueError(f"Cannot serialise {value}.")


def str_serialisation_fallback(value: object) -> str:
    return str(value)


def raising_deserialisation_fallback[T](klass: type[T], value: object) -> T:
    raise ValueError(f"Cannot deserialise {value} to type {klass}.")


default_serialisation_fallback = raising_serialisation_fallback
default_deserialisation_fallback = raising_deserialisation_fallback


def serialise_to_json_value(
    value: object,
    fallback: Callable[[object], JsonValue] = default_serialisation_fallback,
) -> JsonValue:
    if isinstance(value, JsonValueSerialisable):
        return value.serialise(fallback)
    if is_json_value(value):
        return value
    return fallback(value)


def serialise_to_string(
    value: object,
    fallback: Callable[[object], str] = default_serialisation_fallback,
) -> str:
    if isinstance(value, StringSerialisable):
        return value.serialise(fallback)
    if isinstance(value, str):
        return value
    return fallback(value)


def deserialise_from_json_value[T](
    klass: type[T],
    value: object,
    fallback: Callable[
        [type[T], object], T
    ] = default_deserialisation_fallback,
) -> T:
    klass_original = klass
    klass_is_class = isclass(klass)
    klass_origin = get_origin(klass)
    klass_origin_is_class = isclass(klass_origin)

    if value is None and klass is NoneType:
        return cast(T, value)
    for primitive_type in (str, int, float, bool):
        if (
            isinstance(value, primitive_type)
            and klass_is_class
            and issubclass(klass, primitive_type)
        ):
            return cast(T, value)

    value_is_json_array = is_json_array(value)
    klass_is_sequence = klass_is_class and issubclass(klass, Sequence)
    klass_origin_is_sequence = klass_origin_is_class and issubclass(
        klass_origin, Sequence
    )
    if value_is_json_array and (klass_is_sequence or klass_origin_is_sequence):
        mapping = cast(Sequence[Any], value)
        if klass_origin is not None:
            klass_type_args = get_args(klass)
            klass_item_type = klass_type_args[0]

            if klass_item_type is JsonValue or klass_item_type is Any:
                return cast(T, value)

            errors: list[Exception] = []
            for item in mapping:
                try:
                    deserialise_from_json_value(
                        klass_item_type, item, fallback
                    )
                except ValueError as e:
                    errors.append(e)

            if len(errors) > 0:
                return fallback(klass_original, value)

        return cast(T, value)

    value_is_json_object = is_json_object(value)
    klass_is_mapping = klass_is_class and issubclass(klass, Mapping)
    klass_origin_is_mapping = klass_origin_is_class and issubclass(
        klass_origin, Mapping
    )
    if value_is_json_object and (klass_is_mapping or klass_origin_is_mapping):
        mapping = cast(Mapping[Any, Any], value)
        if klass_origin is not None:
            klass_type_args = get_args(klass)
            klass_key_type = klass_type_args[0]
            klass_value_type = klass_type_args[1]

            if klass_key_type is str and (
                klass_value_type is JsonValue or klass_value_type is Any
            ):
                return cast(T, value)

            errors: list[Exception] = []
            for item_key, item_value in mapping.items():
                if not isinstance(item_key, str):
                    errors.append(
                        ValueError(f"Key {item_key} is not a string.")
                    )
                    continue

                try:
                    deserialise_from_json_value(
                        klass_value_type, item_value, fallback
                    )
                except ValueError as e:
                    errors.append(e)

            if len(errors) > 0:
                return fallback(klass_original, value)

        return cast(T, value)

    value_is_json_value = is_json_value(value)
    if value_is_json_value and klass is JsonValueType:
        return cast(T, value)

    if (
        value_is_json_value
        and klass_is_class
        and issubclass(klass, JsonValueDeserialisable)
    ):
        return klass.deserialise(value, fallback)

    return fallback(klass_original, value)


def deserialise_from_string[T](
    klass: type[T],
    value: object,
    fallback: Callable[
        [type[T], object], T
    ] = default_deserialisation_fallback,
) -> T:
    klass_is_class = isclass(klass)
    value_is_string = isinstance(value, str)

    if value_is_string and klass_is_class and issubclass(klass, str):
        return cast(T, value)

    if (
        value_is_string
        and klass_is_class
        and issubclass(klass, JsonValueDeserialisable)
    ):
        return klass.deserialise(value, fallback)

    return fallback(klass, value)
