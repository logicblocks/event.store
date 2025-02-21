from collections.abc import Mapping
from inspect import isclass
from types import NoneType
from typing import Any, Protocol, Self, cast, runtime_checkable


@runtime_checkable
class Codec(Protocol):
    def serialise(self) -> Mapping[str, Any]:
        raise NotImplementedError

    @classmethod
    def deserialise(cls, value: Mapping[str, Any]) -> Self:
        raise NotImplementedError


type CodecOrMapping = Codec | None | Mapping[str, Any]


def serialise(value: CodecOrMapping) -> Mapping[str, Any]:
    if value is None:
        raise ValueError("Cannot serialise None.")
    if isinstance(value, Codec):
        return value.serialise()
    return value


def deserialise[T: CodecOrMapping](
    klass: type[T], value: Mapping[str, Any]
) -> T:
    if klass is NoneType:
        raise ValueError("Cannot deserialise to None type.")
    if isclass(klass) and issubclass(klass, Codec):
        return klass.deserialise(value)
    return cast(T, value)
