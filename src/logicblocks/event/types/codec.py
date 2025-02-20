from collections.abc import Mapping
from inspect import isclass
from typing import Any, Protocol, Self, cast, runtime_checkable


@runtime_checkable
class Codec(Protocol):
    def serialise(self) -> Mapping[str, Any]:
        raise NotImplementedError

    @classmethod
    def deserialise(cls, value: Mapping[str, Any]) -> Self:
        raise NotImplementedError


type CodecOrMapping = Codec | Mapping[str, Any]


def serialise(value: CodecOrMapping) -> Mapping[str, Any]:
    if isinstance(value, Codec):
        return value.serialise()
    return value


def deserialise[T: CodecOrMapping](
    klass: type[T], value: Mapping[str, Any]
) -> T:
    if isclass(klass) and issubclass(klass, Codec):
        return klass.deserialise(value)
    return cast(T, value)
