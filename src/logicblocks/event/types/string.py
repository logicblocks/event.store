from collections.abc import Callable
from typing import Any, Protocol, Self, runtime_checkable


@runtime_checkable
class StringSerialisable(Protocol):
    def serialise(self, fallback: Callable[[object], str]) -> str:
        raise NotImplementedError


@runtime_checkable
class StringDeserialisable(Protocol):
    @classmethod
    def deserialise(
        cls, value: str, fallback: Callable[[type[Any], str], Any]
    ) -> Self:
        raise NotImplementedError


@runtime_checkable
class StringConvertible(StringSerialisable, StringDeserialisable, Protocol):
    pass
