from collections.abc import Callable
from typing import Any, Protocol, Self, runtime_checkable


@runtime_checkable
class StringSerialisable(Protocol):
    """Protocol for objects that can serialise themselves to a string."""

    def serialise(self, fallback: Callable[[object], str]) -> str:
        """Serialise the object to a string.

        Args:
            fallback: Called to serialise any nested value that cannot
                otherwise be serialised.

        Returns:
            the serialised string.
        """
        raise NotImplementedError


@runtime_checkable
class StringDeserialisable(Protocol):
    """Protocol for classes that can deserialise instances from a string."""

    @classmethod
    def deserialise(
        cls, value: str, fallback: Callable[[type[Any], str], Any]
    ) -> Self:
        """Deserialise an instance from a string.

        Args:
            value: The string to deserialise.
            fallback: Called with a target type and value to deserialise
                any nested value that cannot otherwise be deserialised.

        Returns:
            the deserialised instance.
        """
        raise NotImplementedError


@runtime_checkable
class StringConvertible(StringSerialisable, StringDeserialisable, Protocol):
    """Protocol for types that are string serialisable and deserialisable."""

    pass
