"""Write conditions for conditional (optimistic concurrency) publishing.

A write condition is evaluated against the current state of a stream at the
time of a write, under the storage adapter's write lock. If the condition is
not met, the write is rejected with `UnmetWriteConditionError` and no events
are stored.

Conditions can be combined with `&` (all must be met) and `|` (at least one
must be met), e.g. `position_is(3) | stream_is_empty()`.
"""

from abc import ABC
from dataclasses import dataclass
from typing import final, override


class WriteCondition(ABC):
    """Base class for conditions that must hold for a write to succeed.

    Conditions are immutable and support combination with `&` and `|`.
    Combining with a condition of the same compound type flattens the result,
    so `a & b & c` is a single `AndCondition` of three conditions.
    """

    def _combine(
        self,
        other: "WriteCondition",
        combination_type: type["AndCondition | OrCondition"],
    ) -> "WriteCondition":
        if isinstance(self, combination_type) and isinstance(
            other, combination_type
        ):
            return combination_type.construct(
                *self.conditions, *other.conditions
            )
        elif isinstance(self, combination_type):
            return combination_type.construct(*self.conditions, other)
        elif isinstance(other, combination_type):
            return combination_type.construct(self, *other.conditions)
        else:
            return combination_type.construct(self, other)

    def _and(self, other: "WriteCondition") -> "WriteCondition":
        return self._combine(other, AndCondition)

    def _or(self, other: "WriteCondition") -> "WriteCondition":
        return self._combine(other, OrCondition)

    def __and__(self, other: "WriteCondition") -> "WriteCondition":
        """Combine with another condition such that both must be met.

        Returns:
            an `AndCondition` over both conditions.
        """
        return self._and(other)

    def __or__(self, other: "WriteCondition") -> "WriteCondition":
        """Combine with another condition such that either must be met.

        Returns:
            an `OrCondition` over both conditions.
        """
        return self._or(other)


@final
@dataclass(frozen=True)
class AndCondition(WriteCondition):
    """Condition met when all of its constituent conditions are met.

    Typically created with the `&` operator rather than directly.

    Attributes:
        conditions: The conditions that must all be met.
    """

    conditions: frozenset[WriteCondition]

    @classmethod
    def construct(cls, *conditions: WriteCondition) -> "AndCondition":
        """Create an `AndCondition` from the given conditions.

        Args:
            *conditions: The conditions that must all be met.

        Returns:
            the combined condition.
        """
        return cls(conditions=frozenset(conditions))

    @override
    def __eq__(self, other: object) -> bool:
        if not isinstance(other, AndCondition):
            return NotImplemented

        return self.conditions == other.conditions

    @override
    def __hash__(self) -> int:
        return hash((self.__class__.__name__, self.conditions))


@final
@dataclass(frozen=True)
class OrCondition(WriteCondition):
    """Condition met when at least one of its constituent conditions is met.

    Typically created with the `|` operator rather than directly.

    Attributes:
        conditions: The conditions of which at least one must be met.
    """

    conditions: frozenset[WriteCondition]

    @classmethod
    def construct(cls, *conditions: WriteCondition) -> "OrCondition":
        """Create an `OrCondition` from the given conditions.

        Args:
            *conditions: The conditions of which at least one must be met.

        Returns:
            the combined condition.
        """
        return cls(conditions=frozenset(conditions))

    @override
    def __eq__(self, other: object) -> bool:
        if not isinstance(other, OrCondition):
            return NotImplemented

        return self.conditions == other.conditions

    @override
    def __hash__(self) -> int:
        return hash((self.__class__.__name__, self.conditions))


@final
@dataclass(frozen=True)
class NoCondition(WriteCondition):
    """Condition that is always met. The default for unconditional writes."""

    @override
    def __eq__(self, other: object) -> bool:
        if isinstance(other, NoCondition):
            return True

        return NotImplemented

    @override
    def __hash__(self) -> int:
        return hash(self.__class__.__name__)


@final
@dataclass(frozen=True)
class PositionIsCondition(WriteCondition):
    """Condition met when the stream's latest event is at a given position.

    Stream positions are zero-based, so the first event in a stream has
    position 0. A position of `None` is met only when the stream is empty.

    Attributes:
        position: The expected position of the latest event in the stream.
    """

    position: int | None

    @override
    def __eq__(self, other: object) -> bool:
        if not isinstance(other, PositionIsCondition):
            return NotImplemented

        return self.position == other.position

    @override
    def __hash__(self) -> int:
        return hash((self.__class__.__name__, self.position))


@dataclass(frozen=True)
class EmptyStreamCondition(WriteCondition):
    """Condition met when the stream contains no events."""

    @override
    def __eq__(self, other: object) -> bool:
        if isinstance(other, EmptyStreamCondition):
            return True

        return NotImplemented

    @override
    def __hash__(self) -> int:
        return hash(self.__class__.__name__)


def position_is(position: int | None) -> WriteCondition:
    """Create a condition that the stream's latest event is at a position.

    Args:
        position: The expected zero-based position of the latest event, or
            `None` to expect an empty stream.

    Returns:
        a `PositionIsCondition` for the position.
    """
    return PositionIsCondition(position=position)


def stream_is_empty() -> WriteCondition:
    """Create a condition that the stream contains no events.

    Returns:
        an `EmptyStreamCondition`.
    """
    return EmptyStreamCondition()
