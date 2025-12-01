from abc import ABC
from dataclasses import dataclass
from typing import final, override


class WriteCondition(ABC):
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
        return self._and(other)

    def __or__(self, other: "WriteCondition") -> "WriteCondition":
        return self._or(other)


@final
@dataclass(frozen=True)
class AndCondition(WriteCondition):
    conditions: frozenset[WriteCondition]

    @classmethod
    def construct(cls, *conditions: WriteCondition) -> "AndCondition":
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
    conditions: frozenset[WriteCondition]

    @classmethod
    def construct(cls, *conditions: WriteCondition) -> "OrCondition":
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
    @override
    def __eq__(self, other: object) -> bool:
        if isinstance(other, EmptyStreamCondition):
            return True

        return NotImplemented

    @override
    def __hash__(self) -> int:
        return hash(self.__class__.__name__)


def position_is(position: int | None) -> WriteCondition:
    return PositionIsCondition(position=position)


def stream_is_empty() -> WriteCondition:
    return EmptyStreamCondition()
