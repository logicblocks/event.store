from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, final

from logicblocks.event.types import StoredEvent

from .exceptions import UnmetWriteConditionError


class WriteCondition(ABC):
    @abstractmethod
    def assert_met_by(
        self, *, last_event: StoredEvent[Any, Any] | None
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    def __eq__(self, other: object) -> bool:
        raise NotImplementedError

    @abstractmethod
    def __hash__(self) -> int:
        raise NotImplementedError

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

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, AndCondition):
            return NotImplemented
        return self.conditions == other.conditions

    def __hash__(self) -> int:
        return hash(self.conditions)

    def assert_met_by(
        self, *, last_event: StoredEvent[Any, Any] | None
    ) -> None:
        for condition in self.conditions:
            condition.assert_met_by(last_event=last_event)


@final
@dataclass(frozen=True)
class OrCondition(WriteCondition):
    conditions: frozenset[WriteCondition]

    @classmethod
    def construct(cls, *conditions: WriteCondition) -> "OrCondition":
        return cls(conditions=frozenset(conditions))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, OrCondition):
            return NotImplemented
        return self.conditions == other.conditions

    def __hash__(self) -> int:
        return hash(self.conditions)

    def assert_met_by(
        self, *, last_event: StoredEvent[Any, Any] | None
    ) -> None:
        first_exception = None
        for condition in self.conditions:
            try:
                condition.assert_met_by(last_event=last_event)
                return
            except UnmetWriteConditionError as e:
                first_exception = e
        if first_exception is not None:
            raise first_exception


@dataclass(frozen=True)
class NoCondition(WriteCondition):
    def assert_met_by(self, *, last_event: StoredEvent[Any, Any] | None):
        pass


@dataclass(frozen=True)
class PositionIsCondition(WriteCondition):
    position: int | None

    def assert_met_by(self, *, last_event: StoredEvent[Any, Any] | None):
        latest_position = last_event.position if last_event else None
        if latest_position != self.position:
            raise UnmetWriteConditionError("unexpected stream position")


@dataclass(frozen=True)
class EmptyStreamCondition(WriteCondition):
    def assert_met_by(self, *, last_event: StoredEvent[Any, Any] | None):
        if last_event is not None:
            raise UnmetWriteConditionError("stream is not empty")


def position_is(position: int | None) -> WriteCondition:
    return PositionIsCondition(position=position)


def stream_is_empty() -> WriteCondition:
    return EmptyStreamCondition()
