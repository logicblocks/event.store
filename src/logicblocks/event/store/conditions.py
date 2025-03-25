from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import StrEnum
from typing import final

from logicblocks.event.store.exceptions import UnmetWriteConditionError
from logicblocks.event.types import StoredEvent


class ConditionCombinators(StrEnum):
    AND = "and"
    OR = "or"


class WriteCondition(ABC):
    @abstractmethod
    def assert_met_by(self, *, last_event: StoredEvent | None) -> None:
        raise NotImplementedError()

    @abstractmethod
    def __eq__(self, other: object) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def __hash__(self) -> int:
        raise NotImplementedError()

    def _and(self, other: "WriteCondition") -> "WriteCondition":
        make_write_conditions = WriteConditions.with_and

        match self, other:
            case WriteConditions(
                conditions=self_conditions, combinator=ConditionCombinators.AND
            ), WriteConditions(
                conditions=other_conditions,
                combinator=ConditionCombinators.AND,
            ):
                return make_write_conditions(
                    *self_conditions, *other_conditions
                )

            case WriteConditions(
                conditions=self_conditions, combinator=ConditionCombinators.AND
            ), _:
                return make_write_conditions(*self_conditions, other)

            case _, WriteConditions(
                conditions=other_conditions,
                combinator=ConditionCombinators.AND,
            ):
                return make_write_conditions(self, *other_conditions)

            case _, _:
                return make_write_conditions(self, other)

    def _or(self, other: "WriteCondition") -> "WriteCondition":
        make_write_conditions = WriteConditions.with_or

        match self, other:
            case WriteConditions(
                conditions=self_conditions, combinator=ConditionCombinators.OR
            ), WriteConditions(
                conditions=other_conditions, combinator=ConditionCombinators.OR
            ):
                return make_write_conditions(
                    *self_conditions, *other_conditions
                )

            case WriteConditions(
                conditions=self_conditions, combinator=ConditionCombinators.OR
            ), _:
                return make_write_conditions(*self_conditions, other)

            case _, WriteConditions(
                conditions=other_conditions, combinator=ConditionCombinators.OR
            ):
                return make_write_conditions(self, *other_conditions)

            case _, _:
                return make_write_conditions(self, other)

    def __and__(self, other: "WriteCondition") -> "WriteCondition":
        return self._and(other)

    def __or__(self, other: "WriteCondition") -> "WriteCondition":
        return self._or(other)


@final
@dataclass(frozen=True)
class WriteConditions(WriteCondition):
    conditions: frozenset[WriteCondition]
    combinator: ConditionCombinators

    @classmethod
    def for_combinator(
        cls, combinator: ConditionCombinators, *conditions: WriteCondition
    ) -> "WriteConditions":
        return WriteConditions(
            conditions=frozenset(conditions), combinator=combinator
        )

    @classmethod
    def with_or(cls, *conditions: WriteCondition) -> "WriteConditions":
        return WriteConditions.for_combinator(
            ConditionCombinators.OR, *conditions
        )

    @classmethod
    def with_and(cls, *conditions: WriteCondition) -> "WriteConditions":
        return WriteConditions.for_combinator(
            ConditionCombinators.AND, *conditions
        )

    def assert_met_by(self, *, last_event: StoredEvent | None) -> None:
        match self.combinator:
            case ConditionCombinators.AND:
                for condition in self.conditions:
                    condition.assert_met_by(last_event=last_event)
            case ConditionCombinators.OR:
                first_exception = None
                for condition in self.conditions:
                    try:
                        condition.assert_met_by(last_event=last_event)
                        return
                    except UnmetWriteConditionError as e:
                        first_exception = e
                if first_exception is not None:
                    raise first_exception
            case _:
                raise NotImplementedError()


@dataclass(frozen=True)
class _NoCondition(WriteCondition):
    def assert_met_by(self, *, last_event: StoredEvent | None):
        pass


NoCondition = _NoCondition()


@dataclass(frozen=True)
class PositionIsCondition(WriteCondition):
    position: int

    def assert_met_by(self, *, last_event: StoredEvent | None):
        if last_event is None or last_event.position != self.position:
            raise UnmetWriteConditionError("unexpected stream position")


@dataclass(frozen=True)
class EmptyStreamCondition(WriteCondition):
    def assert_met_by(self, *, last_event: StoredEvent | None):
        if last_event is not None:
            raise UnmetWriteConditionError("stream is not empty")


def position_is(position: int) -> WriteCondition:
    return PositionIsCondition(position=position)


def stream_is_empty() -> WriteCondition:
    return EmptyStreamCondition()
