from abc import ABC
from collections.abc import Callable
from dataclasses import dataclass

from logicblocks.event.types import Event, JsonValue

type QueryConstraintCheck[E: Event] = Callable[[E], bool]


class QueryConstraint(ABC): ...


@dataclass(frozen=True)
class OrderingIdAfterConstraint(QueryConstraint):
    ordering_id: JsonValue


def ordering_id_after(ordering_id: JsonValue) -> QueryConstraint:
    return OrderingIdAfterConstraint(ordering_id=ordering_id)


def SequenceNumberAfterConstraint(sequence_number: int):
    return OrderingIdAfterConstraint(ordering_id=sequence_number)


def sequence_number_after(sequence_number: int) -> QueryConstraint:
    return SequenceNumberAfterConstraint(sequence_number=sequence_number)
