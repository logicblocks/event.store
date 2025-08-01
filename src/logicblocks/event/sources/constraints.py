from abc import ABC
from collections.abc import Callable
from dataclasses import dataclass

from logicblocks.event.types import Event

type QueryConstraintCheck[E: Event] = Callable[[E], bool]


class QueryConstraint(ABC): ...


@dataclass(frozen=True)
class SequenceNumberAfterConstraint(QueryConstraint):
    sequence_number: int


def sequence_number_after(sequence_number: int) -> QueryConstraint:
    return SequenceNumberAfterConstraint(sequence_number=sequence_number)
