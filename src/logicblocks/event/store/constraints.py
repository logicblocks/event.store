from abc import ABC
from dataclasses import dataclass


class QueryConstraint(ABC): ...


@dataclass(frozen=True)
class SequenceNumberAfterConstraint(QueryConstraint):
    sequence_number: int


@dataclass(frozen=True)
class StreamNamePrefixConstraint(QueryConstraint):
    prefix: str


def sequence_number_after(sequence_number: int) -> QueryConstraint:
    return SequenceNumberAfterConstraint(sequence_number=sequence_number)
