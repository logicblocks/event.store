from abc import ABC, abstractmethod
from dataclasses import dataclass

from logicblocks.event.types import StoredEvent


class QueryConstraint(ABC):
    @abstractmethod
    def met_by(self, *, event: StoredEvent) -> bool:
        raise NotImplementedError()


@dataclass(frozen=True)
class SequenceNumberAfterConstraint(QueryConstraint):
    sequence_number: int

    def met_by(self, *, event: StoredEvent) -> bool:
        return event.sequence_number > self.sequence_number


def sequence_number_after(sequence_number: int) -> QueryConstraint:
    return SequenceNumberAfterConstraint(sequence_number=sequence_number)
