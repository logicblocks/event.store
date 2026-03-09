from abc import ABC
from collections.abc import Callable
from dataclasses import dataclass

from logicblocks.event.types import Event

type QueryConstraintCheck[E: Event] = Callable[[E], bool]


class QueryConstraint(ABC): ...


@dataclass(frozen=True)
class SequenceNumberAfterConstraint(QueryConstraint):
    sequence_number: int


@dataclass(frozen=True)
class OffsetPagingConstraint(QueryConstraint):
    page_number: int
    item_count: int

    def __init__(self, *, page_number: int = 1, item_count: int = 10):
        if page_number < 1:
            raise ValueError("page_number must be >= 1")
        if item_count < 1:
            raise ValueError("item_count must be >= 1")
        object.__setattr__(self, "page_number", page_number)
        object.__setattr__(self, "item_count", item_count)

    @property
    def offset(self):
        return (self.page_number - 1) * self.item_count


def sequence_number_after(sequence_number: int) -> QueryConstraint:
    return SequenceNumberAfterConstraint(sequence_number=sequence_number)


def offset_paging(
    *, page_number: int = 1, item_count: int = 10
) -> QueryConstraint:
    return OffsetPagingConstraint(
        page_number=page_number, item_count=item_count
    )
