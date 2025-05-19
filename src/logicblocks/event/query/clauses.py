from abc import ABC
from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from .functions import Function
from .utilities import Path


class Operator(StrEnum):
    EQUAL = "equal"
    NOT_EQUAL = "not_equal"
    GREATER_THAN = "greater_than"
    GREATER_THAN_OR_EQUAL = "greater_than_or_equal"
    LESS_THAN = "less_than"
    LESS_THAN_OR_EQUAL = "less_than_or_equal"
    IN = "in"
    CONTAINS = "contains"


class SortOrder(StrEnum):
    ASC = "asc"
    DESC = "desc"


class Clause(ABC):
    pass


@dataclass(frozen=True)
class FilterClause(Clause):
    operator: Operator
    field: Path
    value: Any


@dataclass(frozen=True)
class SortField(Clause):
    field: Path | Function
    order: SortOrder


@dataclass(frozen=True)
class SortClause(Clause):
    fields: Sequence[SortField]


class PagingClause(Clause):
    pass


class PagingDirection(StrEnum):
    FORWARDS = "forwards"
    BACKWARDS = "backwards"


@dataclass(frozen=True)
class KeySetPagingClause(PagingClause):
    last_id: str | None = None
    direction: PagingDirection = PagingDirection.FORWARDS
    item_count: int = 10

    def is_forwards(self):
        return (
            self.last_id is not None
            and self.direction == PagingDirection.FORWARDS
        )

    def is_backwards(self):
        return (
            self.last_id is not None
            and self.direction == PagingDirection.BACKWARDS
        )

    def is_first_page(self):
        return self.last_id is None


@dataclass(frozen=True)
class OffsetPagingClause(PagingClause):
    page_number: int
    item_count: int

    def __init__(self, *, page_number: int = 1, item_count: int = 10):
        object.__setattr__(self, "page_number", page_number)
        object.__setattr__(self, "item_count", item_count)

    @property
    def offset(self):
        return (self.page_number - 1) * self.item_count
