from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Sequence


class Operator(StrEnum):
    EQUAL = "equal"
    NOT_EQUAL = "not_equal"
    GREATER_THAN = "greater_than"
    GREATER_THAN_OR_EQUAL = "greater_than_or_equal"
    LESS_THAN = "less_than"
    LESS_THAN_OR_EQUAL = "less_than_or_equal"


class SortOrder(StrEnum):
    ASC = "asc"
    DESC = "desc"


@dataclass(frozen=True)
class Path:
    top_level: str
    sub_levels: Sequence[str | int]

    def __init__(self, top_level: str, *sub_levels: str | int):
        object.__setattr__(self, "top_level", top_level)
        object.__setattr__(self, "sub_levels", sub_levels)

    def __repr__(self):
        return repr([self.top_level, *self.sub_levels])

    def is_nested(self):
        return len(self.sub_levels) > 0


class Clause:
    pass


@dataclass(frozen=True)
class FilterClause(Clause):
    operator: Operator
    path: Path
    value: Any


@dataclass(frozen=True)
class SortField(Clause):
    path: Path
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


class Query:
    pass


@dataclass(frozen=True)
class Search(Query):
    filters: Sequence[Clause]
    sort: Clause | None
    paging: Clause | None

    def __init__(
        self,
        *,
        filters: Sequence[Clause] | None = None,
        sort: Clause | None = None,
        paging: Clause | None = None,
    ):
        object.__setattr__(
            self, "filters", filters if filters is not None else []
        )
        object.__setattr__(self, "sort", sort)
        object.__setattr__(self, "paging", paging)


@dataclass(frozen=True)
class Lookup(Query):
    filters: Sequence[Clause]

    def __init__(
        self,
        *,
        filters: Sequence[Clause] | None = None,
    ):
        object.__setattr__(
            self, "filters", filters if filters is not None else []
        )
