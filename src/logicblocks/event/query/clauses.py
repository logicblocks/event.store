from abc import ABC
from collections.abc import Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from .functions import Function
from .utilities import Path


class Operator(StrEnum):
    """Comparison operators available to a `FilterClause`.

    In each case the left operand is the value at the clause's field and
    the right operand is the clause's value.

    Attributes:
        EQUAL: The field equals the value. On Postgres, a value of `None`
            matches a null field.
        NOT_EQUAL: The field does not equal the value. On Postgres, a value
            of `None` matches a non-null field.
        GREATER_THAN: The field is greater than the value.
        GREATER_THAN_OR_EQUAL: The field is greater than or equal to the
            value.
        LESS_THAN: The field is less than the value.
        LESS_THAN_OR_EQUAL: The field is less than or equal to the value.
        IN: The field is one of the values in the given sequence.
        CONTAINS: The field contains the value, for example a list
            containing an element or, on Postgres, a JSON object containing
            the given sub-object.
        REGEX_MATCHES: The field is a string matching the regular
            expression given as the value.
        NOT_REGEX_MATCHES: The field is a string not matching the regular
            expression given as the value.
    """

    EQUAL = "equal"
    NOT_EQUAL = "not_equal"
    GREATER_THAN = "greater_than"
    GREATER_THAN_OR_EQUAL = "greater_than_or_equal"
    LESS_THAN = "less_than"
    LESS_THAN_OR_EQUAL = "less_than_or_equal"
    IN = "in"
    CONTAINS = "contains"
    REGEX_MATCHES = "regex_matches"
    NOT_REGEX_MATCHES = "not_regex_matches"


class SortOrder(StrEnum):
    """Direction in which a `SortField` orders results.

    Attributes:
        ASC: Ascending order.
        DESC: Descending order.
    """

    ASC = "asc"
    DESC = "desc"


class Clause(ABC):
    """Base class for the parts of a query: filters, sorts and paging."""


@dataclass(frozen=True)
class FilterClause(Clause):
    """Restrict results to those whose field satisfies a comparison.

    Attributes:
        operator: The comparison to perform.
        field: The path to the value to compare against.
        value: The value to compare the field with.
    """

    operator: Operator
    field: Path
    value: Any


@dataclass(frozen=True)
class SortField(Clause):
    """A single field, or function result, to sort by.

    Attributes:
        field: The path to sort by, or a function whose result is sorted by
            under its alias.
        order: The direction to sort in.
    """

    field: Path | Function
    order: SortOrder


@dataclass(frozen=True)
class SortClause(Clause):
    """Order results by one or more fields.

    Attributes:
        fields: The fields to sort by, in priority order: later fields only
            break ties between earlier ones.
    """

    fields: Sequence[SortField]


class PagingClause(Clause):
    """Base class for clauses that limit results to a single page."""


class PagingDirection(StrEnum):
    """Direction to page in, relative to a known record, in key-set paging.

    Attributes:
        FORWARDS: Page through the records after the known record.
        BACKWARDS: Page through the records before the known record.
    """

    FORWARDS = "forwards"
    BACKWARDS = "backwards"


@dataclass(frozen=True)
class KeySetPagingClause(PagingClause):
    """Page through results relative to a known record.

    Key-set paging identifies a page by the ID of a record at its boundary
    rather than by position, so pages remain consistent while records are
    added or removed. Records are ordered by any sort clause in the query;
    the Postgres implementation additionally orders by ID to break ties and,
    when there is no sort clause, orders by ID alone.

    With no `last_id`, the first `item_count` records are returned. With a
    `last_id` and forwards direction, up to `item_count` records
    immediately after that record are returned. With a `last_id` and
    backwards direction, up to `item_count` records immediately before that
    record are returned, still in sort order. The record with `last_id` is
    itself excluded.

    Typically, pass the ID of the last record of the current page to move
    forwards, or the ID of the first record to move backwards.

    Attributes:
        last_id: The ID of the record to page relative to, or `None` for the
            first page.
        direction: Whether to page after or before the record with
            `last_id`.
        item_count: The maximum number of records in the page.
    """

    last_id: str | None = None
    direction: PagingDirection = PagingDirection.FORWARDS
    item_count: int = 10

    def is_forwards(self):
        """Return whether this requests the page after `last_id`."""
        return (
            self.last_id is not None
            and self.direction == PagingDirection.FORWARDS
        )

    def is_backwards(self):
        """Return whether this requests the page before `last_id`."""
        return (
            self.last_id is not None
            and self.direction == PagingDirection.BACKWARDS
        )

    def is_first_page(self):
        """Return whether this requests the first page."""
        return self.last_id is None


@dataclass(frozen=True)
class OffsetPagingClause(PagingClause):
    """Page through results by page number.

    Page `n` skips the first `(n - 1) * item_count` records, in sort order,
    and returns up to `item_count` records. Unlike key-set paging, pages
    shift if records are added or removed between requests.

    Attributes:
        page_number: The 1-based number of the page to return. Defaults to
            1.
        item_count: The maximum number of records in a page. Defaults to
            10.
    """

    page_number: int
    item_count: int

    def __init__(self, *, page_number: int = 1, item_count: int = 10):
        object.__setattr__(self, "page_number", page_number)
        object.__setattr__(self, "item_count", item_count)

    @property
    def offset(self):
        """The number of records preceding the requested page."""
        return (self.page_number - 1) * self.item_count
