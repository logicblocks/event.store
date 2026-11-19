from abc import ABC
from collections.abc import Sequence
from dataclasses import dataclass

from .clauses import Clause


class Query(ABC):
    """Base class for queries over stored records."""


@dataclass(frozen=True)
class Search(Query):
    """Find the records matching some filters, sorted and paged.

    Filters are applied first, all of which must match, then the sort and
    finally the paging.

    Attributes:
        filters: The clauses that records must satisfy. Defaults to no
            filters.
        sort: The clause by which to order results, if any.
        paging: The clause by which to limit results to a page, if any.
    """

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
    """Find the records matching some filters, typically to locate one.

    Attributes:
        filters: The clauses that records must satisfy, all of which must
            match. Defaults to no filters.
    """

    filters: Sequence[Clause]

    def __init__(
        self,
        *,
        filters: Sequence[Clause] | None = None,
    ):
        object.__setattr__(
            self, "filters", filters if filters is not None else []
        )
