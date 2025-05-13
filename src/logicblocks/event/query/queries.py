from abc import ABC
from collections.abc import Sequence
from dataclasses import dataclass

from .clauses import Clause


class Query(ABC):
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
