from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from functools import reduce
from typing import Any, Self

from logicblocks.event.query import (
    Clause,
    Lookup,
    Query,
    Search,
)
from logicblocks.event.types import Converter

from ..types import (
    ProjectionResultSet,
    ProjectionResultSetTransformer,
)
from .clause import ClauseConverter


class QueryConverter[Q: Query = Query](
    Converter[Q, ProjectionResultSetTransformer], ABC
):
    @abstractmethod
    def convert(self, item: Q) -> ProjectionResultSetTransformer:
        raise NotImplementedError


def compose_transformers(
    functions: Sequence[ProjectionResultSetTransformer],
) -> ProjectionResultSetTransformer:
    def accumulator(
        f: ProjectionResultSetTransformer,
        g: ProjectionResultSetTransformer,
    ) -> ProjectionResultSetTransformer:
        def handler(projections: ProjectionResultSet) -> ProjectionResultSet:
            return g(f(projections))

        return handler

    def initial(projections: ProjectionResultSet) -> ProjectionResultSet:
        return projections

    return reduce(accumulator, functions, initial)


class SearchQueryConverter(QueryConverter[Search]):
    def __init__(self, clause_converter: ClauseConverter[Clause]):
        self._clause_converter = clause_converter

    def convert(self, item: Search) -> ProjectionResultSetTransformer:
        filters = item.filters
        sort = item.sort
        paging = item.paging

        return compose_transformers(
            [
                self._clause_converter.convert(clause)
                for clause in (list(filters) + [sort] + [paging])
                if clause is not None
            ]
        )


class LookupQueryConverter(QueryConverter[Lookup]):
    def __init__(self, clause_converter: ClauseConverter[Clause]):
        self._clause_converter = clause_converter

    def convert(self, item: Lookup) -> ProjectionResultSetTransformer:
        filters = item.filters

        return compose_transformers(
            [self._clause_converter.convert(clause) for clause in filters]
        )


class TypeRegistryQueryConverter(QueryConverter):
    def __init__(
        self, registry: Mapping[type[Query], QueryConverter[Any]] | None = None
    ):
        self._registry = dict(registry) if registry is not None else {}

    def register[Q: Query](
        self, query_type: type[Q], converter: QueryConverter[Q]
    ) -> Self:
        self._registry[query_type] = converter
        return self

    def convert(self, item: Query) -> ProjectionResultSetTransformer:
        if item.__class__ not in self._registry:
            raise ValueError(f"Unsupported query type: {item}.")
        return self._registry[item.__class__].convert(item)
