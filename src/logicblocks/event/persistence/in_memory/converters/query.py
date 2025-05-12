from collections.abc import Sequence
from functools import reduce
from typing import Self

from logicblocks.event.query import (
    Clause,
    FilterClause,
    KeySetPagingClause,
    Lookup,
    OffsetPagingClause,
    Query,
    Search,
    SortClause,
)
from logicblocks.event.types import Converter

from ...converter import TypeRegistryConverter
from .clause import (
    FilterClauseConverter,
    KeySetPagingClauseConverter,
    OffsetPagingClauseConverter,
    SortClauseConverter,
    TypeRegistryClauseConverter,
)
from .types import (
    ClauseConverter,
    Identifiable,
    QueryConverter,
    ResultSet,
    ResultSetTransformer,
)


def compose_transformers[R: Identifiable](
    functions: Sequence[ResultSetTransformer[R]],
) -> ResultSetTransformer[R]:
    def accumulator(
        f: ResultSetTransformer[R],
        g: ResultSetTransformer[R],
    ) -> ResultSetTransformer[R]:
        def handler(results: ResultSet[R]) -> ResultSet[R]:
            return g(f(results))

        return handler

    def initial(results: ResultSet[R]) -> ResultSet[R]:
        return results

    return reduce(accumulator, functions, initial)


class SearchQueryConverter[R: Identifiable](QueryConverter[R, Search]):
    def __init__(
        self, clause_converter: Converter[Clause, ResultSetTransformer[R]]
    ):
        self._clause_converter = clause_converter

    def convert(self, item: Search) -> ResultSetTransformer[R]:
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


class LookupQueryConverter[R: Identifiable](QueryConverter[R, Lookup]):
    def __init__(
        self, clause_converter: Converter[Clause, ResultSetTransformer[R]]
    ):
        self._clause_converter = clause_converter

    def convert(self, item: Lookup) -> ResultSetTransformer[R]:
        filters = item.filters

        return compose_transformers(
            [self._clause_converter.convert(clause) for clause in filters]
        )


class TypeRegistryQueryConverter[R: Identifiable](
    TypeRegistryConverter[Query, ResultSetTransformer[R]]
):
    def register[Q: Query](
        self,
        item_type: type[Q],
        converter: Converter[Q, ResultSetTransformer[R]],
    ) -> Self:
        return super()._register(item_type, converter)


class DelegatingQueryConverter[R: Identifiable](
    Converter[Query, ResultSetTransformer[R]]
):
    def __init__(
        self,
        clause_converter: TypeRegistryClauseConverter[R] | None = None,
        query_converter: TypeRegistryQueryConverter[R] | None = None,
    ):
        self._clause_converter = (
            clause_converter
            if clause_converter is not None
            else TypeRegistryClauseConverter[R]()
        )
        self._query_converter = (
            query_converter
            if query_converter is not None
            else TypeRegistryQueryConverter[R]()
        )

    def with_default_clause_converters(self) -> Self:
        return (
            self.register_clause_converter(
                FilterClause, FilterClauseConverter[R]()
            )
            .register_clause_converter(SortClause, SortClauseConverter[R]())
            .register_clause_converter(
                KeySetPagingClause, KeySetPagingClauseConverter[R]()
            )
            .register_clause_converter(
                OffsetPagingClause, OffsetPagingClauseConverter[R]()
            )
        )

    def with_default_query_converters(self) -> Self:
        return self.register_query_converter(
            Search, SearchQueryConverter[R](self._clause_converter)
        ).register_query_converter(
            Lookup, LookupQueryConverter[R](self._clause_converter)
        )

    def register_clause_converter[C: Clause](
        self, clause_type: type[C], converter: ClauseConverter[R, C]
    ) -> Self:
        self._clause_converter.register(clause_type, converter)
        return self

    def register_query_converter[Q: Query](
        self, query_type: type[Q], converter: QueryConverter[R, Q]
    ) -> Self:
        self._query_converter.register(query_type, converter)
        return self

    def convert_clause(self, item: Clause) -> ResultSetTransformer[R]:
        return self._clause_converter.convert(item)

    def convert_query(self, item: Query) -> ResultSetTransformer[R]:
        return self._query_converter.convert(item)

    def convert(self, item: Query) -> ResultSetTransformer[R]:
        return self.convert_query(item)
