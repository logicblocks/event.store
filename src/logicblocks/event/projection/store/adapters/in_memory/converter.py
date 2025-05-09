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

from .converters import (
    ClauseConverter,
    FilterClauseConverter,
    KeySetPagingClauseConverter,
    LookupQueryConverter,
    OffsetPagingClauseConverter,
    QueryConverter,
    SearchQueryConverter,
    SortClauseConverter,
    TypeRegistryClauseConverter,
    TypeRegistryQueryConverter,
)
from .types import ProjectionResultSetTransformer


class InMemoryQueryConverter(Converter[Query, ProjectionResultSetTransformer]):
    def __init__(
        self,
        clause_converter: TypeRegistryClauseConverter | None = None,
        query_converter: TypeRegistryQueryConverter | None = None,
    ):
        self._clause_converter = (
            clause_converter
            if clause_converter is not None
            else TypeRegistryClauseConverter()
        )
        self._query_converter = (
            query_converter
            if query_converter is not None
            else TypeRegistryQueryConverter()
        )

    def with_default_clause_converters(self) -> Self:
        return (
            self.register_clause_converter(
                FilterClause, FilterClauseConverter()
            )
            .register_clause_converter(SortClause, SortClauseConverter())
            .register_clause_converter(
                KeySetPagingClause, KeySetPagingClauseConverter()
            )
            .register_clause_converter(
                OffsetPagingClause, OffsetPagingClauseConverter()
            )
        )

    def with_default_query_converters(self) -> Self:
        return self.register_query_converter(
            Search, SearchQueryConverter(self._clause_converter)
        ).register_query_converter(
            Lookup, LookupQueryConverter(self._clause_converter)
        )

    def register_clause_converter[C: Clause](
        self, clause_type: type[C], converter: ClauseConverter[C]
    ) -> Self:
        self._clause_converter.register(clause_type, converter)
        return self

    def register_query_converter[Q: Query](
        self, query_type: type[Q], converter: QueryConverter[Q]
    ) -> Self:
        self._query_converter.register(query_type, converter)
        return self

    def convert_clause(self, item: Clause) -> ProjectionResultSetTransformer:
        return self._clause_converter.convert(item)

    def convert_query(self, item: Query) -> ProjectionResultSetTransformer:
        return self._query_converter.convert(item)

    def convert(self, item: Query) -> ProjectionResultSetTransformer:
        return self.convert_query(item)
