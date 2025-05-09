from typing import Self

from logicblocks.event import query as query
from logicblocks.event.types import Converter

from .. import query as postgresquery
from ..settings import TableSettings
from ..types import ParameterisedQuery
from .clause import (
    FilterClauseConverter,
    KeySetPagingClauseConverter,
    OffsetPagingClauseConverter,
    SortClauseConverter,
    TypeRegistryClauseConverter,
)
from .query import (
    LookupQueryConverter,
    SearchQueryConverter,
    TypeRegistryQueryConverter,
)
from .types import ClauseConverter, QueryConverter


class DelegatingQueryConverter(Converter[query.Query, ParameterisedQuery]):
    def __init__(
        self,
        table_settings: TableSettings,
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
        self._table_settings = table_settings

    def with_default_clause_converters(self) -> Self:
        return (
            self.register_clause_converter(
                query.FilterClause, FilterClauseConverter()
            )
            .register_clause_converter(query.SortClause, SortClauseConverter())
            .register_clause_converter(
                query.KeySetPagingClause,
                KeySetPagingClauseConverter(
                    table_settings=self._table_settings
                ),
            )
            .register_clause_converter(
                query.OffsetPagingClause, OffsetPagingClauseConverter()
            )
        )

    def with_default_query_converters(self) -> Self:
        return self.register_query_converter(
            query.Search,
            SearchQueryConverter(self._clause_converter, self._table_settings),
        ).register_query_converter(
            query.Lookup,
            LookupQueryConverter(self._clause_converter, self._table_settings),
        )

    def register_clause_converter[C: query.Clause](
        self, clause_type: type[C], converter: ClauseConverter[C]
    ) -> Self:
        self._clause_converter.register(clause_type, converter)
        return self

    def register_query_converter[Q: query.Query](
        self, query_type: type[Q], converter: QueryConverter[Q]
    ) -> Self:
        self._query_converter.register(query_type, converter)
        return self

    def apply_clause(
        self, clause: query.Clause, query_builder: postgresquery.Query
    ) -> postgresquery.Query:
        return self._clause_converter.convert(clause).apply(query_builder)

    def convert_query(
        self, item: query.Query
    ) -> postgresquery.ParameterisedQuery:
        return self._query_converter.convert(item)

    def convert(self, item: query.Query) -> postgresquery.ParameterisedQuery:
        return self.convert_query(item)
