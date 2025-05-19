from typing import Self

import logicblocks.event.query as genericquery
from logicblocks.event.types import Converter

from ...converter import TypeRegistryConverter
from .. import query as postgresquery
from ..query import QueryApplier
from ..settings import TableSettings
from ..types import ParameterisedQuery
from .clause import (
    FilterClauseConverter,
    KeySetPagingClauseConverter,
    OffsetPagingClauseConverter,
    SortClauseConverter,
    TypeRegistryClauseConverter,
)
from .function import (
    SimilarityFunctionConverter,
    TypeRegistryFunctionConverter,
)
from .types import ClauseConverter, FunctionConverter, QueryConverter


class SearchQueryConverter(QueryConverter[genericquery.Search]):
    def __init__(
        self,
        clause_converter: Converter[genericquery.Clause, QueryApplier],
        table_settings: TableSettings,
    ):
        self._clause_converter = clause_converter
        self._table_settings = table_settings

    def convert(self, item: genericquery.Search) -> ParameterisedQuery:
        filters = item.filters
        sort = item.sort
        paging = item.paging

        builder = (
            postgresquery.Query()
            .select_all()
            .from_table(self._table_settings.table_name)
        )

        for filter in filters:
            builder = self._clause_converter.convert(filter).apply(builder)
        if sort is not None:
            builder = self._clause_converter.convert(sort).apply(builder)
        if paging is not None:
            builder = self._clause_converter.convert(paging).apply(builder)

        return builder.build()


class LookupQueryConverter(QueryConverter[genericquery.Lookup]):
    def __init__(
        self,
        clause_converter: Converter[genericquery.Clause, QueryApplier],
        table_settings: TableSettings,
    ):
        self._clause_converter = clause_converter
        self._table_settings = table_settings

    def convert(self, item: genericquery.Lookup) -> ParameterisedQuery:
        filters = item.filters

        builder = (
            postgresquery.Query()
            .select_all()
            .from_table(self._table_settings.table_name)
        )

        for filter in filters:
            builder = self._clause_converter.convert(filter).apply(builder)

        return builder.build()


class TypeRegistryQueryConverter(
    TypeRegistryConverter[genericquery.Query, ParameterisedQuery]
):
    def register[Q: genericquery.Query](
        self, item_type: type[Q], converter: Converter[Q, ParameterisedQuery]
    ) -> Self:
        return super()._register(item_type, converter)


class DelegatingQueryConverter(
    Converter[genericquery.Query, ParameterisedQuery]
):
    def __init__(
        self,
        table_settings: TableSettings,
        function_converter: TypeRegistryFunctionConverter | None = None,
        clause_converter: TypeRegistryClauseConverter | None = None,
        query_converter: TypeRegistryQueryConverter | None = None,
    ):
        self._function_converter = (
            function_converter
            if function_converter is not None
            else TypeRegistryFunctionConverter()
        )
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

    def with_default_function_converters(self) -> Self:
        return self.register_function_converter(
            genericquery.Similarity, SimilarityFunctionConverter()
        )

    def with_default_clause_converters(self) -> Self:
        return (
            self.register_clause_converter(
                genericquery.FilterClause, FilterClauseConverter()
            )
            .register_clause_converter(
                genericquery.SortClause,
                SortClauseConverter(self._function_converter),
            )
            .register_clause_converter(
                genericquery.KeySetPagingClause,
                KeySetPagingClauseConverter(
                    table_settings=self._table_settings
                ),
            )
            .register_clause_converter(
                genericquery.OffsetPagingClause, OffsetPagingClauseConverter()
            )
        )

    def with_default_query_converters(self) -> Self:
        return self.register_query_converter(
            genericquery.Search,
            SearchQueryConverter(self._clause_converter, self._table_settings),
        ).register_query_converter(
            genericquery.Lookup,
            LookupQueryConverter(self._clause_converter, self._table_settings),
        )

    def with_default_converters(self):
        return (
            self.with_default_function_converters()
            .with_default_clause_converters()
            .with_default_query_converters()
        )

    def register_function_converter[F: genericquery.Function](
        self, function_type: type[F], converter: FunctionConverter[F]
    ) -> Self:
        self._function_converter.register(function_type, converter)
        return self

    def register_clause_converter[C: genericquery.Clause](
        self, clause_type: type[C], converter: ClauseConverter[C]
    ) -> Self:
        self._clause_converter.register(clause_type, converter)
        return self

    def register_query_converter[Q: genericquery.Query](
        self, query_type: type[Q], converter: QueryConverter[Q]
    ) -> Self:
        self._query_converter.register(query_type, converter)
        return self

    def apply_clause(
        self, clause: genericquery.Clause, query_builder: postgresquery.Query
    ) -> postgresquery.Query:
        return self._clause_converter.convert(clause).apply(query_builder)

    def convert_query(
        self, item: genericquery.Query
    ) -> postgresquery.ParameterisedQuery:
        return self._query_converter.convert(item)

    def convert(
        self, item: genericquery.Query
    ) -> postgresquery.ParameterisedQuery:
        return self.convert_query(item)
