from collections.abc import Mapping
from typing import Any, Self

import logicblocks.event.query as genericquery

from .. import query as postgresquery
from ..settings import TableSettings
from .types import ClauseConverter, QueryConverter


class SearchQueryConverter(QueryConverter[genericquery.Search]):
    def __init__(
        self,
        clause_converter: ClauseConverter,
        table_settings: TableSettings,
    ):
        self._clause_converter = clause_converter
        self._table_settings = table_settings

    def convert(
        self, item: genericquery.Search
    ) -> postgresquery.ParameterisedQuery:
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
        clause_converter: ClauseConverter[genericquery.Clause],
        table_settings: TableSettings,
    ):
        self._clause_converter = clause_converter
        self._table_settings = table_settings

    def convert(
        self, item: genericquery.Lookup
    ) -> postgresquery.ParameterisedQuery:
        filters = item.filters

        builder = (
            postgresquery.Query()
            .select_all()
            .from_table(self._table_settings.table_name)
        )

        for filter in filters:
            builder = self._clause_converter.convert(filter).apply(builder)

        return builder.build()


class TypeRegistryQueryConverter(QueryConverter):
    def __init__(
        self,
        registry: Mapping[type[genericquery.Query], QueryConverter[Any]]
        | None = None,
    ):
        self._registry = dict(registry) if registry is not None else {}

    def register[Q: genericquery.Query](
        self, query_type: type[Q], converter: QueryConverter[Q]
    ) -> Self:
        self._registry[query_type] = converter
        return self

    def convert(
        self, item: genericquery.Query
    ) -> postgresquery.ParameterisedQuery:
        if item.__class__ not in self._registry:
            raise ValueError(f"Unsupported query type: {item}.")
        return self._registry[item.__class__].convert(item)
