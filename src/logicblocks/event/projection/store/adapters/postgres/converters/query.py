from collections.abc import Mapping
from typing import Any, Self

from logicblocks.event.db import postgres
from logicblocks.event.query import (
    Clause,
    Lookup,
    Query,
    Search,
)

from ..settings import TableSettings
from ..types import ClauseConverter, QueryConverter


class SearchQueryConverter(QueryConverter[Search]):
    def __init__(
        self,
        clause_converter: ClauseConverter,
        table_settings: TableSettings,
    ):
        self._clause_converter = clause_converter
        self._table_settings = table_settings

    def convert(self, item: Search) -> postgres.ParameterisedQuery:
        filters = item.filters
        sort = item.sort
        paging = item.paging

        builder = (
            postgres.Query()
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


class LookupQueryConverter(QueryConverter[Lookup]):
    def __init__(
        self,
        clause_converter: ClauseConverter[Clause],
        table_settings: TableSettings,
    ):
        self._clause_converter = clause_converter
        self._table_settings = table_settings

    def convert(self, item: Lookup) -> postgres.ParameterisedQuery:
        filters = item.filters

        builder = (
            postgres.Query()
            .select_all()
            .from_table(self._table_settings.table_name)
        )

        for filter in filters:
            builder = self._clause_converter.convert(filter).apply(builder)

        return builder.build()


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

    def convert(self, item: Query) -> postgres.ParameterisedQuery:
        if item.__class__ not in self._registry:
            raise ValueError(f"Unsupported query type: {item}.")
        return self._registry[item.__class__].convert(item)
