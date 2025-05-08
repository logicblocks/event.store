from abc import ABC, abstractmethod
from collections.abc import Mapping
from typing import Any, Self

from logicblocks.event.db import postgres

from ....query import (
    Clause,
    Lookup,
    Query,
    Search,
)
from ..settings import TableSettings
from .clause import ClauseConverter


class QueryConverter[Q: Query = Query](ABC):
    @abstractmethod
    def convert(self, query: Q) -> postgres.ParameterisedQuery:
        raise NotImplementedError


class SearchQueryConverter(QueryConverter[Search]):
    def __init__(
        self,
        clause_converter: ClauseConverter,
        table_settings: TableSettings,
    ):
        self._clause_converter = clause_converter
        self._table_settings = table_settings

    def convert(self, query: Search) -> postgres.ParameterisedQuery:
        filters = query.filters
        sort = query.sort
        paging = query.paging

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

    def convert(self, query: Lookup) -> postgres.ParameterisedQuery:
        filters = query.filters

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

    def convert(self, query: Query) -> postgres.ParameterisedQuery:
        if query.__class__ not in self._registry:
            raise ValueError(f"Unsupported query type: {query}.")
        return self._registry[query.__class__].convert(query)
