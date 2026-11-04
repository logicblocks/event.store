from typing import Self

from logicblocks.event.query import (
    Clause,
    FilterClause,
    Function,
    KeySetPagingClause,
    Lookup,
    OffsetPagingClause,
    Query,
    Search,
    Similarity,
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
from .function import (
    SimilarityFunctionConverter,
    TypeRegistryFunctionConverter,
)
from .helpers import compose_transformers
from .types import (
    ClauseConverter,
    FunctionConverter,
    Identifiable,
    QueryConverter,
    ResultSetTransformer,
)


class SearchQueryConverter[R: Identifiable](QueryConverter[R, Search]):
    """Converts a `Search` into a transformer over result sets.

    The transformer applies the filters, then the sort, then the paging.

    Args:
        clause_converter: The converter used for each clause of the search.
    """

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
    """Converts a `Lookup` into a transformer that applies its filters.

    Args:
        clause_converter: The converter used for each filter of the lookup.
    """

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
    """Converts queries using the converter registered for their type."""

    def register[Q: Query](
        self,
        item_type: type[Q],
        converter: Converter[Q, ResultSetTransformer[R]],
    ) -> Self:
        return super()._register(item_type, converter)


class DelegatingQueryConverter[R: Identifiable](
    Converter[Query, ResultSetTransformer[R]]
):
    """Converts queries into transformers over in-memory result sets.

    Holds separate registries for functions, clauses and queries. The
    registries start empty: call `with_default_converters` to register
    converters for all built-in query types, or register custom ones.

    Args:
        function_converter: The registry of function converters. Defaults
            to an empty registry.
        clause_converter: The registry of clause converters. Defaults to an
            empty registry.
        query_converter: The registry of query converters. Defaults to an
            empty registry.
    """

    def __init__(
        self,
        function_converter: TypeRegistryFunctionConverter[R] | None = None,
        clause_converter: TypeRegistryClauseConverter[R] | None = None,
        query_converter: TypeRegistryQueryConverter[R] | None = None,
    ):
        self._function_converter = (
            function_converter
            if function_converter is not None
            else TypeRegistryFunctionConverter[R]()
        )
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

    def with_default_function_converters(self) -> Self:
        """Register the converter for `Similarity`."""
        return self.register_function_converter(
            Similarity, SimilarityFunctionConverter[R]()
        )

    def with_default_clause_converters(self) -> Self:
        """Register converters for the built-in clause types."""
        return (
            self.register_clause_converter(
                FilterClause, FilterClauseConverter[R]()
            )
            .register_clause_converter(
                SortClause, SortClauseConverter[R](self._function_converter)
            )
            .register_clause_converter(
                KeySetPagingClause, KeySetPagingClauseConverter[R]()
            )
            .register_clause_converter(
                OffsetPagingClause, OffsetPagingClauseConverter[R]()
            )
        )

    def with_default_query_converters(self) -> Self:
        """Register converters for `Search` and `Lookup`."""
        return self.register_query_converter(
            Search, SearchQueryConverter[R](self._clause_converter)
        ).register_query_converter(
            Lookup, LookupQueryConverter[R](self._clause_converter)
        )

    def with_default_converters(self) -> Self:
        """Register all built-in function, clause and query converters."""
        return (
            self.with_default_function_converters()
            .with_default_clause_converters()
            .with_default_query_converters()
        )

    def register_function_converter[F: Function](
        self, function_type: type[F], converter: FunctionConverter[R, F]
    ) -> Self:
        self._function_converter.register(function_type, converter)
        return self

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

    def convert_function(self, item: Function) -> ResultSetTransformer[R]:
        return self._function_converter.convert(item)

    def convert_clause(self, item: Clause) -> ResultSetTransformer[R]:
        return self._clause_converter.convert(item)

    def convert_query(self, item: Query) -> ResultSetTransformer[R]:
        return self._query_converter.convert(item)

    def convert(self, item: Query) -> ResultSetTransformer[R]:
        return self.convert_query(item)
