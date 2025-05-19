from collections.abc import Iterable, Mapping
from typing import Self, Sequence

import logicblocks.event.query as genericquery
from logicblocks.event.types import Converter

from ...converter import TypeRegistryConverter
from .. import query as postgresquery
from ..query import QueryApplier
from ..settings import TableSettings
from .appliers import CombinedQueryApplier
from .helpers import (
    expression_for_field,
    expression_for_path,
    is_multi_valued,
    value_for_path,
)
from .types import ClauseConverter


class FilterClauseQueryApplier(QueryApplier):
    def __init__(
        self,
        clause: genericquery.FilterClause,
        operators: Mapping[genericquery.Operator, postgresquery.Operator],
    ):
        self._operators = operators
        self._path = clause.field
        self._operator = clause.operator
        self._value = clause.value

    @property
    def operator(self) -> postgresquery.Operator:
        if self._operator not in self._operators:
            raise ValueError(f"Unsupported operator: {self._operator}")

        return self._operators[self._operator]

    @property
    def column(self) -> postgresquery.Expression:
        return expression_for_path(self._path)

    @property
    def value(
        self,
    ) -> postgresquery.Expression | Sequence[postgresquery.Expression]:
        if is_multi_valued(self._value):
            return [value_for_path(value, self._path) for value in self._value]
        else:
            return value_for_path(self._value, self._path)

    def apply(self, target: postgresquery.Query) -> postgresquery.Query:
        return target.where(
            postgresquery.Condition()
            .left(self.column)
            .operator(self.operator)
            .right(self.value)
        )


class FilterClauseConverter(ClauseConverter[genericquery.FilterClause]):
    def __init__(
        self,
        operators: Mapping[genericquery.Operator, postgresquery.Operator]
        | None = None,
    ):
        self._operators = (
            operators
            if operators is not None
            else {
                genericquery.Operator.EQUAL: postgresquery.Operator.EQUALS,
                genericquery.Operator.NOT_EQUAL: postgresquery.Operator.NOT_EQUALS,
                genericquery.Operator.LESS_THAN: postgresquery.Operator.LESS_THAN,
                genericquery.Operator.LESS_THAN_OR_EQUAL: postgresquery.Operator.LESS_THAN_OR_EQUAL,
                genericquery.Operator.GREATER_THAN: postgresquery.Operator.GREATER_THAN,
                genericquery.Operator.GREATER_THAN_OR_EQUAL: postgresquery.Operator.GREATER_THAN_OR_EQUAL,
                genericquery.Operator.IN: postgresquery.Operator.IN,
                genericquery.Operator.CONTAINS: postgresquery.Operator.CONTAINS,
            }
        )

    def convert(self, item: genericquery.FilterClause) -> QueryApplier:
        return FilterClauseQueryApplier(clause=item, operators=self._operators)


class SortClauseQueryApplier(QueryApplier):
    def __init__(self, clause: genericquery.SortClause):
        self._clause = clause

    @staticmethod
    def sort_direction(
        sort_order: genericquery.SortOrder,
    ) -> postgresquery.SortDirection:
        match sort_order:
            case genericquery.SortOrder.ASC:
                return postgresquery.SortDirection.ASC
            case genericquery.SortOrder.DESC:
                return postgresquery.SortDirection.DESC
            case _:  # pragma: no cover
                raise ValueError(f"Unsupported sort order: {sort_order}")

    def sortby(
        self,
        sort_field: genericquery.SortField,
    ) -> postgresquery.SortBy:
        expression = expression_for_field(sort_field.field)
        direction = self.sort_direction(sort_field.order)

        return postgresquery.SortBy(
            expression=expression,
            direction=direction,
        )

    def apply(self, target: postgresquery.Query) -> postgresquery.Query:
        return target.order_by(
            *[self.sortby(sort_field) for sort_field in self._clause.fields]
        )


class SortClauseConverter(ClauseConverter[genericquery.SortClause]):
    def __init__(
        self,
        function_converter: Converter[genericquery.Function, QueryApplier],
    ):
        self._function_converter = function_converter

    def convert(self, item: genericquery.SortClause) -> QueryApplier:
        function_query_appliers = [
            self._function_converter.convert(sort_field.field)
            for sort_field in item.fields
            if isinstance(sort_field.field, genericquery.Function)
        ]
        sort_clause_query_applier = SortClauseQueryApplier(clause=item)

        return CombinedQueryApplier(
            *[*function_query_appliers, sort_clause_query_applier]
        )


def row_comparison_condition(
    columns: Iterable[postgresquery.Expression],
    operator: postgresquery.Operator,
    table: str,
) -> postgresquery.Condition:
    right = postgresquery.Query().select_all().from_table(table)
    return (
        postgresquery.Condition().left(columns).operator(operator).right(right)
    )


def field_comparison_condition(
    column: postgresquery.Expression,
    operator: postgresquery.Operator,
    value: postgresquery.Constant,
) -> postgresquery.Condition:
    return (
        postgresquery.Condition().left(column).operator(operator).right(value)
    )


def record_query(
    id: postgresquery.Constant,
    columns: Iterable[postgresquery.Expression],
    table_settings: TableSettings,
) -> postgresquery.Query:
    id_column = postgresquery.ColumnReference(field="id")

    return (
        postgresquery.Query()
        .select(*columns)
        .from_table(table_settings.table_name)
        .where(
            field_comparison_condition(
                id_column, postgresquery.Operator.EQUALS, id
            )
        )
        .limit(1)
    )


def first_page_no_sort_query(
    builder: postgresquery.Query, paging: genericquery.KeySetPagingClause
) -> postgresquery.Query:
    return builder.order_by("id").limit(paging.item_count)


def first_page_existing_sort_query(
    builder: postgresquery.Query,
    paging: genericquery.KeySetPagingClause,
    sort_direction: postgresquery.SortDirection,
) -> postgresquery.Query:
    existing_sortby = builder.sortby_list
    paged_sort = list(existing_sortby) + [
        postgresquery.SortBy(
            expression=postgresquery.ColumnReference(field="id"),
            direction=sort_direction,
        )
    ]

    return builder.replace_order_by(*paged_sort).limit(paging.item_count)


def first_page_existing_sort_all_asc_query(
    builder: postgresquery.Query, paging: genericquery.KeySetPagingClause
) -> postgresquery.Query:
    return first_page_existing_sort_query(
        builder, paging, postgresquery.SortDirection.ASC
    )


def first_page_existing_sort_all_desc_query(
    builder: postgresquery.Query, paging: genericquery.KeySetPagingClause
) -> postgresquery.Query:
    return first_page_existing_sort_query(
        builder, paging, postgresquery.SortDirection.DESC
    )


def first_page_existing_sort_mixed_query(
    builder: postgresquery.Query, paging: genericquery.KeySetPagingClause
) -> postgresquery.Query:
    return first_page_existing_sort_query(
        builder, paging, postgresquery.SortDirection.ASC
    )


def subsequent_page_no_sort_row_selection_query(
    builder: postgresquery.Query, paging: genericquery.KeySetPagingClause
) -> postgresquery.Query:
    id_column = postgresquery.ColumnReference(field="id")

    id = postgresquery.Constant(paging.last_id)
    op = (
        postgresquery.Operator.GREATER_THAN
        if paging.is_forwards()
        else postgresquery.Operator.LESS_THAN
    )
    sort_direction = (
        postgresquery.SortDirection.ASC
        if paging.is_forwards()
        else postgresquery.SortDirection.DESC
    )

    return (
        builder.where(field_comparison_condition(id_column, op, id))
        .order_by(
            postgresquery.SortBy(
                expression=id_column, direction=sort_direction
            )
        )
        .limit(paging.item_count)
    )


def subsequent_page_no_sort_paging_forwards_query(
    query: postgresquery.Query, paging: genericquery.KeySetPagingClause
) -> postgresquery.Query:
    return subsequent_page_no_sort_row_selection_query(query, paging)


def subsequent_page_no_sort_paging_backwards_query(
    query: postgresquery.Query, paging: genericquery.KeySetPagingClause
) -> postgresquery.Query:
    return (
        postgresquery.Query()
        .select_all()
        .from_subquery(
            subsequent_page_no_sort_row_selection_query(query, paging),
            alias="page",
        )
        .order_by("id")
        .limit(paging.item_count)
    )


def subsequent_page_existing_sort_all_asc_forwards_query(
    query: postgresquery.Query,
    paging: genericquery.KeySetPagingClause,
    table_settings: TableSettings,
) -> postgresquery.Query:
    id_column = postgresquery.ColumnReference(field="id")
    last_id = postgresquery.Constant(paging.last_id)

    existing_sort = query.sortby_list
    paged_sort = list(existing_sort) + [
        postgresquery.SortBy(
            expression=id_column, direction=postgresquery.SortDirection.ASC
        )
    ]
    paged_sort_expressions = [sortby.expression for sortby in paged_sort]

    return (
        query.with_query(
            record_query(last_id, paged_sort_expressions, table_settings),
            name="last",
        )
        .where(
            row_comparison_condition(
                paged_sort_expressions,
                postgresquery.Operator.GREATER_THAN,
                table="last",
            )
        )
        .replace_order_by(*paged_sort)
        .limit(paging.item_count)
    )


def subsequent_page_existing_sort_all_asc_backwards_query(
    query: postgresquery.Query,
    paging: genericquery.KeySetPagingClause,
    table_settings: TableSettings,
) -> postgresquery.Query:
    id_column = postgresquery.ColumnReference(field="id")
    last_id = postgresquery.Constant(paging.last_id)

    existing_sort = query.sortby_list

    paged_sort = list(existing_sort) + [
        postgresquery.SortBy(
            expression=id_column, direction=postgresquery.SortDirection.ASC
        )
    ]
    record_sort = [
        postgresquery.SortBy(
            expression=sortby.expression,
            direction=postgresquery.SortDirection.DESC,
        )
        for sortby in paged_sort
    ]
    sort_expressions = [sortby.expression for sortby in paged_sort]

    return (
        postgresquery.Query()
        .with_query(
            record_query(last_id, sort_expressions, table_settings),
            name="last",
        )
        .select_all()
        .from_subquery(
            query.where(
                row_comparison_condition(
                    sort_expressions,
                    postgresquery.Operator.LESS_THAN,
                    table="last",
                )
            )
            .replace_order_by(*record_sort)
            .limit(paging.item_count),
            alias="page",
        )
        .order_by(*paged_sort)
        .limit(paging.item_count)
    )


def subsequent_page_existing_sort_all_desc_forwards_query(
    query: postgresquery.Query,
    paging: genericquery.KeySetPagingClause,
    table_settings: TableSettings,
) -> postgresquery.Query:
    id_column = postgresquery.ColumnReference(field="id")
    last_id = postgresquery.Constant(paging.last_id)

    existing_sort = query.sortby_list
    paged_sort = list(existing_sort) + [
        postgresquery.SortBy(
            expression=id_column, direction=postgresquery.SortDirection.DESC
        )
    ]
    paged_sort_expressions = [sortby.expression for sortby in paged_sort]

    return (
        query.with_query(
            record_query(last_id, paged_sort_expressions, table_settings),
            name="last",
        )
        .where(
            row_comparison_condition(
                paged_sort_expressions,
                postgresquery.Operator.LESS_THAN,
                table="last",
            )
        )
        .replace_order_by(*paged_sort)
        .limit(paging.item_count)
    )


def subsequent_page_existing_sort_all_desc_backwards_query(
    query: postgresquery.Query,
    paging: genericquery.KeySetPagingClause,
    table_settings: TableSettings,
) -> postgresquery.Query:
    id_column = postgresquery.ColumnReference(field="id")
    last_id = postgresquery.Constant(paging.last_id)

    existing_sort = query.sortby_list

    paged_sort = list(existing_sort) + [
        postgresquery.SortBy(
            expression=id_column, direction=postgresquery.SortDirection.DESC
        )
    ]
    record_sort = [
        postgresquery.SortBy(
            expression=sortby.expression,
            direction=postgresquery.SortDirection.ASC,
        )
        for sortby in paged_sort
    ]
    sort_expressions = [sortby.expression for sortby in paged_sort]

    return (
        postgresquery.Query()
        .with_query(
            record_query(last_id, sort_expressions, table_settings),
            name="last",
        )
        .select_all()
        .from_subquery(
            query.where(
                row_comparison_condition(
                    sort_expressions,
                    postgresquery.Operator.GREATER_THAN,
                    table="last",
                )
            )
            .replace_order_by(*record_sort)
            .limit(paging.item_count),
            alias="page",
        )
        .order_by(*paged_sort)
        .limit(paging.item_count)
    )


def subsequent_page_existing_sort_mixed_forwards_query(
    query: postgresquery.Query,
    paging: genericquery.KeySetPagingClause,
    table_settings: TableSettings,
) -> postgresquery.Query:
    last_id = postgresquery.Constant(paging.last_id)

    existing_sort = query.sortby_list
    paged_sort = list(existing_sort) + [
        postgresquery.SortBy(
            expression=postgresquery.ColumnReference(field="id"),
            direction=postgresquery.SortDirection.ASC,
        )
    ]
    paged_sort_expressions = [sortby.expression for sortby in paged_sort]

    last_query = record_query(last_id, paged_sort_expressions, table_settings)

    paged_sort_operators = {
        sortby.expression: postgresquery.Operator.GREATER_THAN
        if sortby.direction == postgresquery.SortDirection.ASC
        else postgresquery.Operator.LESS_THAN
        for sortby in paged_sort
    }

    ordering_column_sets = [
        [
            (
                paged_sort_expressions[j],
                postgresquery.Operator.EQUALS
                if j < i - 1
                else paged_sort_operators[paged_sort_expressions[j]],
            )
            for j in range(0, i)
        ]
        for i in range(1, len(paged_sort_expressions) + 1)
    ]
    record_select_conditions = [
        [
            postgresquery.Condition()
            .left(column)
            .operator(operator)
            .right(postgresquery.Query().select(column).from_table("last"))
            for column, operator in ordering_column_set
        ]
        for ordering_column_set in ordering_column_sets
    ]
    record_select_queries = [
        (
            query.where(*conditions)
            .replace_order_by(*paged_sort)
            .limit(paging.item_count)
        )
        for conditions in record_select_conditions
    ]

    return (
        postgresquery.Query.union(
            *record_select_queries, mode=postgresquery.SetQuantifier.ALL
        )
        .with_query(last_query, name="last")
        .order_by(*paged_sort)
        .limit(paging.item_count)
    )


def subsequent_page_existing_sort_mixed_backwards_query(
    query: postgresquery.Query,
    paging: genericquery.KeySetPagingClause,
    table_settings: TableSettings,
) -> postgresquery.Query:
    last_id = postgresquery.Constant(paging.last_id)

    existing_sort = query.sortby_list
    paged_sort = list(existing_sort) + [
        postgresquery.SortBy(
            expression=postgresquery.ColumnReference(field="id"),
            direction=postgresquery.SortDirection.ASC,
        )
    ]
    record_sort = [sortby.reverse() for sortby in paged_sort]
    sort_expressions = [sortby.expression for sortby in paged_sort]

    last_query = record_query(last_id, sort_expressions, table_settings)

    record_sort_operators = {
        sortby.expression: postgresquery.Operator.GREATER_THAN
        if sortby.direction == postgresquery.SortDirection.ASC
        else postgresquery.Operator.LESS_THAN
        for sortby in record_sort
    }

    ordering_column_sets = [
        [
            (
                sort_expressions[j],
                postgresquery.Operator.EQUALS
                if j < i - 1
                else record_sort_operators[sort_expressions[j]],
            )
            for j in range(0, i)
        ]
        for i in range(1, len(sort_expressions) + 1)
    ]
    record_select_conditions = [
        [
            postgresquery.Condition()
            .left(column)
            .operator(operator)
            .right(postgresquery.Query().select(column).from_table("last"))
            for column, operator in ordering_column_set
        ]
        for ordering_column_set in ordering_column_sets
    ]
    record_select_queries = [
        (
            query.where(*conditions)
            .replace_order_by(*record_sort)
            .limit(paging.item_count)
        )
        for conditions in record_select_conditions
    ]

    return (
        postgresquery.Query()
        .with_query(last_query, name="last")
        .select_all()
        .from_subquery(
            postgresquery.Query.union(
                *record_select_queries, mode=postgresquery.SetQuantifier.ALL
            )
            .order_by(*record_sort)
            .limit(paging.item_count),
            alias="page",
        )
        .order_by(*paged_sort)
        .limit(paging.item_count)
    )


class KeySetPagingClauseQueryApplier(QueryApplier):
    def __init__(
        self,
        clause: genericquery.KeySetPagingClause,
        table_settings: TableSettings,
    ):
        self._clause = clause
        self._table_settings = table_settings

    def apply(self, target: postgresquery.Query) -> postgresquery.Query:
        has_existing_sort = len(target.sortby_list) > 0

        all_sort_asc = all(
            sortby.is_ascending() for sortby in target.sortby_list
        )
        all_sort_desc = all(
            sortby.is_descending() for sortby in target.sortby_list
        )

        if self._clause.is_forwards():
            if has_existing_sort:
                if all_sort_asc:
                    return (
                        subsequent_page_existing_sort_all_asc_forwards_query(
                            target, self._clause, self._table_settings
                        )
                    )
                elif all_sort_desc:
                    return (
                        subsequent_page_existing_sort_all_desc_forwards_query(
                            target, self._clause, self._table_settings
                        )
                    )
                else:
                    return subsequent_page_existing_sort_mixed_forwards_query(
                        target, self._clause, self._table_settings
                    )
            else:
                return subsequent_page_no_sort_paging_forwards_query(
                    target, self._clause
                )
        elif self._clause.is_backwards():
            if has_existing_sort:
                if all_sort_asc:
                    return (
                        subsequent_page_existing_sort_all_asc_backwards_query(
                            target, self._clause, self._table_settings
                        )
                    )
                elif all_sort_desc:
                    return (
                        subsequent_page_existing_sort_all_desc_backwards_query(
                            target, self._clause, self._table_settings
                        )
                    )
                else:
                    return subsequent_page_existing_sort_mixed_backwards_query(
                        target, self._clause, self._table_settings
                    )
            else:
                return subsequent_page_no_sort_paging_backwards_query(
                    target, self._clause
                )
        else:
            if has_existing_sort:
                if all_sort_asc:
                    return first_page_existing_sort_all_asc_query(
                        target, self._clause
                    )
                elif all_sort_desc:
                    return first_page_existing_sort_all_desc_query(
                        target, self._clause
                    )
                else:
                    return first_page_existing_sort_mixed_query(
                        target, self._clause
                    )
            else:
                return first_page_no_sort_query(target, self._clause)


class KeySetPagingClauseConverter(
    ClauseConverter[genericquery.KeySetPagingClause]
):
    def __init__(self, table_settings: TableSettings):
        self._table_settings = table_settings

    def convert(self, item: genericquery.KeySetPagingClause) -> QueryApplier:
        return KeySetPagingClauseQueryApplier(
            clause=item,
            table_settings=self._table_settings,
        )


class OffsetPagingClauseQueryApplier(QueryApplier):
    def __init__(self, clause: genericquery.OffsetPagingClause):
        self._clause = clause

    def apply(self, target: postgresquery.Query) -> postgresquery.Query:
        if self._clause.page_number == 1:
            return target.limit(self._clause.item_count)
        else:
            return target.limit(self._clause.item_count).offset(
                self._clause.offset
            )


class OffsetPagingClauseConverter(
    ClauseConverter[genericquery.OffsetPagingClause]
):
    def convert(self, item: genericquery.OffsetPagingClause) -> QueryApplier:
        return OffsetPagingClauseQueryApplier(clause=item)


class TypeRegistryClauseConverter(
    TypeRegistryConverter[genericquery.Clause, QueryApplier]
):
    def register[C: genericquery.Clause](
        self, item_type: type[C], converter: Converter[C, QueryApplier]
    ) -> Self:
        return super()._register(item_type, converter)
