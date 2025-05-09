from collections.abc import Iterable, Mapping
from typing import Any, Self, Sequence, TypeGuard

from psycopg.types.json import Jsonb

import logicblocks.event.db.postgres as postgres
import logicblocks.event.query as query

from ..settings import TableSettings
from ..types import ClauseConverter, QueryApplier


def column_for_query_path(
    path: query.Path | query.Function,
) -> postgres.Column:
    if isinstance(path, query.Function):
        raise ValueError("Function sorting is not supported.")
    if path.is_nested():
        return postgres.Column(field=path.top_level, path=path.sub_levels)
    else:
        return postgres.Column(field=path.top_level)


def path_expression_for_query_path(path: query.Path) -> str:
    path_list = ",".join([str(sub_level) for sub_level in path.sub_levels])
    return "{" + path_list + "}"


def value_for_path(value: Any, path: query.Path) -> postgres.Value:
    if path == query.Path("source"):
        return postgres.Value(
            Jsonb(value.serialise()),
        )
    elif path.is_nested():
        return postgres.Value(
            value,
            wrapper="to_jsonb",
            cast_to_type="TEXT" if type(value) is str else None,
        )
    else:
        return postgres.Value(value)


def is_multi_valued(value: Any) -> TypeGuard[Sequence[Any]]:
    return (
        not isinstance(value, str)
        and not isinstance(value, bytes)
        and isinstance(value, Sequence)
    )


class FilterClauseQueryApplier(QueryApplier):
    def __init__(
        self,
        clause: query.FilterClause,
        operators: Mapping[query.Operator, postgres.Operator],
    ):
        self._operators = operators
        self._path = clause.path
        self._operator = clause.operator
        self._value = clause.value

    @property
    def operator(self) -> postgres.Operator:
        if self._operator not in self._operators:
            raise ValueError(f"Unsupported operator: {self._operator}")

        return self._operators[self._operator]

    @property
    def column(self) -> postgres.Column:
        return column_for_query_path(self._path)

    @property
    def value(self) -> postgres.Value | Sequence[postgres.Value]:
        if is_multi_valued(self._value):
            return [value_for_path(value, self._path) for value in self._value]
        else:
            return value_for_path(self._value, self._path)

    def apply(self, target: postgres.Query) -> postgres.Query:
        return target.where(
            postgres.Condition()
            .left(self.column)
            .operator(self.operator)
            .right(self.value)
        )


class FilterClauseConverter(ClauseConverter[query.FilterClause]):
    def __init__(
        self,
        operators: Mapping[query.Operator, postgres.Operator] | None = None,
    ):
        self._operators = (
            operators
            if operators is not None
            else {
                query.Operator.EQUAL: postgres.Operator.EQUALS,
                query.Operator.NOT_EQUAL: postgres.Operator.NOT_EQUALS,
                query.Operator.LESS_THAN: postgres.Operator.LESS_THAN,
                query.Operator.LESS_THAN_OR_EQUAL: postgres.Operator.LESS_THAN_OR_EQUAL,
                query.Operator.GREATER_THAN: postgres.Operator.GREATER_THAN,
                query.Operator.GREATER_THAN_OR_EQUAL: postgres.Operator.GREATER_THAN_OR_EQUAL,
                query.Operator.IN: postgres.Operator.IN,
                query.Operator.CONTAINS: postgres.Operator.CONTAINS,
            }
        )

    def convert(self, item: query.FilterClause) -> QueryApplier:
        return FilterClauseQueryApplier(clause=item, operators=self._operators)


class SortClauseQueryApplier(QueryApplier):
    def __init__(self, clause: query.SortClause):
        self._clause = clause

    @staticmethod
    def sort_direction(sort_order: query.SortOrder) -> postgres.SortDirection:
        match sort_order:
            case query.SortOrder.ASC:
                return postgres.SortDirection.ASC
            case query.SortOrder.DESC:
                return postgres.SortDirection.DESC
            case _:  # pragma: no cover
                raise ValueError(f"Unsupported sort order: {sort_order}")

    def apply(self, target: postgres.Query) -> postgres.Query:
        if any(
            isinstance(field.path, query.Function)
            for field in self._clause.fields
        ):
            raise ValueError("Function sorting is not supported.")

        return target.order_by(
            *[
                (
                    column_for_query_path(field.path),
                    self.sort_direction(field.order),
                )
                for field in self._clause.fields
            ]
        )


class SortClauseConverter(ClauseConverter[query.SortClause]):
    def convert(self, item: query.SortClause) -> QueryApplier:
        return SortClauseQueryApplier(clause=item)


def row_comparison_condition(
    columns: Iterable[postgres.Column], operator: postgres.Operator, table: str
) -> postgres.Condition:
    right = postgres.Query().select_all().from_table(table)
    return postgres.Condition().left(columns).operator(operator).right(right)


def field_comparison_condition(
    column: postgres.Column, operator: postgres.Operator, value: postgres.Value
) -> postgres.Condition:
    return postgres.Condition().left(column).operator(operator).right(value)


def record_query(
    id: postgres.Value,
    columns: Iterable[postgres.Column],
    table_settings: TableSettings,
) -> postgres.Query:
    id_column = postgres.Column(field="id")

    return (
        postgres.Query()
        .select(*columns)
        .from_table(table_settings.table_name)
        .where(
            field_comparison_condition(id_column, postgres.Operator.EQUALS, id)
        )
        .limit(1)
    )


def first_page_no_sort_query(
    builder: postgres.Query, paging: query.KeySetPagingClause
) -> postgres.Query:
    return builder.order_by("id").limit(paging.item_count)


def first_page_existing_sort_query(
    builder: postgres.Query,
    paging: query.KeySetPagingClause,
    sort_direction: postgres.SortDirection,
) -> postgres.Query:
    existing_sort = [
        (sort_column.expression, sort_column.direction)
        for sort_column in builder.sort_columns
    ]
    paged_sort = list(existing_sort) + [
        (postgres.Column(field="id"), sort_direction)
    ]

    return builder.replace_order_by(*paged_sort).limit(paging.item_count)


def first_page_existing_sort_all_asc_query(
    builder: postgres.Query, paging: query.KeySetPagingClause
) -> postgres.Query:
    return first_page_existing_sort_query(
        builder, paging, postgres.SortDirection.ASC
    )


def first_page_existing_sort_all_desc_query(
    builder: postgres.Query, paging: query.KeySetPagingClause
) -> postgres.Query:
    return first_page_existing_sort_query(
        builder, paging, postgres.SortDirection.DESC
    )


def first_page_existing_sort_mixed_query(
    builder: postgres.Query, paging: query.KeySetPagingClause
) -> postgres.Query:
    return first_page_existing_sort_query(
        builder, paging, postgres.SortDirection.ASC
    )


def subsequent_page_no_sort_row_selection_query(
    builder: postgres.Query, paging: query.KeySetPagingClause
) -> postgres.Query:
    id_column = postgres.Column(field="id")

    id = postgres.Value(paging.last_id)
    op = (
        postgres.Operator.GREATER_THAN
        if paging.is_forwards()
        else postgres.Operator.LESS_THAN
    )
    sort_direction = (
        postgres.SortDirection.ASC
        if paging.is_forwards()
        else postgres.SortDirection.DESC
    )

    return (
        builder.where(field_comparison_condition(id_column, op, id))
        .order_by((id_column, sort_direction))
        .limit(paging.item_count)
    )


def subsequent_page_no_sort_paging_forwards_query(
    query: postgres.Query, paging: query.KeySetPagingClause
) -> postgres.Query:
    return subsequent_page_no_sort_row_selection_query(query, paging)


def subsequent_page_no_sort_paging_backwards_query(
    query: postgres.Query, paging: query.KeySetPagingClause
) -> postgres.Query:
    return (
        postgres.Query()
        .select_all()
        .from_subquery(
            subsequent_page_no_sort_row_selection_query(query, paging),
            alias="page",
        )
        .order_by("id")
        .limit(paging.item_count)
    )


def subsequent_page_existing_sort_all_asc_forwards_query(
    query: postgres.Query,
    paging: query.KeySetPagingClause,
    table_settings: TableSettings,
) -> postgres.Query:
    id_column = postgres.Column(field="id")
    last_id = postgres.Value(paging.last_id)

    existing_sort = [
        (sort_column.expression, sort_column.direction)
        for sort_column in query.sort_columns
    ]
    paged_sort = list(existing_sort) + [
        (id_column, postgres.SortDirection.ASC)
    ]
    paged_sort_columns = [column for column, _ in paged_sort]

    return (
        query.with_query(
            record_query(last_id, paged_sort_columns, table_settings),
            name="last",
        )
        .where(
            row_comparison_condition(
                paged_sort_columns,
                postgres.Operator.GREATER_THAN,
                table="last",
            )
        )
        .replace_order_by(*paged_sort)
        .limit(paging.item_count)
    )


def subsequent_page_existing_sort_all_asc_backwards_query(
    query: postgres.Query,
    paging: query.KeySetPagingClause,
    table_settings: TableSettings,
) -> postgres.Query:
    id_column = postgres.Column(field="id")
    last_id = postgres.Value(paging.last_id)

    existing_sort = [
        (sort_column.expression, sort_column.direction)
        for sort_column in query.sort_columns
    ]

    paged_sort = list(existing_sort) + [
        (id_column, postgres.SortDirection.ASC)
    ]
    record_sort = [
        (column, postgres.SortDirection.DESC) for column, _ in paged_sort
    ]
    sort_columns = [column for column, _ in paged_sort]

    return (
        postgres.Query()
        .with_query(
            record_query(last_id, sort_columns, table_settings),
            name="last",
        )
        .select_all()
        .from_subquery(
            query.where(
                row_comparison_condition(
                    sort_columns,
                    postgres.Operator.LESS_THAN,
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
    query: postgres.Query,
    paging: query.KeySetPagingClause,
    table_settings: TableSettings,
) -> postgres.Query:
    id_column = postgres.Column(field="id")
    last_id = postgres.Value(paging.last_id)

    existing_sort = [
        (sort_column.expression, sort_column.direction)
        for sort_column in query.sort_columns
    ]
    paged_sort = list(existing_sort) + [
        (id_column, postgres.SortDirection.DESC)
    ]
    paged_sort_columns = [column for column, _ in paged_sort]

    return (
        query.with_query(
            record_query(last_id, paged_sort_columns, table_settings),
            name="last",
        )
        .where(
            row_comparison_condition(
                paged_sort_columns,
                postgres.Operator.LESS_THAN,
                table="last",
            )
        )
        .replace_order_by(*paged_sort)
        .limit(paging.item_count)
    )


def subsequent_page_existing_sort_all_desc_backwards_query(
    query: postgres.Query,
    paging: query.KeySetPagingClause,
    table_settings: TableSettings,
) -> postgres.Query:
    id_column = postgres.Column(field="id")
    last_id = postgres.Value(paging.last_id)

    existing_sort = [
        (sort_column.expression, sort_column.direction)
        for sort_column in query.sort_columns
    ]

    paged_sort = list(existing_sort) + [
        (id_column, postgres.SortDirection.DESC)
    ]
    record_sort = [
        (column, postgres.SortDirection.ASC) for column, _ in paged_sort
    ]
    sort_columns = [column for column, _ in paged_sort]

    return (
        postgres.Query()
        .with_query(
            record_query(last_id, sort_columns, table_settings),
            name="last",
        )
        .select_all()
        .from_subquery(
            query.where(
                row_comparison_condition(
                    sort_columns,
                    postgres.Operator.GREATER_THAN,
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
    query: postgres.Query,
    paging: query.KeySetPagingClause,
    table_settings: TableSettings,
) -> postgres.Query:
    last_id = postgres.Value(paging.last_id)

    existing_sort = [
        (sort_column.expression, sort_column.direction)
        for sort_column in query.sort_columns
    ]
    paged_sort = list(existing_sort) + [
        (postgres.Column(field="id"), postgres.SortDirection.ASC)
    ]
    paged_sort_columns = [column for column, _ in paged_sort]

    last_query = record_query(last_id, paged_sort_columns, table_settings)

    paged_sort_operators = {
        column: postgres.Operator.GREATER_THAN
        if direction == postgres.SortDirection.ASC
        else postgres.Operator.LESS_THAN
        for column, direction in paged_sort
    }

    ordering_column_sets = [
        [
            (
                paged_sort_columns[j],
                postgres.Operator.EQUALS
                if j < i - 1
                else paged_sort_operators[paged_sort_columns[j]],
            )
            for j in range(0, i)
        ]
        for i in range(1, len(paged_sort_columns) + 1)
    ]
    record_select_conditions = [
        [
            postgres.Condition()
            .left(column)
            .operator(operator)
            .right(postgres.Query().select(column).from_table("last"))
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
        postgres.Query.union(
            *record_select_queries, mode=postgres.SetOperationMode.ALL
        )
        .with_query(last_query, name="last")
        .order_by(*paged_sort)
        .limit(paging.item_count)
    )


def subsequent_page_existing_sort_mixed_backwards_query(
    query: postgres.Query,
    paging: query.KeySetPagingClause,
    table_settings: TableSettings,
) -> postgres.Query:
    last_id = postgres.Value(paging.last_id)

    existing_sort = [
        (sort_column.expression, sort_column.direction)
        for sort_column in query.sort_columns
    ]
    paged_sort = list(existing_sort) + [
        (postgres.Column(field="id"), postgres.SortDirection.ASC)
    ]
    record_sort = [
        (column, direction.reverse()) for column, direction in paged_sort
    ]
    sort_columns = [column for column, _ in paged_sort]

    last_query = record_query(last_id, sort_columns, table_settings)

    record_sort_operators = {
        column: postgres.Operator.GREATER_THAN
        if direction == postgres.SortDirection.ASC
        else postgres.Operator.LESS_THAN
        for column, direction in record_sort
    }

    ordering_column_sets = [
        [
            (
                sort_columns[j],
                postgres.Operator.EQUALS
                if j < i - 1
                else record_sort_operators[sort_columns[j]],
            )
            for j in range(0, i)
        ]
        for i in range(1, len(sort_columns) + 1)
    ]
    record_select_conditions = [
        [
            postgres.Condition()
            .left(column)
            .operator(operator)
            .right(postgres.Query().select(column).from_table("last"))
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
        postgres.Query()
        .with_query(last_query, name="last")
        .select_all()
        .from_subquery(
            postgres.Query.union(
                *record_select_queries, mode=postgres.SetOperationMode.ALL
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
        clause: query.KeySetPagingClause,
        table_settings: TableSettings,
    ):
        self._clause = clause
        self._table_settings = table_settings

    def apply(self, target: postgres.Query) -> postgres.Query:
        has_existing_sort = len(target.sort_columns) > 0

        all_sort_asc = all(
            sort_column.is_ascending() for sort_column in target.sort_columns
        )
        all_sort_desc = all(
            sort_column.is_descending() for sort_column in target.sort_columns
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


class KeySetPagingClauseConverter(ClauseConverter[query.KeySetPagingClause]):
    def __init__(self, table_settings: TableSettings):
        self._table_settings = table_settings

    def convert(self, item: query.KeySetPagingClause) -> QueryApplier:
        return KeySetPagingClauseQueryApplier(
            clause=item,
            table_settings=self._table_settings,
        )


class OffsetPagingClauseQueryApplier(QueryApplier):
    def __init__(self, clause: query.OffsetPagingClause):
        self._clause = clause

    def apply(self, target: postgres.Query) -> postgres.Query:
        if self._clause.page_number == 1:
            return target.limit(self._clause.item_count)
        else:
            return target.limit(self._clause.item_count).offset(
                self._clause.offset
            )


class OffsetPagingClauseConverter(ClauseConverter[query.OffsetPagingClause]):
    def convert(self, item: query.OffsetPagingClause) -> QueryApplier:
        return OffsetPagingClauseQueryApplier(clause=item)


class TypeRegistryClauseConverter(ClauseConverter):
    def __init__(
        self,
        registry: dict[type[query.Clause], ClauseConverter[Any]] | None = None,
    ):
        self._registry = dict(registry) if registry is not None else {}

    def register[C: query.Clause](
        self, clause_type: type[C], converter: ClauseConverter[C]
    ) -> Self:
        self._registry[clause_type] = converter
        return self

    def convert(self, item: query.Clause) -> QueryApplier:
        if item.__class__ not in self._registry:
            raise ValueError(f"No converter registered for clause: {item}")
        return self._registry[item.__class__].convert(item)
