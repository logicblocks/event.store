from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, LiteralString, Self, TypedDict, Unpack, cast

from psycopg import AsyncConnection, abc, sql
from psycopg_pool import AsyncConnectionPool


@dataclass(frozen=True)
class ConnectionSettings:
    host: str
    port: int
    dbname: str
    user: str
    password: str

    def __init__(
        self, *, host: str, port: int, dbname: str, user: str, password: str
    ):
        object.__setattr__(self, "host", host)
        object.__setattr__(self, "port", port)
        object.__setattr__(self, "dbname", dbname)
        object.__setattr__(self, "user", user)
        object.__setattr__(self, "password", password)

    def __repr__(self):
        return (
            f"PostgresConnectionSettings("
            f"host={self.host}, "
            f"port={self.port}, "
            f"dbname={self.dbname}, "
            f"user={self.user}, "
            f"password={'*' * len(self.password)})"
        )

    def to_connection_string(self) -> str:
        userspec = f"{self.user}:{self.password}"
        hostspec = f"{self.host}:{self.port}"
        return f"postgresql://{userspec}@{hostspec}/{self.dbname}"


ConnectionSource = ConnectionSettings | AsyncConnectionPool[AsyncConnection]

type SqlFragment = sql.SQL | sql.Composed | None

type ParameterisedQuery = tuple[abc.Query, Sequence[Any]]
type ParameterisedQueryFragment = tuple[SqlFragment, Sequence[Any]]


@dataclass(frozen=True)
class Column:
    table: str | None
    field: str
    path: Sequence[str | int]

    def __init__(
        self,
        *,
        table: str | None = None,
        field: str,
        path: Sequence[str | int] | None = None,
    ):
        object.__setattr__(self, "table", table)
        object.__setattr__(self, "field", field)
        object.__setattr__(self, "path", path if path is not None else [])

    def __repr__(self):
        return f"Column(field={self.field},path={self.path})"

    def __hash__(self):
        return hash(self.__repr__())

    @property
    def name(self) -> SqlFragment:
        if self.field == "*":
            return sql.SQL("*")
        if self.table is None:
            return sql.SQL("{name}").format(name=sql.Identifier(self.field))
        else:
            return (
                sql.Identifier(self.table)
                + sql.SQL(".")
                + sql.Identifier(self.field)
            )

    def is_path(self):
        return len(self.path) > 0

    def to_path_expression(self) -> str:
        path_list = ",".join([str(path_part) for path_part in self.path])
        return "{" + path_list + "}"


type OrderByColumn = str | Column


class SortDirection(StrEnum):
    ASC = "ASC"
    DESC = "DESC"

    def reverse(self) -> "SortDirection":
        return (
            SortDirection.ASC
            if self == SortDirection.DESC
            else SortDirection.DESC
        )


class Operator(StrEnum):
    EQUALS = "="
    NOT_EQUALS = "!="
    GREATER_THAN = ">"
    GREATER_THAN_OR_EQUAL = ">="
    LESS_THAN = "<"
    LESS_THAN_OR_EQUAL = "<="


class SetOperationMode(StrEnum):
    ALL = "ALL"
    DISTINCT = "DISTINCT"


@dataclass(frozen=True)
class SortColumn:
    expression: Column
    direction: SortDirection

    def is_ascending(self):
        return self.direction == SortDirection.ASC

    def is_descending(self):
        return self.direction == SortDirection.DESC


@dataclass(frozen=True)
class Value:
    value: Any
    wrapper: LiteralString | None = None

    def build_fragment(self) -> ParameterisedQueryFragment:
        operand_sql = sql.SQL("%s")
        if self.wrapper is not None:
            operand_sql = (
                sql.SQL("{wrapper}(").format(wrapper=sql.SQL(self.wrapper))
                + operand_sql
                + sql.SQL(")")
            )
        params = [self.value]

        return operand_sql, params


type ConditionOperand = (
    Column | Value | Iterable[Column] | Iterable[Value] | "Query"
)


class ConditionParams(TypedDict, total=False):
    left: ConditionOperand
    right: ConditionOperand
    operator: Operator


@dataclass(frozen=True)
class Condition:
    _left: ConditionOperand | None
    _right: ConditionOperand | None
    _operator: Operator | None

    def __init__(
        self,
        left: ConditionOperand | None = None,
        right: ConditionOperand | None = None,
        operator: Operator | None = None,
    ):
        object.__setattr__(self, "_left", left)
        object.__setattr__(self, "_right", right)
        object.__setattr__(self, "_operator", operator)

    def _clone(self, **kwargs: Unpack[ConditionParams]) -> Self:
        return self.__class__(
            left=kwargs.get("left", self._left),
            right=kwargs.get("right", self._right),
            operator=kwargs.get("operator", self._operator),
        )

    def left(self, left: ConditionOperand) -> Self:
        return self._clone(left=left)

    def right(self, right: ConditionOperand) -> Self:
        return self._clone(right=right)

    def operator(self, operator: Operator) -> Self:
        return self._clone(operator=operator)

    def equals(self) -> Self:
        return self.operator(Operator.EQUALS)

    def not_equals(self) -> Self:
        return self.operator(Operator.NOT_EQUALS)

    def greater_than(self) -> Self:
        return self.operator(Operator.GREATER_THAN)

    def greater_than_or_equal_to(self) -> Self:
        return self.operator(Operator.GREATER_THAN_OR_EQUAL)

    def less_than(self) -> Self:
        return self.operator(Operator.LESS_THAN)

    def less_than_or_equal_to(self) -> Self:
        return self.operator(Operator.LESS_THAN_OR_EQUAL)

    @staticmethod
    def _operand_fragment(
        operand: ConditionOperand,
    ) -> ParameterisedQueryFragment:
        if isinstance(operand, Query):
            subquery, params = operand.build_fragment()
            if subquery is None:
                return None, []

            operand_sql = sql.SQL("(") + subquery + sql.SQL(")")

            return operand_sql, params

        elif isinstance(operand, Column):
            params: Sequence[Any] = []
            operand_sql = sql.SQL("{name}").format(name=operand.name)
            if operand.is_path():
                operand_sql += sql.SQL("#>{path_expression}").format(
                    path_expression=operand.to_path_expression()
                )

            return operand_sql, params

        elif isinstance(operand, Value):
            return operand.build_fragment()

        else:
            operand = cast(Iterable[Any], operand)
            fragments = [
                Condition._operand_fragment(operand_part)
                for operand_part in operand
            ]
            elements = [
                fragment[0]
                for fragment in fragments
                if fragment[0] is not None
            ]
            operand_sql = (
                sql.SQL("(") + sql.SQL(", ").join(elements) + sql.SQL(")")
            )
            params = [param for fragment in fragments for param in fragment[1]]

            return operand_sql, params

    def _left_fragment(self) -> ParameterisedQueryFragment:
        if self._left is None:
            return None, []

        return self._operand_fragment(self._left)

    def _right_fragment(self) -> ParameterisedQueryFragment:
        if self._right is None:
            return None, []

        return self._operand_fragment(self._right)

    def build_fragment(self) -> ParameterisedQueryFragment:
        if self._left is None or self._operator is None or self._right is None:
            raise ValueError("Condition not fully specified.")

        left_sql, left_params = self._left_fragment()
        right_sql, right_params = self._right_fragment()

        if left_sql is None or right_sql is None:
            raise ValueError("Condition not fully specified.")

        clause = (
            left_sql
            + sql.SQL(" {operator} ").format(
                operator=sql.SQL(self._operator.value)
            )
            + right_sql
        )
        params = [*left_params, *right_params]

        return clause, params


def to_postgres_column_definition(
    column: OrderByColumn,
) -> Column:
    if isinstance(column, str):
        return Column(field=column)
    return column


def to_postgres_sort_column(
    column: OrderByColumn | tuple[OrderByColumn, SortDirection],
) -> SortColumn:
    if isinstance(column, tuple):
        return SortColumn(
            expression=to_postgres_column_definition(column[0]),
            direction=column[1],
        )
    else:
        return SortColumn(
            expression=to_postgres_column_definition(column),
            direction=SortDirection.ASC,
        )


class QueryParams(TypedDict, total=False):
    common_table_expressions: Sequence[tuple["Query", str]]
    unions: tuple[Sequence["Query"], SetOperationMode] | None
    select_list: Sequence[Column]
    from_tables: Sequence[str]
    from_subqueries: Sequence[tuple["Query", str]]
    where_conditions: Sequence[Condition]
    sort_columns: Sequence[SortColumn]
    limit_value: int | None
    offset_value: int | None


@dataclass(frozen=True)
class Query:
    common_table_expressions: Sequence[tuple[Self, str]]
    unions: tuple[Sequence[Self], SetOperationMode] | None
    select_list: Sequence[Column]
    from_tables: Sequence[str]
    from_subqueries: Sequence[tuple[Self, str]]
    where_conditions: Sequence[Condition]
    sort_columns: Sequence[SortColumn]
    limit_value: int | None
    offset_value: int | None

    @staticmethod
    def union(
        query1: "Query",
        query2: "Query",
        *more_queries: "Query",
        mode: SetOperationMode = SetOperationMode.DISTINCT,
    ) -> "Query":
        return Query(unions=([query1, query2, *more_queries], mode))

    def __init__(
        self,
        *,
        common_table_expressions: Sequence[tuple[Self, str]] | None = None,
        unions: tuple[Sequence[Self], SetOperationMode] | None = None,
        select_list: Sequence[Column] | None = None,
        from_tables: Sequence[str] | None = None,
        from_subqueries: Sequence[tuple[Self, str]] | None = None,
        where_conditions: Sequence[Condition] | None = None,
        sort_columns: Sequence[SortColumn] | None = None,
        limit_value: int | None = None,
        offset_value: int | None = None,
    ):
        object.__setattr__(
            self,
            "common_table_expressions",
            list(common_table_expressions)
            if common_table_expressions is not None
            else [],
        )
        object.__setattr__(
            self,
            "unions",
            list(unions) if unions is not None else None,
        )
        object.__setattr__(
            self,
            "select_list",
            list(select_list) if select_list is not None else [],
        )
        object.__setattr__(
            self,
            "from_tables",
            list(from_tables) if from_tables is not None else [],
        )
        object.__setattr__(
            self,
            "from_subqueries",
            list(from_subqueries) if from_subqueries is not None else [],
        )
        object.__setattr__(
            self,
            "where_conditions",
            list(where_conditions) if where_conditions is not None else [],
        )
        object.__setattr__(
            self,
            "sort_columns",
            list(sort_columns) if sort_columns is not None else [],
        )
        object.__setattr__(self, "limit_value", limit_value)
        object.__setattr__(self, "offset_value", offset_value)

    def clone(self, **kwargs: Unpack[QueryParams]) -> Self:
        return self.__class__(
            common_table_expressions=kwargs.get(
                "common_table_expressions", self.common_table_expressions
            ),
            unions=kwargs.get("unions", self.unions),
            select_list=kwargs.get("select_list", self.select_list),
            from_tables=kwargs.get("from_tables", self.from_tables),
            from_subqueries=kwargs.get(
                "from_subqueries", self.from_subqueries
            ),
            where_conditions=kwargs.get(
                "where_conditions", self.where_conditions
            ),
            sort_columns=kwargs.get("sort_columns", self.sort_columns),
            limit_value=kwargs.get("limit_value", self.limit_value),
            offset_value=kwargs.get("offset_value", self.offset_value),
        )

    def with_query(self, query: Self, name: str) -> Self:
        return self.clone(
            common_table_expressions=[
                *self.common_table_expressions,
                (query, name),
            ]
        )

    def select(self, *columns: str | Column) -> Self:
        converted = [
            Column(field=column) if isinstance(column, str) else column
            for column in columns
        ]
        return self.clone(select_list=[*self.select_list, *converted])

    def select_all(self) -> Self:
        return self.clone(select_list=[*self.select_list, Column(field="*")])

    def from_table(self, table: str) -> Self:
        return self.clone(from_tables=[*self.from_tables, table])

    def from_subquery(self, subquery: Self, alias: str) -> Self:
        return self.clone(
            from_subqueries=[*self.from_subqueries, (subquery, alias)]
        )

    def where(self, *conditions: Condition) -> Self:
        return self.clone(
            where_conditions=[*self.where_conditions, *conditions]
        )

    def replace_order_by(
        self, *columns: OrderByColumn | tuple[OrderByColumn, SortDirection]
    ) -> Self:
        return self.clone(
            sort_columns=[
                *[to_postgres_sort_column(column) for column in columns],
            ]
        )

    def order_by(
        self,
        *columns: OrderByColumn | tuple[OrderByColumn, SortDirection],
    ) -> Self:
        return self.clone(
            sort_columns=[
                *self.sort_columns,
                *[to_postgres_sort_column(column) for column in columns],
            ]
        )

    def limit(self, limit: int | None) -> Self:
        return self.clone(limit_value=limit)

    def offset(self, offset: int | None) -> Self:
        return self.clone(offset_value=offset)

    def _common_table_expressions_fragment(
        self,
    ) -> ParameterisedQueryFragment:
        if len(self.common_table_expressions) == 0:
            return None, []

        fragments = [
            (query.build_fragment(), name)
            for query, name in self.common_table_expressions
        ]
        expressions = [
            (
                sql.Identifier(fragment[1])
                + sql.SQL(" AS (")
                + fragment[0][0]
                + sql.SQL(")")
            )
            for fragment in fragments
            if fragment[0][0] is not None
        ]
        clause = sql.SQL("WITH ") + sql.SQL(", ").join(expressions)
        params = [param for fragment in fragments for param in fragment[0][1]]

        return clause, params

    def _union_fragment(self) -> ParameterisedQueryFragment:
        if self.unions is None:
            return None, []

        queries, mode = self.unions
        fragments = [query.build_fragment() for query in queries]
        clauses = [
            sql.SQL("(") + fragment[0] + sql.SQL(")")
            for fragment in fragments
            if fragment[0] is not None
        ]
        union_part = (
            sql.SQL(" UNION DISTINCT ")
            if mode == SetOperationMode.DISTINCT
            else sql.SQL(" UNION ALL ")
        )
        clause = union_part.join(clauses)
        params = [param for fragment in fragments for param in fragment[1]]

        return clause, params

    def _select_fragment(self) -> ParameterisedQueryFragment:
        if len(self.select_list) == 0:
            return None, []
        elif self.select_list == ["*"]:
            return sql.SQL("SELECT *"), []
        else:
            return sql.SQL("SELECT ") + sql.SQL(", ").join(
                [
                    column.name
                    for column in self.select_list
                    if column.name is not None
                ]
            ), []

    def _from_fragment(self) -> ParameterisedQueryFragment:
        if len(self.from_tables) == 0 and len(self.from_subqueries) == 0:
            return None, []

        table_from_parts = [
            sql.Identifier(table) for table in self.from_tables
        ]
        fragments = [
            (query.build_fragment(), alias)
            for query, alias in self.from_subqueries
        ]
        subquery_from_parts = [
            sql.SQL("(")
            + fragment[0]
            + sql.SQL(") AS ")
            + sql.Identifier(alias)
            for fragment, alias in fragments
            if fragment[0] is not None
        ]

        clause = sql.SQL("FROM ") + sql.SQL(", ").join(
            table_from_parts + subquery_from_parts
        )
        params = [param for fragment, _ in fragments for param in fragment[1]]

        return clause, params

    def _where_fragment(self) -> ParameterisedQueryFragment:
        if len(self.where_conditions) == 0:
            return None, []

        fragments = [
            condition.build_fragment() for condition in self.where_conditions
        ]
        clauses = [
            fragment[0] for fragment in fragments if fragment[0] is not None
        ]

        clause = sql.SQL("WHERE ") + sql.SQL(" AND ").join(clauses)
        params = [param for fragment in fragments for param in fragment[1]]

        return clause, params

    def _order_by_fragment(self) -> ParameterisedQueryFragment:
        if len(self.sort_columns) == 0:
            return None, []

        def sort_column_expression_to_sql(
            sort_column: SortColumn,
        ) -> sql.Composed:
            if sort_column.expression.is_path():
                return sql.SQL("{column}#>{path_expression}").format(
                    column=sort_column.expression.name,
                    path_expression=sort_column.expression.to_path_expression(),
                )
            else:
                return sql.SQL("{column}").format(
                    column=sort_column.expression.name
                )

        sort_columns = [
            sort_column_expression_to_sql(sort_column)
            + sql.SQL(" ")
            + sql.SQL(sort_column.direction.value)
            for sort_column in self.sort_columns
        ]

        clause = sql.SQL("ORDER BY ") + sql.SQL(", ").join(sort_columns)
        params: Sequence[Any] = []

        return clause, params

    def _limit_fragment(self) -> ParameterisedQueryFragment:
        if self.limit_value is None:
            return None, []

        clause = sql.SQL("LIMIT %s")
        params: Sequence[Any] = [self.limit_value]

        return clause, params

    def _offset_fragment(self) -> ParameterisedQueryFragment:
        if self.offset_value is None:
            return None, []

        clause = sql.SQL("OFFSET %s")
        params: Sequence[Any] = [self.offset_value]

        return clause, params

    def build_fragment(self) -> ParameterisedQueryFragment:
        cte_clause, cte_params = self._common_table_expressions_fragment()
        union_clause, union_params = self._union_fragment()
        select_clause, _ = self._select_fragment()
        from_clause, from_params = self._from_fragment()
        where_clause, where_params = self._where_fragment()
        order_by_clause, _ = self._order_by_fragment()
        limit_clause, limit_params = self._limit_fragment()
        offset_clause, offset_params = self._offset_fragment()

        clauses = [
            clause
            for clause in [
                cte_clause,
                union_clause,
                select_clause,
                from_clause,
                where_clause,
                order_by_clause,
                limit_clause,
                offset_clause,
            ]
            if clause is not None
        ]
        joined = sql.SQL(" ").join(clauses)
        params = [
            *cte_params,
            *union_params,
            *from_params,
            *where_params,
            *limit_params,
            *offset_params,
        ]

        return joined, params

    def build(self) -> ParameterisedQuery:
        fragment, params = self.build_fragment()

        if fragment is None:
            raise ValueError("Empty query.")

        return fragment, params
