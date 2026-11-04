from abc import ABC, abstractmethod
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from enum import Enum, StrEnum
from typing import Any, LiteralString, Self, TypedDict, Unpack, cast

from psycopg import sql

from ...types import Applier
from .types import ParameterisedQuery, ParameterisedQueryFragment


class Node(ABC):
    """Base class for parts of a SQL query that render to a fragment."""

    @abstractmethod
    def to_fragment(self) -> ParameterisedQueryFragment:
        """Render this node as SQL with its parameters."""
        raise NotImplementedError


class Expression(Node, ABC):
    """Base class for nodes that evaluate to a value."""


@dataclass(frozen=True, kw_only=True)
class FunctionApplication(Expression):
    """A call to a SQL function, such as `to_jsonb(...)`.

    Attributes:
        function_name: The name of the function, quoted as an identifier.
        arguments: The arguments to pass to the function.
    """

    function_name: str
    arguments: Sequence[Expression] = ()

    def to_fragment(self) -> ParameterisedQueryFragment:
        argument_query_fragments = [
            argument.to_fragment() for argument in self.arguments
        ]
        argument_sql_fragments = [
            argument_fragment[0]
            for argument_fragment in argument_query_fragments
            if argument_fragment[0] is not None
        ]
        argument_params = [
            argument_param
            for argument_query_fragment in argument_query_fragments
            for argument_param in argument_query_fragment[1]
        ]
        sql_fragment = (
            sql.SQL("{function_name}(").format(
                function_name=sql.Identifier(self.function_name)
            )
            + sql.SQL(", ").join(argument_sql_fragments)
            + sql.SQL(")")
        )
        return sql_fragment, argument_params


@dataclass(frozen=True, kw_only=True)
class Cast(Expression):
    """A `CAST(expression AS typename)` expression.

    Attributes:
        expression: The expression to cast.
        typename: The type to cast to, quoted as an identifier.
    """

    expression: Expression
    typename: str

    def to_fragment(self) -> ParameterisedQueryFragment:
        expression_fragment = self.expression.to_fragment()
        if expression_fragment[0] is None:
            return expression_fragment

        sql_fragment = (
            sql.SQL("CAST(")
            + expression_fragment[0]
            + sql.SQL(" AS {typename})").format(
                typename=sql.Identifier(self.typename)
            )
        )
        params = expression_fragment[1]

        return sql_fragment, params


@dataclass(frozen=True, kw_only=True)
class ColumnReference(Expression):
    """A reference to a column, optionally qualified by table and schema.

    Attributes:
        schema: The schema of the table. Requires `table` to be set.
        table: The table containing the column.
        field: The name of the column.
    """

    schema: str | None = None
    table: str | None = None
    field: str

    def to_fragment(self) -> ParameterisedQueryFragment:
        match (self.schema, self.table):
            case (str(), None):
                raise ValueError(
                    "Cannot resolve column reference with schema but no table."
                )
            case (str(schema), str(table)):
                return (
                    sql.SQL("{schema}.{table}.{name}").format(
                        schema=sql.Identifier(schema),
                        table=sql.Identifier(table),
                        name=sql.Identifier(self.field),
                    )
                ), []
            case (None, str(table)):
                return (
                    sql.SQL("{table}.{name}").format(
                        table=sql.Identifier(table),
                        name=sql.Identifier(self.field),
                    )
                ), []
            case (None, None):
                return (
                    sql.SQL("{name}").format(name=sql.Identifier(self.field))
                ), []
            case _:
                raise TypeError("Invalid schema and/or table name.")


@dataclass(frozen=True)
class Star(Expression):
    """A `*` select target, optionally qualified by table and schema.

    Attributes:
        schema: The schema of the table. Requires `table` to be set.
        table: The table whose columns to select.
    """

    schema: str | None = None
    table: str | None = None

    def to_fragment(self) -> ParameterisedQueryFragment:
        match (self.schema, self.table):
            case (str(), None):
                raise ValueError(
                    "Cannot resolve column reference with schema but no table."
                )
            case (str(schema), str(table)):
                return (
                    sql.SQL("{schema}.{table}.{name}").format(
                        schema=sql.Identifier(schema),
                        table=sql.Identifier(table),
                        name=sql.SQL("*"),
                    )
                ), []
            case (None, str(table)):
                return (
                    sql.SQL("{table}.{name}").format(
                        table=sql.Identifier(table),
                        name=sql.SQL("*"),
                    )
                ), []
            case (None, None):
                return (sql.SQL("{name}").format(name=sql.SQL("*"))), []
            case _:
                raise TypeError("Invalid schema and/or table name.")


@dataclass(frozen=True)
class ResultTarget(Node):
    """An entry in a select list, optionally labelled with `AS`.

    Attributes:
        expression: The expression to select.
        label: The name to give the selected value, if any.
    """

    expression: Expression
    label: str | None = None

    def to_fragment(self) -> ParameterisedQueryFragment:
        expression_fragment = self.expression.to_fragment()

        sql_fragment = expression_fragment[0]
        params = expression_fragment[1]

        if self.label is not None and sql_fragment is not None:
            sql_fragment = sql_fragment + sql.SQL(" AS {label}").format(
                label=sql.Identifier(self.label)
            )

        return sql_fragment, params


@dataclass(frozen=True)
class Constant(Expression):
    """A value passed to the query as a bound parameter.

    Attributes:
        value: The value to bind.
    """

    value: Any

    def to_fragment(self) -> ParameterisedQueryFragment:
        operand_sql = sql.SQL("%s")
        params = [self.value]

        return operand_sql, params


@dataclass(frozen=True)
class Raw(Expression):
    """Literal SQL included verbatim, without escaping.

    Attributes:
        sql: The SQL text to include.
    """

    sql: Any

    def to_fragment(self) -> ParameterisedQueryFragment:
        return sql.SQL(self.sql), []


null = Raw("NULL")
empty = Raw("")

type OrderByColumn = str | ColumnReference


class ComparisonType(Enum):
    """Whether an operator compares nested values as text or JSONB."""

    TEXT = "TEXT"
    JSONB = "JSONB"


class Operator(StrEnum):
    """SQL comparison operators, with values as they appear in SQL."""

    EQUALS = "="
    NOT_EQUALS = "!="
    GREATER_THAN = ">"
    GREATER_THAN_OR_EQUAL = ">="
    LESS_THAN = "<"
    LESS_THAN_OR_EQUAL = "<="
    IN = "IN"
    CONTAINS = "@>"
    REGEX_MATCHES = "~"
    NOT_REGEX_MATCHES = "!~"
    IS_NULL = "IS NULL"
    IS_NOT_NULL = "IS NOT NULL"

    @property
    def comparison_type(self) -> ComparisonType:
        """How nested values are compared by this operator.

        Regular expression and null checks compare text; all other
        operators compare JSONB.
        """
        if self in {
            Operator.REGEX_MATCHES,
            Operator.NOT_REGEX_MATCHES,
            Operator.IS_NULL,
            Operator.IS_NOT_NULL,
        }:
            return ComparisonType.TEXT
        return ComparisonType.JSONB

    def has_value(self) -> bool:
        """Return whether the operator takes a right-hand operand."""
        return self not in {Operator.IS_NULL, Operator.IS_NOT_NULL}


class SetQuantifier(StrEnum):
    """Whether a `UNION` keeps (`ALL`) or removes (`DISTINCT`) duplicates."""

    ALL = "ALL"
    DISTINCT = "DISTINCT"


class SortDirection(StrEnum):
    """Direction of an `ORDER BY` term."""

    ASC = "ASC"
    DESC = "DESC"

    def reverse(self) -> "SortDirection":
        """Return the opposite direction."""
        return (
            SortDirection.ASC
            if self == SortDirection.DESC
            else SortDirection.DESC
        )


@dataclass(frozen=True)
class SortBy(Node):
    """A single `ORDER BY` term.

    Attributes:
        expression: The expression to sort by.
        direction: The direction to sort in. If `None`, no direction is
            rendered, so the database default of ascending applies.
    """

    expression: Expression
    direction: SortDirection | None = None

    def is_ascending(self):
        """Return whether the term sorts ascending, including by default."""
        return self.direction == SortDirection.ASC or self.direction is None

    def is_descending(self):
        return self.direction == SortDirection.DESC

    def reverse(self):
        """Return this term with its direction reversed, if it has one."""
        return self.__class__(
            expression=self.expression,
            direction=self.direction.reverse() if self.direction else None,
        )

    def to_fragment(self) -> ParameterisedQueryFragment:
        expression_fragment = self.expression.to_fragment()

        sql_fragment = expression_fragment[0]
        params = expression_fragment[1]

        if sql_fragment is None or self.direction is None:
            return expression_fragment

        sql_fragment = sql_fragment + sql.SQL(" {direction}").format(
            direction=sql.SQL(cast(LiteralString, self.direction.value))
        )

        return sql_fragment, params


type ConditionOperand = Expression | Iterable[Expression] | "Query"


class ConditionParams(TypedDict, total=False):
    left: ConditionOperand
    right: ConditionOperand
    operator: Operator


@dataclass(frozen=True)
class Condition(Expression):
    """A binary condition, built fluently, for use in a `WHERE` clause.

    Each operand may be an expression, a sequence of expressions (rendered
    as a row, for row comparisons) or a query (rendered as a subquery).
    Builder methods return a new condition rather than modifying this one.

    Args:
        left: The left-hand operand.
        right: The right-hand operand.
        operator: The comparison operator.
    """

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

    def regex_matches(self):
        return self.operator(Operator.REGEX_MATCHES)

    def not_regex_matches(self):
        return self.operator(Operator.NOT_REGEX_MATCHES)

    @staticmethod
    def _operand_fragment(
        operand: ConditionOperand,
    ) -> ParameterisedQueryFragment:
        if isinstance(operand, Query):
            subquery, params = operand.to_fragment()
            if subquery is None:
                return None, []

            operand_sql = sql.SQL("(") + subquery + sql.SQL(")")

            return operand_sql, params

        if isinstance(operand, Iterable):
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

        return operand.to_fragment()

    def _left_fragment(self) -> ParameterisedQueryFragment:
        if self._left is None:
            return None, []

        return self._operand_fragment(self._left)

    def _right_fragment(self) -> ParameterisedQueryFragment:
        if self._right is None:
            return None, []

        return self._operand_fragment(self._right)

    def to_fragment(self) -> ParameterisedQueryFragment:
        if self._left is None or self._operator is None or self._right is None:
            raise ValueError("Condition not fully specified.")

        left_sql, left_params = self._left_fragment()
        right_sql, right_params = self._right_fragment()

        if left_sql is None or right_sql is None:
            raise ValueError("Condition not fully specified.")

        clause = (
            left_sql
            + sql.SQL(" {operator} ").format(
                operator=sql.SQL(cast(LiteralString, self._operator.value))
            )
            + right_sql
        )
        params = [*left_params, *right_params]

        return clause, params


def to_result_target_or_star(
    column: str | Expression | ResultTarget,
) -> ResultTarget | Star:
    if isinstance(column, str):
        return ResultTarget(expression=ColumnReference(field=column))
    elif isinstance(column, Expression):
        return ResultTarget(expression=column)
    else:
        return column


def to_sortby(sortby: str | SortBy) -> SortBy:
    if isinstance(sortby, str):
        return SortBy(expression=ColumnReference(field=sortby))
    return sortby


class QueryParams(TypedDict, total=False):
    common_table_expressions: Sequence[tuple["Query", str]]
    unions: tuple[Sequence["Query"], SetQuantifier] | None
    select_target_list: Sequence[ResultTarget | Star]
    from_tables: Sequence[str]
    from_subqueries: Sequence[tuple["Query", str]]
    where_conditions: Sequence[Condition]
    sortby_list: Sequence[SortBy]
    limit_value: int | None
    offset_value: int | None


@dataclass(frozen=True)
class Query:
    """An immutable, fluent builder for SQL `SELECT` queries.

    Each builder method returns a new query rather than modifying this one.
    Call `build` to render the query and its parameters for execution.

    Example:
        ```python
        query = (
            Query()
            .select_all()
            .from_table("projections")
            .where(
                Condition()
                .left(ColumnReference(field="name"))
                .equals()
                .right(Constant("thing"))
            )
            .order_by("id")
            .limit(10)
        )
        sql, params = query.build()
        ```
    """

    common_table_expressions: Sequence[tuple[Self, str]]
    unions: tuple[Sequence[Self], SetQuantifier] | None
    select_target_list: Sequence[ResultTarget | Star]
    from_tables: Sequence[str]
    from_subqueries: Sequence[tuple[Self, str]]
    where_conditions: Sequence[Condition]
    sortby_list: Sequence[SortBy]
    limit_value: int | None
    offset_value: int | None

    @staticmethod
    def union(
        query1: "Query",
        query2: "Query",
        *more_queries: "Query",
        mode: SetQuantifier = SetQuantifier.DISTINCT,
    ) -> "Query":
        """Combine two or more queries with `UNION`.

        Args:
            query1: The first query.
            query2: The second query.
            *more_queries: Any further queries.
            mode: Whether to keep or remove duplicate rows. Defaults to
                removing them.

        Returns:
            a query over the union of the given queries.
        """
        return Query(unions=([query1, query2, *more_queries], mode))

    def __init__(
        self,
        *,
        common_table_expressions: Sequence[tuple[Self, str]] | None = None,
        unions: tuple[Sequence[Self], SetQuantifier] | None = None,
        select_target_list: Sequence[ResultTarget | Star] | None = None,
        from_tables: Sequence[str] | None = None,
        from_subqueries: Sequence[tuple[Self, str]] | None = None,
        where_conditions: Sequence[Condition] | None = None,
        sortby_list: Sequence[SortBy] | None = None,
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
            "select_target_list",
            list(select_target_list) if select_target_list is not None else [],
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
            "sortby_list",
            list(sortby_list) if sortby_list is not None else [],
        )
        object.__setattr__(self, "limit_value", limit_value)
        object.__setattr__(self, "offset_value", offset_value)

    def clone(self, **kwargs: Unpack[QueryParams]) -> Self:
        """Return a copy of this query with the given parts replaced."""
        return self.__class__(
            common_table_expressions=kwargs.get(
                "common_table_expressions", self.common_table_expressions
            ),
            unions=kwargs.get("unions", self.unions),
            select_target_list=kwargs.get(
                "select_target_list", self.select_target_list
            ),
            from_tables=kwargs.get("from_tables", self.from_tables),
            from_subqueries=kwargs.get(
                "from_subqueries", self.from_subqueries
            ),
            where_conditions=kwargs.get(
                "where_conditions", self.where_conditions
            ),
            sortby_list=kwargs.get("sortby_list", self.sortby_list),
            limit_value=kwargs.get("limit_value", self.limit_value),
            offset_value=kwargs.get("offset_value", self.offset_value),
        )

    def with_query(self, query: Self, name: str) -> Self:
        """Add a common table expression, rendered as `WITH name AS (...)`."""
        return self.clone(
            common_table_expressions=[
                *self.common_table_expressions,
                (query, name),
            ]
        )

    def select(self, *target_list: str | Expression | ResultTarget) -> Self:
        converted = [
            to_result_target_or_star(column) for column in target_list
        ]
        return self.clone(
            select_target_list=[*self.select_target_list, *converted]
        )

    def select_all(self) -> Self:
        return self.clone(
            select_target_list=[*self.select_target_list, Star()]
        )

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

    def replace_order_by(self, *sortby_list: str | SortBy) -> Self:
        """Replace any existing `ORDER BY` terms with the given terms."""
        return self.clone(
            sortby_list=[to_sortby(sortby) for sortby in sortby_list]
        )

    def order_by(self, *sortby_list: str | SortBy) -> Self:
        """Append the given `ORDER BY` terms; strings name columns."""
        return self.clone(
            sortby_list=[
                *self.sortby_list,
                *[to_sortby(sortby) for sortby in sortby_list],
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
            (query.to_fragment(), name)
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
        fragments = [query.to_fragment() for query in queries]
        clauses = [
            sql.SQL("(") + fragment[0] + sql.SQL(")")
            for fragment in fragments
            if fragment[0] is not None
        ]
        union_part = (
            sql.SQL(" UNION DISTINCT ")
            if mode == SetQuantifier.DISTINCT
            else sql.SQL(" UNION ALL ")
        )
        clause = union_part.join(clauses)
        params = [param for fragment in fragments for param in fragment[1]]

        return clause, params

    def _select_fragment(self) -> ParameterisedQueryFragment:
        if len(self.select_target_list) == 0:
            return None, []
        else:
            target_element_fragments = [
                target_element.to_fragment()
                for target_element in self.select_target_list
            ]
            target_element_sql_composables = [
                target_element_fragment[0]
                for target_element_fragment in target_element_fragments
                if target_element_fragment[0] is not None
            ]
            target_element_params = [
                target_element_param
                for target_element_fragment in target_element_fragments
                for target_element_param in target_element_fragment[1]
            ]
            return sql.SQL("SELECT ") + sql.SQL(", ").join(
                target_element_sql_composables
            ), target_element_params

    def _from_fragment(self) -> ParameterisedQueryFragment:
        if len(self.from_tables) == 0 and len(self.from_subqueries) == 0:
            return None, []

        table_from_parts = [
            sql.Identifier(table) for table in self.from_tables
        ]
        fragments = [
            (query.to_fragment(), alias)
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
            condition.to_fragment() for condition in self.where_conditions
        ]
        clauses = [
            fragment[0] for fragment in fragments if fragment[0] is not None
        ]

        clause = sql.SQL("WHERE ") + sql.SQL(" AND ").join(clauses)
        params = [param for fragment in fragments for param in fragment[1]]

        return clause, params

    def _order_by_fragment(self) -> ParameterisedQueryFragment:
        if len(self.sortby_list) == 0:
            return None, []

        sortby_fragments = [
            sortby.to_fragment() for sortby in self.sortby_list
        ]
        sortby_sql_fragments = [
            sortby_fragment[0]
            for sortby_fragment in sortby_fragments
            if sortby_fragment[0] is not None
        ]
        sortby_params = [
            sortby_param
            for sortby_fragment in sortby_fragments
            for sortby_param in sortby_fragment[1]
        ]

        clause = sql.SQL("ORDER BY ") + sql.SQL(", ").join(
            sortby_sql_fragments
        )

        return clause, sortby_params

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

    def to_fragment(self) -> ParameterisedQueryFragment:
        cte_clause, cte_params = self._common_table_expressions_fragment()
        union_clause, union_params = self._union_fragment()
        select_clause, select_params = self._select_fragment()
        from_clause, from_params = self._from_fragment()
        where_clause, where_params = self._where_fragment()
        order_by_clause, order_by_params = self._order_by_fragment()
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
            *select_params,
            *from_params,
            *where_params,
            *order_by_params,
            *limit_params,
            *offset_params,
        ]

        return joined, params

    def build(self) -> ParameterisedQuery:
        """Render the query for execution.

        Returns:
            the SQL query and the parameters to bind into it, in order.

        Raises:
            ValueError: If the query is empty.
        """
        fragment, params = self.to_fragment()

        match fragment:
            case sql.SQL() | sql.Composed():
                return fragment, params
            case None:
                raise ValueError("Empty query.")
            case _:
                raise ValueError("Invalid query.")


class QueryApplier(Applier[Query], ABC):
    """Transforms a query, for example by adding a clause to it."""
