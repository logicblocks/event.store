from typing import Any, Sequence, cast

import pytest
from psycopg import abc, sql

from logicblocks.event.persistence.postgres import (
    Cast,
    ColumnReference,
    Condition,
    Constant,
    FunctionApplication,
    Operator,
    ParameterisedQuery,
    ParameterisedQueryFragment,
    Query,
    ResultTarget,
    SortBy,
    SortDirection,
    Star,
)


def sql_query_to_string(query: abc.Query) -> str:
    if isinstance(query, bytes):
        return query.decode()
    if isinstance(query, sql.SQL) or isinstance(query, sql.Composed):
        return query.as_string()
    return cast(str, query)


def parameterised_query_to_string(
    parameterised_query: ParameterisedQuery,
) -> tuple[str, Sequence[Any]]:
    query, params = parameterised_query
    return sql_query_to_string(query), params


def parameterised_query_fragment_to_string(
    parameterised_query: ParameterisedQueryFragment,
) -> tuple[str, Sequence[Any]]:
    fragment, params = parameterised_query
    if fragment is None:
        return "", params
    return fragment.as_string(), params


class TestColumnReference:
    def test_column_reference_with_no_table_or_schema(self):
        column_reference = ColumnReference(field="name")
        fragment = column_reference.to_fragment()

        assert parameterised_query_fragment_to_string(fragment) == (
            '"name"',
            [],
        )

    def test_column_reference_with_table_but_no_schema(self):
        column_reference = ColumnReference(table="users", field="state")
        fragment = column_reference.to_fragment()

        assert parameterised_query_fragment_to_string(fragment) == (
            '"users"."state"',
            [],
        )

    def test_column_reference_with_table_and_schema(self):
        column_reference = ColumnReference(
            schema="service", table="users", field="state"
        )
        fragment = column_reference.to_fragment()

        assert parameterised_query_fragment_to_string(fragment) == (
            '"service"."users"."state"',
            [],
        )

    def test_column_reference_with_schema_but_no_table(self):
        column_reference = ColumnReference(schema="service", field="state")

        with pytest.raises(ValueError):
            column_reference.to_fragment()


class TestFunctionApplication:
    def test_with_no_arguments(self):
        function_application = FunctionApplication(function_name="now")
        fragment = function_application.to_fragment()

        assert parameterised_query_fragment_to_string(fragment) == (
            '"now"()',
            [],
        )

    def test_with_single_argument(self):
        function_application = FunctionApplication(
            function_name="upper",
            arguments=[Constant("abc")],
        )
        fragment = function_application.to_fragment()

        assert parameterised_query_fragment_to_string(fragment) == (
            '"upper"(%s)',
            ["abc"],
        )

    def test_with_multiple_arguments(self):
        function_application = FunctionApplication(
            function_name="concat",
            arguments=[Constant("abc"), Constant("def")],
        )
        fragment = function_application.to_fragment()

        assert parameterised_query_fragment_to_string(fragment) == (
            '"concat"(%s, %s)',
            ["abc", "def"],
        )

    def test_with_expression_argument(self):
        function_application = FunctionApplication(
            function_name="upper",
            arguments=[
                ColumnReference(table="users", field="name"),
            ],
        )
        fragment = function_application.to_fragment()

        assert parameterised_query_fragment_to_string(fragment) == (
            '"upper"("users"."name")',
            [],
        )

    def test_with_function_application_argument(self):
        function_application = FunctionApplication(
            function_name="concat",
            arguments=[
                FunctionApplication(
                    function_name="upper",
                    arguments=[ColumnReference(table="users", field="name")],
                ),
                Constant("def"),
            ],
        )
        fragment = function_application.to_fragment()

        assert parameterised_query_fragment_to_string(fragment) == (
            '"concat"("upper"("users"."name"), %s)',
            ["def"],
        )


class TestCast:
    def test_with_simple_column(self):
        cast = Cast(
            expression=ColumnReference(field="name"),
            typename="text",
        )
        fragment = cast.to_fragment()

        assert parameterised_query_fragment_to_string(fragment) == (
            'CAST("name" AS "text")',
            [],
        )


class TestResultTarget:
    def test_with_no_label(self):
        result_target = ResultTarget(expression=ColumnReference(field="name"))
        fragment = result_target.to_fragment()

        assert parameterised_query_fragment_to_string(fragment) == (
            '"name"',
            [],
        )

    def test_with_label(self):
        result_target = ResultTarget(
            expression=ColumnReference(field="name"), label="user_name"
        )
        fragment = result_target.to_fragment()

        assert parameterised_query_fragment_to_string(fragment) == (
            '"name" AS "user_name"',
            [],
        )

    def test_with_complex_expression(self):
        result_target = ResultTarget(
            expression=FunctionApplication(
                function_name="upper",
                arguments=[ColumnReference(field="name")],
            ),
            label="upper_name",
        )
        fragment = result_target.to_fragment()

        assert parameterised_query_fragment_to_string(fragment) == (
            '"upper"("name") AS "upper_name"',
            [],
        )


class TestStar:
    def test_star_with_no_table_or_schema(self):
        star = Star()
        fragment = star.to_fragment()

        assert parameterised_query_fragment_to_string(fragment) == (
            "*",
            [],
        )

    def test_star_with_table_but_no_schema(self):
        star = Star(table="users")
        fragment = star.to_fragment()

        assert parameterised_query_fragment_to_string(fragment) == (
            '"users".*',
            [],
        )

    def test_star_with_table_and_schema(self):
        star = Star(schema="service", table="users")
        fragment = star.to_fragment()

        assert parameterised_query_fragment_to_string(fragment) == (
            '"service"."users".*',
            [],
        )

    def test_star_with_schema_but_no_table(self):
        star = Star(schema="service")

        with pytest.raises(ValueError):
            star.to_fragment()


class TestSortBy:
    def test_with_simple_column_no_direction(self):
        sort_by = SortBy(expression=ColumnReference(field="name"))
        fragment = sort_by.to_fragment()

        assert parameterised_query_fragment_to_string(fragment) == (
            '"name"',
            [],
        )

    def test_with_complex_expression_no_direction(self):
        sort_by = SortBy(
            expression=FunctionApplication(
                function_name="upper",
                arguments=[ColumnReference(field="name")],
            )
        )
        fragment = sort_by.to_fragment()

        assert parameterised_query_fragment_to_string(fragment) == (
            '"upper"("name")',
            [],
        )

    def test_with_simple_column_ascending_direction(self):
        sort_by = SortBy(
            expression=ColumnReference(field="name"),
            direction=SortDirection.ASC,
        )
        fragment = sort_by.to_fragment()

        assert parameterised_query_fragment_to_string(fragment) == (
            '"name" ASC',
            [],
        )

    def test_with_complex_expression_ascending_direction(self):
        sort_by = SortBy(
            expression=FunctionApplication(
                function_name="upper",
                arguments=[ColumnReference(field="name")],
            ),
            direction=SortDirection.ASC,
        )
        fragment = sort_by.to_fragment()

        assert parameterised_query_fragment_to_string(fragment) == (
            '"upper"("name") ASC',
            [],
        )

    def test_with_simple_column_descending_direction(self):
        sort_by = SortBy(
            expression=ColumnReference(field="name"),
            direction=SortDirection.DESC,
        )
        fragment = sort_by.to_fragment()

        assert parameterised_query_fragment_to_string(fragment) == (
            '"name" DESC',
            [],
        )

    def test_with_complex_expression_descending_direction(self):
        sort_by = SortBy(
            expression=FunctionApplication(
                function_name="upper",
                arguments=[ColumnReference(field="name")],
            ),
            direction=SortDirection.DESC,
        )
        fragment = sort_by.to_fragment()

        assert parameterised_query_fragment_to_string(fragment) == (
            '"upper"("name") DESC',
            [],
        )


class TestCondition:
    def test_equals_condition(self):
        condition = (
            Condition()
            .left(ColumnReference(field="id"))
            .equals()
            .right(Constant("123"))
            .to_fragment()
        )

        assert parameterised_query_fragment_to_string(condition) == (
            '"id" = %s',
            ["123"],
        )

    def test_not_equals_condition(self):
        condition = (
            Condition()
            .left(ColumnReference(field="id"))
            .not_equals()
            .right(Constant("123"))
            .to_fragment()
        )

        assert parameterised_query_fragment_to_string(condition) == (
            '"id" != %s',
            ["123"],
        )

    def test_greater_than_condition(self):
        condition = (
            Condition()
            .left(ColumnReference(field="id"))
            .greater_than()
            .right(Constant("123"))
            .to_fragment()
        )

        assert parameterised_query_fragment_to_string(condition) == (
            '"id" > %s',
            ["123"],
        )

    def test_greater_than_or_equal_to_condition(self):
        condition = (
            Condition()
            .left(ColumnReference(field="id"))
            .greater_than_or_equal_to()
            .right(Constant("123"))
            .to_fragment()
        )

        assert parameterised_query_fragment_to_string(condition) == (
            '"id" >= %s',
            ["123"],
        )

    def test_less_than_condition(self):
        condition = (
            Condition()
            .left(ColumnReference(field="id"))
            .less_than()
            .right(Constant("123"))
            .to_fragment()
        )

        assert parameterised_query_fragment_to_string(condition) == (
            '"id" < %s',
            ["123"],
        )

    def test_less_than_or_equal_to_condition(self):
        condition = (
            Condition()
            .left(ColumnReference(field="id"))
            .less_than_or_equal_to()
            .right(Constant("123"))
            .to_fragment()
        )

        assert parameterised_query_fragment_to_string(condition) == (
            '"id" <= %s',
            ["123"],
        )

    def test_operator_condition(self):
        condition = (
            Condition()
            .left(ColumnReference(field="id"))
            .operator(Operator.GREATER_THAN_OR_EQUAL)
            .right(Constant("123"))
            .to_fragment()
        )

        assert parameterised_query_fragment_to_string(condition) == (
            '"id" >= %s',
            ["123"],
        )

    def test_column_with_path_to_jsonb_value(self):
        condition = (
            Condition()
            .left(
                FunctionApplication(
                    function_name="jsonb_extract_path",
                    arguments=[
                        ColumnReference(field="state"),
                        Constant(value="value_1"),
                        Constant(value=0),
                        Constant(value="value_2"),
                    ],
                )
            )
            .operator(Operator.GREATER_THAN_OR_EQUAL)
            .right(
                FunctionApplication(
                    function_name="to_jsonb", arguments=[Constant("123")]
                )
            )
            .to_fragment()
        )

        assert parameterised_query_fragment_to_string(condition) == (
            '"jsonb_extract_path"("state", %s, %s, %s) >= "to_jsonb"(%s)',
            ["value_1", 0, "value_2", "123"],
        )

    def test_row_comparison_row_condition(self):
        condition = (
            Condition()
            .left((ColumnReference(field="id"), ColumnReference(field="name")))
            .equals()
            .right((Constant("123"), Constant("test")))
            .to_fragment()
        )

        assert parameterised_query_fragment_to_string(condition) == (
            '("id", "name") = (%s, %s)',
            ["123", "test"],
        )

    def test_row_comparison_subquery_condition(self):
        condition = (
            Condition()
            .left((ColumnReference(field="id"), ColumnReference(field="name")))
            .equals()
            .right(Query().select("id", "name").from_table("records"))
            .to_fragment()
        )

        assert parameterised_query_fragment_to_string(condition) == (
            '("id", "name") = (SELECT "id", "name" FROM "records")',
            [],
        )

    def test_subquery_comparison_row_condition(self):
        condition = (
            Condition()
            .left(Query().select("id", "name").from_table("records").limit(1))
            .equals()
            .right(
                (ColumnReference(field="id"), ColumnReference(field="name"))
            )
            .to_fragment()
        )

        assert parameterised_query_fragment_to_string(condition) == (
            '(SELECT "id", "name" FROM "records" LIMIT %s) = ("id", "name")',
            [1],
        )

    def test_subquery_comparison_subquery_condition(self):
        condition = (
            Condition()
            .left(Query().select("id", "name").from_table("table_1").limit(1))
            .equals()
            .right(Query().select("id", "name").from_table("table_2").limit(1))
            .to_fragment()
        )

        assert parameterised_query_fragment_to_string(condition) == (
            '(SELECT "id", "name" FROM "table_1" LIMIT %s) = '
            '(SELECT "id", "name" FROM "table_2" LIMIT %s)',
            [1, 1],
        )

    def test_regex_matches_condition(self):
        condition = (
            Condition()
            .left(ColumnReference(field="id"))
            .regex_matches()
            .right(Constant("123.*"))
            .to_fragment()
        )

        assert parameterised_query_fragment_to_string(condition) == (
            '"id" ~ %s',
            ["123.*"],
        )

    def test_like_condition(self):
        condition = (
            Condition()
            .left(ColumnReference(field="name"))
            .operator(Operator.LIKE)
            .right(Constant("test_%"))
            .to_fragment()
        )

        assert parameterised_query_fragment_to_string(condition) == (
            '"name" LIKE %s',
            ["test_%"],
        )


class TestQuerySelect:
    def test_allows_selecting_all_columns_from_single_table(self):
        query = Query().select_all().from_table("events").build()

        assert parameterised_query_to_string(query) == (
            'SELECT * FROM "events"',
            [],
        )

    def test_allows_selecting_specific_columns_from_single_table(self):
        query = Query().select("id", "name").from_table("events").build()

        assert parameterised_query_to_string(query) == (
            'SELECT "id", "name" FROM "events"',
            [],
        )

    def test_allows_selecting_using_column_reference_instances(self):
        query = (
            Query()
            .select(ColumnReference(field="id"), ColumnReference(field="name"))
            .from_table("events")
            .build()
        )

        assert parameterised_query_to_string(query) == (
            'SELECT "id", "name" FROM "events"',
            [],
        )

    def test_allows_selecting_from_subquery(self):
        query = (
            Query()
            .select_all()
            .from_subquery(
                Query().select("id").from_table("events"),
                alias="event_ids",
            )
            .build()
        )

        assert parameterised_query_to_string(query) == (
            'SELECT * FROM (SELECT "id" FROM "events") AS "event_ids"',
            [],
        )

    def test_allows_selecting_from_subquery_with_params(self):
        query = (
            Query()
            .select_all()
            .from_subquery(
                Query().select("id").from_table("events").limit(1),
                alias="event_ids",
            )
            .build()
        )

        assert parameterised_query_to_string(query) == (
            'SELECT * FROM (SELECT "id" FROM "events" LIMIT %s) AS "event_ids"',
            [1],
        )

    def test_allows_selecting_from_function_application_derived_column(self):
        result_target = ResultTarget(
            expression=FunctionApplication(
                function_name="count",
                arguments=[Star()],
            ),
            label="row_count",
        )
        query = Query().select(result_target).from_table("events").build()

        assert parameterised_query_to_string(query) == (
            'SELECT "count"(*) AS "row_count" FROM "events"',
            [],
        )

    def test_allows_single_simple_where_condition_on_column(self):
        query = (
            Query()
            .select_all()
            .from_table("events")
            .where(
                Condition()
                .left(ColumnReference(field="id"))
                .equals()
                .right(Constant("123"))
            )
            .build()
        )

        assert parameterised_query_to_string(query) == (
            'SELECT * FROM "events" WHERE "id" = %s',
            ["123"],
        )

    def test_allows_single_simple_where_condition_on_column_with_path(self):
        query = (
            Query()
            .select_all()
            .from_table("projections")
            .where(
                Condition()
                .left(
                    FunctionApplication(
                        function_name="jsonb_extract_path",
                        arguments=[
                            ColumnReference(field="state"),
                            Constant(value="value_1"),
                            Constant(value=0),
                            Constant(value="value_2"),
                        ],
                    )
                )
                .equals()
                .right(Constant("123"))
            )
            .build()
        )

        assert parameterised_query_to_string(query) == (
            'SELECT * FROM "projections" '
            'WHERE "jsonb_extract_path"("state", %s, %s, %s) = %s',
            ["value_1", 0, "value_2", "123"],
        )

    def test_allows_single_order_by_column(self):
        query = (
            Query().select_all().from_table("events").order_by("id").build()
        )

        assert parameterised_query_to_string(query) == (
            'SELECT * FROM "events" ORDER BY "id"',
            [],
        )

    def test_allows_single_order_by_column_with_path(self):
        query = (
            Query()
            .select_all()
            .from_table("events")
            .order_by(
                SortBy(
                    expression=FunctionApplication(
                        function_name="jsonb_extract_path",
                        arguments=[
                            ColumnReference(field="state"),
                            Constant("value_1"),
                            Constant(0),
                            Constant("value_2"),
                        ],
                    )
                )
            )
            .build()
        )

        assert parameterised_query_to_string(query) == (
            'SELECT * FROM "events" '
            'ORDER BY "jsonb_extract_path"("state", %s, %s, %s)',
            ["value_1", 0, "value_2"],
        )

    def test_allows_multiple_order_by_columns(self):
        query = (
            Query()
            .select_all()
            .from_table("events")
            .order_by("id", "name")
            .build()
        )

        assert parameterised_query_to_string(query) == (
            'SELECT * FROM "events" ORDER BY "id", "name"',
            [],
        )

    def test_allows_multiple_order_by_columns_with_paths(self):
        query = (
            Query()
            .select_all()
            .from_table("events")
            .order_by(
                SortBy(
                    expression=FunctionApplication(
                        function_name="jsonb_extract_path",
                        arguments=[
                            ColumnReference(field="state"),
                            Constant("value_1"),
                            Constant(0),
                            Constant("value_2"),
                        ],
                    )
                ),
                SortBy(
                    expression=FunctionApplication(
                        function_name="jsonb_extract_path",
                        arguments=[
                            ColumnReference(field="state"),
                            Constant("value_3"),
                        ],
                    )
                ),
            )
            .build()
        )

        assert parameterised_query_to_string(query) == (
            'SELECT * FROM "events" '
            "ORDER BY "
            '"jsonb_extract_path"("state", %s, %s, %s), '
            '"jsonb_extract_path"("state", %s)',
            ["value_1", 0, "value_2", "value_3"],
        )

    def test_allows_setting_direction_on_single_order_by_column(self):
        query = (
            Query()
            .select_all()
            .from_table("events")
            .order_by(
                SortBy(
                    expression=ColumnReference(field="id"),
                    direction=SortDirection.DESC,
                )
            )
            .build()
        )

        assert parameterised_query_to_string(query) == (
            'SELECT * FROM "events" ORDER BY "id" DESC',
            [],
        )

    def test_allows_setting_direction_on_single_order_by_column_with_path(
        self,
    ):
        query = (
            Query()
            .select_all()
            .from_table("events")
            .order_by(
                SortBy(
                    expression=FunctionApplication(
                        function_name="jsonb_extract_path",
                        arguments=[
                            ColumnReference(field="state"),
                            Constant("value_1"),
                            Constant(0),
                            Constant("value_2"),
                        ],
                    ),
                    direction=SortDirection.DESC,
                )
            )
            .build()
        )

        assert parameterised_query_to_string(query) == (
            'SELECT * FROM "events" '
            'ORDER BY "jsonb_extract_path"("state", %s, %s, %s) DESC',
            ["value_1", 0, "value_2"],
        )

    def test_allows_setting_direction_on_multiple_order_by_columns(self):
        query = (
            Query()
            .select_all()
            .from_table("events")
            .order_by(
                SortBy(
                    expression=ColumnReference(field="id"),
                    direction=SortDirection.DESC,
                ),
                SortBy(
                    expression=ColumnReference(field="name"),
                    direction=SortDirection.ASC,
                ),
            )
            .build()
        )

        assert parameterised_query_to_string(query) == (
            'SELECT * FROM "events" ORDER BY "id" DESC, "name" ASC',
            [],
        )

    def test_allows_setting_direction_on_multiple_order_by_columns_with_paths(
        self,
    ):
        query = (
            Query()
            .select_all()
            .from_table("events")
            .order_by(
                SortBy(
                    expression=FunctionApplication(
                        function_name="jsonb_extract_path",
                        arguments=[
                            ColumnReference(field="state"),
                            Constant("value_1"),
                            Constant(0),
                            Constant("value_2"),
                        ],
                    ),
                    direction=SortDirection.DESC,
                ),
                SortBy(
                    expression=FunctionApplication(
                        function_name="jsonb_extract_path",
                        arguments=[
                            ColumnReference(field="state"),
                            Constant("value_3"),
                        ],
                    ),
                    direction=SortDirection.ASC,
                ),
            )
            .build()
        )

        assert parameterised_query_to_string(query) == (
            'SELECT * FROM "events" '
            "ORDER BY "
            '"jsonb_extract_path"("state", %s, %s, %s) DESC, '
            '"jsonb_extract_path"("state", %s) ASC',
            ["value_1", 0, "value_2", "value_3"],
        )

    def test_limits_record_count(self):
        query = Query().select_all().from_table("events").limit(10).build()

        assert parameterised_query_to_string(query) == (
            'SELECT * FROM "events" LIMIT %s',
            [10],
        )

    def test_offsets_by_record_count(self):
        query = Query().select_all().from_table("events").offset(10).build()

        assert parameterised_query_to_string(query) == (
            'SELECT * FROM "events" OFFSET %s',
            [10],
        )

    def test_allows_single_common_table_expression(self):
        query = (
            Query()
            .with_query(
                Query().select("id").from_table("events"),
                name="target",
            )
            .select_all()
            .from_table("target")
            .build()
        )

        assert parameterised_query_to_string(query) == (
            'WITH "target" AS (SELECT "id" FROM "events") '
            'SELECT * FROM "target"',
            [],
        )

    def test_allows_multiple_common_table_expressions(self):
        query = (
            Query()
            .with_query(
                (
                    Query()
                    .select_all()
                    .from_table("records")
                    .where(
                        Condition()
                        .left(ColumnReference(field="id"))
                        .equals()
                        .right(Constant("123"))
                    )
                ),
                name="first",
            )
            .with_query(
                (
                    Query()
                    .select_all()
                    .from_table("records")
                    .where(
                        Condition()
                        .left(ColumnReference(field="id"))
                        .equals()
                        .right(Constant("456"))
                    )
                ),
                name="second",
            )
            .select_all()
            .from_table("people")
            .where(
                Condition()
                .left(ColumnReference(field="name"))
                .equals()
                .right(ColumnReference(table="first", field="name"))
            )
            .where(
                Condition()
                .left(ColumnReference(field="age"))
                .equals()
                .right(ColumnReference(table="second", field="age"))
            )
            .build()
        )

        assert parameterised_query_to_string(query) == (
            "WITH "
            '"first" AS (SELECT * FROM "records" WHERE "id" = %s), '
            '"second" AS (SELECT * FROM "records" WHERE "id" = %s) '
            'SELECT * FROM "people" '
            'WHERE "name" = "first"."name" '
            'AND "age" = "second"."age"',
            ["123", "456"],
        )

    def test_allows_query_union(self):
        query = Query.union(
            Query().select_all().from_table("table_1"),
            Query().select_all().from_table("table_2"),
            Query().select_all().from_table("table_3"),
        ).build()

        assert parameterised_query_to_string(query) == (
            '(SELECT * FROM "table_1")'
            " UNION DISTINCT "
            '(SELECT * FROM "table_2")'
            " UNION DISTINCT "
            '(SELECT * FROM "table_3")',
            [],
        )
