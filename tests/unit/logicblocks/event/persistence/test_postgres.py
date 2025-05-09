from typing import Any, Sequence, cast

from psycopg import abc, sql

from logicblocks.event.persistence.postgres import (
    Column,
    Condition,
    ConnectionSettings,
    Operator,
    ParameterisedQuery,
    ParameterisedQueryFragment,
    Query,
    SortDirection,
    Value,
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


class TestPostgresConnectionSettings:
    def test_includes_all_settings_in_representation_obscuring_password(self):
        settings = ConnectionSettings(
            host="localhost",
            port=5432,
            dbname="event_store",
            user="user",
            password="supersecret",
        )

        assert repr(settings) == (
            "PostgresConnectionSettings("
            "host=localhost, "
            "port=5432, "
            "dbname=event_store, "
            "user=user, "
            "password=***********"
            ")"
        )

    def test_generates_connection_string_from_parameters(self):
        settings = ConnectionSettings(
            host="localhost",
            port=5432,
            dbname="event_store",
            user="user",
            password="supersecret",
        )

        assert (
            settings.to_connection_string()
            == "postgresql://user:supersecret@localhost:5432/event_store"
        )


class TestCondition:
    def test_equals_condition(self):
        condition = (
            Condition()
            .left(Column(field="id"))
            .equals()
            .right(Value("123"))
            .build_fragment()
        )

        assert parameterised_query_fragment_to_string(condition) == (
            '"id" = %s',
            ["123"],
        )

    def test_not_equals_condition(self):
        condition = (
            Condition()
            .left(Column(field="id"))
            .not_equals()
            .right(Value("123"))
            .build_fragment()
        )

        assert parameterised_query_fragment_to_string(condition) == (
            '"id" != %s',
            ["123"],
        )

    def test_greater_than_condition(self):
        condition = (
            Condition()
            .left(Column(field="id"))
            .greater_than()
            .right(Value("123"))
            .build_fragment()
        )

        assert parameterised_query_fragment_to_string(condition) == (
            '"id" > %s',
            ["123"],
        )

    def test_greater_than_or_equal_to_condition(self):
        condition = (
            Condition()
            .left(Column(field="id"))
            .greater_than_or_equal_to()
            .right(Value("123"))
            .build_fragment()
        )

        assert parameterised_query_fragment_to_string(condition) == (
            '"id" >= %s',
            ["123"],
        )

    def test_less_than_condition(self):
        condition = (
            Condition()
            .left(Column(field="id"))
            .less_than()
            .right(Value("123"))
            .build_fragment()
        )

        assert parameterised_query_fragment_to_string(condition) == (
            '"id" < %s',
            ["123"],
        )

    def test_less_than_or_equal_to_condition(self):
        condition = (
            Condition()
            .left(Column(field="id"))
            .less_than_or_equal_to()
            .right(Value("123"))
            .build_fragment()
        )

        assert parameterised_query_fragment_to_string(condition) == (
            '"id" <= %s',
            ["123"],
        )

    def test_operator_condition(self):
        condition = (
            Condition()
            .left(Column(field="id"))
            .operator(Operator.GREATER_THAN_OR_EQUAL)
            .right(Value("123"))
            .build_fragment()
        )

        assert parameterised_query_fragment_to_string(condition) == (
            '"id" >= %s',
            ["123"],
        )

    def test_column_with_path_to_jsonb_value(self):
        condition = (
            Condition()
            .left(Column(field="state", path=["value_1", 0, "value_2"]))
            .operator(Operator.GREATER_THAN_OR_EQUAL)
            .right(Value("123", wrapper="to_jsonb"))
            .build_fragment()
        )

        assert parameterised_query_fragment_to_string(condition) == (
            "\"state\"#>'{value_1,0,value_2}' >= to_jsonb(%s)",
            ["123"],
        )

    def test_row_comparison_row_condition(self):
        condition = (
            Condition()
            .left((Column(field="id"), Column(field="name")))
            .equals()
            .right((Value("123"), Value("test")))
            .build_fragment()
        )

        assert parameterised_query_fragment_to_string(condition) == (
            '("id", "name") = (%s, %s)',
            ["123", "test"],
        )

    def test_row_comparison_subquery_condition(self):
        condition = (
            Condition()
            .left((Column(field="id"), Column(field="name")))
            .equals()
            .right(Query().select("id", "name").from_table("records"))
            .build_fragment()
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
            .right((Column(field="id"), Column(field="name")))
            .build_fragment()
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
            .build_fragment()
        )

        assert parameterised_query_fragment_to_string(condition) == (
            '(SELECT "id", "name" FROM "table_1" LIMIT %s) = '
            '(SELECT "id", "name" FROM "table_2" LIMIT %s)',
            [1, 1],
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

    def test_allows_selecting_using_column_instances(self):
        query = (
            Query()
            .select(Column(field="id"), Column(field="name"))
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

    def test_allows_single_simple_where_condition_on_column(self):
        query = (
            Query()
            .select_all()
            .from_table("events")
            .where(
                Condition()
                .left(Column(field="id"))
                .equals()
                .right(Value("123"))
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
                .left(Column(field="state", path=["value_1", 0, "value_2"]))
                .equals()
                .right(Value("123"))
            )
            .build()
        )

        assert parameterised_query_to_string(query) == (
            'SELECT * FROM "projections" '
            "WHERE \"state\"#>'{value_1,0,value_2}' = %s",
            ["123"],
        )

    def test_allows_single_order_by_column(self):
        query = (
            Query().select_all().from_table("events").order_by("id").build()
        )

        assert parameterised_query_to_string(query) == (
            'SELECT * FROM "events" ORDER BY "id" ASC',
            [],
        )

    def test_allows_single_order_by_column_with_path(self):
        query = (
            Query()
            .select_all()
            .from_table("events")
            .order_by(Column(field="state", path=["value_1", 0, "value_2"]))
            .build()
        )

        assert parameterised_query_to_string(query) == (
            'SELECT * FROM "events" '
            "ORDER BY \"state\"#>'{value_1,0,value_2}' ASC",
            [],
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
            'SELECT * FROM "events" ORDER BY "id" ASC, "name" ASC',
            [],
        )

    def test_allows_multiple_order_by_columns_with_paths(self):
        query = (
            Query()
            .select_all()
            .from_table("events")
            .order_by(
                Column(field="state", path=["value_1", 0, "value_2"]),
                Column(field="state", path=["value_3"]),
            )
            .build()
        )

        assert parameterised_query_to_string(query) == (
            'SELECT * FROM "events" '
            "ORDER BY "
            "\"state\"#>'{value_1,0,value_2}' ASC, "
            "\"state\"#>'{value_3}' ASC",
            [],
        )

    def test_allows_setting_direction_on_single_order_by_column(self):
        query = (
            Query()
            .select_all()
            .from_table("events")
            .order_by(("id", SortDirection.DESC))
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
                (
                    Column(field="state", path=["value_1", 0, "value_2"]),
                    SortDirection.DESC,
                )
            )
            .build()
        )

        assert parameterised_query_to_string(query) == (
            'SELECT * FROM "events" '
            "ORDER BY \"state\"#>'{value_1,0,value_2}' DESC",
            [],
        )

    def test_allows_setting_direction_on_multiple_order_by_columns(self):
        query = (
            Query()
            .select_all()
            .from_table("events")
            .order_by(
                ("id", SortDirection.DESC),
                ("name", SortDirection.ASC),
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
                (
                    Column(field="state", path=["value_1", 0, "value_2"]),
                    SortDirection.DESC,
                ),
                (
                    Column(field="state", path=["value_3"]),
                    SortDirection.ASC,
                ),
            )
            .build()
        )

        assert parameterised_query_to_string(query) == (
            'SELECT * FROM "events" '
            "ORDER BY "
            "\"state\"#>'{value_1,0,value_2}' DESC, "
            "\"state\"#>'{value_3}' ASC",
            [],
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
                        .left(Column(field="id"))
                        .equals()
                        .right(Value("123"))
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
                        .left(Column(field="id"))
                        .equals()
                        .right(Value("456"))
                    )
                ),
                name="second",
            )
            .select_all()
            .from_table("people")
            .where(
                Condition()
                .left(Column(field="name"))
                .equals()
                .right(Column(table="first", field="name"))
            )
            .where(
                Condition()
                .left(Column(field="age"))
                .equals()
                .right(Column(table="second", field="age"))
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
