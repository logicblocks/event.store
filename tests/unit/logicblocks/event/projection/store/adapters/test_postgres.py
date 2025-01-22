from collections.abc import Sequence
from typing import Any, cast

import pytest
from psycopg import abc, sql

from logicblocks.event.projection.store import (
    FilterClause,
    KeySetPagingClause,
    Lookup,
    OffsetPagingClause,
    Operator,
    Path,
    Search,
    SortClause,
    SortField,
    SortOrder,
)
from logicblocks.event.projection.store.adapters import (
    PostgresParameterisedQuery,
    PostgresQueryConverter,
    PostgresTableSettings,
)
from logicblocks.event.testing.data import random_projection_id


def query_converter_with_default_clause_converters(
    table_settings: PostgresTableSettings = PostgresTableSettings(),
):
    return PostgresQueryConverter(
        table_settings=table_settings
    ).with_default_clause_applicators()


def sql_query_to_string(query: abc.Query) -> str:
    if isinstance(query, bytes):
        return query.decode()
    if isinstance(query, sql.SQL) or isinstance(query, sql.Composed):
        return query.as_string()
    return cast(str, query)


def parameterised_query_to_string(
    parameterised_query: PostgresParameterisedQuery,
) -> tuple[str, Sequence[Any]]:
    query, params = parameterised_query
    return sql_query_to_string(query), params


class TestPostgresQueryConverterQueryConversion:
    @pytest.mark.parametrize("query_type", [Lookup, Search])
    def test_converts_query_with_single_filter_on_top_level_attribute(
        self, query_type
    ):
        converter = query_converter_with_default_clause_converters()

        query = query_type(
            filters=[
                FilterClause(
                    operator=Operator.EQUAL, path=Path("id"), value="test"
                )
            ]
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" WHERE "id" = %s',
            ["test"],
        )

    @pytest.mark.parametrize("query_type", [Lookup, Search])
    def test_converts_query_with_multiple_filters_on_top_level_attributes(
        self, query_type
    ):
        converter = query_converter_with_default_clause_converters()
        query = query_type(
            filters=[
                FilterClause(
                    operator=Operator.EQUAL, path=Path("id"), value="test"
                ),
                FilterClause(
                    operator=Operator.EQUAL, path=Path("name"), value="thing"
                ),
            ]
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" WHERE "id" = %s AND "name" = %s',
            ["test", "thing"],
        )

    @pytest.mark.parametrize("query_type", [Lookup, Search])
    def test_converts_query_with_single_filter_on_nested_attribute(
        self, query_type
    ):
        converter = query_converter_with_default_clause_converters()
        query = query_type(
            filters=[
                FilterClause(
                    operator=Operator.EQUAL,
                    path=Path("state", "value"),
                    value=5,
                )
            ]
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" '
            "WHERE \"state\"#>'{value}' = to_jsonb(%s)",
            [5],
        )

    @pytest.mark.parametrize("query_type", [Lookup, Search])
    def test_converts_query_with_multiple_filters_on_nested_attributes(
        self, query_type
    ):
        converter = query_converter_with_default_clause_converters()
        query = query_type(
            filters=[
                FilterClause(
                    operator=Operator.EQUAL,
                    path=Path("state", "value_1"),
                    value=5,
                ),
                FilterClause(
                    operator=Operator.EQUAL,
                    path=Path("state", "value_2", 0, "value_3"),
                    value=6,
                ),
            ]
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" '
            "WHERE \"state\"#>'{value_1}' = to_jsonb(%s) "
            "AND \"state\"#>'{value_2,0,value_3}' = to_jsonb(%s)",
            [5, 6],
        )

    @pytest.mark.parametrize("query_type", [Lookup, Search])
    def test_converts_query_with_top_level_not_equal_filter(self, query_type):
        converter = query_converter_with_default_clause_converters()
        query = query_type(
            filters=[
                FilterClause(
                    operator=Operator.NOT_EQUAL,
                    path=Path("id"),
                    value=5,
                ),
            ]
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" ' 'WHERE "id" != %s',
            [5],
        )

    @pytest.mark.parametrize("query_type", [Lookup, Search])
    def test_converts_query_with_nested_not_equal_filter(self, query_type):
        converter = query_converter_with_default_clause_converters()
        query = query_type(
            filters=[
                FilterClause(
                    operator=Operator.NOT_EQUAL,
                    path=Path("state", "value_1"),
                    value=5,
                ),
            ]
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" '
            "WHERE \"state\"#>'{value_1}' != to_jsonb(%s)",
            [5],
        )

    @pytest.mark.parametrize("query_type", [Lookup, Search])
    def test_converts_query_with_top_level_greater_than_filter(
        self, query_type
    ):
        converter = query_converter_with_default_clause_converters()
        query = query_type(
            filters=[
                FilterClause(
                    operator=Operator.GREATER_THAN,
                    path=Path("id"),
                    value=5,
                ),
            ]
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" ' 'WHERE "id" > %s',
            [5],
        )

    @pytest.mark.parametrize("query_type", [Lookup, Search])
    def test_converts_query_with_nested_greater_than_filter(self, query_type):
        converter = query_converter_with_default_clause_converters()
        query = query_type(
            filters=[
                FilterClause(
                    operator=Operator.GREATER_THAN,
                    path=Path("state", "value_1"),
                    value=5,
                ),
            ]
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" '
            "WHERE \"state\"#>'{value_1}' > to_jsonb(%s)",
            [5],
        )

    @pytest.mark.parametrize("query_type", [Lookup, Search])
    def test_converts_query_with_top_level_greater_than_or_equal_filter(
        self, query_type
    ):
        converter = query_converter_with_default_clause_converters()
        query = query_type(
            filters=[
                FilterClause(
                    operator=Operator.GREATER_THAN_OR_EQUAL,
                    path=Path("id"),
                    value=5,
                ),
            ]
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" ' 'WHERE "id" >= %s',
            [5],
        )

    @pytest.mark.parametrize("query_type", [Lookup, Search])
    def test_converts_query_with_nested_greater_than_or_equal_filter(
        self, query_type
    ):
        converter = query_converter_with_default_clause_converters()
        query = query_type(
            filters=[
                FilterClause(
                    operator=Operator.GREATER_THAN_OR_EQUAL,
                    path=Path("state", "value_1"),
                    value=5,
                ),
            ]
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" '
            "WHERE \"state\"#>'{value_1}' >= to_jsonb(%s)",
            [5],
        )

    @pytest.mark.parametrize("query_type", [Lookup, Search])
    def test_converts_query_with_top_level_less_than_filter(self, query_type):
        converter = query_converter_with_default_clause_converters()
        query = query_type(
            filters=[
                FilterClause(
                    operator=Operator.LESS_THAN,
                    path=Path("id"),
                    value=5,
                ),
            ]
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" ' 'WHERE "id" < %s',
            [5],
        )

    @pytest.mark.parametrize("query_type", [Lookup, Search])
    def test_converts_query_with_nested_less_than_filter(self, query_type):
        converter = query_converter_with_default_clause_converters()
        query = query_type(
            filters=[
                FilterClause(
                    operator=Operator.LESS_THAN,
                    path=Path("state", "value_1"),
                    value=5,
                ),
            ]
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" '
            "WHERE \"state\"#>'{value_1}' < to_jsonb(%s)",
            [5],
        )

    @pytest.mark.parametrize("query_type", [Lookup, Search])
    def test_converts_query_with_top_level_less_than_or_equal_filter(
        self, query_type
    ):
        converter = query_converter_with_default_clause_converters()
        query = query_type(
            filters=[
                FilterClause(
                    operator=Operator.LESS_THAN_OR_EQUAL,
                    path=Path("id"),
                    value=5,
                ),
            ]
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" ' 'WHERE "id" <= %s',
            [5],
        )

    @pytest.mark.parametrize("query_type", [Lookup, Search])
    def test_converts_query_with_nested_less_than_or_equal_filter(
        self, query_type
    ):
        converter = query_converter_with_default_clause_converters()
        query = query_type(
            filters=[
                FilterClause(
                    operator=Operator.LESS_THAN_OR_EQUAL,
                    path=Path("state", "value_1"),
                    value=5,
                ),
            ]
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" '
            "WHERE \"state\"#>'{value_1}' <= to_jsonb(%s)",
            [5],
        )

    def test_converts_query_with_single_sort_on_top_level_attribute(self):
        converter = query_converter_with_default_clause_converters()
        query = Search(
            sort=SortClause(
                fields=[SortField(path=Path("id"), order=SortOrder.ASC)]
            )
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" ORDER BY "id" ASC',
            [],
        )

    def test_converts_query_with_multiple_field_sort_on_top_level_attributes(
        self,
    ):
        converter = query_converter_with_default_clause_converters()
        query = Search(
            sort=SortClause(
                fields=[
                    SortField(path=Path("name"), order=SortOrder.ASC),
                    SortField(path=Path("id"), order=SortOrder.DESC),
                ]
            )
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" ORDER BY "name" ASC, "id" DESC',
            [],
        )

    def test_converts_query_with_single_field_sort_on_nested_state_attribute(
        self,
    ):
        converter = query_converter_with_default_clause_converters()
        query = Search(
            sort=SortClause(
                fields=[
                    SortField(
                        path=Path("state", "value_1"), order=SortOrder.ASC
                    )
                ]
            )
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" '
            "ORDER BY \"state\"#>'{value_1}' ASC",
            [],
        )

    def test_converts_query_with_multiple_field_sort_on_nested_state_attributes(
        self,
    ):
        converter = query_converter_with_default_clause_converters()
        query = Search(
            sort=SortClause(
                fields=[
                    SortField(
                        path=Path("state", "value_1"), order=SortOrder.DESC
                    ),
                    SortField(
                        path=Path("state", "value_2"), order=SortOrder.ASC
                    ),
                ]
            )
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" ORDER BY '
            "\"state\"#>'{value_1}' DESC, "
            "\"state\"#>'{value_2}' ASC",
            [],
        )

    def test_converts_query_with_offset_paging_clause_for_first_page(self):
        converter = query_converter_with_default_clause_converters()
        query = Search(paging=OffsetPagingClause(item_count=10))

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" LIMIT %s',
            [10],
        )

    def test_converts_query_with_offset_paging_clause_for_subsequent_page(
        self,
    ):
        converter = query_converter_with_default_clause_converters()
        query = Search(paging=OffsetPagingClause(page_number=3, item_count=10))

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" LIMIT %s OFFSET %s',
            [10, 20],
        )

    def test_converts_query_with_key_set_paging_clause_for_first_page(self):
        converter = query_converter_with_default_clause_converters()
        query = Search(paging=KeySetPagingClause(item_count=10))

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" ORDER BY "id" ASC LIMIT %s',
            [10],
        )

    def test_converts_query_with_key_set_paging_clause_subsequent_page_paging_forwards(
        self,
    ):
        converter = query_converter_with_default_clause_converters()

        after_projection_id = random_projection_id()

        query = Search(
            paging=KeySetPagingClause(
                after_id=after_projection_id, item_count=10
            )
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" '
            'WHERE "id" > %s '
            'ORDER BY "id" ASC LIMIT %s',
            [after_projection_id, 10],
        )

    def test_converts_query_with_key_set_paging_clause_subsequent_page_paging_backwards(
        self,
    ):
        converter = query_converter_with_default_clause_converters()

        before_projection_id = random_projection_id()

        query = Search(
            paging=KeySetPagingClause(
                before_id=before_projection_id, item_count=10
            )
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            "SELECT * FROM ("
            'SELECT * FROM "projections" '
            'WHERE "id" < %s '
            'ORDER BY "id" DESC LIMIT %s'
            ') AS "page" '
            'ORDER BY "id" ASC LIMIT %s',
            [before_projection_id, 10, 10],
        )

    def test_converts_query_with_key_set_paging_clause_with_both_before_and_after_ids(
        self,
    ):
        converter = query_converter_with_default_clause_converters()

        after_projection_id = random_projection_id()
        before_projection_id = random_projection_id()

        query = Search(
            paging=KeySetPagingClause(
                after_id=after_projection_id,
                before_id=before_projection_id,
                item_count=10,
            )
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" '
            'WHERE "id" > %s '
            'AND "id" < %s '
            'ORDER BY "id" ASC LIMIT %s',
            [after_projection_id, before_projection_id, 10],
        )

    def test_converts_key_set_paging_query_other_asc_sorts_first_page(self):
        converter = query_converter_with_default_clause_converters()

        query = Search(
            sort=SortClause(
                fields=[SortField(path=Path("name"), order=SortOrder.ASC)]
            ),
            paging=KeySetPagingClause(item_count=10),
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" '
            'ORDER BY "name" ASC, "id" ASC '
            "LIMIT %s",
            [10],
        )

    def test_converts_key_set_paging_query_other_asc_sorts_next_page_forwards(
        self,
    ):
        converter = query_converter_with_default_clause_converters()

        after_projection_id = random_projection_id()

        query = Search(
            sort=SortClause(
                fields=[SortField(path=Path("name"), order=SortOrder.ASC)]
            ),
            paging=KeySetPagingClause(
                after_id=after_projection_id, item_count=10
            ),
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'WITH "after" AS '
            '(SELECT "name", "id" FROM "projections" WHERE "id" = %s LIMIT %s) '
            'SELECT * FROM "projections" '
            'WHERE ("name", "id") > (SELECT * FROM "after") '
            'ORDER BY "name" ASC, "id" ASC '
            "LIMIT %s",
            [after_projection_id, 1, 10],
        )

    def test_converts_key_set_paging_query_other_desc_sorts_next_page_forwards(
        self,
    ):
        pytest.skip("Not implemented")

        converter = query_converter_with_default_clause_converters()

        after_projection_id = random_projection_id()

        query = Search(
            sort=SortClause(
                fields=[SortField(path=Path("name"), order=SortOrder.DESC)]
            ),
            paging=KeySetPagingClause(
                after_id=after_projection_id, item_count=10
            ),
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'WITH "after" AS '
            '(SELECT "name", "id" FROM "projections" WHERE "id" = %s LIMIT %s) '
            'SELECT * FROM "projections" '
            'WHERE ("name", "id") < (SELECT * FROM "after") '
            'ORDER BY "name" DESC, "id" DESC '
            "LIMIT %s",
            [after_projection_id, 1, 10],
        )
