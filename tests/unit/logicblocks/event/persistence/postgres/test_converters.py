from collections.abc import Sequence
from typing import Any, cast

import pytest
from psycopg import abc, sql

from logicblocks.event.persistence.postgres import (
    ParameterisedQuery,
    TableSettings,
)
from logicblocks.event.persistence.postgres.converters import (
    DelegatingQueryConverter,
)
from logicblocks.event.query import (
    FilterClause,
    KeySetPagingClause,
    Lookup,
    OffsetPagingClause,
    Operator,
    PagingDirection,
    Path,
    Search,
    Similarity,
    SortClause,
    SortField,
    SortOrder,
)
from logicblocks.event.testing import data
from logicblocks.event.testing.data import random_projection_id


def query_converter_with_default_converters(
    table_settings: TableSettings = TableSettings(table_name="projections"),
):
    return DelegatingQueryConverter(
        table_settings=table_settings
    ).with_default_converters()


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


class TestPostgresQueryConverterQueryConversion:
    @pytest.mark.parametrize("query_type", [Lookup, Search])
    def test_converts_single_filter_query_on_top_level_attribute(
        self, query_type
    ):
        converter = query_converter_with_default_converters()

        query = query_type(
            filters=[
                FilterClause(
                    operator=Operator.EQUAL, field=Path("id"), value="test"
                )
            ]
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" WHERE "id" = %s',
            ["test"],
        )

    @pytest.mark.parametrize("query_type", [Lookup, Search])
    def test_converts_multiple_filter_query_on_top_level_attributes(
        self, query_type
    ):
        converter = query_converter_with_default_converters()
        query = query_type(
            filters=[
                FilterClause(
                    operator=Operator.EQUAL, field=Path("id"), value="test"
                ),
                FilterClause(
                    operator=Operator.EQUAL, field=Path("name"), value="thing"
                ),
            ]
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" WHERE "id" = %s AND "name" = %s',
            ["test", "thing"],
        )

    @pytest.mark.parametrize("query_type", [Lookup, Search])
    def test_converts_single_filter_query_on_nested_attribute(
        self, query_type
    ):
        converter = query_converter_with_default_converters()
        query = query_type(
            filters=[
                FilterClause(
                    operator=Operator.EQUAL,
                    field=Path("state", "value"),
                    value=5,
                )
            ]
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" '
            'WHERE "jsonb_extract_path"("state", %s) = "to_jsonb"(%s)',
            ["value", 5],
        )

    @pytest.mark.parametrize("query_type", [Lookup, Search])
    def test_converts_single_string_filter_query_on_nested_attribute(
        self, query_type
    ):
        converter = query_converter_with_default_converters()
        query = query_type(
            filters=[
                FilterClause(
                    operator=Operator.EQUAL,
                    field=Path("state", "value"),
                    value="test",
                )
            ]
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" '
            'WHERE "jsonb_extract_path"("state", %s) = '
            '"to_jsonb"(CAST(%s AS "text"))',
            ["value", "test"],
        )

    @pytest.mark.parametrize("query_type", [Lookup, Search])
    def test_converts_none_values_to_null_on_top_level_attribute(
        self, query_type
    ):
        converter = query_converter_with_default_converters()

        query = query_type(
            filters=[
                FilterClause(
                    operator=Operator.EQUAL, field=Path("id"), value=None
                )
            ]
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" WHERE "id" IS NULL ',
            [],
        )

    @pytest.mark.parametrize("query_type", [Lookup, Search])
    def test_converts_none_values_to_null_on_nested_attribute(
        self, query_type
    ):
        converter = query_converter_with_default_converters()
        query = query_type(
            filters=[
                FilterClause(
                    operator=Operator.EQUAL,
                    field=Path("state", "value"),
                    value=None,
                )
            ]
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" '
            'WHERE "jsonb_extract_path_text"("state", %s) IS NULL ',
            ["value"],
        )

    @pytest.mark.parametrize("query_type", [Lookup, Search])
    def test_converts_none_values_to_not_null_on_top_level_attribute(
        self, query_type
    ):
        converter = query_converter_with_default_converters()

        query = query_type(
            filters=[
                FilterClause(
                    operator=Operator.NOT_EQUAL, field=Path("id"), value=None
                )
            ]
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" WHERE "id" IS NOT NULL ',
            [],
        )

    @pytest.mark.parametrize("query_type", [Lookup, Search])
    def test_converts_none_values_to_not_null_on_nested_attribute(
        self, query_type
    ):
        converter = query_converter_with_default_converters()
        query = query_type(
            filters=[
                FilterClause(
                    operator=Operator.NOT_EQUAL,
                    field=Path("state", "value"),
                    value=None,
                )
            ]
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" '
            'WHERE "jsonb_extract_path_text"("state", %s) IS NOT NULL ',
            ["value"],
        )

    @pytest.mark.parametrize("query_type", [Lookup, Search])
    def test_converts_list_of_strings_filter_query_on_nested_attribute(
        self, query_type
    ):
        value_1 = data.random_ascii_alphanumerics_string(10)
        value_2 = data.random_ascii_alphanumerics_string(10)
        converter = query_converter_with_default_converters()
        query = query_type(
            filters=[
                FilterClause(
                    operator=Operator.IN,
                    field=Path("state", "value"),
                    value=[value_1, value_2],
                )
            ]
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" '
            'WHERE "jsonb_extract_path"("state", %s) IN ('
            '"to_jsonb"(CAST(%s AS "text")), '
            '"to_jsonb"(CAST(%s AS "text"))'
            ")",
            ["value", value_1, value_2],
        )

    @pytest.mark.parametrize("query_type", [Lookup, Search])
    def test_converts_multiple_filter_query_on_nested_attributes(
        self, query_type
    ):
        converter = query_converter_with_default_converters()
        query = query_type(
            filters=[
                FilterClause(
                    operator=Operator.EQUAL,
                    field=Path("state", "value_1"),
                    value=5,
                ),
                FilterClause(
                    operator=Operator.EQUAL,
                    field=Path("state", "value_2", 0, "value_3"),
                    value=6,
                ),
            ]
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" '
            'WHERE "jsonb_extract_path"("state", %s) = "to_jsonb"(%s) '
            'AND "jsonb_extract_path"("state", %s, %s, %s) = "to_jsonb"(%s)',
            ["value_1", 5, "value_2", 0, "value_3", 6],
        )

    @pytest.mark.parametrize("query_type", [Lookup, Search])
    def test_converts_not_equal_filter_query_on_top_level_attribute(
        self, query_type
    ):
        converter = query_converter_with_default_converters()
        query = query_type(
            filters=[
                FilterClause(
                    operator=Operator.NOT_EQUAL,
                    field=Path("id"),
                    value=5,
                ),
            ]
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" WHERE "id" != %s',
            [5],
        )

    @pytest.mark.parametrize("query_type", [Lookup, Search])
    def test_converts_not_equal_filter_query_on_nested_attribute(
        self, query_type
    ):
        converter = query_converter_with_default_converters()
        query = query_type(
            filters=[
                FilterClause(
                    operator=Operator.NOT_EQUAL,
                    field=Path("state", "value_1"),
                    value=5,
                ),
            ]
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" '
            'WHERE "jsonb_extract_path"("state", %s) != "to_jsonb"(%s)',
            ["value_1", 5],
        )

    @pytest.mark.parametrize("query_type", [Lookup, Search])
    def test_converts_greater_than_filter_query_on_top_level_attribute(
        self, query_type
    ):
        converter = query_converter_with_default_converters()
        query = query_type(
            filters=[
                FilterClause(
                    operator=Operator.GREATER_THAN,
                    field=Path("id"),
                    value=5,
                ),
            ]
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" WHERE "id" > %s',
            [5],
        )

    @pytest.mark.parametrize("query_type", [Lookup, Search])
    def test_converts_greater_than_filter_query_on_nested_attribute(
        self, query_type
    ):
        converter = query_converter_with_default_converters()
        query = query_type(
            filters=[
                FilterClause(
                    operator=Operator.GREATER_THAN,
                    field=Path("state", "value_1"),
                    value=5,
                ),
            ]
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" '
            'WHERE "jsonb_extract_path"("state", %s) > "to_jsonb"(%s)',
            ["value_1", 5],
        )

    @pytest.mark.parametrize("query_type", [Lookup, Search])
    def test_converts_greater_than_or_equal_filter_query_on_top_level_attribute(
        self, query_type
    ):
        converter = query_converter_with_default_converters()
        query = query_type(
            filters=[
                FilterClause(
                    operator=Operator.GREATER_THAN_OR_EQUAL,
                    field=Path("id"),
                    value=5,
                ),
            ]
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" WHERE "id" >= %s',
            [5],
        )

    @pytest.mark.parametrize("query_type", [Lookup, Search])
    def test_converts_greater_than_or_equal_filter_query_on_nested_attribute(
        self, query_type
    ):
        converter = query_converter_with_default_converters()
        query = query_type(
            filters=[
                FilterClause(
                    operator=Operator.GREATER_THAN_OR_EQUAL,
                    field=Path("state", "value_1"),
                    value=5,
                ),
            ]
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" '
            'WHERE "jsonb_extract_path"("state", %s) >= "to_jsonb"(%s)',
            ["value_1", 5],
        )

    @pytest.mark.parametrize("query_type", [Lookup, Search])
    def test_converts_less_than_filter_query_on_top_level_attribute(
        self, query_type
    ):
        converter = query_converter_with_default_converters()
        query = query_type(
            filters=[
                FilterClause(
                    operator=Operator.LESS_THAN,
                    field=Path("id"),
                    value=5,
                ),
            ]
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" WHERE "id" < %s',
            [5],
        )

    @pytest.mark.parametrize("query_type", [Lookup, Search])
    def test_converts_less_than_filter_query_on_nested_attribute(
        self, query_type
    ):
        converter = query_converter_with_default_converters()
        query = query_type(
            filters=[
                FilterClause(
                    operator=Operator.LESS_THAN,
                    field=Path("state", "value_1"),
                    value=5,
                ),
            ]
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" '
            'WHERE "jsonb_extract_path"("state", %s) < "to_jsonb"(%s)',
            ["value_1", 5],
        )

    @pytest.mark.parametrize("query_type", [Lookup, Search])
    def test_converts_less_than_or_equal_filter_query_on_top_level_attribute(
        self, query_type
    ):
        converter = query_converter_with_default_converters()
        query = query_type(
            filters=[
                FilterClause(
                    operator=Operator.LESS_THAN_OR_EQUAL,
                    field=Path("id"),
                    value=5,
                ),
            ]
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" WHERE "id" <= %s',
            [5],
        )

    @pytest.mark.parametrize("query_type", [Lookup, Search])
    def test_converts_less_than_or_equal_filter_query_on_nested_attribute(
        self, query_type
    ):
        converter = query_converter_with_default_converters()
        query = query_type(
            filters=[
                FilterClause(
                    operator=Operator.LESS_THAN_OR_EQUAL,
                    field=Path("state", "value_1"),
                    value=5,
                ),
            ]
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" '
            'WHERE "jsonb_extract_path"("state", %s) <= "to_jsonb"(%s)',
            ["value_1", 5],
        )

    def test_converts_single_sort_query_on_top_level_attribute(self):
        converter = query_converter_with_default_converters()
        query = Search(
            sort=SortClause(
                fields=[SortField(field=Path("id"), order=SortOrder.ASC)]
            )
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" ORDER BY "id" ASC',
            [],
        )

    def test_converts_multiple_field_sort_query_on_top_level_attributes(
        self,
    ):
        converter = query_converter_with_default_converters()
        query = Search(
            sort=SortClause(
                fields=[
                    SortField(field=Path("name"), order=SortOrder.ASC),
                    SortField(field=Path("id"), order=SortOrder.DESC),
                ]
            )
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" ORDER BY "name" ASC, "id" DESC',
            [],
        )

    def test_converts_single_field_sort_query_on_nested_attribute(
        self,
    ):
        converter = query_converter_with_default_converters()
        query = Search(
            sort=SortClause(
                fields=[
                    SortField(
                        field=Path("state", "value_1"), order=SortOrder.ASC
                    )
                ]
            )
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" '
            'ORDER BY "jsonb_extract_path"("state", %s) ASC',
            ["value_1"],
        )

    def test_converts_multiple_field_sort_query_on_nested_attributes(
        self,
    ):
        converter = query_converter_with_default_converters()
        query = Search(
            sort=SortClause(
                fields=[
                    SortField(
                        field=Path("state", "value_1"), order=SortOrder.DESC
                    ),
                    SortField(
                        field=Path("state", "value_2"), order=SortOrder.ASC
                    ),
                ]
            )
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" ORDER BY '
            '"jsonb_extract_path"("state", %s) DESC, '
            '"jsonb_extract_path"("state", %s) ASC',
            ["value_1", "value_2"],
        )

    def test_converts_single_sort_query_where_field_is_function(self):
        converter = query_converter_with_default_converters()
        query = Search(
            sort=SortClause(
                fields=[
                    SortField(
                        field=Similarity(
                            left=Path("state", "value"),
                            right="xyz",
                            alias="value_score",
                        ),
                        order=SortOrder.DESC,
                    )
                ]
            )
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            "SELECT "
            "*, "
            '"similarity"('
            'CAST("jsonb_extract_path"("state", %s) AS "text"), %s'
            ') AS "value_score" '
            'FROM "projections" '
            'ORDER BY "value_score" DESC',
            ["value", "xyz"],
        )

    def test_converts_offset_paging_query_for_first_page(self):
        converter = query_converter_with_default_converters()
        query = Search(paging=OffsetPagingClause(item_count=10))

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" LIMIT %s',
            [10],
        )

    def test_converts_offset_paging_query_for_next_page(
        self,
    ):
        converter = query_converter_with_default_converters()
        query = Search(paging=OffsetPagingClause(page_number=3, item_count=10))

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" LIMIT %s OFFSET %s',
            [10, 20],
        )

    def test_converts_key_set_paging_query_no_sorts_first_page(self):
        converter = query_converter_with_default_converters()
        query = Search(paging=KeySetPagingClause(item_count=10))

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" ORDER BY "id" LIMIT %s',
            [10],
        )

    def test_converts_key_set_paging_query_no_sorts_next_page_forwards(
        self,
    ):
        converter = query_converter_with_default_converters()

        last_id = random_projection_id()

        query = Search(
            paging=KeySetPagingClause(
                last_id=last_id,
                direction=PagingDirection.FORWARDS,
                item_count=10,
            )
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" '
            'WHERE "id" > %s '
            'ORDER BY "id" ASC LIMIT %s',
            [last_id, 10],
        )

    def test_converts_key_set_paging_query_no_sorts_next_page_backwards(
        self,
    ):
        converter = query_converter_with_default_converters()

        last_id = random_projection_id()

        query = Search(
            paging=KeySetPagingClause(
                last_id=last_id,
                direction=PagingDirection.BACKWARDS,
                item_count=10,
            )
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            "SELECT * FROM ("
            'SELECT * FROM "projections" '
            'WHERE "id" < %s '
            'ORDER BY "id" DESC LIMIT %s'
            ') AS "page" '
            'ORDER BY "id" LIMIT %s',
            [last_id, 10, 10],
        )

    def test_converts_key_set_paging_query_other_asc_sorts_first_page(self):
        converter = query_converter_with_default_converters()

        query = Search(
            sort=SortClause(
                fields=[SortField(field=Path("name"), order=SortOrder.ASC)]
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

    def test_converts_key_set_paging_query_other_desc_sorts_first_page(self):
        converter = query_converter_with_default_converters()

        query = Search(
            sort=SortClause(
                fields=[SortField(field=Path("name"), order=SortOrder.DESC)]
            ),
            paging=KeySetPagingClause(item_count=10),
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" '
            'ORDER BY "name" DESC, "id" DESC '
            "LIMIT %s",
            [10],
        )

    def test_converts_key_set_paging_query_other_asc_sorts_next_page_forwards(
        self,
    ):
        converter = query_converter_with_default_converters()

        last_id = random_projection_id()

        query = Search(
            sort=SortClause(
                fields=[SortField(field=Path("name"), order=SortOrder.ASC)]
            ),
            paging=KeySetPagingClause(
                last_id=last_id,
                direction=PagingDirection.FORWARDS,
                item_count=10,
            ),
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'WITH "last" AS '
            '(SELECT "name", "id" FROM "projections" WHERE "id" = %s LIMIT %s) '
            'SELECT * FROM "projections" '
            'WHERE ("name", "id") > (SELECT * FROM "last") '
            'ORDER BY "name" ASC, "id" ASC '
            "LIMIT %s",
            [last_id, 1, 10],
        )

    def test_converts_key_set_paging_query_other_desc_sorts_next_page_forwards(
        self,
    ):
        converter = query_converter_with_default_converters()

        last_id = random_projection_id()

        query = Search(
            sort=SortClause(
                fields=[SortField(field=Path("name"), order=SortOrder.DESC)]
            ),
            paging=KeySetPagingClause(
                last_id=last_id,
                direction=PagingDirection.FORWARDS,
                item_count=10,
            ),
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'WITH "last" AS '
            '(SELECT "name", "id" FROM "projections" WHERE "id" = %s LIMIT %s) '
            'SELECT * FROM "projections" '
            'WHERE ("name", "id") < (SELECT * FROM "last") '
            'ORDER BY "name" DESC, "id" DESC '
            "LIMIT %s",
            [last_id, 1, 10],
        )

    def test_converts_key_set_paging_query_other_asc_sorts_next_page_backwards(
        self,
    ):
        converter = query_converter_with_default_converters()

        last_id = random_projection_id()

        query = Search(
            sort=SortClause(
                fields=[SortField(field=Path("name"), order=SortOrder.ASC)]
            ),
            paging=KeySetPagingClause(
                last_id=last_id,
                direction=PagingDirection.BACKWARDS,
                item_count=10,
            ),
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'WITH "last" AS '
            '(SELECT "name", "id" FROM "projections" WHERE "id" = %s LIMIT %s) '
            "SELECT * FROM "
            '(SELECT * FROM "projections" '
            'WHERE ("name", "id") < (SELECT * FROM "last") '
            'ORDER BY "name" DESC, "id" DESC '
            'LIMIT %s) AS "page" '
            'ORDER BY "name" ASC, "id" ASC '
            "LIMIT %s",
            [last_id, 1, 10, 10],
        )

    def test_converts_key_set_paging_query_other_desc_sorts_next_page_backwards(
        self,
    ):
        converter = query_converter_with_default_converters()

        last_id = random_projection_id()

        query = Search(
            sort=SortClause(
                fields=[SortField(field=Path("name"), order=SortOrder.DESC)]
            ),
            paging=KeySetPagingClause(
                last_id=last_id,
                direction=PagingDirection.BACKWARDS,
                item_count=10,
            ),
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'WITH "last" AS '
            '(SELECT "name", "id" FROM "projections" WHERE "id" = %s LIMIT %s) '
            "SELECT * FROM "
            '(SELECT * FROM "projections" '
            'WHERE ("name", "id") > (SELECT * FROM "last") '
            'ORDER BY "name" ASC, "id" ASC '
            'LIMIT %s) AS "page" '
            'ORDER BY "name" DESC, "id" DESC '
            "LIMIT %s",
            [last_id, 1, 10, 10],
        )

    def test_converts_key_set_paging_query_other_mixed_sorts_first_page(
        self,
    ):
        converter = query_converter_with_default_converters()

        query = Search(
            sort=SortClause(
                fields=[
                    SortField(field=Path("name"), order=SortOrder.DESC),
                    SortField(field=Path("version"), order=SortOrder.ASC),
                ]
            ),
            paging=KeySetPagingClause(item_count=10),
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" '
            'ORDER BY "name" DESC, "version" ASC, "id" ASC '
            "LIMIT %s",
            [10],
        )

    def test_converts_key_set_paging_query_other_mixed_sorts_next_page_forwards(
        self,
    ):
        converter = query_converter_with_default_converters()

        last_id = random_projection_id()

        query = Search(
            sort=SortClause(
                fields=[
                    SortField(field=Path("name"), order=SortOrder.DESC),
                    SortField(field=Path("version"), order=SortOrder.ASC),
                ]
            ),
            paging=KeySetPagingClause(
                last_id=last_id,
                direction=PagingDirection.FORWARDS,
                item_count=10,
            ),
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'WITH "last" AS '
            '(SELECT "name", "version", "id" '
            'FROM "projections" '
            'WHERE "id" = %s LIMIT %s) '
            '(SELECT * FROM "projections" '
            'WHERE "name" < (SELECT "name" FROM "last") '
            'ORDER BY "name" DESC, "version" ASC, "id" ASC '
            "LIMIT %s) "
            "UNION ALL "
            '(SELECT * FROM "projections" '
            'WHERE "name" = (SELECT "name" FROM "last") '
            'AND "version" > (SELECT "version" FROM "last") '
            'ORDER BY "name" DESC, "version" ASC, "id" ASC '
            "LIMIT %s) "
            "UNION ALL "
            '(SELECT * FROM "projections" '
            'WHERE "name" = (SELECT "name" FROM "last") '
            'AND "version" = (SELECT "version" FROM "last") '
            'AND "id" > (SELECT "id" FROM "last") '
            'ORDER BY "name" DESC, "version" ASC, "id" ASC '
            "LIMIT %s) "
            'ORDER BY "name" DESC, "version" ASC, "id" ASC '
            "LIMIT %s",
            [last_id, 1, 10, 10, 10, 10],
        )

    def test_converts_key_set_paging_query_other_mixed_sorts_next_page_backwards(
        self,
    ):
        converter = query_converter_with_default_converters()

        last_id = random_projection_id()

        query = Search(
            sort=SortClause(
                fields=[
                    SortField(field=Path("name"), order=SortOrder.DESC),
                    SortField(field=Path("version"), order=SortOrder.ASC),
                ]
            ),
            paging=KeySetPagingClause(
                last_id=last_id,
                direction=PagingDirection.BACKWARDS,
                item_count=10,
            ),
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'WITH "last" AS '
            '(SELECT "name", "version", "id" '
            'FROM "projections" '
            'WHERE "id" = %s LIMIT %s) '
            "SELECT * FROM "
            '((SELECT * FROM "projections" '
            'WHERE "name" > (SELECT "name" FROM "last") '
            'ORDER BY "name" ASC, "version" DESC, "id" DESC '
            "LIMIT %s) "
            "UNION ALL "
            '(SELECT * FROM "projections" '
            'WHERE "name" = (SELECT "name" FROM "last") '
            'AND "version" < (SELECT "version" FROM "last") '
            'ORDER BY "name" ASC, "version" DESC, "id" DESC '
            "LIMIT %s) "
            "UNION ALL "
            '(SELECT * FROM "projections" '
            'WHERE "name" = (SELECT "name" FROM "last") '
            'AND "version" = (SELECT "version" FROM "last") '
            'AND "id" < (SELECT "id" FROM "last") '
            'ORDER BY "name" ASC, "version" DESC, "id" DESC '
            "LIMIT %s) "
            'ORDER BY "name" ASC, "version" DESC, "id" DESC '
            'LIMIT %s) AS "page" '
            'ORDER BY "name" DESC, "version" ASC, "id" ASC '
            "LIMIT %s",
            [last_id, 1, 10, 10, 10, 10, 10],
        )

    def test_converts_string_array_contains_query_on_nested_attribute(self):
        converter = query_converter_with_default_converters()
        value = data.random_ascii_alphanumerics_string(10)
        query = Search(
            filters=[
                FilterClause(
                    operator=Operator.CONTAINS,
                    field=Path("state", "arr"),
                    value=value,
                )
            ]
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" '
            "WHERE "
            '"jsonb_extract_path"("state", %s) @> '
            '"to_jsonb"(CAST(%s AS "text"))',
            ["arr", value],
        )

    def test_converts_string_regex_query_on_nested_attribute(self):
        converter = query_converter_with_default_converters()
        value = "regex.*"
        query = Search(
            filters=[
                FilterClause(
                    operator=Operator.REGEX_MATCHES,
                    field=Path("state", "field"),
                    value=value,
                )
            ]
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            "SELECT * FROM "
            '"projections" WHERE '
            '"jsonb_extract_path_text"("state", %s) ~ '
            "%s",
            ["field", "regex.*"],
        )

    @pytest.mark.parametrize("query_type", [Lookup, Search])
    def test_converts_regex_matches_filter_query_on_nested_attribute(
        self, query_type
    ):
        converter = query_converter_with_default_converters()
        query = query_type(
            filters=[
                FilterClause(
                    operator=Operator.REGEX_MATCHES,
                    field=Path("state", "description"),
                    value="world$",
                )
            ]
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" '
            'WHERE "jsonb_extract_path_text"("state", %s) ~ %s',
            ["description", "world$"],
        )

    @pytest.mark.parametrize("query_type", [Lookup, Search])
    def test_converts_not_regex_matches_filter_query_on_nested_attribute(
        self, query_type
    ):
        converter = query_converter_with_default_converters()
        query = query_type(
            filters=[
                FilterClause(
                    operator=Operator.NOT_REGEX_MATCHES,
                    field=Path("state", "description"),
                    value="world$",
                )
            ]
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            'SELECT * FROM "projections" '
            'WHERE "jsonb_extract_path_text"("state", %s) !~ %s',
            ["description", "world$"],
        )

    def test_converts_string_not_regex_query_on_nested_attribute(self):
        converter = query_converter_with_default_converters()
        value = "regex.*"
        query = Search(
            filters=[
                FilterClause(
                    operator=Operator.NOT_REGEX_MATCHES,
                    field=Path("state", "field"),
                    value=value,
                )
            ]
        )

        converted = converter.convert_query(query)

        assert parameterised_query_to_string(converted) == (
            "SELECT * FROM "
            '"projections" WHERE '
            '"jsonb_extract_path_text"("state", %s) !~ '
            "%s",
            ["field", "regex.*"],
        )
