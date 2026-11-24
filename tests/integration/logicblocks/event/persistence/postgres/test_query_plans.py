import os
import re
from collections.abc import Iterator, Mapping
from itertools import count
from typing import Any

import pytest_asyncio
from logicblocks.event.testsupport import (
    connection_pool,
    create_table,
    drop_table,
)
from psycopg import AsyncConnection, abc, sql
from psycopg_pool import AsyncConnectionPool

from logicblocks.event.persistence.postgres import (
    ConnectionSettings,
    QueryConverter,
    TableSettings,
)
from logicblocks.event.query import FilterClause, Operator, Path, Search

connection_settings = ConnectionSettings(
    user="admin",
    password="super-secret",
    host=os.getenv("DB_HOST", "localhost"),
    port=int(os.getenv("DB_PORT", "5432")),
    dbname="some-database",
)

table_name = "query_plan_projections"
index_name = "query_plan_projections_name_kind_index"


def with_numbered_placeholders(query: abc.Query) -> str:
    rendered = (
        query.as_string() if isinstance(query, sql.Composable) else query
    )
    assert isinstance(rendered, str)
    positions = count(1)
    return re.sub(
        r"%(%|s)",
        lambda match: "%" if match[1] == "%" else f"${next(positions)}",
        rendered,
    )


def plan_nodes(node: Mapping[str, Any]) -> Iterator[Mapping[str, Any]]:
    yield node
    for child in node.get("Plans", []):
        yield from plan_nodes(child)


def index_conditions(plan: Mapping[str, Any], index: str) -> list[str]:
    return [
        node["Index Cond"]
        for node in plan_nodes(plan)
        if node.get("Index Name") == index and "Index Cond" in node
    ]


async def generic_plan(
    pool: AsyncConnectionPool[AsyncConnection], query: str
) -> Mapping[str, Any]:
    async with pool.connection() as connection:
        await connection.execute("SET enable_seqscan = off")
        cursor = await connection.execute(
            f"EXPLAIN (GENERIC_PLAN, FORMAT JSON) {query}".encode()
        )
        result = await cursor.fetchone()
        assert result is not None
        return result[0][0]["Plan"]


@pytest_asyncio.fixture
async def open_connection_pool():
    async with connection_pool(connection_settings) as pool:
        yield pool


@pytest_asyncio.fixture(autouse=True)
async def seeded_table(open_connection_pool):
    await drop_table(open_connection_pool, table_name)
    await create_table(
        open_connection_pool, "projections", {"projections": table_name}
    )
    async with open_connection_pool.connection() as connection:
        await connection.execute(
            sql.SQL(
                """
                INSERT INTO {table} (id, name, source, state, metadata)
                SELECT
                    i::text,
                    'name-' || (i % 5),
                    '{{}}'::jsonb,
                    jsonb_build_object(
                        'kind',
                        CASE WHEN i % 100 = 0 THEN 'rare' ELSE 'common' END
                    ),
                    '{{}}'::jsonb
                FROM generate_series(1, 5000) AS i
                """
            ).format(table=sql.Identifier(table_name))
        )
        await connection.execute(
            sql.SQL(
                "CREATE INDEX {index} ON {table} "
                "(name, jsonb_extract_path(state, 'kind'))"
            ).format(
                index=sql.Identifier(index_name),
                table=sql.Identifier(table_name),
            )
        )
        await connection.execute(
            sql.SQL("ANALYZE {table}").format(table=sql.Identifier(table_name))
        )
    yield
    await drop_table(open_connection_pool, table_name)


class TestGenericPlansForNestedFilters:
    async def test_uses_expression_index_for_nested_filter(
        self, open_connection_pool
    ):
        converter = QueryConverter(
            table_settings=TableSettings(table_name=table_name)
        ).with_default_converters()
        query, _ = converter.convert_query(
            Search(
                filters=[
                    FilterClause(Operator.EQUAL, Path("name"), "name-1"),
                    FilterClause(
                        Operator.EQUAL, Path("state", "kind"), "rare"
                    ),
                ]
            )
        )

        plan = await generic_plan(
            open_connection_pool, with_numbered_placeholders(query)
        )

        assert any(
            "jsonb_extract_path" in condition
            for condition in index_conditions(plan, index_name)
        )

    async def test_does_not_use_expression_index_when_path_keys_are_bound(
        self, open_connection_pool
    ):
        query = (
            f'SELECT * FROM "{table_name}" '
            'WHERE "name" = $1 '
            'AND "jsonb_extract_path"("state", $2) = '
            '"to_jsonb"(CAST($3 AS "text"))'
        )

        plan = await generic_plan(open_connection_pool, query)

        assert not any(
            "jsonb_extract_path" in condition
            for condition in index_conditions(plan, index_name)
        )
