import os
from collections.abc import Mapping
from contextlib import asynccontextmanager
from typing import cast

from psycopg import AsyncConnection, abc, sql
from psycopg_pool import AsyncConnectionPool

from logicblocks.event.persistence.postgres import ConnectionSettings

project_root = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "..")
)


def relative_to_root(*path_parts: str) -> str:
    return os.path.join(project_root, *path_parts)


def create_table_query(
    table: str, renames: Mapping[str, str] = cast(Mapping[str, str], {})
) -> abc.Query:
    with open(relative_to_root("sql", f"create_{table}_table.sql")) as f:
        create_table_sql = f.read()
        for old, new in renames.items():
            create_table_sql = create_table_sql.replace(old, new)

    return create_table_sql.encode()


def create_indices_query(
    table: str, renames: Mapping[str, str] = cast(Mapping[str, str], {})
) -> abc.Query:
    with open(relative_to_root("sql", f"create_{table}_indices.sql")) as f:
        create_indices_sql = f.read()
        for old, new in renames.items():
            create_indices_sql = create_indices_sql.replace(old, new)

    return create_indices_sql.encode()


def enable_extension_query(extension: str) -> abc.Query:
    return sql.SQL("CREATE EXTENSION IF NOT EXISTS {0}").format(
        sql.Identifier(extension)
    )


def drop_table_query(table_name: str) -> abc.Query:
    return sql.SQL("DROP TABLE IF EXISTS {0}").format(
        sql.Identifier(table_name)
    )


def truncate_table_query(table_name: str) -> abc.Query:
    return sql.SQL("TRUNCATE {0}").format(sql.Identifier(table_name))


async def enable_extension(
    pool: AsyncConnectionPool[AsyncConnection], extension: str
) -> None:
    async with pool.connection() as connection:
        await connection.execute(enable_extension_query(extension))


async def create_table(
    pool: AsyncConnectionPool[AsyncConnection],
    table: str,
    renames: Mapping[str, str] = cast(Mapping[str, str], {}),
) -> None:
    async with pool.connection() as connection:
        await connection.execute(create_table_query(table, renames))
        await connection.execute(create_indices_query(table, renames))


async def clear_table(
    pool: AsyncConnectionPool[AsyncConnection], table: str
) -> None:
    async with pool.connection() as connection:
        await connection.execute(truncate_table_query(table))


async def drop_table(
    pool: AsyncConnectionPool[AsyncConnection], table: str
) -> None:
    async with pool.connection() as connection:
        await connection.execute(drop_table_query(table))


@asynccontextmanager
async def connection_pool(connection_settings: ConnectionSettings):
    conninfo = connection_settings.to_connection_string()
    pool = AsyncConnectionPool[AsyncConnection](conninfo, open=False)

    await pool.open()

    try:
        yield pool
    finally:
        await pool.close()
