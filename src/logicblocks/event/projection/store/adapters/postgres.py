from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from psycopg import AsyncConnection, AsyncCursor, abc, sql
from psycopg.rows import TupleRow
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from logicblocks.event.types import Projection
from logicblocks.event.utils.postgres import (
    PostgresConnectionSettings,
    PostgresConnectionSource,
)

from ..query import (
    Lookup,
    Query,
    Search,
)
from .base import ProjectionStorageAdapter


@dataclass(frozen=True)
class TableSettings:
    projections_table_name: str

    def __init__(self, *, projections_table_name: str = "projections"):
        object.__setattr__(
            self, "projections_table_name", projections_table_name
        )


type ParameterisedQuery = tuple[abc.Query, Sequence[Any]]
type ParameterisedQueryFragment = tuple[sql.SQL, Sequence[Any]]


def insert_query(
    projection: Projection[Mapping[str, Any]],
    table_settings: TableSettings,
) -> ParameterisedQuery:
    return (
        sql.SQL(
            """
            INSERT INTO {0} (
              id, 
              name, 
              state, 
              source,
              version
            )
              VALUES (%s, %s, %s, %s, %s)
              ON CONFLICT (id) 
              DO UPDATE
            SET (state, version) = (%s, %s);
            """
        ).format(sql.Identifier(table_settings.projections_table_name)),
        [
            projection.id,
            projection.name,
            Jsonb(projection.state),
            Jsonb(projection.source.dict()),
            projection.version,
            Jsonb(projection.state),
            projection.version,
        ],
    )


async def upsert(
    cursor: AsyncCursor[TupleRow],
    *,
    projection: Projection[Mapping[str, Any]],
    table_settings: TableSettings,
):
    await cursor.execute(*insert_query(projection, table_settings))


def lift_projection[S, T](
    projection: Projection[S],
    converter: Callable[[S], T],
) -> Projection[T]:
    return Projection[T](
        id=projection.id,
        name=projection.name,
        state=converter(projection.state),
        version=projection.version,
        source=projection.source,
    )


class PostgresProjectionStorageAdapter[OQ: Query = Lookup, MQ: Query = Search](
    ProjectionStorageAdapter[OQ, MQ]
):
    def __init__(
        self,
        *,
        connection_source: PostgresConnectionSource,
        table_settings: TableSettings = TableSettings(),
    ):
        if isinstance(connection_source, PostgresConnectionSettings):
            self._connection_pool_owner = True
            self.connection_pool = AsyncConnectionPool[AsyncConnection](
                connection_source.to_connection_string(), open=False
            )
        else:
            self._connection_pool_owner = False
            self.connection_pool = connection_source

        self.table_settings = table_settings

    async def open(self) -> None:
        if self._connection_pool_owner:
            await self.connection_pool.open()

    async def close(self) -> None:
        if self._connection_pool_owner:
            await self.connection_pool.close()

    async def save[T](
        self,
        *,
        projection: Projection[T],
        converter: Callable[[T], Mapping[str, Any]],
    ) -> None:
        async with self.connection_pool.connection() as connection:
            async with connection.cursor() as cursor:
                await upsert(
                    cursor,
                    projection=lift_projection(projection, converter),
                    table_settings=self.table_settings,
                )

    async def find_one[T](
        self, *, lookup: OQ, converter: Callable[[Mapping[str, Any]], T]
    ) -> Projection[T] | None:
        raise NotImplementedError()

    async def find_many[T](
        self, *, search: MQ, converter: Callable[[Mapping[str, Any]], T]
    ) -> Sequence[Projection[T]]:
        raise NotImplementedError()
