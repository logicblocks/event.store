from collections.abc import Sequence

from psycopg import AsyncConnection, AsyncCursor, sql
from psycopg.rows import TupleRow, dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

import logicblocks.event.persistence.postgres as postgres
from logicblocks.event.query import (
    Lookup,
    Query,
    Search,
)
from logicblocks.event.types import (
    JsonPersistable,
    JsonValue,
    JsonValueType,
    Projection,
    deserialise_projection,
    identifier,
    serialise_projection,
)

from .base import ProjectionStorageAdapter


def insert_query(
    projection: Projection[JsonValue, JsonValue],
    table_settings: postgres.TableSettings,
) -> postgres.ParameterisedQuery:
    return (
        sql.SQL(
            """
            INSERT INTO {0} (id,
                             name,
                             source,
                             state,
                             metadata)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (name, id)
                DO
            UPDATE
                SET (state, metadata) = (%s, %s);
            """
        ).format(sql.Identifier(table_settings.table_name)),
        [
            projection.id,
            projection.name,
            Jsonb(projection.source.serialise()),
            Jsonb(projection.state),
            Jsonb(projection.metadata),
            Jsonb(projection.state),
            Jsonb(projection.metadata),
        ],
    )


async def upsert(
    cursor: AsyncCursor[TupleRow],
    *,
    projection: Projection[JsonValue, JsonValue],
    table_settings: postgres.TableSettings,
):
    await cursor.execute(*insert_query(projection, table_settings))


class PostgresProjectionStorageAdapter[
    ItemQuery: Query = Lookup,
    CollectionQuery: Query = Search,
](ProjectionStorageAdapter[ItemQuery, CollectionQuery]):
    def __init__(
        self,
        *,
        connection_source: postgres.ConnectionSource,
        table_settings: postgres.TableSettings = postgres.TableSettings(
            table_name="projections"
        ),
        query_converter: postgres.QueryConverter | None = None,
    ):
        if isinstance(connection_source, postgres.ConnectionSettings):
            self._connection_pool_owner = True
            self.connection_pool = AsyncConnectionPool[AsyncConnection](
                connection_source.to_connection_string(), open=False
            )
        else:
            self._connection_pool_owner = False
            self.connection_pool = connection_source

        self.table_settings = table_settings
        self.query_converter = (
            query_converter
            if query_converter is not None
            else (
                postgres.QueryConverter(
                    table_settings=table_settings
                ).with_default_converters()
            )
        )

    async def open(self) -> None:
        if self._connection_pool_owner:
            await self.connection_pool.open()

    async def close(self) -> None:
        if self._connection_pool_owner:
            await self.connection_pool.close()

    async def save(
        self,
        *,
        projection: Projection[JsonPersistable, JsonPersistable],
    ) -> None:
        async with self.connection_pool.connection() as connection:
            async with connection.cursor() as cursor:
                await upsert(
                    cursor,
                    projection=serialise_projection(projection),
                    table_settings=self.table_settings,
                )

    async def find_one[
        State: JsonPersistable = JsonValue,
        Metadata: JsonPersistable = JsonValue,
    ](
        self,
        *,
        lookup: ItemQuery,
        state_type: type[State] = JsonValueType,
        metadata_type: type[Metadata] = JsonValueType,
    ) -> Projection[State, Metadata] | None:
        query = self.query_converter.convert_query(lookup)
        async with self.connection_pool.connection() as connection:
            async with connection.cursor(row_factory=dict_row) as cursor:
                results = await cursor.execute(*query)
                if results.rowcount > 1:
                    raise ValueError(
                        f"Expected single projection for query: {lookup} "
                        f"but found {results.rowcount} projections: "
                        f"{await results.fetchmany()}."
                    )

                projection_dict = await results.fetchone()
                if projection_dict is None:
                    return None

                projection = Projection[JsonValue, JsonValue](
                    id=projection_dict["id"],
                    name=projection_dict["name"],
                    source=identifier.event_sequence_identifier(
                        projection_dict["source"]
                    ),
                    state=projection_dict["state"],
                    metadata=projection_dict["metadata"],
                )

                return deserialise_projection(
                    projection, state_type, metadata_type
                )

    async def find_many[
        State: JsonPersistable = JsonValue,
        Metadata: JsonPersistable = JsonValue,
    ](
        self,
        *,
        search: CollectionQuery,
        state_type: type[State] = JsonValueType,
        metadata_type: type[Metadata] = JsonValueType,
    ) -> Sequence[Projection[State, Metadata]]:
        query = self.query_converter.convert_query(search)
        async with self.connection_pool.connection() as connection:
            async with connection.cursor(row_factory=dict_row) as cursor:
                results = await cursor.execute(*query)

                projection_dicts = await results.fetchall()

                projections = [
                    Projection[JsonValue, JsonValue](
                        id=projection_dict["id"],
                        name=projection_dict["name"],
                        source=identifier.event_sequence_identifier(
                            projection_dict["source"]
                        ),
                        state=projection_dict["state"],
                        metadata=projection_dict["metadata"],
                    )
                    for projection_dict in projection_dicts
                ]

                return [
                    deserialise_projection(
                        projection, state_type, metadata_type
                    )
                    for projection in projections
                ]
