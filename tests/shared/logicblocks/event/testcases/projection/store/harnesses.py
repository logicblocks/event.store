from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Sequence

from logicblocks.event.testsupport import clear_table
from psycopg import AsyncConnection, sql
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from logicblocks.event.projection import (
    InMemoryProjectionStorageAdapter,
    PostgresProjectionStorageAdapter,
    ProjectionStorageAdapter,
)
from logicblocks.event.query import Search
from logicblocks.event.types import JsonValue, Projection, identifier


class ProjectionStoreAdapterHarness(ABC):
    @abstractmethod
    def construct_storage_adapter(self) -> ProjectionStorageAdapter:
        raise NotImplementedError()

    @abstractmethod
    async def clear_storage(self) -> None:
        raise NotImplementedError()

    @abstractmethod
    async def retrieve_projections(
        self, *, adapter: ProjectionStorageAdapter
    ) -> Sequence[Projection[JsonValue, JsonValue]]:
        raise NotImplementedError()


@dataclass(frozen=True)
class PostgresProjectionStoreAdapterHarness(ProjectionStoreAdapterHarness):
    pool: AsyncConnectionPool[AsyncConnection]

    def construct_storage_adapter(self) -> ProjectionStorageAdapter:
        return PostgresProjectionStorageAdapter(connection_source=self.pool)

    async def clear_storage(self) -> None:
        await clear_table(self.pool, "projections")

    async def retrieve_projections(
        self, *, adapter: ProjectionStorageAdapter
    ) -> Sequence[Projection[JsonValue, JsonValue]]:
        async with self.pool.connection() as connection:
            async with connection.cursor(row_factory=dict_row) as cursor:
                query = sql.SQL("SELECT * FROM {0}").format(
                    sql.Identifier("projections")
                )
                results = await cursor.execute(query)
                return [
                    Projection(
                        id=projection["id"],
                        name=projection["name"],
                        source=identifier.event_sequence_identifier(
                            projection["source"]
                        ),
                        state=projection["state"],
                        metadata=projection["metadata"],
                    )
                    for projection in await results.fetchall()
                ]


@dataclass(frozen=True)
class InMemoryProjectionStorageAdapterHarness(ProjectionStoreAdapterHarness):
    def construct_storage_adapter(self) -> ProjectionStorageAdapter:
        return InMemoryProjectionStorageAdapter()

    async def clear_storage(self) -> None:
        pass

    async def retrieve_projections(
        self,
        *,
        adapter: ProjectionStorageAdapter,
    ) -> Sequence[Projection[JsonValue]]:
        return await adapter.find_many(search=Search())
