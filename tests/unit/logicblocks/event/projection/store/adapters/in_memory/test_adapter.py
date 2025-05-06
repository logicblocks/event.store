from collections.abc import Sequence

from logicblocks.event.projection import InMemoryProjectionStorageAdapter
from logicblocks.event.projection.store import (
    ProjectionStorageAdapter,
    Search,
)
from logicblocks.event.testcases.projection.store.adapters import (
    ProjectionStorageAdapterCases,
)
from logicblocks.event.types import JsonValue, Projection


class TestInMemoryProjectionStorageAdapter(ProjectionStorageAdapterCases):
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
