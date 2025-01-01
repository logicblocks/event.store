import sys
from collections.abc import Sequence

import pytest

from logicblocks.event.adaptertests import cases
from logicblocks.event.adaptertests.cases import ConcurrencyParameters
from logicblocks.event.store.adapters import (
    InMemoryStorageAdapter,
    StorageAdapter,
)
from logicblocks.event.types import StoredEvent, identifier


class TestInMemoryStorageAdapterCommonCases(cases.StorageAdapterCases):
    @property
    def concurrency_parameters(self):
        return ConcurrencyParameters(concurrent_writes=40, repeats=200)

    def construct_storage_adapter(self) -> StorageAdapter:
        return InMemoryStorageAdapter()

    async def clear_storage(self) -> None:
        pass

    async def retrieve_events(
        self,
        *,
        adapter: StorageAdapter,
        category: str | None = None,
        stream: str | None = None,
    ) -> Sequence[StoredEvent]:
        return [
            event
            async for event in adapter.scan(
                target=identifier.target(category=category, stream=stream)
            )
        ]


if __name__ == "__main__":
    sys.exit(pytest.main([__file__]))
