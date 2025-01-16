import sys
from collections.abc import Sequence

import pytest

from logicblocks.event.store.adapters import (
    EventStorageAdapter,
    InMemoryEventStorageAdapter,
)
from logicblocks.event.testcases.store.adapters import (
    ConcurrencyParameters,
    EventStorageAdapterCases,
)
from logicblocks.event.types import StoredEvent, identifier


class TestInMemoryEventStorageAdapterCommonCases(EventStorageAdapterCases):
    @property
    def concurrency_parameters(self):
        return ConcurrencyParameters(concurrent_writes=40, repeats=200)

    def construct_storage_adapter(self) -> EventStorageAdapter:
        return InMemoryEventStorageAdapter()

    async def clear_storage(self) -> None:
        pass

    async def retrieve_events(
        self,
        *,
        adapter: EventStorageAdapter,
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
