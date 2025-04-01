import logging
import sys
from collections.abc import Sequence

import pytest
import structlog

from logicblocks.event.store.adapters import (
    EventSerialisationGuarantee,
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
        return ConcurrencyParameters(concurrent_writes=2, repeats=1)

    def construct_storage_adapter(
            self,
            *,
            serialisation_guarantee: EventSerialisationGuarantee = EventSerialisationGuarantee.LOG,
    ) -> EventStorageAdapter:
        return InMemoryEventStorageAdapter(
            serialisation_guarantee=serialisation_guarantee
        )

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
