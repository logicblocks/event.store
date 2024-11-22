import sys
from collections.abc import Sequence

import pytest

from logicblocks.event.adaptertests import cases
from logicblocks.event.types import StoredEvent, identifier
from logicblocks.event.store.adapters import (
    InMemoryStorageAdapter,
    StorageAdapter,
)


class TestInMemoryStorageAdapter(cases.StorageAdapterCases):
    _adapter: StorageAdapter

    def setup_method(self):
        self._adapter = InMemoryStorageAdapter()

    @property
    def adapter(self) -> StorageAdapter:
        return self._adapter

    def retrieve_events(
        self, category: str | None = None, stream: str | None = None
    ) -> Sequence[StoredEvent]:
        return list(
            self.adapter.scan(
                target=identifier.target(category=category, stream=stream)
            )
        )


if __name__ == "__main__":
    sys.exit(pytest.main([__file__]))
