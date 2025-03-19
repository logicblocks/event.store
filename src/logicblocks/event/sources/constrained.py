from collections.abc import AsyncIterator, Set
from typing import Any

from logicblocks.event.store import EventSource
from logicblocks.event.store.constraints import QueryConstraint
from logicblocks.event.types import EventSourceIdentifier, StoredEvent


class ConstrainedEventSource[I: EventSourceIdentifier](EventSource[I]):
    def __init__(
        self, delegate: EventSource[I], constraints: Set[QueryConstraint]
    ):
        self._delegate = delegate
        self._constraints = constraints

    @property
    def identifier(self) -> I:
        return self._delegate.identifier

    async def latest(self) -> StoredEvent | None:
        return await self._delegate.latest()

    def iterate(
        self, *, constraints: Set[QueryConstraint] = frozenset()
    ) -> AsyncIterator[StoredEvent]:
        return self._delegate.iterate(
            constraints=self._constraints | constraints
        )

    def __eq__(self, other: Any) -> bool:
        raise NotImplementedError
