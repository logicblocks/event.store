import asyncio
from collections.abc import AsyncIterator, Sequence, Set
from typing import Any, cast

from logicblocks.event.store import EventSource
from logicblocks.event.store.constraints import QueryConstraint
from logicblocks.event.types import StoredEvent
from logicblocks.event.types.identifier import EventSourceIdentifier


class InMemoryEventSource[I: EventSourceIdentifier](EventSource[I]):
    def __init__(self, events: Sequence[StoredEvent], identifier: I):
        self._events = events
        self._identifier = identifier

    @property
    def identifier(self) -> I:
        return self._identifier

    async def latest(self) -> StoredEvent | None:
        return self._events[-1] if self._events else None

    async def iterate(
        self, *, constraints: Set[QueryConstraint] = frozenset()
    ) -> AsyncIterator[StoredEvent]:
        for event in self._events:
            await asyncio.sleep(0)
            yield event

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, InMemoryEventSource):
            return NotImplemented

        # TODO: Implement equality check for events
        other_identifier = cast(Any, other.identifier)  # type: ignore
        return self._identifier == other_identifier
