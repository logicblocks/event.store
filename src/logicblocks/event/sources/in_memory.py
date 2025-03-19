import asyncio
from collections.abc import AsyncIterator, Sequence, Set
from typing import Any, cast

from logicblocks.event.store import EventSource
from logicblocks.event.store.constraints import QueryConstraint
from logicblocks.event.types import EventSourceIdentifier, StoredEvent


class InMemoryEventSource[I: EventSourceIdentifier](EventSource[I]):
    def __init__(self, events: Sequence[StoredEvent], identifier: I):
        self._events = events
        self._identifier = identifier

    @property
    def identifier(self) -> I:
        return self._identifier

    async def latest(self) -> StoredEvent | None:
        return self._events[-1] if len(self._events) > 0 else None

    async def iterate(
        self, *, constraints: Set[QueryConstraint] = frozenset()
    ) -> AsyncIterator[StoredEvent]:
        for event in self._events:
            await asyncio.sleep(0)
            if all(
                constraint.met_by(event=event) for constraint in constraints
            ):
                yield event

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, InMemoryEventSource):
            return NotImplemented

        return (
            self._identifier == cast(Any, other.identifier)  # type: ignore
            and self._events == other._events
        )
