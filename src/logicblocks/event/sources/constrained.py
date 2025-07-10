from collections.abc import AsyncIterator, Set
from typing import Any

from logicblocks.event.sources import EventSource
from logicblocks.event.sources.constraints import QueryConstraint
from logicblocks.event.types import (
    BaseEvent,
    EventSourceIdentifier,
)


class ConstrainedEventSource[I: EventSourceIdentifier, E: BaseEvent](
    EventSource[I, E]
):
    def __init__(
        self, delegate: EventSource[I, E], constraints: Set[QueryConstraint]
    ):
        self._delegate = delegate
        self._constraints = constraints

    @property
    def identifier(self) -> I:
        return self._delegate.identifier

    async def latest(self) -> E | None:
        return await self._delegate.latest()

    def iterate(
        self, *, constraints: Set[QueryConstraint] = frozenset()
    ) -> AsyncIterator[E]:
        return self._delegate.iterate(
            constraints=self._constraints | constraints
        )

    def __eq__(self, other: Any) -> bool:
        raise NotImplementedError
