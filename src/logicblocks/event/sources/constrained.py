from collections.abc import AsyncIterator, Set
from typing import Any, cast

from logicblocks.event.store import EventSource
from logicblocks.event.store.constraints import QueryConstraint
from logicblocks.event.types import (
    EventSourceIdentifier,
    JsonValue,
    StoredEvent,
)


class ConstrainedEventSource[I: EventSourceIdentifier](EventSource[I]):
    def __init__(
        self,
        identifier: I,
        delegate: EventSource[Any],
        constraints: Set[QueryConstraint],
    ):
        self._identifier = identifier
        self._delegate = delegate
        self._constraints = constraints

    @property
    def identifier(self) -> I:
        return self._identifier

    async def latest(self) -> StoredEvent[str, JsonValue] | None:
        return await self._delegate.latest()

    def iterate(
        self, *, constraints: Set[QueryConstraint] = frozenset()
    ) -> AsyncIterator[StoredEvent[str, JsonValue]]:
        return self._delegate.iterate(
            constraints=self._constraints | constraints
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, ConstrainedEventSource):
            return False

        other = cast(ConstrainedEventSource[Any], other)

        return (
            self._identifier == other._identifier
            and self._delegate == other._delegate
            and self._constraints == other._constraints
        )
