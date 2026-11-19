from collections.abc import AsyncIterator, Set
from typing import Any

from logicblocks.event.types import (
    Event,
    EventSourceIdentifier,
)

from .base import EventSource
from .constraints import QueryConstraint


class ConstrainedEventSource[I: EventSourceIdentifier, E: Event](
    EventSource[I, E]
):
    """An event source that applies a fixed set of constraints to a delegate.

    Every call to `iterate` (and therefore `read` and async iteration) is
    forwarded to the delegate with the fixed constraints merged into any
    constraints supplied by the caller. `latest` is forwarded unconstrained.

    Args:
        delegate: The event source to read events from.
        constraints: Query constraints applied to every iteration.
    """

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
