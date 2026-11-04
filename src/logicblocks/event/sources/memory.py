import asyncio
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Sequence, Set
from typing import Any, cast

from logicblocks.event.types import (
    Converter,
    Event,
    EventSourceIdentifier,
)

from .base import EventSource
from .constraints import QueryConstraint, QueryConstraintCheck


class InMemoryEventSource[I: EventSourceIdentifier, E: Event](
    EventSource[I, E], ABC
):
    """Abstract event source backed by an in-memory sequence of events.

    Events are yielded in the order of the supplied sequence. Each query
    constraint is converted into a predicate by the constraint converter and
    an event is yielded only if it satisfies all predicates. Subclasses must
    implement `_get_default_constraint_converter` to supply the converter used
    when none is provided.

    Args:
        events: The events in the source, in order.
        identifier: The identifier of the source.
        constraint_converter: Converts query constraints into predicates
            over events. Defaults to the subclass's default converter.
    """

    def __init__(
        self,
        events: Sequence[E],
        identifier: I,
        constraint_converter: Converter[
            QueryConstraint, QueryConstraintCheck[E]
        ]
        | None = None,
    ):
        self._events = events
        self._identifier = identifier
        self._constraint_converter = (
            constraint_converter or self._get_default_constraint_converter()
        )

    @abstractmethod
    def _get_default_constraint_converter(
        self,
    ) -> Converter[QueryConstraint, QueryConstraintCheck[E]]:
        raise NotImplementedError

    @property
    def identifier(self) -> I:
        return self._identifier

    async def latest(self) -> E | None:
        return self._events[-1] if len(self._events) > 0 else None

    async def iterate(
        self, *, constraints: Set[QueryConstraint] = frozenset()
    ) -> AsyncIterator[E]:
        for event in self._events:
            await asyncio.sleep(0)
            if all(
                self._constraint_converter.convert(constraint)(event)
                for constraint in constraints
            ):
                yield event

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, InMemoryEventSource):
            return NotImplemented

        return (
            self._identifier == cast(Any, other.identifier)
            and self._events == other._events
        )
