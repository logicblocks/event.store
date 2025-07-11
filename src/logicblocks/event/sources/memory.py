import asyncio
from collections.abc import AsyncIterator, Sequence, Set
from typing import Any, cast

from logicblocks.event.store.adapters.memory.converters import (
    QueryConstraintCheck,
    TypeRegistryConstraintConverter,
)
from logicblocks.event.types import (
    Converter,
    Event,
    EventSourceIdentifier,
)

from .base import EventSource
from .constraints import QueryConstraint


class InMemoryEventSource[I: EventSourceIdentifier, E: Event](
    EventSource[I, E]
):
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
            constraint_converter
            if constraint_converter is not None
            else (
                TypeRegistryConstraintConverter().with_default_constraint_converters()
            )
        )

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
            self._identifier == cast(Any, other.identifier)  # pyright: ignore[reportUnknownMemberType]
            and self._events == other._events  # pyright: ignore[reportUnknownMemberType]
        )
