import asyncio
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Sequence, Set
from typing import Any, cast

from logicblocks.event.types import (
    Applier,
    Converter,
    Event,
    EventSourceIdentifier,
)

from .base import EventSource
from .constraints import (
    QueryConstraint,
)


class InMemoryEventSourceQueryApplier[E: Event](
    Applier[AsyncIterator[E]], ABC
):
    @property
    @abstractmethod
    def order(self) -> int:
        raise NotImplementedError

    @abstractmethod
    def apply(self, target: AsyncIterator[E]) -> AsyncIterator[E]:
        raise NotImplementedError


class InMemoryEventSource[I: EventSourceIdentifier, E: Event](
    EventSource[I, E], ABC
):
    def __init__(
        self,
        events: Sequence[E],
        identifier: I,
        constraint_converter: Converter[
            QueryConstraint, InMemoryEventSourceQueryApplier[E]
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
    ) -> Converter[QueryConstraint, InMemoryEventSourceQueryApplier[E]]:
        raise NotImplementedError

    @property
    def identifier(self) -> I:
        return self._identifier

    async def latest(self) -> E | None:
        return self._events[-1] if len(self._events) > 0 else None

    async def iterate(
        self, *, constraints: Set[QueryConstraint] = frozenset()
    ) -> AsyncIterator[E]:
        appliers = sorted(
            (
                self._constraint_converter.convert(constraint)
                for constraint in constraints
            ),
            key=lambda a: a.order,
        )

        async def base_iterator() -> AsyncIterator[E]:
            for event in self._events:
                await asyncio.sleep(0)
                yield event

        source: AsyncIterator[E] = base_iterator()
        for applier in appliers:
            source = applier.apply(source)

        async for event in source:
            yield event

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, InMemoryEventSource):
            return NotImplemented

        return (
            self._identifier == cast(Any, other.identifier)
            and self._events == other._events
        )
