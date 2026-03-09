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
from .constraints import (
    OffsetPagingConstraint,
    QueryConstraint,
    QueryConstraintCheck,
)


class InMemoryEventSource[I: EventSourceIdentifier, E: Event](
    EventSource[I, E], ABC
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
        paging = None
        filter_constraints = set()
        for constraint in constraints:
            if isinstance(constraint, OffsetPagingConstraint):
                paging = constraint
            else:
                filter_constraints.add(constraint)

        matched = 0
        skipped = 0
        offset = paging.offset if paging else 0
        limit = paging.item_count if paging else None

        for event in self._events:
            await asyncio.sleep(0)
            if all(
                self._constraint_converter.convert(constraint)(event)
                for constraint in filter_constraints
            ):
                if skipped < offset:
                    skipped += 1
                    continue
                yield event
                matched += 1
                if limit is not None and matched >= limit:
                    return

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, InMemoryEventSource):
            return NotImplemented

        return (
            self._identifier == cast(Any, other.identifier)
            and self._events == other._events
        )
