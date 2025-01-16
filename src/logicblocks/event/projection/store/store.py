from collections.abc import Callable, Mapping, Sequence
from typing import Any

from logicblocks.event.types import EventSequenceIdentifier, Projection

from . import Search
from .adapters import ProjectionStorageAdapter
from .query import (
    FilterClause,
    Lookup,
    Operator,
    PagingClause,
    Path,
    SortClause,
)


class ProjectionStore:
    def __init__(self, adapter: ProjectionStorageAdapter):
        self.adapter = adapter

    async def save[T](
        self,
        *,
        projection: Projection[T],
        converter: Callable[[T], Mapping[str, Any]],
    ) -> None:
        await self.adapter.save(projection=projection, converter=converter)

    async def locate[T](
        self,
        *,
        source: EventSequenceIdentifier,
        name: str,
        converter: Callable[[Mapping[str, Any]], T],
    ) -> Projection[T] | None:
        return await self.adapter.find_one(
            lookup=Lookup(
                filters=[
                    FilterClause(Operator.EQUAL, Path("source"), source),
                    FilterClause(Operator.EQUAL, Path("name"), name),
                ]
            ),
            converter=converter,
        )

    async def load[T](
        self, *, id: str, converter: Callable[[Mapping[str, Any]], T]
    ) -> Projection[T] | None:
        return await self.adapter.find_one(
            lookup=Lookup(
                filters=[FilterClause(Operator.EQUAL, Path("id"), id)]
            ),
            converter=converter,
        )

    async def search[T](
        self,
        *,
        filters: Sequence[FilterClause],
        sort: SortClause,
        paging: PagingClause,
        converter: Callable[[Mapping[str, Any]], T],
    ) -> Sequence[Projection[T]]:
        return await self.adapter.find_many(
            search=Search(filters=filters, sort=sort, paging=paging),
            converter=converter,
        )
