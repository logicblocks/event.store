import logging
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from structlog.typing import FilteringBoundLogger

from logicblocks.event.types import EventSourceIdentifier, Projection

from ..logger import default_logger
from .adapters import ProjectionStorageAdapter
from .query import (
    FilterClause,
    Lookup,
    Operator,
    PagingClause,
    Path,
    Search,
    SortClause,
)


def log_event_name(event: str) -> str:
    return f"event.projection.{event}"


class ProjectionStore:
    def __init__(
        self,
        adapter: ProjectionStorageAdapter,
        logger: FilteringBoundLogger = default_logger,
    ):
        self._adapter = adapter
        self._logger = logger

    async def save[T](
        self,
        *,
        projection: Projection[T],
        converter: Callable[[T], Mapping[str, Any]],
    ) -> None:
        await self._adapter.save(projection=projection, converter=converter)

        if self._logger.is_enabled_for(logging.DEBUG):
            await self._logger.ainfo(
                log_event_name("saved"), projection=projection.dict(converter)
            )
        else:
            await self._logger.ainfo(
                log_event_name("saved"), projection=projection.envelope()
            )

    async def locate[T](
        self,
        *,
        source: EventSourceIdentifier,
        name: str,
        converter: Callable[[Mapping[str, Any]], T],
    ) -> Projection[T] | None:
        await self._logger.adebug(
            log_event_name("locating"),
            projection_name=name,
            projection_source=source.dict(),
        )

        return await self._adapter.find_one(
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
        await self._logger.adebug(log_event_name("loading"), projection_id=id)

        return await self._adapter.find_one(
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
        await self._logger.adebug(
            log_event_name("searching"),
            filters=[repr(filter) for filter in filters],
            sort=repr(sort),
            paging=repr(paging),
        )

        return await self._adapter.find_many(
            search=Search(filters=filters, sort=sort, paging=paging),
            converter=converter,
        )
