import logging
from collections.abc import Mapping, Sequence
from typing import Any

from structlog.typing import FilteringBoundLogger

from logicblocks.event.types import (
    CodecOrMapping,
    EventSourceIdentifier,
    Projection,
)

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

    async def save(
        self,
        *,
        projection: Projection[CodecOrMapping, CodecOrMapping],
    ) -> None:
        await self._adapter.save(projection=projection)

        if self._logger.is_enabled_for(logging.DEBUG):
            await self._logger.ainfo(
                log_event_name("saved"), projection=projection.dict()
            )
        else:
            await self._logger.ainfo(
                log_event_name("saved"), projection=projection.envelope()
            )

    async def locate[
        State: CodecOrMapping = Mapping[str, Any],
        Metadata: CodecOrMapping = Mapping[str, Any],
    ](
        self,
        *,
        source: EventSourceIdentifier,
        name: str,
        state_type: type[State] = Mapping[str, Any],
        metadata_type: type[Metadata] = Mapping[str, Any],
    ) -> Projection[State, Metadata] | None:
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
            state_type=state_type,
            metadata_type=metadata_type,
        )

    async def load[
        State: CodecOrMapping = Mapping[str, Any],
        Metadata: CodecOrMapping = Mapping[str, Any],
    ](
        self,
        *,
        id: str,
        name: str,
        state_type: type[State] = Mapping[str, Any],
        metadata_type: type[Metadata] = Mapping[str, Any],
    ) -> Projection[State, Metadata] | None:
        await self._logger.adebug(log_event_name("loading"), projection_id=id)

        return await self._adapter.find_one(
            lookup=Lookup(
                filters=[
                    FilterClause(Operator.EQUAL, Path("name"), name),
                    FilterClause(Operator.EQUAL, Path("id"), id),
                ]
            ),
            state_type=state_type,
            metadata_type=metadata_type,
        )

    async def search[
        State: CodecOrMapping = Mapping[str, Any],
        Metadata: CodecOrMapping = Mapping[str, Any],
    ](
        self,
        *,
        filters: Sequence[FilterClause],
        sort: SortClause,
        paging: PagingClause,
        state_type: type[State] = Mapping[str, Any],
        metadata_type: type[Metadata] = Mapping[str, Any],
    ) -> Sequence[Projection[State, Metadata]]:
        await self._logger.adebug(
            log_event_name("searching"),
            filters=[repr(filter) for filter in filters],
            sort=repr(sort),
            paging=repr(paging),
        )

        return await self._adapter.find_many(
            search=Search(filters=filters, sort=sort, paging=paging),
            state_type=state_type,
            metadata_type=metadata_type,
        )
