import logging
from collections.abc import Mapping, Sequence
from typing import Any

from structlog.typing import FilteringBoundLogger

from logicblocks.event.query import (
    FilterClause,
    Lookup,
    Operator,
    PagingClause,
    Path,
    Search,
    SortClause,
)
from logicblocks.event.types import (
    EventSourceIdentifier,
    JsonPersistable,
    JsonValue,
    JsonValueType,
    Projection,
    str_serialisation_fallback,
)

from ..logger import default_logger
from .adapters import ProjectionStorageAdapter


def log_event_name(event: str) -> str:
    return f"event.projection.{event}"


class ProjectionStore:
    """Saves and retrieves projections using a storage adapter.

    Args:
        adapter: The storage adapter to persist projections with.
        logger: The logger to log operations to.
    """

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
        projection: Projection[JsonPersistable, JsonPersistable],
    ) -> None:
        """Save a projection, inserting or updating it.

        If a projection with the same name and id is already stored, its
        state and metadata are replaced.

        Args:
            projection: The projection to save.
        """
        await self._adapter.save(projection=projection)

        if self._logger.is_enabled_for(logging.DEBUG):
            await self._logger.ainfo(
                log_event_name("saved"),
                projection=projection.serialise(
                    fallback=str_serialisation_fallback
                ),
            )
        else:
            await self._logger.ainfo(
                log_event_name("saved"),
                projection=projection.summarise(
                    fallback=str_serialisation_fallback
                ),
            )

    async def locate[
        State: JsonPersistable = JsonValue,
        Metadata: JsonPersistable = JsonValue,
    ](
        self,
        *,
        source: EventSourceIdentifier,
        name: str,
        state_type: type[State] = JsonValueType,
        metadata_type: type[Metadata] = JsonValueType,
    ) -> Projection[State, Metadata] | None:
        """Find a projection by its source and name.

        Args:
            source: The identifier of the event source the projection was
                projected from.
            name: The projection name.
            state_type: The type to deserialise the state into. Defaults to
                a JSON value.
            metadata_type: The type to deserialise the metadata into.
                Defaults to a JSON value.

        Returns:
            the matching projection, or `None` if there is no match.

        Raises:
            ValueError: If more than one projection matches.
        """
        await self._logger.adebug(
            log_event_name("locating"),
            projection_name=name,
            projection_source=source.serialise(
                fallback=str_serialisation_fallback
            ),
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
        State: JsonPersistable = Mapping[str, Any],
        Metadata: JsonPersistable = Mapping[str, Any],
    ](
        self,
        *,
        id: str,
        name: str,
        state_type: type[State],
        metadata_type: type[Metadata],
    ) -> Projection[State, Metadata] | None:
        """Load a projection by its id and name.

        Args:
            id: The projection id.
            name: The projection name.
            state_type: The type to deserialise the state into.
            metadata_type: The type to deserialise the metadata into.

        Returns:
            the matching projection, or `None` if there is no match.
        """
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
        State: JsonPersistable = JsonValue,
        Metadata: JsonPersistable = JsonValue,
    ](
        self,
        *,
        filters: Sequence[FilterClause],
        sort: SortClause,
        paging: PagingClause,
        state_type: type[State] = JsonValueType,
        metadata_type: type[Metadata] = JsonValueType,
    ) -> Sequence[Projection[State, Metadata]]:
        """Find all projections matching the given clauses.

        Args:
            filters: The filter clauses projections must match.
            sort: How to order the results.
            paging: Which page of results to return.
            state_type: The type to deserialise each state into. Defaults to
                a JSON value.
            metadata_type: The type to deserialise each metadata into.
                Defaults to a JSON value.

        Returns:
            the matching projections, possibly empty.
        """
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
