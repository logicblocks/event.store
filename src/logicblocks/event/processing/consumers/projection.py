from collections.abc import Callable, Mapping
from typing import Any

from logicblocks.event.projection import ProjectionStore, Projector
from logicblocks.event.types import StoredEvent, StreamIdentifier

from ...store.store import StoredEvents
from .types import EventProcessor


class ProjectionEventProcessor[T](EventProcessor):
    projection_store: ProjectionStore
    projector: Projector[T]

    def __init__(
        self,
        projector: Projector[T],
        projection_store: ProjectionStore,
        from_dict_converter: Callable[[Mapping[str, Any]], T],
        to_dict_converter: Callable[[T], Mapping[str, Any]],
    ):
        self._projector = projector
        self._projection_store = projection_store
        self._from_dict_converter = from_dict_converter
        self._to_dict_converter = to_dict_converter

    async def process_event(self, event: StoredEvent) -> None:
        identifier = StreamIdentifier(
            category=event.category, stream=event.stream
        )
        current_state = await self._projection_store.locate(
            source=identifier,
            name=self._projector.resolve_name(),
            converter=self._from_dict_converter,
        )
        projection = await self._projector.project(
            state=current_state.state if current_state else None,
            source=StoredEvents(events=[event], stream=identifier),
        )
        await self._projection_store.save(
            projection=projection, converter=self._to_dict_converter
        )
