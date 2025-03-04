from collections.abc import Mapping
from typing import Any

from logicblocks.event.projection import ProjectionStore, Projector
from logicblocks.event.sources import InMemoryEventSource
from logicblocks.event.types import StoredEvent, StreamIdentifier
from logicblocks.event.types.codec import CodecOrMapping

from .types import EventProcessor


class ProjectionEventProcessor[
    State: CodecOrMapping = Mapping[str, Any],
    Metadata: CodecOrMapping = Mapping[str, Any],
](EventProcessor):
    def __init__(
        self,
        projector: Projector[State, StreamIdentifier, Metadata],
        projection_store: ProjectionStore,
        state_type: type[State] = Mapping[str, Any],
        metadata_type: type[Metadata] = Mapping[str, Any],
    ):
        self._projector = projector
        self._projection_store = projection_store
        self._state_type = state_type
        self._metadata_type = metadata_type

    async def process_event(self, event: StoredEvent) -> None:
        identifier = StreamIdentifier(
            category=event.category, stream=event.stream
        )
        current_projection = await self._projection_store.locate(
            source=identifier,
            name=self._projector.projection_name,
            state_type=self._state_type,
            metadata_type=self._metadata_type,
        )
        source = InMemoryEventSource[StreamIdentifier](
            events=[event], identifier=identifier
        )
        updated_projection = await self._projector.project(
            state=current_projection.state if current_projection else None,
            metadata=current_projection.metadata
            if current_projection
            else None,
            source=source,
        )
        await self._projection_store.save(projection=updated_projection)
