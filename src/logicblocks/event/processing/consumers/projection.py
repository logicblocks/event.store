from logicblocks.event.projection import ProjectionStore, Projector
from logicblocks.event.sources import InMemoryEventSource
from logicblocks.event.types import (
    JsonPersistable,
    JsonValue,
    JsonValueType,
    StoredEvent,
    StreamIdentifier,
)

from .types import EventProcessor


class ProjectionEventProcessor[
    State: JsonPersistable = JsonValue,
    Metadata: JsonPersistable = JsonValue,
](EventProcessor):
    def __init__(
        self,
        projector: Projector[StreamIdentifier, State, Metadata],
        projection_store: ProjectionStore,
        state_type: type[State] = JsonValueType,
        metadata_type: type[Metadata] = JsonValueType,
    ):
        self._projector = projector
        self._projection_store = projection_store
        self._state_type = state_type
        self._metadata_type = metadata_type

    async def process_event(self, event: StoredEvent[str, JsonValue]) -> None:
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
        state = current_projection.state if current_projection else None
        metadata = current_projection.metadata if current_projection else None
        updated_projection = await self._projector.project(
            state=state,
            metadata=metadata,
            source=source,
        )
        await self._projection_store.save(projection=updated_projection)
