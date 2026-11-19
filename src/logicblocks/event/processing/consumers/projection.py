from logicblocks.event.projection import ProjectionStore, Projector
from logicblocks.event.store import InMemoryStoredEventSource
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
](EventProcessor[StoredEvent]):
    """Event processor that updates projections as events are processed.

    For each event, the processor locates the current projection for the
    event's stream, applies the event using the projector and saves the
    updated projection.

    Args:
        projector: The projector used to apply events to projections.
        projection_store: The store in which projections are kept.
        state_type: The type to deserialise projection state into.
        metadata_type: The type to deserialise projection metadata into.
    """

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

    async def process_event(self, event: StoredEvent) -> None:
        """Apply the event to its stream's projection and save the result.

        Args:
            event: The stored event to project.
        """
        identifier = StreamIdentifier(
            category=event.category, stream=event.stream
        )
        current_projection = await self._projection_store.locate(
            source=identifier,
            name=self._projector.projection_name,
            state_type=self._state_type,
            metadata_type=self._metadata_type,
        )
        source = InMemoryStoredEventSource[StreamIdentifier](
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
