import dataclasses
from typing import Any, Mapping, Self

from logicblocks.event.processing.consumers import ProjectionEventProcessor
from logicblocks.event.projection import (
    InMemoryProjectionStorageAdapter,
    Projector,
)
from logicblocks.event.projection.store.store import ProjectionStore
from logicblocks.event.testing import data
from logicblocks.event.testing.builders import (
    BaseProjectionBuilder,
    StoredEventBuilder,
)
from logicblocks.event.types import Codec, StoredEvent, StreamIdentifier


@dataclasses.dataclass
class State(Codec):
    value: int

    def serialise(self) -> Mapping[str, Any]:
        return {"value": self.value}

    @classmethod
    def deserialise(cls, value: Mapping[str, Any]) -> Self:
        return cls(value=int(value["value"]))


class StateProjector(Projector[State, StreamIdentifier]):
    def __init__(self, projection_name: str):
        self.name = projection_name

    def initial_state_factory(self) -> State:
        return State(value=0)

    def initial_metadata_factory(self) -> Mapping[str, Any]:
        return {}

    def id_factory(self, state: State, source: StreamIdentifier) -> str:
        return source.stream

    def update_metadata(
        self, state: State, metadata: Mapping[str, Any], event: StoredEvent
    ) -> Mapping[str, Any]:
        metadata = dict(metadata)
        metadata["event_count"] = metadata.get("event_count", 0) + 1
        return metadata

    @staticmethod
    def thing_occurred(state: State, event: StoredEvent) -> State:
        return State(
            value=state.value + event.payload["value"],
        )


class StateProjectionBuilder(BaseProjectionBuilder[State]):
    def default_state_factory(self) -> State:
        return State(value=0)

    def default_metadata_factory(self) -> Mapping[str, Any]:
        return {}


class TestProjectionEventProcessor:
    async def test_saves_new_projection(self):
        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()

        projection_name = data.random_projection_name()

        source = StreamIdentifier(category=category_name, stream=stream_name)

        projector = StateProjector(projection_name=projection_name)
        adapter = InMemoryProjectionStorageAdapter()
        store = ProjectionStore(adapter=adapter)
        processor = ProjectionEventProcessor(
            projector=projector, projection_store=store, state_type=State
        )

        event = (
            StoredEventBuilder()
            .with_stream(stream_name)
            .with_category(category_name)
            .with_name("thing-occurred")
            .with_payload({"value": 10})
            .build()
        )

        await processor.process_event(event)

        loaded = await store.locate(
            source=source, name=projection_name, state_type=State
        )

        assert loaded is not None
        assert loaded.state == State(value=10)
        assert loaded.metadata["event_count"] == 1

    async def test_updates_existing_projection(self):
        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()

        projection_name = data.random_projection_name()

        source = StreamIdentifier(category=category_name, stream=stream_name)

        projector = StateProjector(projection_name=projection_name)
        adapter = InMemoryProjectionStorageAdapter()
        store = ProjectionStore(adapter=adapter)
        processor = ProjectionEventProcessor(
            projector=projector, projection_store=store, state_type=State
        )

        projection = (
            StateProjectionBuilder()
            .with_name(projection_name)
            .with_id(stream_name)
            .with_source(source)
            .with_state(State(value=5))
            .with_metadata({"event_count": 3})
            .build()
        )

        await store.save(projection=projection)

        event = (
            StoredEventBuilder()
            .with_stream(stream_name)
            .with_category(category_name)
            .with_name("thing-occurred")
            .with_payload({"value": 10})
            .build()
        )

        await processor.process_event(event)

        loaded = await store.locate(
            source=source, name=projection_name, state_type=State
        )

        assert loaded is not None
        assert loaded.state == State(value=15)
        assert loaded.metadata["event_count"] == 4
