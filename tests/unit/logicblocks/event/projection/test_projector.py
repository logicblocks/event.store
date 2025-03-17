import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import reduce
from typing import Any, Mapping, MutableMapping, Self

import pytest

from logicblocks.event.projection import (
    MissingHandlerBehaviour,
    MissingProjectionHandlerError,
    Projector,
)
from logicblocks.event.store import EventStore
from logicblocks.event.store.adapters import InMemoryEventStorageAdapter
from logicblocks.event.testing import NewEventBuilder, StoredEventBuilder, data
from logicblocks.event.types import (
    JsonValue,
    JsonValueConvertible,
    Projection,
    StoredEvent,
    StreamIdentifier,
)

generic_event = (
    StoredEventBuilder[Mapping[str, Any]]()
    .with_category(data.random_event_category_name())
    .with_stream(data.random_event_stream_name())
    .with_payload({})
)


def to_datetime_or_none(value: JsonValue) -> datetime | None:
    if not isinstance(value, str):
        return None
    return datetime.fromisoformat(value)


@dataclass
class Aggregate(JsonValueConvertible):
    @classmethod
    def deserialise(cls, value: JsonValue) -> Self:
        if not isinstance(value, Mapping):
            raise ValueError("Cannot deserialise Aggregate.")

        something_occurred_at = to_datetime_or_none(
            value.get("something_occurred_at", None)
        )
        something_else_occurred_at = to_datetime_or_none(
            value.get("something_else_occurred_at", None)
        )

        return cls(
            something_occurred_at=something_occurred_at,
            something_else_occurred_at=something_else_occurred_at,
        )

    def __init__(
        self,
        something_occurred_at: datetime | None = None,
        something_else_occurred_at: datetime | None = None,
    ):
        self.something_occurred_at = something_occurred_at
        self.something_else_occurred_at = something_else_occurred_at

    def something_occurred_at_string(self) -> str | None:
        if self.something_occurred_at is None:
            return None
        return self.something_occurred_at.isoformat()

    def something_else_occurred_at_string(self) -> str | None:
        if self.something_else_occurred_at is None:
            return None
        return self.something_else_occurred_at.isoformat()

    def serialise(self) -> JsonValue:
        return {
            "something_occurred_at": self.something_occurred_at_string(),
            "something_else_occurred_at": self.something_else_occurred_at_string(),
        }


class AggregateProjector(Projector[Aggregate, StreamIdentifier]):
    def initial_state_factory(self) -> Aggregate:
        return Aggregate()

    def initial_metadata_factory(self) -> Mapping[str, Any]:
        return {}

    def id_factory(self, state: Aggregate, source: StreamIdentifier) -> str:
        return source.stream

    @staticmethod
    def something_occurred(state: Aggregate, event: StoredEvent) -> Aggregate:
        return Aggregate(
            something_occurred_at=event.occurred_at,
            something_else_occurred_at=state.something_else_occurred_at,
        )

    @staticmethod
    def something_else_occurred(
        state: Aggregate, event: StoredEvent
    ) -> Aggregate:
        return Aggregate(
            something_occurred_at=state.something_occurred_at,
            something_else_occurred_at=event.occurred_at,
        )


class TestProjectorEventApplication:
    def test_applies_one_event_when_handler_available(self):
        occurred_at = datetime.now(UTC)
        position = 0

        stored_event = (
            generic_event.with_name("something-occurred")
            .with_position(position)
            .with_occurred_at(occurred_at)
            .build()
        )

        projector = AggregateProjector()

        actual_state = projector.apply(event=stored_event)
        expected_state = Aggregate(something_occurred_at=occurred_at)

        assert expected_state == actual_state

    def test_applies_many_events_when_handlers_available(self):
        something_occurred_at = datetime.now(UTC)
        something_stored_event = (
            generic_event.with_name("something-occurred")
            .with_position(1)
            .with_occurred_at(something_occurred_at)
            .build()
        )

        something_else_occurred_at = datetime.now(UTC)
        second_position = 2
        something_else_stored_event = (
            generic_event.with_name("something-else-occurred")
            .with_position(second_position)
            .with_occurred_at(something_else_occurred_at)
            .build()
        )

        projector = AggregateProjector()

        actual_state = reduce(
            lambda state, event: projector.apply(state=state, event=event),
            [something_stored_event, something_else_stored_event],
            projector.initial_state_factory(),
        )
        expected_state = Aggregate(
            something_occurred_at=something_occurred_at,
            something_else_occurred_at=something_else_occurred_at,
        )

        assert expected_state == actual_state

    def test_allows_camel_case_event_names(self):
        occurred_at = datetime.now(UTC)
        position = 0

        stored_event = (
            generic_event.with_name("SomethingOccurred")
            .with_position(position)
            .with_occurred_at(occurred_at)
            .build()
        )

        projector = AggregateProjector()

        actual_state = projector.apply(event=stored_event)
        expected_state = Aggregate(something_occurred_at=occurred_at)

        assert expected_state == actual_state

    def test_allows_lower_camel_case_event_names(self):
        occurred_at = datetime.now(UTC)
        position = 0

        stored_event = (
            generic_event.with_name("somethingOccurred")
            .with_position(position)
            .with_occurred_at(occurred_at)
            .build()
        )

        projector = AggregateProjector()

        actual_state = projector.apply(event=stored_event)
        expected_state = Aggregate(something_occurred_at=occurred_at)

        assert expected_state == actual_state

    def test_allows_snake_case_event_names(self):
        occurred_at = datetime.now(UTC)
        position = 0

        stored_event = (
            generic_event.with_name("something_occurred")
            .with_position(position)
            .with_occurred_at(occurred_at)
            .build()
        )

        projector = AggregateProjector()

        actual_state = projector.apply(event=stored_event)
        expected_state = Aggregate(something_occurred_at=occurred_at)

        assert expected_state == actual_state

    def test_allows_space_separated_event_names(self):
        occurred_at = datetime.now(UTC)
        position = 0

        stored_event = (
            generic_event.with_name("something occurred")
            .with_position(position)
            .with_occurred_at(occurred_at)
            .build()
        )

        projector = AggregateProjector()

        actual_state = projector.apply(event=stored_event)
        expected_state = Aggregate(something_occurred_at=occurred_at)

        assert expected_state == actual_state

    def test_allows_allows_capitalised_event_names(self):
        occurred_at = datetime.now(UTC)
        position = 0

        stored_event = (
            generic_event.with_name("SOMETHING-OCCURRED")
            .with_position(position)
            .with_occurred_at(occurred_at)
            .build()
        )

        projector = AggregateProjector()

        actual_state = projector.apply(event=stored_event)
        expected_state = Aggregate(something_occurred_at=occurred_at)

        assert expected_state == actual_state

    def test_allows_special_characters_in_event_names(self):
        occurred_at = datetime.now(UTC)
        position = 0

        stored_event = (
            generic_event.with_name("something$occurred")
            .with_position(position)
            .with_occurred_at(occurred_at)
            .build()
        )

        projector = AggregateProjector()

        actual_state = projector.apply(event=stored_event)
        expected_state = Aggregate(something_occurred_at=occurred_at)

        assert expected_state == actual_state

    def test_raises_on_missing_handler_by_default(self):
        stored_event = generic_event.with_name(
            "something-weird-occurred"
        ).build()

        class StrictAggregateProjector(AggregateProjector):
            pass

        projector = StrictAggregateProjector()

        with pytest.raises(MissingProjectionHandlerError):
            projector.apply(event=stored_event)

    def test_ignores_event_on_missing_handler_when_requested(self):
        stored_event = generic_event.with_name(
            "something-weird-occurred"
        ).build()

        class LenientAggregateProjector(AggregateProjector):
            missing_handler_behaviour = MissingHandlerBehaviour.IGNORE

        projector = LenientAggregateProjector()

        actual_state = projector.apply(event=stored_event, state=Aggregate())
        expected_state = Aggregate()

        assert expected_state == actual_state


class TestProjectorProjection:
    async def test_projects_state_using_event_source_as_generic_type(self):
        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()

        store = EventStore(adapter=InMemoryEventStorageAdapter())
        stream = store.stream(category=category_name, stream=stream_name)

        something_occurred_at = datetime.now(UTC)
        something_else_occurred_at = datetime.now(UTC)

        new_events = [
            (
                NewEventBuilder()
                .with_name("something-occurred")
                .with_occurred_at(something_occurred_at)
                .build()
            ),
            (
                NewEventBuilder()
                .with_name("something-else-occurred")
                .with_occurred_at(something_else_occurred_at)
                .build()
            ),
        ]

        await stream.publish(events=new_events)

        projector = AggregateProjector()

        actual_projection = await projector.project(source=stream)
        expected_projection = Projection[Aggregate](
            id=stream_name,
            state=Aggregate(
                something_occurred_at=something_occurred_at,
                something_else_occurred_at=something_else_occurred_at,
            ),
            source=StreamIdentifier(
                category=category_name, stream=stream_name
            ),
            name="aggregate",
            metadata={},
        )

        assert expected_projection == actual_projection

    async def test_projects_state_using_event_source_as_mapping_type(self):
        class MappingProjector(Projector):
            def initial_state_factory(self):
                return {"names": []}

            def initial_metadata_factory(self):
                return {}

            def id_factory(self, state, source):
                return source.stream

            @staticmethod
            def thing_1_occurred(state, event):
                state["names"].append(event.name)
                return state

            @staticmethod
            def thing_2_occurred(state, event):
                state["names"].append(event.name)
                return state

        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()

        store = EventStore(adapter=InMemoryEventStorageAdapter())
        stream = store.stream(category=category_name, stream=stream_name)

        new_events = [NewEventBuilder().with_name("thing-2-occurred").build()]

        await stream.publish(events=new_events)

        projector = MappingProjector()

        actual_projection = await projector.project(
            source=stream, state={"names": ["thing-1-occurred"]}
        )
        expected_projection = Projection(
            id=stream_name,
            state={"names": ["thing-1-occurred", "thing-2-occurred"]},
            source=StreamIdentifier(
                category=category_name, stream=stream_name
            ),
            name="mapping",
            metadata={},
        )

        assert expected_projection == actual_projection

    async def test_projects_using_provided_initial_state(self):
        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()

        store = EventStore(adapter=InMemoryEventStorageAdapter())
        stream = store.stream(category=category_name, stream=stream_name)

        something_occurred_at = datetime.now(UTC)
        something_else_occurred_at = datetime.now(UTC)

        new_events = [
            (
                NewEventBuilder()
                .with_name("something-occurred")
                .with_occurred_at(something_occurred_at)
                .build()
            ),
            (
                NewEventBuilder()
                .with_name("something-else-occurred")
                .with_occurred_at(something_else_occurred_at)
                .build()
            ),
        ]

        await stream.publish(events=new_events)

        projector = AggregateProjector()

        actual_projection = await projector.project(source=stream)
        expected_projection = Projection[Aggregate](
            id=stream_name,
            state=Aggregate(
                something_occurred_at=something_occurred_at,
                something_else_occurred_at=something_else_occurred_at,
            ),
            source=StreamIdentifier(
                category=category_name, stream=stream_name
            ),
            name="aggregate",
            metadata={},
        )

        assert expected_projection == actual_projection

    async def test_updates_metadata_as_mapping_type_during_projection(self):
        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()

        store = EventStore(adapter=InMemoryEventStorageAdapter())
        stream = store.stream(category=category_name, stream=stream_name)

        something_occurred_at = datetime.now(UTC)
        something_else_occurred_at = datetime.now(UTC)

        new_events = [
            (
                NewEventBuilder()
                .with_name("something-occurred")
                .with_occurred_at(something_occurred_at)
                .build()
            ),
            (
                NewEventBuilder()
                .with_name("something-else-occurred")
                .with_occurred_at(something_else_occurred_at)
                .build()
            ),
        ]

        await stream.publish(events=new_events)

        class MetadataMapProjector(
            Projector[Aggregate, StreamIdentifier, MutableMapping[str, Any]],
        ):
            def initial_state_factory(self) -> Aggregate:
                return Aggregate()

            def initial_metadata_factory(self) -> MutableMapping[str, Any]:
                return {"call_count": 0}

            def id_factory(
                self, state: Aggregate, source: StreamIdentifier
            ) -> str:
                return source.stream

            def update_metadata(
                self,
                state: Aggregate,
                metadata: MutableMapping[str, Any],
                event: StoredEvent,
            ) -> MutableMapping[str, Any]:
                metadata["call_count"] += 1
                return metadata

            @staticmethod
            def something_occurred(
                state: Aggregate, event: StoredEvent
            ) -> Aggregate:
                return Aggregate(
                    something_occurred_at=event.occurred_at,
                    something_else_occurred_at=state.something_else_occurred_at,
                )

            @staticmethod
            def something_else_occurred(
                state: Aggregate, event: StoredEvent
            ) -> Aggregate:
                return Aggregate(
                    something_occurred_at=state.something_occurred_at,
                    something_else_occurred_at=event.occurred_at,
                )

        projector = MetadataMapProjector()

        projection = await projector.project(source=stream)

        assert projection.metadata == {"call_count": 2}

    async def test_updates_metadata_as_generic_type_during_projection(self):
        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()

        store = EventStore(adapter=InMemoryEventStorageAdapter())
        stream = store.stream(category=category_name, stream=stream_name)

        something_occurred_at = datetime.now(UTC)
        something_else_occurred_at = datetime.now(UTC)

        new_events = [
            (
                NewEventBuilder()
                .with_name("something-occurred")
                .with_occurred_at(something_occurred_at)
                .build()
            ),
            (
                NewEventBuilder()
                .with_name("something-else-occurred")
                .with_occurred_at(something_else_occurred_at)
                .build()
            ),
        ]

        await stream.publish(events=new_events)

        @dataclass
        class AggregateMeta:
            call_count: int = 0

            @classmethod
            def deserialise(cls, value: Mapping[str, Any]) -> Self:
                return cls(call_count=value["call_count"])

            def serialise(self) -> Mapping[str, Any]:
                return {"call_count": self.call_count}

            def count_call(self) -> "AggregateMeta":
                self.call_count += 1
                return self

        class AggregateMetaProjector(
            Projector[Aggregate, StreamIdentifier, AggregateMeta],
        ):
            def initial_state_factory(self) -> Aggregate:
                return Aggregate()

            def initial_metadata_factory(self) -> AggregateMeta:
                return AggregateMeta()

            def id_factory(
                self, state: Aggregate, source: StreamIdentifier
            ) -> str:
                return source.stream

            def update_metadata(
                self,
                state: Aggregate,
                metadata: AggregateMeta,
                event: StoredEvent,
            ) -> AggregateMeta:
                return metadata.count_call()

            @staticmethod
            def something_occurred(
                state: Aggregate, event: StoredEvent
            ) -> Aggregate:
                return Aggregate(
                    something_occurred_at=event.occurred_at,
                    something_else_occurred_at=state.something_else_occurred_at,
                )

            @staticmethod
            def something_else_occurred(
                state: Aggregate, event: StoredEvent
            ) -> Aggregate:
                return Aggregate(
                    something_occurred_at=state.something_occurred_at,
                    something_else_occurred_at=event.occurred_at,
                )

        projector = AggregateMetaProjector()

        projection = await projector.project(source=stream)

        assert projection.metadata == AggregateMeta(call_count=2)

    async def test_projects_using_provided_initial_metadata(self):
        class MetadataMapProjector(
            Projector[
                MutableMapping[str, Any],
                StreamIdentifier,
                MutableMapping[str, Any],
            ],
        ):
            def initial_state_factory(self) -> MutableMapping[str, Any]:
                return {"names": []}

            def initial_metadata_factory(self) -> MutableMapping[str, Any]:
                return {"call_count": 0}

            def id_factory(
                self, state: MutableMapping[str, Any], source: StreamIdentifier
            ) -> str:
                return source.stream

            def update_metadata(
                self,
                state: MutableMapping[str, Any],
                metadata: MutableMapping[str, Any],
                event: StoredEvent,
            ) -> MutableMapping[str, Any]:
                metadata["call_count"] += 1
                return metadata

            @staticmethod
            def first_thing_occurred(
                state: MutableMapping[str, Any], event: StoredEvent
            ) -> MutableMapping[str, Any]:
                state["names"] += event.name
                return state

            @staticmethod
            def second_thing_occurred(
                state: MutableMapping[str, Any], event: StoredEvent
            ) -> MutableMapping[str, Any]:
                state["names"] += event.name
                return state

        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()

        store = EventStore(adapter=InMemoryEventStorageAdapter())
        stream = store.stream(category=category_name, stream=stream_name)

        new_events = [
            NewEventBuilder().with_name("second-thing-occurred").build()
        ]

        await stream.publish(events=new_events)

        projector = MetadataMapProjector()

        projection = await projector.project(
            source=stream, metadata={"call_count": 1}
        )

        assert projection.metadata == {"call_count": 2}

    async def test_uses_defined_projection_name_when_property_overridden(self):
        projection_name = "specific-thing"

        @dataclass
        class Thing:
            value: int = 5

            @classmethod
            def deserialise(cls, value: Mapping[str, Any]) -> Self:
                return cls(value=int(value["value"]))

            def serialise(self) -> Mapping[str, Any]:
                return {"value": self.value}

        class CustomTypeProjector(Projector[Thing, StreamIdentifier]):
            name = projection_name

            def initial_state_factory(self) -> Thing:
                return Thing()

            def initial_metadata_factory(self):
                return {}

            def id_factory(self, state: Thing, source: StreamIdentifier):
                return source.stream

            @staticmethod
            def thing_got_value(
                state: Thing, event: StoredEvent[Mapping[str, Any]]
            ) -> Thing:
                state.value = event.payload["value"]
                return state

        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()

        store = EventStore(adapter=InMemoryEventStorageAdapter())
        stream = store.stream(category=category_name, stream=stream_name)

        new_events = [
            (
                NewEventBuilder()
                .with_name("thing-got-value")
                .with_payload({"value": 42})
                .build()
            )
        ]

        await stream.publish(events=new_events)

        projector = CustomTypeProjector()

        projection = await projector.project(source=stream)

        assert projection.name == projection_name


if __name__ == "__main__":
    sys.exit(pytest.main([__file__]))
