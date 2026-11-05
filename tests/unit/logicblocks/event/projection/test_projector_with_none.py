from inspect import Parameter
from typing import Any, Mapping, Optional, Union

import pytest

from logicblocks.event.projection.projector_with_none import (
    ProjectorWithNone,
    StateAnnotationType,
    extract_state_annotations_from_handler,
)
from logicblocks.event.sources import InMemoryEventSource
from logicblocks.event.testing import StoredEventBuilder
from logicblocks.event.types import LogIdentifier, StoredEvent


class TestExtractStateAnnotationsFromHandler:
    def test_no_arg(self):
        def handler(event):
            pass

        assert extract_state_annotations_from_handler(handler) == (
            StateAnnotationType.NO_ANNOTATION,
            Parameter.empty,
        )

    def test_no_annotation(self):
        def handler(state, event):
            pass

        assert extract_state_annotations_from_handler(handler) == (
            StateAnnotationType.NO_ANNOTATION,
            Parameter.empty,
        )

    def test_with_lambda(self):
        assert extract_state_annotations_from_handler(
            lambda state, event: ...
        ) == (
            StateAnnotationType.NO_ANNOTATION,
            Parameter.empty,
        )

    def test_none(self):
        def handler(state: None, event):
            pass

        assert extract_state_annotations_from_handler(handler) == (
            StateAnnotationType.NONE,
            None,
        )

    def test_optional(self):
        def handler(state: Optional[str], event):
            pass

        assert extract_state_annotations_from_handler(handler) == (
            StateAnnotationType.NONE_OR_STATE,
            Optional[str],
        )

    def test_union_none(self):
        def handler(state: Union[str, None], event):
            pass

        assert extract_state_annotations_from_handler(handler) == (
            StateAnnotationType.NONE_OR_STATE,
            Union[str, None],
        )

    def test_union_none_shorthand(self):
        def handler(state: str | None, event):
            pass

        assert extract_state_annotations_from_handler(handler) == (
            StateAnnotationType.NONE_OR_STATE,
            str | None,
        )

    def test_other_annotation(self):
        def handler(state: str, event):
            pass

        assert extract_state_annotations_from_handler(handler) == (
            StateAnnotationType.STATE,
            str,
        )


class TestProjectorWithNone:
    class MyProjector(ProjectorWithNone[Mapping[str, str], LogIdentifier]):
        def initial_metadata_factory(self) -> Mapping[str, Any]:
            return {}

        def id_factory(
            self, state: Mapping[str, str] | None, source: LogIdentifier
        ) -> str:
            return str(source)

        @staticmethod
        def test_first_event(
            state: None, event: StoredEvent
        ) -> Mapping[str, str]:
            return {}

        @staticmethod
        def test_later_event(
            state: Mapping[str, str], event: StoredEvent
        ) -> Mapping[str, str]:
            return state

        @staticmethod
        def test_any_position_event(
            state: Mapping[str, str] | None, event: StoredEvent
        ) -> Mapping[str, str]:
            return {}

    async def test_events_work_in_correct_position(self):
        events = [
            StoredEventBuilder(name="test_first_event", position=0).build(),
            StoredEventBuilder(name="test_later_event", position=1).build(),
        ]
        source = InMemoryEventSource(events, LogIdentifier())

        await self.MyProjector().project(source=source)

    async def test_later_event_raises_exception_is_used_first(self):
        events = [
            StoredEventBuilder(name="test_later_event", position=0).build(),
        ]
        source = InMemoryEventSource(events, LogIdentifier())

        with pytest.raises(ValueError):
            await self.MyProjector().project(source=source)

    async def test_first_event_raises_exception_is_used_later(self):
        events = [
            StoredEventBuilder(name="test_later_event", position=0).build(),
            StoredEventBuilder(name="test_first_event", position=1).build(),
        ]
        source = InMemoryEventSource(events, LogIdentifier())

        with pytest.raises(ValueError):
            await self.MyProjector().project(source=source)

    async def test_any_events_work_in_any_position(self):
        events = [
            StoredEventBuilder(
                name="test_any_position_event", position=0
            ).build(),
            StoredEventBuilder(
                name="test_any_position_event", position=1
            ).build(),
        ]
        source = InMemoryEventSource(events, LogIdentifier())

        await self.MyProjector().project(source=source)
