from abc import ABC, abstractmethod
from collections.abc import Callable
from enum import StrEnum
from typing import Any

from pyheck import kebab as to_kebab_case
from pyheck import snake as to_snake_case

from logicblocks.event.store import EventSource
from logicblocks.event.types import (
    EventSourceIdentifier,
    JsonValue,
    Projection,
    StoredEvent,
)


class MissingProjectionHandlerError(Exception):
    def __init__(self, event: StoredEvent, projection_class: type):
        super().__init__(
            f"Missing handler for event with name '{event.name}' "
            + f"in projection class {projection_class.__name__}"
        )


class MissingHandlerBehaviour(StrEnum):
    RAISE = "raise"
    IGNORE = "ignore"


class Projector[
    Identifier: EventSourceIdentifier,
    State,
    Metadata = JsonValue,
](ABC):
    name: str | None = None

    missing_handler_behaviour: MissingHandlerBehaviour = (
        MissingHandlerBehaviour.RAISE
    )

    @abstractmethod
    def initial_state_factory(self) -> State:
        raise NotImplementedError()

    @abstractmethod
    def initial_metadata_factory(self) -> Metadata:
        raise NotImplementedError()

    @abstractmethod
    def id_factory(self, state: State, source: Identifier) -> str:
        raise NotImplementedError()

    def update_metadata(
        self, state: State, metadata: Metadata, event: StoredEvent
    ) -> Metadata:
        return metadata

    @property
    def projection_name(self):
        return self.name if self.name is not None else self._default_name()

    def apply(
        self, *, event: StoredEvent[Any], state: State | None = None
    ) -> State:
        state = self._resolve_state(state)
        handler = self._resolve_handler(event)

        return handler(state, event)

    async def project(
        self,
        *,
        source: EventSource[Identifier],
        state: State | None = None,
        metadata: Metadata | None = None,
    ) -> Projection[State, Metadata]:
        state = self._resolve_state(state)
        metadata = self._resolve_metadata(metadata)

        async for event in source:
            state = self.apply(state=state, event=event)
            metadata = self.update_metadata(state, metadata, event)

        return Projection[State, Metadata](
            id=self.id_factory(state, source.identifier),
            name=self.projection_name,
            source=source.identifier,
            state=state,
            metadata=metadata,
        )

    def _resolve_state(self, state: State | None) -> State:
        if state is None:
            return self.initial_state_factory()

        return state

    def _resolve_metadata(self, metadata: Metadata | None) -> Metadata:
        if metadata is None:
            return self.initial_metadata_factory()

        return metadata

    def _resolve_handler(
        self, event: StoredEvent
    ) -> Callable[[State, StoredEvent], State]:
        handler_name = to_snake_case(event.name)
        handler = getattr(self, handler_name, None)

        if handler is None:
            if self.missing_handler_behaviour == MissingHandlerBehaviour.RAISE:
                raise MissingProjectionHandlerError(event, self.__class__)
            else:
                return lambda state, _: state

        return handler

    def _default_name(self) -> str:
        projector_name = self.__class__.__name__.replace("Projector", "")
        return to_kebab_case(projector_name)
