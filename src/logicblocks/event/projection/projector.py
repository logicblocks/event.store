from abc import ABC, abstractmethod
from collections.abc import Callable
from enum import StrEnum
import inspect
from inspect import Parameter

from pyheck import kebab as to_kebab_case
from pyheck import snake as to_snake_case

from logicblocks.event.store import EventSource
from logicblocks.event.types import (
    EventSourceIdentifier,
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


class Projector[T, I: EventSourceIdentifier](ABC):
    name: str | None = None

    missing_handler_behaviour: MissingHandlerBehaviour = (
        MissingHandlerBehaviour.RAISE
    )

    @abstractmethod
    def initial_state_factory(self) -> T:
        raise NotImplementedError()

    @abstractmethod
    def id_factory(self, state: T, coordinates: I) -> str:
        raise NotImplementedError()

    def apply(self, *, event: StoredEvent, state: T | None = None) -> T:
        state = self._resolve_state(state)
        handler = self._resolve_handler(event)

        return handler(state, event)

    async def project(
        self, *, source: EventSource[I], state: T | None = None
    ) -> Projection[T]:
        state = self._resolve_state(state)
        version = 0

        async for event in source:
            state = self.apply(state=state, event=event)
            version = event.position + 1

        return Projection[T](
            id=self.id_factory(state, source.identifier),
            state=state,
            version=version,
            source=source.identifier,
            name=self.resolve_name(),
        )

    def _resolve_state(self, state: T | None) -> T:
        if state is None:
            return self.initial_state_factory()

        return state

    @staticmethod
    def _default_handler(state: T, _: StoredEvent) -> T:
        return state

    def _resolve_handler(
        self, event: StoredEvent
    ) -> Callable[[T, StoredEvent], T]:
        handler_name = to_snake_case(event.name)
        handler = getattr(self, handler_name, None)

        if handler is None:
            if self.missing_handler_behaviour == MissingHandlerBehaviour.RAISE:
                raise MissingProjectionHandlerError(event, self.__class__)
            else:
                return self._default_handler

        return handler

    def resolve_name(self) -> str:
        return self.name if self.name is not None else self._default_name()

    def _default_name(self) -> str:
        projector_name = self.__class__.__name__.replace("Projector", "")
        return to_kebab_case(projector_name)

class XProjector[T, I: EventSourceIdentifier](Projector[T | None, I], ABC):
    def initial_state_factory(self) -> T | None:
        return None

    def _resolve_handler(
            self, event: StoredEvent
    ) -> Callable[[T | None, StoredEvent], T | None]:
        handler = super()._resolve_handler(event)
        handler_sig = inspect.signature(handler)
        state_param = handler_sig.parameters.get("state")
        state_annotation = state_param.annotation if state_param else Parameter.empty

        def _wrapped_handler(state: T | None, event: StoredEvent) -> T | None:
            match state_annotation, state:
                case None, None:
                    return handler(state, event)
                case None, _:
                    raise ValueError(f"Initial handler {handler.__name__} should not be passed a state")
                case _, None:
                    raise ValueError(f"Handler {handler.__name__} was passed None")
                case _, _:
                    return handler(state, event)

        return _wrapped_handler
