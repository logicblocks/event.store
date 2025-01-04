from collections.abc import Callable, Mapping
from enum import StrEnum, auto
from typing import Any

from pyheck import snake as to_snake_case

from logicblocks.event.store import EventSource
from logicblocks.event.types import Projection, StoredEvent


class MissingProjectionHandlerError(Exception):
    def __init__(self, event: StoredEvent, projection_class: type):
        super().__init__(
            f"Missing handler for event with name '{event.name}' "
            + f"in projection class {projection_class.__name__}"
        )


class MissingHandlerBehaviour(StrEnum):
    RAISE = auto()
    IGNORE = auto()


class Projector[T]:
    initial_state_factory: Callable[[], T]
    missing_handler_behaviour: MissingHandlerBehaviour = (
        MissingHandlerBehaviour.RAISE
    )

    def __init_subclass__(cls, **kwargs: Mapping[Any, Any]) -> None:
        for required in ("initial_state_factory",):
            if getattr(cls, required, None) is None:
                raise TypeError(
                    f"Can't extend abstract class {Projector.__name__} "
                    f"as {cls.__name__} without {required} attribute defined."
                )

        return super().__init_subclass__(**kwargs)

    def apply(self, *, event: StoredEvent, state: T | None = None) -> T:
        state = self._resolve_state(state)
        handler = self._resolve_handler(event)

        return handler(state, event)

    async def project(
        self, *, source: EventSource, state: T | None = None
    ) -> Projection[T]:
        state = self._resolve_state(state)
        version = 0
        async for event in source:
            state = self.apply(state=state, event=event)
            version = event.position + 1

        return Projection[T](state=state, version=version)

    def _resolve_state(self, state: T | None) -> T:
        if state is None:
            return self.initial_state_factory()

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
                return lambda state, event: state

        return handler
