import functools
from dataclasses import dataclass

from typing import Any, List, Callable, Mapping

from logicblocks.event.types import StoredEvent, Projection

from logicblocks.event.projection.exceptions import MissingHandlerError


@dataclass(frozen=True)
class Projector:
    handlers: Mapping[
        str, Callable[[Mapping[str, Any], StoredEvent], Mapping[str, Any]]
    ]

    def __init__(
        self,
        *,
        handlers: Mapping[
            str, Callable[[Mapping[str, Any], StoredEvent], Mapping[str, Any]]
        ],
    ):
        object.__setattr__(self, "handlers", handlers)

    def call_handler_func(self, state: Mapping[str, Any], event: StoredEvent):
        if event.name in self.handlers:
            handler_function = self.handlers[event.name]
            return handler_function(state, event)
        else:
            raise MissingHandlerError(event)

    def project(self, state: Mapping[str, Any], events: List[StoredEvent]):
        return Projection(
            state=functools.reduce(self.call_handler_func, events, state),
            position=events[-1].position,
        )
