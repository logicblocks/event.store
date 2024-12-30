import functools
from dataclasses import dataclass
from typing import Any, Callable, List, Mapping

from logicblocks.event.projection.exceptions import MissingHandlerError
from logicblocks.event.types import Projection, StoredEvent


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
