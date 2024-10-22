import functools

from typing import Any, Dict, List

from logicblocks.event.store.types import StoredEvent


class ProjectionResult:
    def __init__(self, projection: Dict[str, Any]):
        self.projection = projection


class Projection:
    def call_handler_func(self, state: Dict[str, Any], event: StoredEvent):
        handler_function = getattr(self, event.name)

        return handler_function(state, event)

    def project(self, state: Dict[str, Any], events: List[StoredEvent]):
        result = ProjectionResult(
            functools.reduce(self.call_handler_func, events, state)
        )

        return result
