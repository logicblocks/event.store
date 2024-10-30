import functools

from typing import Any, Dict, List

from logicblocks.event.types import StoredEvent, Projection


class Projector:
    def call_handler_func(self, state: Dict[str, Any], event: StoredEvent):
        handler_function = getattr(self, event.name)

        return handler_function(state, event)

    def project(self, state: Dict[str, Any], events: List[StoredEvent]):
        return Projection(
            state=functools.reduce(self.call_handler_func, events, state)
        )
