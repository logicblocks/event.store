from abc import ABC, abstractmethod

from logicblocks.event.sources import constraints
from logicblocks.event.types import Event, JsonObject


class EventConsumerStateConverter[E: Event](ABC):
    @abstractmethod
    def event_to_state(self, event: E) -> JsonObject:
        raise NotImplementedError()

    @abstractmethod
    def state_to_query_constraint(
        self, state: JsonObject
    ) -> constraints.QueryConstraint | None:
        raise NotImplementedError()
