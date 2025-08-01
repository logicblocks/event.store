from abc import ABC, abstractmethod

from logicblocks.event.sources.constraints import QueryConstraint
from logicblocks.event.types import Event, JsonObject


class EventConsumerStateConverter[E: Event](ABC):
    @abstractmethod
    def event_to_state(self, event: E) -> JsonObject:
        raise NotImplementedError()

    @abstractmethod
    def state_to_query_constraint(
        self, state: JsonObject
    ) -> QueryConstraint | None:
        raise NotImplementedError()


class EventConsumer(ABC):
    @abstractmethod
    async def consume_all(self) -> None:
        raise NotImplementedError()


class EventProcessor[E: Event](ABC):
    @abstractmethod
    async def process_event(self, event: E) -> None:
        raise NotImplementedError()
