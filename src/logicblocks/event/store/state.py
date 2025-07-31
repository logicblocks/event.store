from logicblocks.event.processing.consumers.types import ConsumerStateConverter
from logicblocks.event.sources.constraints import (
    QueryConstraint,
    sequence_number_after,
)
from logicblocks.event.types import JsonObject, StoredEvent


class StoredEventConsumerStateConverter(ConsumerStateConverter[StoredEvent]):
    def event_to_state(self, event: StoredEvent) -> JsonObject:
        return {
            "last_sequence_number": event.sequence_number,
        }

    def state_to_query_constraint(
        self, state: JsonObject
    ) -> QueryConstraint | None:
        last_sequence_number = state.get("last_sequence_number", None)
        if not isinstance(last_sequence_number, int):
            return None

        return sequence_number_after(last_sequence_number)


stored_event_consumer_state_converter = StoredEventConsumerStateConverter()
