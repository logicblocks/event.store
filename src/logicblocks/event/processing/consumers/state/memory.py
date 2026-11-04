from logicblocks.event.sources import constraints
from logicblocks.event.types import JsonObject, StoredEvent

from .types import EventConsumerStateConverter


class StoredEventEventConsumerStateConverter(
    EventConsumerStateConverter[StoredEvent]
):
    """Tracks consumption of stored events by their sequence number.

    State takes the form `{"last_sequence_number": <int | None>}`.
    """

    def event_to_state(self, event: StoredEvent) -> JsonObject:
        return {
            "last_sequence_number": event.sequence_number,
        }

    def initial_state(self) -> JsonObject:
        return {
            "last_sequence_number": None,
        }

    def state_to_query_constraint(
        self, state: JsonObject
    ) -> constraints.QueryConstraint | None:
        last_sequence_number = state.get("last_sequence_number", None)
        if not isinstance(last_sequence_number, int):
            return None

        return constraints.sequence_number_after(last_sequence_number)
