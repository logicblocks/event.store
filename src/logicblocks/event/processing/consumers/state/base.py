from collections import defaultdict
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, Self

from logicblocks.event.sources import constraints
from logicblocks.event.store import EventCategory, conditions
from logicblocks.event.types import (
    Event,
    JsonObject,
    JsonValue,
    JsonValueConvertible,
    NewEvent,
    default_deserialisation_fallback,
    default_serialisation_fallback,
    is_json_object,
)

from .types import EventConsumerStateConverter


def _normalise_state_for_backwards_compat(
    state: JsonObject | None, maybe_last_sequence_number: JsonValue | None
) -> JsonObject:
    """
    Support bare last_sequence_number and missing state for backwards compatibility.
    """
    state = dict(state or {})
    if isinstance(maybe_last_sequence_number, int):
        state["last_sequence_number"] = maybe_last_sequence_number

    return state


@dataclass(frozen=True)
class EventConsumerState(JsonValueConvertible):
    state: JsonObject

    @classmethod
    def deserialise(
        cls,
        value: JsonValue,
        fallback: Callable[
            [Any, JsonValue], Any
        ] = default_deserialisation_fallback,
    ) -> Self:
        if not is_json_object(value):
            return fallback(cls, value)

        state = value.get("state", None)
        if state is not None and not is_json_object(state):
            return fallback(cls, value)

        state = _normalise_state_for_backwards_compat(
            state, value.get("last_sequence_number", None)
        )

        return cls(state)

    def serialise(
        self,
        fallback: Callable[
            [object], JsonValue
        ] = default_serialisation_fallback,
    ) -> JsonValue:
        return {
            "state": self.state,
        }


class EventCount(int):
    def increment(self) -> Self:
        return self.__class__(self + 1)


class EventConsumerStateStore[E: Event]:
    _states: dict[str, EventConsumerState | None]
    _positions: dict[str, int | None]
    _persistence_lags: dict[str, EventCount]

    def __init__(
        self,
        category: EventCategory,
        converter: EventConsumerStateConverter[E],
        persistence_interval: EventCount | None = EventCount(100),
    ):
        self._category = category
        self._converter = converter
        self._persistence_interval = persistence_interval
        self._persistence_lags = defaultdict(EventCount)
        self._states = {}
        self._positions = {}

    def record_processed(
        self,
        event: E,
        *,
        extra_state: JsonObject | None = None,
        partition: str = "default",
    ) -> EventConsumerState:
        state_object = self._converter.event_to_state(event)
        combined_state = {
            **state_object,
            **(extra_state or {}),
        }

        self._states[partition] = EventConsumerState(combined_state)
        self._persistence_lags[partition] = self._persistence_lags[
            partition
        ].increment()

        return EventConsumerState(combined_state)

    async def save_if_needed(self, *, partition: str = "default") -> None:
        if (
            self._persistence_interval
            and self._persistence_lags[partition] >= self._persistence_interval
        ):
            await self.save(partition=partition)

    async def save(self, partition: str | None = None) -> None:
        partitions: Sequence[str]
        if partition is None:
            partitions = list(self._persistence_lags.keys())
        else:
            partitions = [partition]

        for partition in partitions:
            state = self._states.get(partition, None)
            if state is None:
                continue

            lag = self._persistence_lags[partition]
            if lag == 0:
                continue

            position = self._positions.get(partition, None)
            if position is None:
                event = await self._category.stream(stream=partition).latest()
                if event is not None:
                    position = event.position

            condition = (
                conditions.stream_is_empty()
                if position is None
                else conditions.position_is(position)
            )

            stored_events = await self._category.stream(
                stream=partition
            ).publish(
                events=[
                    NewEvent(name="state-changed", payload=state.serialise())
                ],
                condition=condition,
            )
            self._positions[partition] = stored_events[0].position
            self._persistence_lags[partition] = EventCount(0)

    async def load(
        self, *, partition: str = "default"
    ) -> EventConsumerState | None:
        if self._states.get(partition, None) is None:
            event = await self._category.stream(stream=partition).latest()
            if event is None:
                self._states[partition] = None
                self._positions[partition] = None
            else:
                self._states[partition] = EventConsumerState.deserialise(
                    event.payload
                )
                self._positions[partition] = event.position

        return self._states.get(partition, None)

    async def load_to_query_constraint(
        self, *, partition: str = "default"
    ) -> constraints.QueryConstraint | None:
        state = await self.load(partition=partition)
        if state is None:
            return None

        return self._converter.state_to_query_constraint(state.state)
