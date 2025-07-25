from collections import defaultdict
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, Self

from logicblocks.event.store import EventCategory, conditions
from logicblocks.event.types import (
    Event,
    JsonValue,
    NewEvent,
    default_deserialisation_fallback,
    default_serialisation_fallback,
    is_json_object,
)
from logicblocks.event.types.json import JsonValueConvertible


@dataclass(frozen=True)
class EventConsumerState(JsonValueConvertible):
    last_ordering_id: JsonValue
    state: JsonValue

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

        last_ordering_id = value.get(
            "last_ordering_id", value.get("last_sequence_number")
        )
        state = value.get("state", None)

        return cls(last_ordering_id, state)

    def serialise(
        self,
        fallback: Callable[
            [object], JsonValue
        ] = default_serialisation_fallback,
    ) -> JsonValue:
        return {
            "last_ordering_id": self.last_ordering_id,
            "state": self.state,
        }


class EventCount(int):
    def increment(self) -> Self:
        return self.__class__(self + 1)


class EventConsumerStateStore:
    _states: dict[str, EventConsumerState | None]
    _positions: dict[str, int | None]
    _persistence_lags: dict[str, EventCount]

    def __init__(
        self,
        category: EventCategory,
        persistence_interval: EventCount = EventCount(100),
    ):
        self._category = category
        self._persistence_interval = persistence_interval
        self._persistence_lags = defaultdict(EventCount)
        self._states = {}
        self._positions = {}

    async def record_processed(
        self,
        event: Event,
        *,
        state: JsonValue = None,
        partition: str = "default",
    ) -> EventConsumerState:
        self._states[partition] = EventConsumerState(
            last_ordering_id=event.ordering_id,
            state=state,
        )
        self._persistence_lags[partition] = self._persistence_lags[
            partition
        ].increment()

        if self._persistence_lags[partition] >= self._persistence_interval:
            await self.save(partition=partition)

        return EventConsumerState(
            last_ordering_id=event.ordering_id,
            state=state,
        )

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
