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
    """The persisted state of an event consumer.

    Attributes:
        state: The state as produced by an `EventConsumerStateConverter`.
    """

    state: JsonObject

    @classmethod
    def deserialise(
        cls,
        value: JsonValue,
        fallback: Callable[
            [Any, JsonValue], Any
        ] = default_deserialisation_fallback,
    ) -> Self:
        """Deserialise consumer state from a JSON value.

        For backwards compatibility, a top-level `last_sequence_number` is
        merged into the state and a missing state is treated as empty.

        Args:
            value: The JSON value to deserialise.
            fallback: Called with the class and value if the value is not a
                valid consumer state.

        Returns:
            the deserialised consumer state, or the result of the fallback.
        """
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
    """A count of events."""

    def increment(self) -> Self:
        """Return a new count one greater than this one."""
        return self.__class__(self + 1)


class EventConsumerStateStore[E: Event]:
    """Records and persists event consumer state in an event category.

    State is held in memory per partition and persisted as `state-changed`
    events in the stream named after the partition.

    Args:
        category: The category in which to store consumer state.
        converter: Converts between events and consumer state.
        persistence_interval: The number of recorded events after which
            `save_if_needed` persists state. If `None`, `save_if_needed`
            never saves.
    """

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
        """Record that an event has been processed.

        The state is held in memory until saved.

        Args:
            event: The processed event.
            extra_state: Additional state to merge into the converted state.
            partition: The partition to record the state against.

        Returns:
            the new consumer state.
        """
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

    def reset(
        self,
        *,
        extra_state: JsonObject | None = None,
        partition: str = "default",
    ):
        """Reset a partition's state to the converter's initial state.

        The reset state is held in memory only and is never saved.

        Args:
            extra_state: Additional state to merge into the initial state.
            partition: The partition to reset.
        """
        initial_state = self._converter.initial_state()
        combined_state = {
            **initial_state,
            **(extra_state or {}),
        }

        self._states[partition] = EventConsumerState(combined_state)
        self._persistence_lags[partition] = EventCount(0)

    async def save_if_needed(self, *, partition: str = "default") -> None:
        """Save a partition's state if the persistence interval is reached.

        Args:
            partition: The partition to save.
        """
        if (
            self._persistence_interval
            and self._persistence_lags[partition] >= self._persistence_interval
        ):
            await self.save(partition=partition)

    async def save(self, partition: str | None = None) -> None:
        """Persist unsaved state.

        Partitions with no state or no changes since the last save are
        skipped.

        Args:
            partition: The partition to save. If `None`, all partitions
                with recorded state are saved.

        Raises:
            UnmetWriteConditionError: If the partition's stream has been
                written to by another writer since it was last read.
        """
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
                    NewEvent(
                        name="state-changed",
                        payload=state.serialise(),
                        metadata=None,
                    )
                ],
                condition=condition,
            )
            self._positions[partition] = stored_events[0].position
            self._persistence_lags[partition] = EventCount(0)

    async def load(
        self, *, partition: str = "default"
    ) -> EventConsumerState | None:
        """Load a partition's state.

        State held in memory is returned if present; otherwise the latest
        persisted state is read from the partition's stream.

        Args:
            partition: The partition to load.

        Returns:
            the consumer state, or `None` if there is none.
        """
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
        """Load a partition's state as a constraint for resuming consumption.

        Args:
            partition: The partition to load.

        Returns:
            a constraint selecting events after those already processed,
                or `None` if there is no state or all events should be
                consumed.
        """
        state = await self.load(partition=partition)
        if state is None:
            return None

        return self._converter.state_to_query_constraint(state.state)
