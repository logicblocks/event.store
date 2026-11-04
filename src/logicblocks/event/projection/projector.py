from abc import ABC, abstractmethod
from collections.abc import Callable
from enum import StrEnum
from typing import Any

from pyheck import kebab as to_kebab_case
from pyheck import snake as to_snake_case

from logicblocks.event.sources import EventSource
from logicblocks.event.types import (
    EventSourceIdentifier,
    JsonValue,
    Projection,
    StoredEvent,
)


class MissingProjectionHandlerError(Exception):
    """Raised when a projector has no handler for an event it is applying.

    Only raised when the projector's `missing_handler_behaviour` is
    `MissingHandlerBehaviour.RAISE`.

    Args:
        event: The event for which no handler was found.
        projection_class: The projector class that was missing the handler.
    """

    def __init__(self, event: StoredEvent, projection_class: type):
        super().__init__(
            f"Missing handler for event with name '{event.name}' "
            + f"in projection class {projection_class.__name__}"
        )


class MissingHandlerBehaviour(StrEnum):
    """What a projector does when it has no handler for an event.

    Attributes:
        RAISE: Raise a `MissingProjectionHandlerError`.
        IGNORE: Skip the event, leaving the state unchanged.
    """

    RAISE = "raise"
    IGNORE = "ignore"


class Projector[
    Identifier: EventSourceIdentifier,
    State,
    Metadata = JsonValue,
](ABC):
    """Base class for projectors, which fold events into a projection.

    Subclass `Projector` and implement `initial_state_factory`,
    `initial_metadata_factory` and `id_factory`, then add one handler method
    per event name. Handlers are found by converting the event name to snake
    case, so an event named `profile-created` is handled by
    `profile_created`. Each handler takes the current state and the event and
    returns the new state:

    ```python
    class ProfileProjector(Projector[StreamIdentifier, dict[str, str]]):
        def initial_state_factory(self) -> dict[str, str]:
            return {}

        def initial_metadata_factory(self) -> JsonValue:
            return {}

        def id_factory(self, state, source: StreamIdentifier) -> str:
            return source.stream

        def profile_created(self, state, event):
            return {**state, "name": event.payload["name"]}
    ```

    Event names that convert to an existing projector method, such as
    `finalise-state`, `apply`, `project` or `id-factory`, are routed to that
    method as if it were a handler, so avoid them.

    Type Parameters:
        Identifier: The type of event source identifier the projector
            projects from, for example `StreamIdentifier`.
        State: The type of the projection state.
        Metadata: The type of the projection metadata. Defaults to
            `JsonValue`.

    Attributes:
        name: The projection name. Defaults to `None`, in which case
            `projection_name` derives one from the class name.
        missing_handler_behaviour: What to do when an event has no handler.
            Defaults to `MissingHandlerBehaviour.RAISE`.
    """

    name: str | None = None

    missing_handler_behaviour: MissingHandlerBehaviour = (
        MissingHandlerBehaviour.RAISE
    )

    @abstractmethod
    def initial_state_factory(self) -> State:
        """Create the state to start from when no state is provided.

        Returns:
            the initial projection state.
        """
        raise NotImplementedError()

    @abstractmethod
    def initial_metadata_factory(self) -> Metadata:
        """Create the metadata to start from when no metadata is provided.

        Returns:
            the initial projection metadata.
        """
        raise NotImplementedError()

    @abstractmethod
    def id_factory(self, state: State, source: Identifier) -> str:
        """Derive the projection id.

        Called by `project` with the finalised state.

        Args:
            state: The finalised projection state.
            source: The identifier of the event source being projected.

        Returns:
            the projection id.
        """
        raise NotImplementedError()

    def update_metadata(
        self, state: State, metadata: Metadata, event: StoredEvent
    ) -> Metadata:
        """Update the metadata after an event has been applied.

        Called by `project` once per event, after the event's handler. The
        default implementation returns the metadata unchanged.

        Args:
            state: The (unfinalised) state after applying the event.
            metadata: The current metadata.
            event: The event that was just applied.

        Returns:
            the new metadata.
        """
        return metadata

    def finalise_state(self, state: State) -> State:
        """Transform the state once all events have been applied.

        Override this for work, such as validation or normalisation, that
        only needs doing once per projection rather than in every handler.
        Called once at the end of each `project` call, including when the
        source has no events, and before `id_factory`. The default
        implementation returns the state unchanged.

        The output becomes the starting state when a projection is resumed,
        so it must not change what later handlers, `update_metadata` or
        `id_factory` compute, and it must survive a round trip through the
        projection store. See the README's "Finalising State" section for
        details.

        Args:
            state: The state after all events have been applied.

        Returns:
            the finalised state.
        """
        return state

    @property
    def projection_name(self):
        """The name given to projections produced by this projector.

        This is `name` if set, otherwise the class name with `Projector`
        removed, in kebab case; for example `ProfileProjector` gives
        `profile`.
        """
        return self.name if self.name is not None else self._default_name()

    def apply(
        self, *, event: StoredEvent[Any], state: State | None = None
    ) -> State:
        """Apply a single event to a state using the event's handler.

        Does not call `update_metadata` or `finalise_state`.

        Args:
            event: The event to apply.
            state: The state to apply the event to. Defaults to the result
                of `initial_state_factory`.

        Returns:
            the new state, or the given state unchanged if there is no
                handler and `missing_handler_behaviour` is `IGNORE`.

        Raises:
            MissingProjectionHandlerError: If there is no handler for the
                event and `missing_handler_behaviour` is `RAISE`.
        """
        state = self._resolve_state(state)
        handler = self._resolve_handler(event)

        return handler(state, event)

    async def project(
        self,
        *,
        source: EventSource[Identifier, StoredEvent],
        state: State | None = None,
        metadata: Metadata | None = None,
    ) -> Projection[State, Metadata]:
        """Project all events from an event source into a projection.

        Applies each event in turn, calling `update_metadata` after each
        one, then calls `finalise_state` and derives the id from the
        finalised state using `id_factory`.

        Args:
            source: The event source to read events from.
            state: The state to start from, for example to resume a
                previous projection. Defaults to the result of
                `initial_state_factory`.
            metadata: The metadata to start from. Defaults to the result of
                `initial_metadata_factory`.

        Returns:
            the projection, named `projection_name`, with the source's
                identifier as its source.

        Raises:
            MissingProjectionHandlerError: If an event has no handler and
                `missing_handler_behaviour` is `RAISE`.
        """
        state = self._resolve_state(state)
        metadata = self._resolve_metadata(metadata)

        async for event in source:
            state = self.apply(state=state, event=event)
            metadata = self.update_metadata(state, metadata, event)

        finalised_state = self.finalise_state(state)

        return Projection[State, Metadata](
            id=self.id_factory(finalised_state, source.identifier),
            name=self.projection_name,
            source=source.identifier,
            state=finalised_state,
            metadata=metadata,
        )

    def _resolve_state(self, state: State | None) -> State:
        if state is None:
            return self.initial_state_factory()

        return state

    def _resolve_metadata(self, metadata: Metadata | None) -> Metadata:
        if metadata is None:
            return self.initial_metadata_factory()

        return metadata

    def _resolve_handler(
        self, event: StoredEvent
    ) -> Callable[[State, StoredEvent], State]:
        handler_name = to_snake_case(event.name)
        handler = getattr(self, handler_name, None)

        if handler is None:
            if self.missing_handler_behaviour == MissingHandlerBehaviour.RAISE:
                raise MissingProjectionHandlerError(event, self.__class__)
            else:
                return lambda state, _: state

        return handler

    def _default_name(self) -> str:
        projector_name = self.__class__.__name__.replace("Projector", "")
        return to_kebab_case(projector_name)
