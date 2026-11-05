import inspect
from abc import ABC
from collections.abc import Callable, Mapping
from enum import StrEnum, auto
from inspect import Parameter
from types import NoneType, UnionType
from typing import Any, Union, get_args, get_origin

from logicblocks.event.types import (
    EventSourceIdentifier,
    StoredEvent,
)

from .projector import Projector


class StateAnnotationType(StrEnum):
    NONE = auto()
    STATE = auto()
    NONE_OR_STATE = auto()
    NO_ANNOTATION = auto()


def get_param_annotation(param: Parameter | None) -> Any | Parameter.empty:
    if param is None:
        return Parameter.empty

    return param.annotation


def get_state_annotation_type(
    annotation: Any | Parameter.empty,
) -> StateAnnotationType:
    if annotation == Parameter.empty:
        return StateAnnotationType.NO_ANNOTATION

    if annotation is None:
        return StateAnnotationType.NONE

    if (
        get_origin(annotation) is Union or get_origin(annotation) is UnionType
    ) and NoneType in get_args(annotation):
        return StateAnnotationType.NONE_OR_STATE

    return StateAnnotationType.STATE


def extract_state_annotations_from_handler(
    handler: Callable[..., Any],
) -> tuple[StateAnnotationType, Any]:
    handler_sig = inspect.signature(handler)
    state_annotation = get_param_annotation(
        handler_sig.parameters.get("state")
    )
    state_annotation_type = get_state_annotation_type(state_annotation)

    return state_annotation_type, state_annotation


class ProjectorWithNone[
    State,
    Identifier: EventSourceIdentifier,
    Metadata = Mapping[str, Any],
](Projector[State | None, Identifier, Metadata], ABC):
    def initial_state_factory(self) -> State | None:
        return None

    def _resolve_handler(
        self, event: StoredEvent
    ) -> Callable[[State | None, StoredEvent], State | None]:
        handler = super()._resolve_handler(event)
        state_annotation_type, state_annotation = (
            extract_state_annotations_from_handler(handler)
        )

        def _wrapped_handler(
            state: State | None, event: StoredEvent
        ) -> State | None:
            match state_annotation_type, state:
                case StateAnnotationType.NONE_OR_STATE, _:
                    return handler(state, event)

                case StateAnnotationType.NONE, None:
                    return handler(state, event)
                case StateAnnotationType.NONE, _:
                    raise ValueError(
                        f"Initial handler {handler.__name__} should not be passed a state"
                    )

                case StateAnnotationType.STATE, None:
                    raise ValueError(
                        f"Handler {handler.__name__} was passed None but expected {state_annotation}"
                    )
                case StateAnnotationType.STATE, _:
                    return handler(state, event)

                case StateAnnotationType.NO_ANNOTATION, _:
                    return handler(state, event)

        return _wrapped_handler
