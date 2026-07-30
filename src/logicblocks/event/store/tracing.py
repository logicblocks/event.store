from abc import ABC
from collections.abc import Callable
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, asdict
from typing import Any, Self
from uuid import uuid4

from logicblocks.event.types import JsonValue, is_json_object


@dataclass(frozen=True, kw_only=True)
class TracingMetadata:
    trace_id: str
    causation_event_id: str | None

    def serialise(self, fallback: Callable[[object], JsonValue]) -> JsonValue:
        return asdict(self)

    @classmethod
    def deserialise(
            cls, value: JsonValue, fallback: Callable[[type[Any], JsonValue], Any]
    ) -> Self:
        if is_json_object(value):
            trace_id = value.get("trace_id")
            causation_event_id = value.get("causation_event_id")
            if isinstance(trace_id, str) and isinstance(causation_event_id, (str, None)):
                return cls(
                    trace_id=trace_id,
                    causation_event_id=causation_event_id,
                )

        return fallback(cls, value)


class TracingMixin(ABC):
    tracing: TracingMetadata | None

    def with_tracing(self):
        tracing = get_tracing_metadata()




_tracing_metadata_var = ContextVar[TracingMetadata | None](
    "event.tracing.metadata", default=None
)


@contextmanager
def start_tracing(*, event_id: str | None):
    existing_metadata = _tracing_metadata_var.get()
    new_metadata = TracingMetadata(
        trace_id=existing_metadata.trace_id
        if existing_metadata
        else str(uuid4()),
        causation_event_id=event_id,
    )

    token = _tracing_metadata_var.set(new_metadata)
    try:
        yield new_metadata
    finally:
        _tracing_metadata_var.reset(token)


def maybe_get_tracing_metadata() -> TracingMetadata | None:
    return _tracing_metadata_var.get()


def get_tracing_metadata() -> TracingMetadata:
    metadata = maybe_get_tracing_metadata()
    return (
        metadata
        if metadata
        else TracingMetadata(trace_id=str(uuid4()), causation_event_id=None)
    )
