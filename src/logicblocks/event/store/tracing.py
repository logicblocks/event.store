from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from uuid import uuid4


@dataclass
class TracingMetadata:
    trace_id: str
    causation_event_id: str | None


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
