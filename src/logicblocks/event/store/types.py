from collections.abc import Sequence
from typing import NotRequired, TypedDict

from logicblocks.event.types import (
    JsonPersistable,
    JsonValue,
    NewEvent,
    StringPersistable,
)

from .conditions import WriteCondition


class StreamPublishDefinition[
    Name: StringPersistable = str,
    Payload: JsonPersistable = JsonValue,
    Metadata: JsonPersistable = JsonValue,
](TypedDict):
    """The events to publish to a single stream and an optional condition.

    Used as the per-stream value when publishing to multiple streams of a
    category at once.

    Attributes:
        events: The events to publish to the stream, in order.
        condition: The write condition for the stream. If absent, the stream
            is written unconditionally.
    """

    events: Sequence[NewEvent[Name, Payload, Metadata]]
    condition: NotRequired[WriteCondition]


def stream_publish_definition[
    Name: StringPersistable = str,
    Payload: JsonPersistable = JsonValue,
    Metadata: JsonPersistable = JsonValue,
](
    *,
    events: Sequence[NewEvent[Name, Payload, Metadata]],
    condition: WriteCondition | None = None,
) -> StreamPublishDefinition[Name, Payload, Metadata]:
    """Create a `StreamPublishDefinition`.

    Args:
        events: The events to publish to the stream, in order.
        condition: The write condition for the stream. If `None`, the
            `condition` key is omitted and the stream is written
            unconditionally.

    Returns:
        the stream publish definition.
    """
    definition: StreamPublishDefinition[Name, Payload, Metadata] = {
        "events": events
    }
    if condition is not None:
        definition["condition"] = condition
    return definition
