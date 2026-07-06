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
    definition: StreamPublishDefinition[Name, Payload, Metadata] = {
        "events": events
    }
    if condition is not None:
        definition["condition"] = condition
    return definition
