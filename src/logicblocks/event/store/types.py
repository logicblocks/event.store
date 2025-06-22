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
](TypedDict):
    events: Sequence[NewEvent[Name, Payload]]
    condition: NotRequired[WriteCondition]


def stream_publish_definition[
    Name: StringPersistable = str,
    Payload: JsonPersistable = JsonValue,
](
    *,
    events: Sequence[NewEvent[Name, Payload]],
    condition: WriteCondition | None = None,
) -> StreamPublishDefinition[Name, Payload]:
    definition: StreamPublishDefinition[Name, Payload] = {"events": events}
    if condition is not None:
        definition["condition"] = condition
    return definition
