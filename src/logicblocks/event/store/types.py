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
    metadata: NotRequired[Metadata]


def stream_publish_definition[
    Name: StringPersistable = str,
    Payload: JsonPersistable = JsonValue,
    Metadata: JsonPersistable = JsonValue,
](
    *,
    events: Sequence[NewEvent[Name, Payload, Metadata]],
    condition: WriteCondition | None = None,
    metadata: Metadata | None = None,
) -> StreamPublishDefinition[Name, Payload, Metadata]:
    definition: StreamPublishDefinition[Name, Payload, Metadata] = {
        "events": events
    }
    if condition is not None:
        definition["condition"] = condition
    if metadata is not None:
        definition["metadata"] = metadata
    return definition


def resolve_batch_metadata[
    Name: StringPersistable = str,
    Payload: JsonPersistable = JsonValue,
    Metadata: JsonPersistable = JsonValue,
](
    events: Sequence[NewEvent[Name, Payload, Metadata]],
    batch_metadata: Metadata | None,
) -> Sequence[NewEvent[Name, Payload, Metadata]]:
    if batch_metadata is None:
        return events
    return [
        event
        if event.metadata is not None
        else NewEvent[Name, Payload, Metadata](
            name=event.name,
            payload=event.payload,
            metadata=batch_metadata,
            observed_at=event.observed_at,
            occurred_at=event.occurred_at,
        )
        for event in events
    ]
