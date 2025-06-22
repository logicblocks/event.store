from collections.abc import Sequence
from typing import NotRequired, TypedDict

from logicblocks.event.types import (
    JsonPersistable,
    JsonValue,
    NewEvent,
    StringPersistable,
)

from .conditions import WriteCondition


class StreamPublishRequest[
    Name: StringPersistable = str,
    Payload: JsonPersistable = JsonValue,
](TypedDict):
    events: Sequence[NewEvent[Name, Payload]]
    condition: NotRequired[WriteCondition]
