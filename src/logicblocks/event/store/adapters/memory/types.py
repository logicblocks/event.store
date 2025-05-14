from collections.abc import Callable

from logicblocks.event.types import (
    JsonValue,
    StoredEvent,
)

type QueryConstraintCheck = Callable[[StoredEvent[str, JsonValue]], bool]
