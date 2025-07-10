from collections.abc import Callable

from logicblocks.event.types import (
    BaseEvent,
)

type QueryConstraintCheck[E: BaseEvent] = Callable[[E], bool]
