from collections.abc import Callable

from logicblocks.event.types import (
    Event,
)

type QueryConstraintCheck[E: Event] = Callable[[E], bool]
