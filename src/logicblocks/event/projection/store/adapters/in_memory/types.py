from collections.abc import Callable, Sequence

from logicblocks.event.types import JsonValue, Projection

type ProjectionResultSet = Sequence[Projection[JsonValue, JsonValue]]
type ProjectionResultSetTransformer = Callable[
    [ProjectionResultSet], ProjectionResultSet
]
