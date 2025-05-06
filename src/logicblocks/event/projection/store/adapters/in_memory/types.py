from abc import ABC, abstractmethod
from collections.abc import Callable, Sequence

from logicblocks.event.types import JsonValue, Projection

type ProjectionResultSet = Sequence[Projection[JsonValue, JsonValue]]
type ProjectionResultSetTransformer = Callable[
    [ProjectionResultSet], ProjectionResultSet
]


class Converter[T](ABC):
    @abstractmethod
    def convert(self, item: T) -> ProjectionResultSetTransformer:
        raise NotImplementedError
