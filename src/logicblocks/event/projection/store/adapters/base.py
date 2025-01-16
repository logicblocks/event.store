from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from logicblocks.event.types import Projection

from ..query import Lookup, Query, Search


class ProjectionStorageAdapter[OQ: Query = Lookup, MQ: Query = Search](ABC):
    @abstractmethod
    async def save[T](
        self,
        *,
        projection: Projection[T],
        converter: Callable[[T], Mapping[str, Any]],
    ) -> None:
        raise NotImplementedError()

    @abstractmethod
    async def find_one[T](
        self,
        *,
        lookup: OQ,
        converter: Callable[[Mapping[str, Any]], T],
    ) -> Projection[T] | None:
        raise NotImplementedError()

    @abstractmethod
    async def find_many[T](
        self,
        *,
        search: MQ,
        converter: Callable[[Mapping[str, Any]], T],
    ) -> Sequence[Projection[T]]:
        raise NotImplementedError()
