from abc import ABC, abstractmethod
from collections.abc import Sequence

from logicblocks.event.types import (
    JsonValue,
    JsonValueType,
    Persistable,
    Projection,
)

from ..query import Lookup, Query, Search


class ProjectionStorageAdapter[
    ItemQuery: Query = Lookup,
    CollectionQuery: Query = Search,
](ABC):
    @abstractmethod
    async def save(
        self,
        *,
        projection: Projection[Persistable, Persistable],
    ) -> None:
        raise NotImplementedError()

    @abstractmethod
    async def find_one[
        State: Persistable = JsonValue,
        Metadata: Persistable = JsonValue,
    ](
        self,
        *,
        lookup: ItemQuery,
        state_type: type[State] = JsonValueType,
        metadata_type: type[Metadata] = JsonValueType,
    ) -> Projection[State, Metadata] | None:
        raise NotImplementedError()

    @abstractmethod
    async def find_many[
        State: Persistable = JsonValue,
        Metadata: Persistable = JsonValue,
    ](
        self,
        *,
        search: CollectionQuery,
        state_type: type[State] = JsonValueType,
        metadata_type: type[Metadata] = JsonValueType,
    ) -> Sequence[Projection[State, Metadata]]:
        raise NotImplementedError()
