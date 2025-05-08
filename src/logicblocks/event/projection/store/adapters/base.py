from abc import ABC, abstractmethod
from collections.abc import Sequence

from logicblocks.event.query import Lookup, Query, Search
from logicblocks.event.types import (
    JsonPersistable,
    JsonValue,
    JsonValueType,
    Projection,
)


class ProjectionStorageAdapter[
    ItemQuery: Query = Lookup,
    CollectionQuery: Query = Search,
](ABC):
    @abstractmethod
    async def save(
        self,
        *,
        projection: Projection[JsonPersistable, JsonPersistable],
    ) -> None:
        raise NotImplementedError()

    @abstractmethod
    async def find_one[
        State: JsonPersistable = JsonValue,
        Metadata: JsonPersistable = JsonValue,
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
        State: JsonPersistable = JsonValue,
        Metadata: JsonPersistable = JsonValue,
    ](
        self,
        *,
        search: CollectionQuery,
        state_type: type[State] = JsonValueType,
        metadata_type: type[Metadata] = JsonValueType,
    ) -> Sequence[Projection[State, Metadata]]:
        raise NotImplementedError()
