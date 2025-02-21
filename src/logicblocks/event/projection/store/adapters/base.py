from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from typing import Any

from logicblocks.event.types import CodecOrMapping, Projection

from ..query import Lookup, Query, Search


class ProjectionStorageAdapter[
    ItemQuery: Query = Lookup,
    CollectionQuery: Query = Search,
](ABC):
    @abstractmethod
    async def save(
        self,
        *,
        projection: Projection[CodecOrMapping, CodecOrMapping],
    ) -> None:
        raise NotImplementedError()

    @abstractmethod
    async def find_one[
        State: CodecOrMapping | None = Mapping[str, Any],
        Metadata: CodecOrMapping | None = Mapping[str, Any],
    ](
        self,
        *,
        lookup: ItemQuery,
        state_type: type[State] = Mapping[str, Any],
        metadata_type: type[Metadata] = Mapping[str, Any],
    ) -> Projection[State, Metadata] | None:
        raise NotImplementedError()

    @abstractmethod
    async def find_many[
        State: CodecOrMapping | None = Mapping[str, Any],
        Metadata: CodecOrMapping | None = Mapping[str, Any],
    ](
        self,
        *,
        search: CollectionQuery,
        state_type: type[State] = Mapping[str, Any],
        metadata_type: type[Metadata] = Mapping[str, Any],
    ) -> Sequence[Projection[State, Metadata]]:
        raise NotImplementedError()
