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
    """Abstract base class for projection storage backends.

    Implement the abstract methods to provide a custom backend for use with
    `ProjectionStore`. Projections are keyed by their name and id together.
    Queries refer to the projection fields `id`, `name`, `source`, `state`
    and `metadata` by path.

    Type Parameters:
        ItemQuery: The query type accepted by `find_one`. Defaults to
            `Lookup`.
        CollectionQuery: The query type accepted by `find_many` and
            `count`. Defaults to `Search`.
    """

    @abstractmethod
    async def save(
        self,
        *,
        projection: Projection[JsonPersistable, JsonPersistable],
    ) -> None:
        """Insert a projection, or update it if it already exists.

        A projection already exists if one with the same name and id is
        stored, in which case its state and metadata are replaced. State and
        metadata must be serialised to JSON values for storage.

        Args:
            projection: The projection to save.
        """
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
        """Find the single projection matching a query.

        Args:
            lookup: The query identifying the projection.
            state_type: The type to deserialise the state into. Defaults to
                a JSON value.
            metadata_type: The type to deserialise the metadata into.
                Defaults to a JSON value.

        Returns:
            the matching projection, or `None` if there is no match.

        Raises:
            ValueError: If more than one projection matches.
        """
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
        """Find all projections matching a query.

        Args:
            search: The query to match, including any sorting and paging.
            state_type: The type to deserialise each state into. Defaults to
                a JSON value.
            metadata_type: The type to deserialise each metadata into.
                Defaults to a JSON value.

        Returns:
            the matching projections, possibly empty.
        """
        raise NotImplementedError()

    @abstractmethod
    async def count(self, *, search: CollectionQuery) -> int:
        """Count the projections matching a query.

        Args:
            search: The query to match.

        Returns:
            the number of matching projections.
        """
        raise NotImplementedError()
