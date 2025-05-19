from collections.abc import Sequence

from logicblocks.event.persistence.memory import (
    DelegatingQueryConverter,
    ResultSet,
    ResultSetTransformer,
)
from logicblocks.event.query import (
    Lookup,
    Query,
    Search,
)
from logicblocks.event.types import (
    Converter,
    JsonPersistable,
    JsonValue,
    JsonValueType,
    Projection,
    deserialise_projection,
    serialise_projection,
    serialise_to_json_value,
)

from .base import ProjectionStorageAdapter


class InMemoryProjectionStorageAdapter[
    ItemQuery: Query = Lookup,
    CollectionQuery: Query = Search,
](ProjectionStorageAdapter[ItemQuery, CollectionQuery]):
    def __init__(
        self,
        query_converter: Converter[
            Query, ResultSetTransformer[Projection[JsonValue, JsonValue]]
        ]
        | None = None,
    ):
        self._projections: dict[
            tuple[str, str], Projection[JsonValue, JsonValue]
        ] = {}
        self._query_converter = (
            query_converter
            if query_converter is not None
            else (
                DelegatingQueryConverter[
                    Projection[JsonValue, JsonValue]
                ]().with_default_converters()
            )
        )

    async def save(
        self,
        *,
        projection: Projection[JsonPersistable, JsonPersistable],
    ) -> None:
        projection_key = (projection.name, projection.id)
        existing = self._projections.get(projection_key, None)
        if existing is not None:
            self._projections[projection_key] = Projection[
                JsonValue, JsonValue
            ](
                id=existing.id,
                name=existing.name,
                source=existing.source,
                state=serialise_to_json_value(projection.state),
                metadata=serialise_to_json_value(projection.metadata),
            )
        else:
            self._projections[projection_key] = serialise_projection(
                projection
            )

    async def _find_raw(
        self, query: Query
    ) -> Sequence[Projection[JsonValue, JsonValue]]:
        initial_result_set = ResultSet[Projection[JsonValue, JsonValue]].of(
            *(self._projections.values())
        )
        transformer = self._query_converter.convert(query)
        transformed_result_set = transformer(initial_result_set)

        return transformed_result_set.records

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
        projections = await self._find_raw(lookup)

        if len(projections) > 1:
            raise ValueError(
                f"Expected single projection for query: {lookup} "
                f"but found {len(projections)} projections: {projections}."
            )
        if len(projections) == 0:
            return None

        projection = projections[0]

        return deserialise_projection(projection, state_type, metadata_type)

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
        return [
            deserialise_projection(projection, state_type, metadata_type)
            for projection in (await self._find_raw(search))
        ]
