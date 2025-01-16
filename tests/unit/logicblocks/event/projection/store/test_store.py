from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from logicblocks.event.projection.store import (
    FilterClause,
    InMemoryProjectionStorageAdapter,
    Operator,
    PagingClause,
    Path,
    ProjectionStore,
    SortClause,
    SortField,
    SortOrder,
)
from logicblocks.event.testing import BaseProjectionBuilder, data
from logicblocks.event.types import StreamIdentifier


@dataclass
class Thing:
    value: int


def to_dict(thing: object) -> Mapping[str, Any]:
    return thing.__dict__


def from_dict[T](target_type: type[T]) -> Callable[[Mapping[str, Any]], T]:
    def converter(mapping: Mapping[str, Any]) -> T:
        return target_type(**mapping)

    return converter


class ThingProjectionBuilder(BaseProjectionBuilder[Thing]):
    def default_state_factory(self) -> Thing:
        return Thing(
            value=data.random_int(1, 10),
        )


class TestProjectionStoreSave:
    async def test_stores_and_loads_projection(self):
        projection_id = data.random_projection_id()

        adapter = InMemoryProjectionStorageAdapter()
        store = ProjectionStore(adapter=adapter)

        projection = ThingProjectionBuilder().with_id(projection_id).build()

        await store.save(projection=projection, converter=to_dict)

        located = await store.load(
            id=projection_id, converter=from_dict(Thing)
        )

        assert located == projection

    async def test_updates_existing_projection(self):
        projection_id = data.random_projection_id()

        adapter = InMemoryProjectionStorageAdapter()
        store = ProjectionStore(adapter=adapter)

        source = StreamIdentifier(
            category=data.random_event_category_name(),
            stream=data.random_event_stream_name(),
        )
        projection_name = data.random_projection_name()

        projection = (
            ThingProjectionBuilder()
            .with_id(projection_id)
            .with_name(projection_name)
            .with_state(Thing(value=5))
            .with_source(source)
            .with_version(1)
            .build()
        )

        await store.save(projection=projection, converter=to_dict)

        updated_projection = (
            ThingProjectionBuilder()
            .with_id(projection_id)
            .with_name(projection_name)
            .with_state(Thing(value=10))
            .with_source(source)
            .with_version(2)
            .build()
        )

        await store.save(projection=updated_projection, converter=to_dict)

        located = await store.load(
            id=projection_id, converter=from_dict(Thing)
        )

        assert located == updated_projection


class TestProjectionStoreLoad:
    async def test_loads_correct_projection(self):
        projection_1_id = data.random_projection_id()
        projection_2_id = data.random_projection_id()

        adapter = InMemoryProjectionStorageAdapter()
        store = ProjectionStore(adapter=adapter)

        projection_1 = (
            ThingProjectionBuilder().with_id(projection_1_id).build()
        )
        projection_2 = (
            ThingProjectionBuilder().with_id(projection_2_id).build()
        )

        await store.save(projection=projection_1, converter=to_dict)
        await store.save(projection=projection_2, converter=to_dict)

        located_1 = await store.load(
            id=projection_1_id, converter=from_dict(Thing)
        )
        located_2 = await store.load(
            id=projection_2_id, converter=from_dict(Thing)
        )

        assert [located_1, located_2] == [projection_1, projection_2]

    async def test_returns_none_when_no_projection(self):
        adapter = InMemoryProjectionStorageAdapter()
        store = ProjectionStore(adapter=adapter)

        located = await store.load(
            id=data.random_projection_id(), converter=from_dict(Thing)
        )

        assert located is None


class TestProjectionStoreLocate:
    async def test_locates_projection_by_source_and_name(self):
        category_name = data.random_event_category_name()
        stream_1_name = data.random_event_stream_name()
        stream_2_name = data.random_event_stream_name()

        source_1 = StreamIdentifier(
            category=category_name, stream=stream_1_name
        )
        source_2 = StreamIdentifier(
            category=category_name, stream=stream_2_name
        )

        projection_name_1 = data.random_projection_name()
        projection_name_2 = data.random_projection_name()

        adapter = InMemoryProjectionStorageAdapter()
        store = ProjectionStore(adapter=adapter)

        projection_1 = (
            ThingProjectionBuilder()
            .with_name(projection_name_1)
            .with_source(source_1)
            .build()
        )
        projection_2 = (
            ThingProjectionBuilder()
            .with_name(projection_name_2)
            .with_source(source_1)
            .build()
        )
        projection_3 = (
            ThingProjectionBuilder()
            .with_name(projection_name_1)
            .with_source(source_2)
            .build()
        )

        await store.save(projection=projection_1, converter=to_dict)
        await store.save(projection=projection_2, converter=to_dict)
        await store.save(projection=projection_3, converter=to_dict)

        located = await store.locate(
            source=source_1, name=projection_name_1, converter=from_dict(Thing)
        )

        assert located == projection_1

    async def test_returns_none_when_no_projection(self):
        adapter = InMemoryProjectionStorageAdapter()
        store = ProjectionStore(adapter=adapter)

        located = await store.locate(
            source=StreamIdentifier(
                category=data.random_event_category_name(),
                stream=data.random_event_stream_name(),
            ),
            name=data.random_projection_name(),
            converter=from_dict(Thing),
        )

        assert located is None


class TestProjectionStoreSearch:
    async def test_searches_projections(self):
        adapter = InMemoryProjectionStorageAdapter()
        store = ProjectionStore(adapter=adapter)

        projection_1 = (
            ThingProjectionBuilder()
            .with_id("a")
            .with_state(Thing(value=5))
            .build()
        )
        projection_2 = (
            ThingProjectionBuilder()
            .with_id("b")
            .with_state(Thing(value=5))
            .build()
        )
        projection_3 = (
            ThingProjectionBuilder()
            .with_id("c")
            .with_state(Thing(value=10))
            .build()
        )
        projection_4 = (
            ThingProjectionBuilder()
            .with_id("d")
            .with_state(Thing(value=5))
            .build()
        )

        await store.save(projection=projection_1, converter=to_dict)
        await store.save(projection=projection_2, converter=to_dict)
        await store.save(projection=projection_3, converter=to_dict)
        await store.save(projection=projection_4, converter=to_dict)

        located = await store.search(
            filters=[FilterClause(Operator.EQUAL, Path("state", "value"), 5)],
            sort=SortClause(fields=[SortField(Path("id"), SortOrder.DESC)]),
            paging=PagingClause(item_count=2),
            converter=from_dict(Thing),
        )

        assert located == [projection_4, projection_2]

    async def test_returns_no_projections_when_no_matches(self):
        adapter = InMemoryProjectionStorageAdapter()
        store = ProjectionStore(adapter=adapter)

        projection_1 = (
            ThingProjectionBuilder()
            .with_name("aggregate")
            .with_id("a")
            .with_state(Thing(value=5))
            .build()
        )
        projection_2 = (
            ThingProjectionBuilder()
            .with_name("aggregate")
            .with_id("b")
            .with_state(Thing(value=5))
            .build()
        )
        projection_3 = (
            ThingProjectionBuilder()
            .with_name("aggregate")
            .with_id("c")
            .with_state(Thing(value=10))
            .build()
        )
        projection_4 = (
            ThingProjectionBuilder()
            .with_name("aggregate")
            .with_id("d")
            .with_state(Thing(value=5))
            .build()
        )

        await store.save(projection=projection_1, converter=to_dict)
        await store.save(projection=projection_2, converter=to_dict)
        await store.save(projection=projection_3, converter=to_dict)
        await store.save(projection=projection_4, converter=to_dict)

        located = await store.search(
            filters=[
                FilterClause(Operator.EQUAL, Path("state", "value"), 5),
                FilterClause(Operator.EQUAL, Path("name"), "metrics"),
            ],
            sort=SortClause(fields=[SortField(Path("id"), SortOrder.DESC)]),
            paging=PagingClause(item_count=2),
            converter=from_dict(Thing),
        )

        assert located == []
