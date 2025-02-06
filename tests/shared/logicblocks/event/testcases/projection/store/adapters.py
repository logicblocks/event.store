from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import pytest
from psycopg.types.json import Jsonb

from logicblocks.event.projection.store import (
    FilterClause,
    KeySetPagingClause,
    Lookup,
    Operator,
    Path,
    ProjectionStorageAdapter,
    Search,
    SortClause,
    SortField,
    SortOrder,
)
from logicblocks.event.projection.store.query import PagingDirection
from logicblocks.event.testing import (
    BaseProjectionBuilder,
    data,
)
from logicblocks.event.types import Projection, StreamIdentifier


@dataclass
class Thing:
    value_1: int
    value_2: str

    @staticmethod
    def to_dict(thing: "Thing") -> Mapping[str, Any]:
        return thing.dict()

    @staticmethod
    def from_dict(mapping: Mapping[str, Any]) -> "Thing":
        return Thing(value_1=mapping["value_1"], value_2=mapping["value_2"])

    def dict(self) -> Mapping[str, Any]:
        return {"value_1": self.value_1, "value_2": self.value_2}


class ThingProjectionBuilder(BaseProjectionBuilder[Thing]):
    def default_state_factory(self) -> Thing:
        return Thing(
            value_1=data.random_int(1, 10),
            value_2=data.random_ascii_alphanumerics_string(),
        )


def lift_projection[S, T](
    projection: Projection[S],
    converter: Callable[[S], T],
) -> Projection[T]:
    return Projection[T](
        id=projection.id,
        name=projection.name,
        state=converter(projection.state),
        version=projection.version,
        source=projection.source,
    )


class Base(ABC):
    @abstractmethod
    def construct_storage_adapter(self) -> ProjectionStorageAdapter:
        raise NotImplementedError()

    @abstractmethod
    async def clear_storage(self) -> None:
        raise NotImplementedError()

    @abstractmethod
    async def retrieve_projections(
        self, *, adapter: ProjectionStorageAdapter
    ) -> Sequence[Projection[Mapping[str, Any]]]:
        raise NotImplementedError()


class SaveCases(Base, ABC):
    async def test_stores_single_projection_for_later_retrieval(self):
        projection_1_id = data.random_projection_id()

        adapter = self.construct_storage_adapter()

        projection = ThingProjectionBuilder().with_id(projection_1_id).build()

        await adapter.save(projection=projection, converter=Thing.to_dict)

        retrieved_projections = await self.retrieve_projections(
            adapter=adapter
        )

        assert retrieved_projections == [
            lift_projection(projection, Thing.to_dict)
        ]

    async def test_stores_many_projections_for_later_retrieval(self):
        projection_1_id = data.random_projection_id()
        projection_2_id = data.random_projection_id()

        adapter = self.construct_storage_adapter()

        projection_1 = (
            ThingProjectionBuilder().with_id(projection_1_id).build()
        )
        projection_2 = (
            ThingProjectionBuilder().with_id(projection_2_id).build()
        )

        await adapter.save(projection=projection_1, converter=Thing.to_dict)
        await adapter.save(projection=projection_2, converter=Thing.to_dict)

        retrieved_projections = await self.retrieve_projections(
            adapter=adapter
        )

        assert retrieved_projections == [
            lift_projection(projection_1, Thing.to_dict),
            lift_projection(projection_2, Thing.to_dict),
        ]

    async def test_updates_existing_projection_state_and_version(self):
        projection_id = data.random_projection_id()

        projection_v1_name = data.random_projection_name()
        projection_v1_state = Thing(value_1=5, value_2="first version")
        projection_v1_version = data.random_projection_version()
        projection_v1_source = StreamIdentifier(
            category=data.random_event_category_name(),
            stream=data.random_event_stream_name(),
        )

        projection_v2_name = data.random_projection_name()
        projection_v2_state = Thing(value_1=10, value_2="second version")
        projection_v2_version = projection_v1_version + 1
        projection_v2_source = StreamIdentifier(
            category=data.random_event_category_name(),
            stream=data.random_event_stream_name(),
        )

        adapter = self.construct_storage_adapter()

        projection_v1 = (
            ThingProjectionBuilder()
            .with_id(projection_id)
            .with_name(projection_v1_name)
            .with_state(projection_v1_state)
            .with_version(projection_v1_version)
            .with_source(projection_v1_source)
            .build()
        )
        provided_projection_v2 = (
            ThingProjectionBuilder()
            .with_id(projection_id)
            .with_name(projection_v2_name)
            .with_state(projection_v2_state)
            .with_version(projection_v2_version)
            .with_source(projection_v2_source)
            .build()
        )

        await adapter.save(projection=projection_v1, converter=Thing.to_dict)
        await adapter.save(
            projection=provided_projection_v2, converter=Thing.to_dict
        )

        retrieved_projections = await self.retrieve_projections(
            adapter=adapter
        )

        expected_updated_projection_v2 = (
            ThingProjectionBuilder()
            .with_id(projection_id)
            .with_name(projection_v1_name)
            .with_state(projection_v2_state)
            .with_version(projection_v2_version)
            .with_source(projection_v1_source)
            .build()
        )

        assert retrieved_projections == [
            lift_projection(expected_updated_projection_v2, Thing.to_dict)
        ]


class FindOneCases(Base, ABC):
    async def test_finds_one_projection_by_single_filter(self):
        projection_1_name = data.random_projection_name()
        projection_2_name = data.random_projection_name()

        adapter = self.construct_storage_adapter()

        projection_1 = (
            ThingProjectionBuilder().with_name(projection_1_name).build()
        )
        projection_2 = (
            ThingProjectionBuilder().with_name(projection_2_name).build()
        )

        await adapter.save(projection=projection_1, converter=Thing.to_dict)
        await adapter.save(projection=projection_2, converter=Thing.to_dict)

        located = await adapter.find_one(
            lookup=Lookup(
                filters=[
                    FilterClause(
                        Operator.EQUAL, Path("name"), projection_1_name
                    )
                ]
            ),
            converter=Thing.from_dict,
        )

        assert located == projection_1

    async def test_finds_one_projection_by_source_filter(self):
        projection_1_name = data.random_projection_name()
        projection_2_name = data.random_projection_name()

        adapter = self.construct_storage_adapter()

        source = StreamIdentifier(
            category=data.random_event_category_name(),
            stream=data.random_event_stream_name(),
        )

        projection_1 = (
            ThingProjectionBuilder()
            .with_name(projection_1_name)
            .with_source(source)
            .build()
        )
        projection_2 = (
            ThingProjectionBuilder()
            .with_name(projection_2_name)
            .with_source(
                StreamIdentifier(
                    category=data.random_event_category_name(),
                    stream=data.random_event_stream_name(),
                )
            )
            .build()
        )

        await adapter.save(projection=projection_1, converter=Thing.to_dict)
        await adapter.save(projection=projection_2, converter=Thing.to_dict)

        located = await adapter.find_one(
            lookup=Lookup(
                filters=[
                    FilterClause(
                        Operator.EQUAL, Path("source"), Jsonb(source.dict())
                    ),
                    FilterClause(
                        Operator.EQUAL, Path("name"), projection_1_name
                    ),
                ]
            ),
            converter=Thing.from_dict,
        )

        assert located == projection_1

    async def test_finds_one_projection_by_multiple_filters(self):
        projection_name = data.random_event_stream_name()

        adapter = self.construct_storage_adapter()

        projection_1 = (
            ThingProjectionBuilder()
            .with_name(projection_name)
            .with_state(Thing(value_1=5, value_2="text"))
            .build()
        )
        projection_2 = (
            ThingProjectionBuilder()
            .with_name(projection_name)
            .with_state(Thing(value_1=10, value_2="text"))
            .build()
        )

        await adapter.save(projection=projection_1, converter=Thing.to_dict)
        await adapter.save(projection=projection_2, converter=Thing.to_dict)

        located = await adapter.find_one(
            lookup=Lookup(
                filters=[
                    FilterClause(
                        Operator.EQUAL, Path("name"), projection_name
                    ),
                    FilterClause(
                        Operator.GREATER_THAN, Path("state", "value_1"), 5
                    ),
                ]
            ),
            converter=Thing.from_dict,
        )

        assert located == projection_2

    async def test_finds_none_when_no_projection_matches_lookup(self):
        adapter = self.construct_storage_adapter()

        located = await adapter.find_one(
            lookup=Lookup(
                filters=[
                    FilterClause(
                        Operator.EQUAL,
                        Path("name"),
                        data.random_projection_name(),
                    )
                ]
            ),
            converter=Thing.from_dict,
        )

        assert located is None

    async def test_raises_when_finding_one_and_multiple_found(self):
        projection_name = data.random_event_stream_name()

        adapter = self.construct_storage_adapter()

        projection_1 = (
            ThingProjectionBuilder()
            .with_name(projection_name)
            .with_state(Thing(value_1=5, value_2="text"))
            .build()
        )
        projection_2 = (
            ThingProjectionBuilder()
            .with_name(projection_name)
            .with_state(Thing(value_1=10, value_2="text"))
            .build()
        )

        await adapter.save(projection=projection_1, converter=Thing.to_dict)
        await adapter.save(projection=projection_2, converter=Thing.to_dict)

        with pytest.raises(ValueError):
            await adapter.find_one(
                lookup=Lookup(
                    filters=[
                        FilterClause(
                            Operator.EQUAL, Path("name"), projection_name
                        )
                    ]
                ),
                converter=Thing.from_dict,
            )


class FindManyCases(Base, ABC):
    async def test_finds_many_projections_by_single_filter(self):
        projection_1_name = data.random_projection_name()
        projection_2_name = data.random_projection_name()

        adapter = self.construct_storage_adapter()

        projection_1 = (
            ThingProjectionBuilder().with_name(projection_1_name).build()
        )
        projection_2 = (
            ThingProjectionBuilder().with_name(projection_2_name).build()
        )

        await adapter.save(projection=projection_1, converter=Thing.to_dict)
        await adapter.save(projection=projection_2, converter=Thing.to_dict)

        located = await adapter.find_many(
            search=Search(
                filters=[
                    FilterClause(
                        Operator.EQUAL, Path("name"), projection_1_name
                    )
                ]
            ),
            converter=Thing.from_dict,
        )

        assert located == [projection_1]

    async def test_finds_many_projections_by_many_filters(self):
        adapter = self.construct_storage_adapter()

        projection_1 = (
            ThingProjectionBuilder()
            .with_name("a")
            .with_state(Thing(value_1=5, value_2="text"))
            .build()
        )
        projection_2 = (
            ThingProjectionBuilder()
            .with_name("b")
            .with_state(Thing(value_1=6, value_2="text"))
            .build()
        )
        projection_3 = (
            ThingProjectionBuilder()
            .with_name("a")
            .with_state(Thing(value_1=7, value_2="text"))
            .build()
        )
        projection_4 = (
            ThingProjectionBuilder()
            .with_name("a")
            .with_state(Thing(value_1=8, value_2="text"))
            .build()
        )

        await adapter.save(projection=projection_1, converter=Thing.to_dict)
        await adapter.save(projection=projection_2, converter=Thing.to_dict)
        await adapter.save(projection=projection_3, converter=Thing.to_dict)
        await adapter.save(projection=projection_4, converter=Thing.to_dict)

        located = await adapter.find_many(
            search=Search(
                filters=[
                    FilterClause(Operator.EQUAL, Path("name"), "a"),
                    FilterClause(
                        Operator.GREATER_THAN, Path("state", "value_1"), 5
                    ),
                ]
            ),
            converter=Thing.from_dict,
        )

        assert located == [projection_3, projection_4]

    async def test_finds_many_projections_with_sorting(self):
        adapter = self.construct_storage_adapter()

        projection_1 = (
            ThingProjectionBuilder()
            .with_name("a")
            .with_state(Thing(value_1=5, value_2="text"))
            .build()
        )
        projection_2 = (
            ThingProjectionBuilder()
            .with_name("a")
            .with_state(Thing(value_1=6, value_2="text"))
            .build()
        )
        projection_3 = (
            ThingProjectionBuilder()
            .with_name("a")
            .with_state(Thing(value_1=7, value_2="text"))
            .build()
        )

        await adapter.save(projection=projection_1, converter=Thing.to_dict)
        await adapter.save(projection=projection_2, converter=Thing.to_dict)
        await adapter.save(projection=projection_3, converter=Thing.to_dict)

        search = Search(
            sort=SortClause(
                fields=[
                    SortField(
                        path=Path("state", "value_1"), order=SortOrder.DESC
                    )
                ]
            )
        )
        located = await adapter.find_many(
            search=search, converter=Thing.from_dict
        )

        assert located == [projection_3, projection_2, projection_1]

    async def test_finds_many_projections_with_paging(self):
        adapter = self.construct_storage_adapter()

        projection_1 = (
            ThingProjectionBuilder()
            .with_id("1")
            .with_state(Thing(value_1=5, value_2="text"))
            .build()
        )
        projection_2 = (
            ThingProjectionBuilder()
            .with_id("2")
            .with_state(Thing(value_1=6, value_2="text"))
            .build()
        )
        projection_3 = (
            ThingProjectionBuilder()
            .with_id("3")
            .with_state(Thing(value_1=7, value_2="text"))
            .build()
        )

        await adapter.save(projection=projection_1, converter=Thing.to_dict)
        await adapter.save(projection=projection_2, converter=Thing.to_dict)
        await adapter.save(projection=projection_3, converter=Thing.to_dict)

        search = Search(
            paging=KeySetPagingClause(
                last_id="1", direction=PagingDirection.FORWARDS, item_count=2
            )
        )
        located = await adapter.find_many(
            search=search, converter=Thing.from_dict
        )

        assert located == [projection_2, projection_3]

    async def test_finds_many_sorts_before_paging(self):
        adapter = self.construct_storage_adapter()

        projection_1 = (
            ThingProjectionBuilder()
            .with_id("1")
            .with_state(Thing(value_1=5, value_2="text"))
            .build()
        )
        projection_2 = (
            ThingProjectionBuilder()
            .with_id("2")
            .with_state(Thing(value_1=6, value_2="text"))
            .build()
        )
        projection_3 = (
            ThingProjectionBuilder()
            .with_id("3")
            .with_state(Thing(value_1=7, value_2="text"))
            .build()
        )

        await adapter.save(projection=projection_1, converter=Thing.to_dict)
        await adapter.save(projection=projection_2, converter=Thing.to_dict)
        await adapter.save(projection=projection_3, converter=Thing.to_dict)

        search = Search(
            sort=SortClause(
                fields=[
                    SortField(
                        path=Path("state", "value_1"), order=SortOrder.DESC
                    )
                ]
            ),
            paging=KeySetPagingClause(item_count=2),
        )
        located = await adapter.find_many(
            search=search, converter=Thing.from_dict
        )

        assert located == [projection_3, projection_2]

    async def test_finds_many_filters_before_paging(self):
        adapter = self.construct_storage_adapter()

        projection_1 = (
            ThingProjectionBuilder()
            .with_id("1")
            .with_state(Thing(value_1=5, value_2="text"))
            .build()
        )
        projection_2 = (
            ThingProjectionBuilder()
            .with_id("2")
            .with_state(Thing(value_1=6, value_2="text"))
            .build()
        )
        projection_3 = (
            ThingProjectionBuilder()
            .with_id("3")
            .with_state(Thing(value_1=7, value_2="text"))
            .build()
        )
        projection_4 = (
            ThingProjectionBuilder()
            .with_id("4")
            .with_state(Thing(value_1=8, value_2="text"))
            .build()
        )

        await adapter.save(projection=projection_1, converter=Thing.to_dict)
        await adapter.save(projection=projection_2, converter=Thing.to_dict)
        await adapter.save(projection=projection_3, converter=Thing.to_dict)
        await adapter.save(projection=projection_4, converter=Thing.to_dict)

        search = Search(
            filters=[
                FilterClause(
                    Operator.GREATER_THAN, Path("state", "value_1"), 5
                )
            ],
            paging=KeySetPagingClause(item_count=2),
        )
        located = await adapter.find_many(
            search=search, converter=Thing.from_dict
        )

        assert located == [projection_2, projection_3]

    async def test_finds_many_filters_sorts_and_pages(self):
        adapter = self.construct_storage_adapter()

        projection_1 = (
            ThingProjectionBuilder()
            .with_id("1")
            .with_state(Thing(value_1=5, value_2="text"))
            .build()
        )
        projection_2 = (
            ThingProjectionBuilder()
            .with_id("2")
            .with_state(Thing(value_1=6, value_2="text"))
            .build()
        )
        projection_3 = (
            ThingProjectionBuilder()
            .with_id("3")
            .with_state(Thing(value_1=7, value_2="text"))
            .build()
        )
        projection_4 = (
            ThingProjectionBuilder()
            .with_id("4")
            .with_state(Thing(value_1=8, value_2="text"))
            .build()
        )

        await adapter.save(projection=projection_1, converter=Thing.to_dict)
        await adapter.save(projection=projection_2, converter=Thing.to_dict)
        await adapter.save(projection=projection_3, converter=Thing.to_dict)
        await adapter.save(projection=projection_4, converter=Thing.to_dict)

        search = Search(
            filters=[
                FilterClause(Operator.NOT_EQUAL, Path("state", "value_1"), 7)
            ],
            sort=SortClause(
                fields=[
                    SortField(
                        path=Path("state", "value_1"), order=SortOrder.DESC
                    )
                ]
            ),
            paging=KeySetPagingClause(item_count=2),
        )
        located = await adapter.find_many(
            search=search, converter=Thing.from_dict
        )

        assert located == [projection_4, projection_2]


class ProjectionStorageAdapterCases(
    SaveCases, FindOneCases, FindManyCases, ABC
):
    pass
