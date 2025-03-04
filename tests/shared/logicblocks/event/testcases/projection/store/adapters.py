from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Self

import pytest

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
from logicblocks.event.types.projection import (
    serialise_projection,
)


@dataclass
class Thing:
    value_1: int
    value_2: str = ""
    value_3: list[str] = field(default_factory=list[str])

    @classmethod
    def deserialise(cls, value: Mapping[str, Any]) -> Self:
        return cls(
            value_1=value["value_1"],
            value_2=value["value_2"],
            value_3=value["value_3"],
        )

    def serialise(self) -> Mapping[str, Any]:
        return {
            "value_1": self.value_1,
            "value_2": self.value_2,
            "value_3": self.value_3,
        }


class ThingProjectionBuilder(BaseProjectionBuilder[Thing]):
    def default_state_factory(self) -> Thing:
        return Thing(
            value_1=data.random_int(1, 10),
            value_2=data.random_ascii_alphanumerics_string(10),
            value_3=[
                data.random_ascii_alphanumerics_string(10) for _ in range(3)
            ],
        )

    def default_metadata_factory(self) -> Mapping[str, Any]:
        return {}


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
    ) -> Sequence[Projection[Mapping[str, Any], Mapping[str, Any]]]:
        raise NotImplementedError()


class SaveCases(Base, ABC):
    async def test_stores_single_projection_for_later_retrieval(self):
        projection_1_id = data.random_projection_id()

        adapter = self.construct_storage_adapter()

        projection = ThingProjectionBuilder().with_id(projection_1_id).build()

        await adapter.save(projection=projection)

        retrieved_projections = await self.retrieve_projections(
            adapter=adapter
        )

        assert retrieved_projections == [serialise_projection(projection)]

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

        await adapter.save(projection=projection_1)
        await adapter.save(projection=projection_2)

        retrieved_projections = await self.retrieve_projections(
            adapter=adapter
        )

        assert retrieved_projections == [
            serialise_projection(projection_1),
            serialise_projection(projection_2),
        ]

    async def test_updates_existing_projection_state_and_metadata(self):
        projection_name = data.random_projection_name()
        projection_id = data.random_projection_id()

        projection_v1_source = StreamIdentifier(
            category=data.random_event_category_name(),
            stream=data.random_event_stream_name(),
        )
        projection_v1_state = Thing(value_1=5, value_2="first version")
        projection_v1_metadata = {"updated_at": "2024-01-01T00:00:00Z"}

        projection_v2_source = StreamIdentifier(
            category=data.random_event_category_name(),
            stream=data.random_event_stream_name(),
        )
        projection_v2_state = Thing(value_1=10, value_2="second version")
        projection_v2_metadata = {"updated_at": "2024-02-02T00:00:00Z"}

        adapter = self.construct_storage_adapter()

        projection_v1 = (
            ThingProjectionBuilder()
            .with_id(projection_id)
            .with_name(projection_name)
            .with_source(projection_v1_source)
            .with_state(projection_v1_state)
            .with_metadata(projection_v1_metadata)
            .build()
        )
        provided_projection_v2 = (
            ThingProjectionBuilder()
            .with_id(projection_id)
            .with_name(projection_name)
            .with_source(projection_v2_source)
            .with_state(projection_v2_state)
            .with_metadata(projection_v2_metadata)
            .build()
        )

        await adapter.save(projection=projection_v1)
        await adapter.save(projection=provided_projection_v2)

        retrieved_projections = await self.retrieve_projections(
            adapter=adapter
        )

        expected_updated_projection_v2 = (
            ThingProjectionBuilder()
            .with_id(projection_id)
            .with_name(projection_name)
            .with_source(projection_v1_source)
            .with_state(projection_v2_state)
            .with_metadata(projection_v2_metadata)
            .build()
        )

        assert retrieved_projections == [
            serialise_projection(expected_updated_projection_v2)
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

        await adapter.save(projection=projection_1)
        await adapter.save(projection=projection_2)

        located = await adapter.find_one(
            lookup=Lookup(
                filters=[
                    FilterClause(
                        Operator.EQUAL, Path("name"), projection_1_name
                    )
                ]
            ),
            state_type=Thing,
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

        await adapter.save(projection=projection_1)
        await adapter.save(projection=projection_2)

        located = await adapter.find_one(
            lookup=Lookup(
                filters=[
                    FilterClause(Operator.EQUAL, Path("source"), source),
                    FilterClause(
                        Operator.EQUAL, Path("name"), projection_1_name
                    ),
                ]
            ),
            state_type=Thing,
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

        await adapter.save(projection=projection_1)
        await adapter.save(projection=projection_2)

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
            state_type=Thing,
        )

        assert located == projection_2

    async def test_finds_one_projection_by_nested_string_filter(self):
        projection_name = data.random_event_stream_name()

        adapter = self.construct_storage_adapter()

        filter_value = data.random_lowercase_ascii_alphabetics_string(10)
        projection_1 = (
            ThingProjectionBuilder()
            .with_name(projection_name)
            .with_state(Thing(value_1=5, value_2="text"))
            .build()
        )
        projection_2 = (
            ThingProjectionBuilder()
            .with_name(projection_name)
            .with_state(Thing(value_1=10, value_2=filter_value))
            .build()
        )

        await adapter.save(projection=projection_1)
        await adapter.save(projection=projection_2)

        located = await adapter.find_one(
            lookup=Lookup(
                filters=[
                    FilterClause(
                        Operator.EQUAL, Path("name"), projection_name
                    ),
                    FilterClause(
                        Operator.EQUAL, Path("state", "value_2"), filter_value
                    ),
                ]
            ),
            state_type=Thing,
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
            state_type=Thing,
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

        await adapter.save(projection=projection_1)
        await adapter.save(projection=projection_2)

        with pytest.raises(ValueError):
            await adapter.find_one(
                lookup=Lookup(
                    filters=[
                        FilterClause(
                            Operator.EQUAL, Path("name"), projection_name
                        )
                    ]
                ),
                state_type=Thing,
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

        await adapter.save(projection=projection_1)
        await adapter.save(projection=projection_2)

        located = await adapter.find_many(
            search=Search(
                filters=[
                    FilterClause(
                        Operator.EQUAL, Path("name"), projection_1_name
                    )
                ]
            ),
            state_type=Thing,
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

        await adapter.save(projection=projection_1)
        await adapter.save(projection=projection_2)
        await adapter.save(projection=projection_3)
        await adapter.save(projection=projection_4)

        located = await adapter.find_many(
            search=Search(
                filters=[
                    FilterClause(Operator.EQUAL, Path("name"), "a"),
                    FilterClause(
                        Operator.GREATER_THAN, Path("state", "value_1"), 5
                    ),
                ]
            ),
            state_type=Thing,
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

        await adapter.save(projection=projection_1)
        await adapter.save(projection=projection_2)
        await adapter.save(projection=projection_3)

        search = Search(
            sort=SortClause(
                fields=[
                    SortField(
                        path=Path("state", "value_1"), order=SortOrder.DESC
                    )
                ]
            )
        )
        located = await adapter.find_many(search=search, state_type=Thing)

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

        await adapter.save(projection=projection_1)
        await adapter.save(projection=projection_2)
        await adapter.save(projection=projection_3)

        search = Search(
            paging=KeySetPagingClause(
                last_id="1", direction=PagingDirection.FORWARDS, item_count=2
            )
        )
        located = await adapter.find_many(search=search, state_type=Thing)

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

        await adapter.save(projection=projection_1)
        await adapter.save(projection=projection_2)
        await adapter.save(projection=projection_3)

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
        located = await adapter.find_many(search=search, state_type=Thing)

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

        await adapter.save(projection=projection_1)
        await adapter.save(projection=projection_2)
        await adapter.save(projection=projection_3)
        await adapter.save(projection=projection_4)

        search = Search(
            filters=[
                FilterClause(
                    Operator.GREATER_THAN, Path("state", "value_1"), 5
                )
            ],
            paging=KeySetPagingClause(item_count=2),
        )
        located = await adapter.find_many(search=search, state_type=Thing)

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

        await adapter.save(projection=projection_1)
        await adapter.save(projection=projection_2)
        await adapter.save(projection=projection_3)
        await adapter.save(projection=projection_4)

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
        located = await adapter.find_many(search=search, state_type=Thing)

        assert located == [projection_4, projection_2]

    async def test_filter_on_value_in_list(self):
        adapter = self.construct_storage_adapter()

        value_to_filter_1 = data.random_ascii_alphanumerics_string(10)
        value_to_filter_2 = data.random_ascii_alphanumerics_string(10)
        other_value = data.random_ascii_alphanumerics_string(10)

        projection_1 = (
            ThingProjectionBuilder()
            .with_id("1")
            .with_state(Thing(value_1=5, value_2=value_to_filter_1))
            .build()
        )
        projection_2 = (
            ThingProjectionBuilder()
            .with_id("2")
            .with_state(Thing(value_1=6, value_2=value_to_filter_2))
            .build()
        )
        projection_3 = (
            ThingProjectionBuilder()
            .with_id("3")
            .with_state(Thing(value_1=7, value_2=other_value))
            .build()
        )

        await adapter.save(projection=projection_1)
        await adapter.save(projection=projection_2)
        await adapter.save(projection=projection_3)

        search = Search(
            filters=[
                FilterClause(
                    Operator.IN,
                    Path("state", "value_2"),
                    [value_to_filter_1, value_to_filter_2],
                )
            ],
            sort=SortClause(
                fields=[
                    SortField(
                        path=Path("state", "value_1"), order=SortOrder.ASC
                    )
                ]
            ),
            paging=KeySetPagingClause(item_count=2),
        )
        located = await adapter.find_many(search=search, state_type=Thing)

        assert located == [projection_1, projection_2]

    async def test_filter_on_list_contains_value(self):
        adapter = self.construct_storage_adapter()

        value_to_filter = data.random_ascii_alphanumerics_string(10)
        other_value1 = data.random_ascii_alphanumerics_string(10)
        other_value2 = data.random_ascii_alphanumerics_string(10)

        projection_1 = (
            ThingProjectionBuilder()
            .with_id("1")
            .with_state(
                Thing(
                    value_1=5,
                    value_3=[other_value1, other_value2],
                )
            )
            .build()
        )
        projection_2 = (
            ThingProjectionBuilder()
            .with_id("2")
            .with_state(Thing(value_1=6, value_3=[value_to_filter]))
            .build()
        )
        projection_3 = (
            ThingProjectionBuilder()
            .with_id("3")
            .with_state(
                Thing(
                    value_1=7,
                    value_3=[value_to_filter, other_value1],
                )
            )
            .build()
        )
        projection_4 = (
            ThingProjectionBuilder()
            .with_id("4")
            .with_state(Thing(value_1=8, value_2="whatever", value_3=[]))
            .build()
        )

        await adapter.save(projection=projection_1)
        await adapter.save(projection=projection_2)
        await adapter.save(projection=projection_3)
        await adapter.save(projection=projection_4)

        search = Search(
            filters=[
                FilterClause(
                    Operator.CONTAINS,
                    Path("state", "value_3"),
                    value_to_filter,
                )
            ],
            sort=SortClause(
                fields=[
                    SortField(
                        path=Path("state", "value_1"), order=SortOrder.ASC
                    )
                ]
            ),
            paging=KeySetPagingClause(item_count=2),
        )
        located = await adapter.find_many(search=search, state_type=Thing)

        assert located == [projection_2, projection_3]


class ProjectionStorageAdapterCases(
    SaveCases, FindOneCases, FindManyCases, ABC
):
    pass
