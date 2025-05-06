from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, Self

from logicblocks.event.projection.store import (
    FilterClause,
    InMemoryProjectionStorageAdapter,
    KeySetPagingClause,
    Operator,
    Path,
    ProjectionStore,
    Similarity,
    SortClause,
    SortField,
    SortOrder,
)
from logicblocks.event.testing import BaseProjectionBuilder, data
from logicblocks.event.testlogging.logger import CapturingLogger, LogLevel
from logicblocks.event.types import (
    JsonValue,
    StreamIdentifier,
    default_deserialisation_fallback,
    str_serialisation_fallback,
)


@dataclass
class Thing:
    amount: int
    description: str

    @classmethod
    def deserialise(
        cls,
        value: JsonValue,
        fallback: Callable[
            [Any, JsonValue], Any
        ] = default_deserialisation_fallback,
    ) -> Self:
        if (
            not isinstance(value, Mapping)
            or "amount" not in value
            or not isinstance(value["amount"], int)
            or "description" not in value
            or not isinstance(value["description"], str)
        ):
            return fallback(cls, value)

        return cls(amount=value["amount"], description=value["description"])

    def serialise(self, fallback: Callable[[object], JsonValue]) -> JsonValue:
        return {"amount": self.amount, "description": self.description}


class ThingProjectionBuilder(BaseProjectionBuilder[Thing, Mapping[str, str]]):
    def default_state_factory(self) -> Thing:
        return Thing(
            amount=data.random_int(1, 10),
            description=data.random_lowercase_ascii_alphabetics_string(
                length=10
            ),
        )

    def default_metadata_factory(self) -> Mapping[str, str]:
        return {}


class TestProjectionStoreSave:
    async def test_stores_and_loads_projection(self):
        projection_id = data.random_projection_id()
        projection_name = data.random_projection_name()

        adapter = InMemoryProjectionStorageAdapter()
        store = ProjectionStore(adapter=adapter)

        projection = (
            ThingProjectionBuilder()
            .with_name(projection_name)
            .with_id(projection_id)
            .build()
        )

        await store.save(projection=projection)

        located = await store.load(
            name=projection_name, id=projection_id, state_type=Thing
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
            .with_state(Thing(amount=5, description="a"))
            .with_source(source)
            .with_metadata({"updated_at": "2024-01-01T00:00:00Z"})
            .build()
        )

        await store.save(projection=projection)

        updated_projection = (
            ThingProjectionBuilder()
            .with_id(projection_id)
            .with_name(projection_name)
            .with_state(Thing(amount=10, description="a"))
            .with_source(source)
            .with_metadata({"updated_at": "2024-02-02T00:00:00Z"})
            .build()
        )

        await store.save(projection=updated_projection)

        located = await store.load(
            name=projection_name, id=projection_id, state_type=Thing
        )

        assert located == updated_projection


class TestProjectionStoreLoad:
    async def test_loads_correct_projection_by_name_and_id(self):
        projection_id_1 = data.random_projection_id()
        projection_id_2 = data.random_projection_id()

        projection_name_1 = data.random_projection_name()
        projection_name_2 = data.random_projection_name()

        adapter = InMemoryProjectionStorageAdapter()
        store = ProjectionStore(adapter=adapter)

        projection_1 = (
            ThingProjectionBuilder()
            .with_name(projection_name_1)
            .with_id(projection_id_1)
            .build()
        )
        projection_2 = (
            ThingProjectionBuilder()
            .with_name(projection_name_2)
            .with_id(projection_id_2)
            .build()
        )
        projection_3 = (
            ThingProjectionBuilder()
            .with_name(projection_name_2)
            .with_id(projection_id_1)
            .build()
        )

        await store.save(projection=projection_1)
        await store.save(projection=projection_2)
        await store.save(projection=projection_3)

        located_1 = await store.load(
            name=projection_name_1, id=projection_id_1, state_type=Thing
        )
        located_2 = await store.load(
            name=projection_name_2, id=projection_id_2, state_type=Thing
        )
        located_3 = await store.load(
            name=projection_name_2, id=projection_id_1, state_type=Thing
        )

        assert [located_1, located_2, located_3] == [
            projection_1,
            projection_2,
            projection_3,
        ]

    async def test_returns_none_when_no_projection(self):
        adapter = InMemoryProjectionStorageAdapter()
        store = ProjectionStore(adapter=adapter)

        located = await store.load(
            name=data.random_projection_name(),
            id=data.random_projection_id(),
            state_type=Thing,
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

        await store.save(projection=projection_1)
        await store.save(projection=projection_2)
        await store.save(projection=projection_3)

        located = await store.locate(
            source=source_1, name=projection_name_1, state_type=Thing
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
            state_type=Thing,
        )

        assert located is None


class TestProjectionStoreSearch:
    async def test_filters_projections_applying_sort_and_paging(self):
        adapter = InMemoryProjectionStorageAdapter()
        store = ProjectionStore(adapter=adapter)

        projection_1 = (
            ThingProjectionBuilder()
            .with_id("a")
            .with_state(Thing(amount=5, description="a"))
            .build()
        )
        projection_2 = (
            ThingProjectionBuilder()
            .with_id("b")
            .with_state(Thing(amount=5, description="a"))
            .build()
        )
        projection_3 = (
            ThingProjectionBuilder()
            .with_id("c")
            .with_state(Thing(amount=10, description="a"))
            .build()
        )
        projection_4 = (
            ThingProjectionBuilder()
            .with_id("d")
            .with_state(Thing(amount=5, description="a"))
            .build()
        )

        await store.save(projection=projection_1)
        await store.save(projection=projection_2)
        await store.save(projection=projection_3)
        await store.save(projection=projection_4)

        located = await store.search(
            filters=[FilterClause(Operator.EQUAL, Path("state", "amount"), 5)],
            sort=SortClause(fields=[SortField(Path("id"), SortOrder.DESC)]),
            paging=KeySetPagingClause(item_count=2),
            state_type=Thing,
        )

        assert located == [projection_4, projection_2]

    async def test_allows_use_of_calculations_in_sort(self):
        adapter = InMemoryProjectionStorageAdapter()
        store = ProjectionStore(adapter=adapter)

        projection_1 = (
            ThingProjectionBuilder()
            .with_id("a")
            .with_state(Thing(amount=5, description="abcd"))
            .build()
        )
        projection_2 = (
            ThingProjectionBuilder()
            .with_id("b")
            .with_state(Thing(amount=5, description="defg"))
            .build()
        )
        projection_3 = (
            ThingProjectionBuilder()
            .with_id("c")
            .with_state(Thing(amount=5, description="hij"))
            .build()
        )
        projection_4 = (
            ThingProjectionBuilder()
            .with_id("d")
            .with_state(Thing(amount=5, description="bcd"))
            .build()
        )

        await store.save(projection=projection_1)
        await store.save(projection=projection_2)
        await store.save(projection=projection_3)
        await store.save(projection=projection_4)

        located = await store.search(
            filters=[],
            sort=SortClause(
                fields=[
                    SortField(
                        Similarity(Path("state", "description"), "abc"),
                        SortOrder.DESC,
                    )
                ]
            ),
            paging=KeySetPagingClause(item_count=10),
            state_type=Thing,
        )

        assert located == [
            projection_1,
            projection_4,
            projection_2,
            projection_3,
        ]

    async def test_returns_no_projections_when_no_matches(self):
        adapter = InMemoryProjectionStorageAdapter()
        store = ProjectionStore(adapter=adapter)

        projection_1 = (
            ThingProjectionBuilder()
            .with_name("aggregate")
            .with_id("a")
            .with_state(Thing(amount=5, description="a"))
            .build()
        )
        projection_2 = (
            ThingProjectionBuilder()
            .with_name("aggregate")
            .with_id("b")
            .with_state(Thing(amount=5, description="a"))
            .build()
        )
        projection_3 = (
            ThingProjectionBuilder()
            .with_name("aggregate")
            .with_id("c")
            .with_state(Thing(amount=10, description="a"))
            .build()
        )
        projection_4 = (
            ThingProjectionBuilder()
            .with_name("aggregate")
            .with_id("d")
            .with_state(Thing(amount=5, description="a"))
            .build()
        )

        await store.save(projection=projection_1)
        await store.save(projection=projection_2)
        await store.save(projection=projection_3)
        await store.save(projection=projection_4)

        located = await store.search(
            filters=[
                FilterClause(Operator.EQUAL, Path("state", "amount"), 5),
                FilterClause(Operator.EQUAL, Path("name"), "metrics"),
            ],
            sort=SortClause(fields=[SortField(Path("id"), SortOrder.DESC)]),
            paging=KeySetPagingClause(item_count=2),
            state_type=Thing,
        )

        assert located == []


class TestProjectionStoreLogging:
    async def test_logs_projection_on_save_when_debug(self):
        logger = CapturingLogger.create(log_level=LogLevel.DEBUG)

        projection_id = data.random_projection_id()

        adapter = InMemoryProjectionStorageAdapter()
        store = ProjectionStore(adapter=adapter, logger=logger)

        source = StreamIdentifier(
            category=data.random_event_category_name(),
            stream=data.random_event_stream_name(),
        )

        projection = (
            ThingProjectionBuilder()
            .with_id(projection_id)
            .with_name("thing")
            .with_state(Thing(amount=5, description="a"))
            .with_source(source)
            .build()
        )

        await store.save(projection=projection)

        log_event = logger.find_event("event.projection.saved")

        assert log_event is not None
        assert log_event.level == LogLevel.INFO
        assert log_event.is_async is True
        assert log_event.context == {
            "projection": projection.serialise(
                fallback=str_serialisation_fallback
            ),
        }

    async def test_logs_envelope_on_save_when_not_debug(self):
        logger = CapturingLogger.create(log_level=LogLevel.INFO)

        projection_id = data.random_projection_id()

        adapter = InMemoryProjectionStorageAdapter()
        store = ProjectionStore(adapter=adapter, logger=logger)

        source = StreamIdentifier(
            category=data.random_event_category_name(),
            stream=data.random_event_stream_name(),
        )

        projection = (
            ThingProjectionBuilder()
            .with_id(projection_id)
            .with_name("thing")
            .with_state(Thing(amount=5, description="a"))
            .with_source(source)
            .build()
        )

        await store.save(projection=projection)

        log_event = logger.find_event("event.projection.saved")

        assert log_event is not None
        assert log_event.level == LogLevel.INFO
        assert log_event.is_async is True
        assert log_event.context == {
            "projection": projection.summarise(),
        }

    async def test_logs_on_load(self):
        logger = CapturingLogger.create()

        projection_id = data.random_projection_id()
        projection_name = data.random_projection_name()

        adapter = InMemoryProjectionStorageAdapter()
        store = ProjectionStore(adapter=adapter, logger=logger)

        projection = (
            ThingProjectionBuilder()
            .with_name(projection_name)
            .with_id(projection_id)
            .build()
        )

        await store.save(projection=projection)

        await store.load(
            name=projection_name, id=projection_id, state_type=Thing
        )

        log_event = logger.find_event("event.projection.loading")

        assert log_event is not None
        assert log_event.level == LogLevel.DEBUG
        assert log_event.is_async is True
        assert log_event.context == {
            "projection_id": projection_id,
        }

    async def test_logs_on_locate(self):
        logger = CapturingLogger.create()

        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()
        projection_name = data.random_projection_name()

        source = StreamIdentifier(category=category_name, stream=stream_name)

        adapter = InMemoryProjectionStorageAdapter()
        store = ProjectionStore(adapter=adapter, logger=logger)

        projection = (
            ThingProjectionBuilder()
            .with_name(projection_name)
            .with_source(source)
            .build()
        )

        await store.save(projection=projection)
        await store.locate(
            source=source, name=projection_name, state_type=Thing
        )

        log_event = logger.find_event("event.projection.locating")

        assert log_event is not None
        assert log_event.level == LogLevel.DEBUG
        assert log_event.is_async is True
        assert log_event.context == {
            "projection_name": projection_name,
            "projection_source": source.serialise(
                fallback=str_serialisation_fallback
            ),
        }

    async def test_logs_on_search(self):
        logger = CapturingLogger.create()

        adapter = InMemoryProjectionStorageAdapter()
        store = ProjectionStore(adapter=adapter, logger=logger)

        filter = FilterClause(Operator.EQUAL, Path("state", "amount"), 5)
        sort = SortClause(fields=[SortField(Path("id"), SortOrder.DESC)])
        paging = KeySetPagingClause(item_count=2)

        await store.search(
            filters=[filter], sort=sort, paging=paging, state_type=Thing
        )

        log_event = logger.find_event("event.projection.searching")

        assert log_event is not None
        assert log_event.level == LogLevel.DEBUG
        assert log_event.is_async is True
        assert log_event.context == {
            "filters": [repr(filter)],
            "sort": repr(sort),
            "paging": repr(paging),
        }
