from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import pytest

from logicblocks.event.projection.store import (
    Clause,
    FilterClause,
    KeySetPagingClause,
    Lookup,
    OffsetPagingClause,
    Operator,
    Path,
    ProjectionStorageAdapter,
    Query,
    Search,
    SortClause,
    SortField,
    SortOrder,
)
from logicblocks.event.projection.store.adapters import (
    InMemoryProjectionResultSetTransformer,
    InMemoryProjectionStorageAdapter,
    InMemoryQueryConverter,
)
from logicblocks.event.projection.store.adapters.in_memory import (
    key_set_paging_clause_converter,
    sort_clause_converter,
)
from logicblocks.event.projection.store.query import PagingDirection
from logicblocks.event.testcases.projection.store.adapters import (
    ProjectionStorageAdapterCases,
)
from logicblocks.event.testing import (
    BaseProjectionBuilder,
    MappingProjectionBuilder,
    data,
)
from logicblocks.event.types import Projection


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


class TestInMemoryProjectionStorageAdapter(ProjectionStorageAdapterCases):
    def construct_storage_adapter(self) -> ProjectionStorageAdapter:
        return InMemoryProjectionStorageAdapter()

    async def clear_storage(self) -> None:
        pass

    async def retrieve_projections(
        self,
        *,
        adapter: ProjectionStorageAdapter,
    ) -> Sequence[Projection[Mapping[str, Any]]]:
        def identity(mapping: Mapping[str, Any]) -> Mapping[str, Any]:
            return mapping

        return await adapter.find_many(search=Search(), converter=identity)


class TestInMemoryQueryConverterClauseConverterRegistration:
    def test_registers_clause_converter_and_converts_clause(self):
        registry = InMemoryQueryConverter()

        class TakeClause(Clause):
            value: int = 2

        def convert_clause(
            take_clause: TakeClause,
        ) -> InMemoryProjectionResultSetTransformer:
            def apply_clause(
                projections: Sequence[Projection[Mapping[str, Any]]],
            ) -> Sequence[Projection[Mapping[str, Any]]]:
                return projections[: take_clause.value]

            return apply_clause

        registry.register_clause_converter(TakeClause, convert_clause)

        clause = TakeClause()
        transformer = registry.convert_clause(clause)

        projection_1 = MappingProjectionBuilder().build()
        projection_2 = MappingProjectionBuilder().build()
        projection_3 = MappingProjectionBuilder().build()

        transformed_projections = transformer(
            [projection_1, projection_2, projection_3]
        )

        assert transformed_projections == [projection_1, projection_2]

    def test_replaces_existing_clause_converter(self):
        registry = InMemoryQueryConverter()

        class TakeClause(Clause):
            value: int = 2

        def convert_clause_1(
            take_clause: TakeClause,
        ) -> InMemoryProjectionResultSetTransformer:
            def apply_clause(
                projections: Sequence[Projection[Mapping[str, Any]]],
            ) -> Sequence[Projection[Mapping[str, Any]]]:
                return projections[: take_clause.value]

            return apply_clause

        def convert_clause_2(
            _take_clause: TakeClause,
        ) -> InMemoryProjectionResultSetTransformer:
            def apply_clause(
                projections: Sequence[Projection[Mapping[str, Any]]],
            ) -> Sequence[Projection[Mapping[str, Any]]]:
                return projections

            return apply_clause

        registry = registry.register_clause_converter(
            TakeClause, convert_clause_1
        ).register_clause_converter(TakeClause, convert_clause_2)

        clause = TakeClause()
        transformer = registry.convert_clause(clause)

        projection_1 = MappingProjectionBuilder().build()
        projection_2 = MappingProjectionBuilder().build()
        projection_3 = MappingProjectionBuilder().build()

        transformed_projections = transformer(
            [projection_1, projection_2, projection_3]
        )

        assert transformed_projections == [
            projection_1,
            projection_2,
            projection_3,
        ]


class TestInMemoryQueryConverterClauseConversion:
    def test_raises_for_unregistered_clause_type(self):
        registry = InMemoryQueryConverter()

        class TakeClause(Clause):
            value: int = 2

        clause = TakeClause()

        with pytest.raises(ValueError):
            registry.convert_clause(clause)


class TestInMemoryQueryConverterQueryConversion:
    def test_converts_lookup_with_multiple_filter_clauses(self):
        registry = InMemoryQueryConverter()

        class SkipClause(Clause):
            number: int = 2

        class TakeClause(Clause):
            number: int = 1

        def make_skip_transformer(
            skip_clause: SkipClause,
        ) -> InMemoryProjectionResultSetTransformer:
            def apply_clause(
                projections: Sequence[Projection[Mapping[str, Any]]],
            ) -> Sequence[Projection[Mapping[str, Any]]]:
                return projections[skip_clause.number :]

            return apply_clause

        def make_take_transformer(
            take_clause: TakeClause,
        ) -> InMemoryProjectionResultSetTransformer:
            def apply_clause(
                projections: Sequence[Projection[Mapping[str, Any]]],
            ) -> Sequence[Projection[Mapping[str, Any]]]:
                return projections[: take_clause.number]

            return apply_clause

        registry = registry.register_clause_converter(
            SkipClause, make_skip_transformer
        ).register_clause_converter(TakeClause, make_take_transformer)

        query = Lookup(
            filters=[
                SkipClause(),
                TakeClause(),
            ]
        )

        transformer = registry.convert_query(query)

        projection_1 = MappingProjectionBuilder().build()
        projection_2 = MappingProjectionBuilder().build()
        projection_3 = MappingProjectionBuilder().build()
        projection_4 = MappingProjectionBuilder().build()
        projection_5 = MappingProjectionBuilder().build()

        results = transformer(
            [
                projection_1,
                projection_2,
                projection_3,
                projection_4,
                projection_5,
            ]
        )

        assert results == [projection_3]

    def test_converts_search_with_sort_clause(self):
        registry = InMemoryQueryConverter()
        registry.register_clause_converter(SortClause, sort_clause_converter)

        transformer = registry.convert_query(
            Search(
                sort=(
                    SortClause(
                        fields=[
                            SortField(path=Path("id"), order=SortOrder.ASC)
                        ]
                    )
                )
            )
        )

        projection_1 = MappingProjectionBuilder().with_id("456").build()
        projection_2 = MappingProjectionBuilder().with_id("123").build()
        projection_3 = MappingProjectionBuilder().with_id("789").build()

        results = transformer([projection_1, projection_2, projection_3])

        assert results == [projection_2, projection_1, projection_3]

    def test_converts_search_with_paging_clause(self):
        registry = InMemoryQueryConverter()
        registry.register_clause_converter(
            KeySetPagingClause, key_set_paging_clause_converter
        )

        transformer = registry.convert_query(
            Search(paging=KeySetPagingClause(item_count=2))
        )

        projection_1 = MappingProjectionBuilder().build()
        projection_2 = MappingProjectionBuilder().build()
        projection_3 = MappingProjectionBuilder().build()

        results = transformer([projection_1, projection_2, projection_3])

        assert results == [projection_1, projection_2]

    def test_raises_for_unsupported_query_type(self):
        registry = InMemoryQueryConverter()

        class UnsupportedQuery(Query):
            pass

        query = UnsupportedQuery()

        with pytest.raises(ValueError):
            registry.convert_query(query)


class TestInMemoryQueryConverterDefaultClauseConverters:
    def test_filter_top_level_attribute(self):
        registry = InMemoryQueryConverter().with_default_clause_converters()

        clause = FilterClause(Operator.EQUAL, Path("id"), "123")

        transformer = registry.convert_clause(clause)

        projection_1 = MappingProjectionBuilder().with_id("123").build()
        projection_2 = MappingProjectionBuilder().with_id("456").build()

        results = transformer([projection_1, projection_2])

        assert results == [projection_1]

    def test_filter_nested_state_attribute(self):
        registry = InMemoryQueryConverter().with_default_clause_converters()

        clause = FilterClause(Operator.EQUAL, Path("state", "value_1"), 5)

        transformer = registry.convert_clause(clause)

        projection_1 = (
            MappingProjectionBuilder()
            .with_state({"value_1": 5, "value_2": "text"})
            .build()
        )
        projection_2 = (
            MappingProjectionBuilder()
            .with_state({"value_1": 10, "value_2": "text"})
            .build()
        )

        results = transformer([projection_1, projection_2])

        assert results == [projection_1]

    def test_filter_not_equal(self):
        registry = InMemoryQueryConverter().with_default_clause_converters()

        clause = FilterClause(Operator.NOT_EQUAL, Path("state", "value_1"), 5)

        transformer = registry.convert_clause(clause)

        projection_1 = (
            MappingProjectionBuilder()
            .with_state({"value_1": 5, "value_2": "text"})
            .build()
        )
        projection_2 = (
            MappingProjectionBuilder()
            .with_state({"value_1": 10, "value_2": "text"})
            .build()
        )

        results = transformer([projection_1, projection_2])

        assert results == [projection_2]

    def test_filter_greater_than(self):
        registry = InMemoryQueryConverter().with_default_clause_converters()

        clause = FilterClause(
            Operator.GREATER_THAN, Path("state", "value_1"), 5
        )

        transformer = registry.convert_clause(clause)

        projection_1 = (
            MappingProjectionBuilder()
            .with_state({"value_1": 5, "value_2": "text"})
            .build()
        )
        projection_2 = (
            MappingProjectionBuilder()
            .with_state({"value_1": 10, "value_2": "text"})
            .build()
        )

        results = transformer([projection_1, projection_2])

        assert results == [projection_2]

    def test_filter_greater_than_or_equal(self):
        registry = InMemoryQueryConverter().with_default_clause_converters()

        clause = FilterClause(
            Operator.GREATER_THAN_OR_EQUAL, Path("state", "value_1"), 6
        )

        transformer = registry.convert_clause(clause)

        projection_1 = (
            MappingProjectionBuilder()
            .with_state({"value_1": 5, "value_2": "text"})
            .build()
        )
        projection_2 = (
            MappingProjectionBuilder()
            .with_state({"value_1": 6, "value_2": "text"})
            .build()
        )
        projection_3 = (
            MappingProjectionBuilder()
            .with_state({"value_1": 10, "value_2": "text"})
            .build()
        )

        results = transformer([projection_1, projection_2, projection_3])

        assert results == [projection_2, projection_3]

    def test_filter_less_than(self):
        registry = InMemoryQueryConverter().with_default_clause_converters()

        clause = FilterClause(Operator.LESS_THAN, Path("state", "value_1"), 10)

        transformer = registry.convert_clause(clause)

        projection_1 = (
            MappingProjectionBuilder()
            .with_state({"value_1": 5, "value_2": "text"})
            .build()
        )
        projection_2 = (
            MappingProjectionBuilder()
            .with_state({"value_1": 10, "value_2": "text"})
            .build()
        )

        results = transformer([projection_1, projection_2])

        assert results == [projection_1]

    def test_filter_less_than_or_equal(self):
        registry = InMemoryQueryConverter().with_default_clause_converters()

        clause = FilterClause(
            Operator.LESS_THAN_OR_EQUAL, Path("state", "value_1"), 6
        )

        transformer = registry.convert_clause(clause)

        projection_1 = (
            MappingProjectionBuilder()
            .with_state({"value_1": 5, "value_2": "text"})
            .build()
        )
        projection_2 = (
            MappingProjectionBuilder()
            .with_state({"value_1": 6, "value_2": "text"})
            .build()
        )
        projection_3 = (
            MappingProjectionBuilder()
            .with_state({"value_1": 10, "value_2": "text"})
            .build()
        )

        results = transformer([projection_1, projection_2, projection_3])

        assert results == [projection_1, projection_2]

    def test_filter_on_non_existent_top_level_attribute(self):
        registry = InMemoryQueryConverter().with_default_clause_converters()

        clause = FilterClause(Operator.EQUAL, Path("non_existent"), 10)

        transformer = registry.convert_clause(clause)

        projection_1 = MappingProjectionBuilder().build()
        projection_2 = MappingProjectionBuilder().build()

        with pytest.raises(ValueError) as e:
            transformer([projection_1, projection_2])

        assert str(e.value) == f"Invalid projection path: {['non_existent']}."

    def test_filter_on_non_existent_nested_attribute(self):
        registry = InMemoryQueryConverter().with_default_clause_converters()

        clause = FilterClause(Operator.EQUAL, Path("state", "value_3"), 10)

        transformer = registry.convert_clause(clause)

        projection_1 = (
            MappingProjectionBuilder()
            .with_state({"value_1": 5, "value_2": "text"})
            .build()
        )
        projection_2 = (
            MappingProjectionBuilder()
            .with_state({"value_1": 10, "value_2": "text"})
            .build()
        )

        with pytest.raises(ValueError) as e:
            transformer([projection_1, projection_2])

        assert (
            str(e.value) == f"Invalid projection path: {['state', 'value_3']}."
        )

    def test_sort_clause_over_single_field_on_top_level_attribute(self):
        registry = InMemoryQueryConverter().with_default_clause_converters()

        clause = SortClause(
            fields=[SortField(path=Path("id"), order=SortOrder.ASC)]
        )

        transformer = registry.convert_clause(clause)

        projection_1 = MappingProjectionBuilder().with_id("456").build()
        projection_2 = MappingProjectionBuilder().with_id("123").build()
        projection_3 = MappingProjectionBuilder().with_id("789").build()

        results = transformer([projection_1, projection_2, projection_3])

        assert results == [projection_2, projection_1, projection_3]

    def test_sort_clause_over_multiple_fields_on_top_level_attributes(self):
        registry = InMemoryQueryConverter().with_default_clause_converters()

        clause = SortClause(
            fields=[
                SortField(path=Path("name"), order=SortOrder.ASC),
                SortField(path=Path("id"), order=SortOrder.DESC),
            ]
        )

        transformer = registry.convert_clause(clause)

        projection_1 = (
            MappingProjectionBuilder()
            .with_name("thing2")
            .with_id("123")
            .build()
        )
        projection_2 = (
            MappingProjectionBuilder()
            .with_name("thing2")
            .with_id("456")
            .build()
        )
        projection_3 = (
            MappingProjectionBuilder()
            .with_name("thing1")
            .with_id("789")
            .build()
        )

        results = transformer([projection_1, projection_2, projection_3])

        assert results == [projection_3, projection_2, projection_1]

    def test_sort_clause_over_single_field_on_nested_state_attribute(self):
        registry = InMemoryQueryConverter().with_default_clause_converters()

        clause = SortClause(
            fields=[
                SortField(path=Path("state", "value_1"), order=SortOrder.ASC),
            ]
        )

        transformer = registry.convert_clause(clause)

        projection_1 = (
            MappingProjectionBuilder()
            .with_state({"value_1": 5, "value_2": "C"})
            .build()
        )
        projection_2 = (
            MappingProjectionBuilder()
            .with_state({"value_1": 8, "value_2": "B"})
            .build()
        )
        projection_3 = (
            MappingProjectionBuilder()
            .with_state({"value_1": 6, "value_2": "C"})
            .build()
        )

        results = transformer([projection_1, projection_2, projection_3])

        assert results == [projection_1, projection_3, projection_2]

    def test_sort_clause_over_multiple_fields_on_nested_state_attribute(self):
        registry = InMemoryQueryConverter().with_default_clause_converters()

        clause = SortClause(
            fields=[
                SortField(path=Path("state", "value_1"), order=SortOrder.DESC),
                SortField(path=Path("state", "value_2"), order=SortOrder.ASC),
            ]
        )

        transformer = registry.convert_clause(clause)

        projection_1 = (
            MappingProjectionBuilder()
            .with_state({"value_1": 6, "value_2": "C"})
            .build()
        )
        projection_2 = (
            MappingProjectionBuilder()
            .with_state({"value_1": 8, "value_2": "B"})
            .build()
        )
        projection_3 = (
            MappingProjectionBuilder()
            .with_state({"value_1": 6, "value_2": "A"})
            .build()
        )

        results = transformer([projection_1, projection_2, projection_3])

        assert results == [projection_2, projection_3, projection_1]

    def test_offset_paging_clause_first_page(self):
        registry = InMemoryQueryConverter().with_default_clause_converters()

        clause = OffsetPagingClause(item_count=2)

        transformer = registry.convert_clause(clause)

        projection_1 = MappingProjectionBuilder().build()
        projection_2 = MappingProjectionBuilder().build()
        projection_3 = MappingProjectionBuilder().build()

        results = transformer([projection_1, projection_2, projection_3])

        assert results == [projection_1, projection_2]

    def test_offset_paging_clause_subsequent_page(self):
        registry = InMemoryQueryConverter().with_default_clause_converters()

        clause = OffsetPagingClause(page_number=2, item_count=2)

        transformer = registry.convert_clause(clause)

        projection_1 = MappingProjectionBuilder().build()
        projection_2 = MappingProjectionBuilder().build()
        projection_3 = MappingProjectionBuilder().build()
        projection_4 = MappingProjectionBuilder().build()
        projection_5 = MappingProjectionBuilder().build()

        results = transformer(
            [
                projection_1,
                projection_2,
                projection_3,
                projection_4,
                projection_5,
            ]
        )

        assert results == [projection_3, projection_4]

    def test_offset_paging_clause_empty_page(self):
        registry = InMemoryQueryConverter().with_default_clause_converters()

        clause = OffsetPagingClause(page_number=5, item_count=2)

        transformer = registry.convert_clause(clause)

        projection_1 = MappingProjectionBuilder().build()
        projection_2 = MappingProjectionBuilder().build()
        projection_3 = MappingProjectionBuilder().build()

        results = transformer([projection_1, projection_2, projection_3])

        assert results == []

    def test_key_set_paging_clause_first_page(self):
        registry = InMemoryQueryConverter().with_default_clause_converters()

        clause = KeySetPagingClause(item_count=2)

        transformer = registry.convert_clause(clause)

        projection_1 = MappingProjectionBuilder().build()
        projection_2 = MappingProjectionBuilder().build()
        projection_3 = MappingProjectionBuilder().build()

        results = transformer([projection_1, projection_2, projection_3])

        assert results == [projection_1, projection_2]

    def test_key_set_paging_clause_full_page_paging_forwards(self):
        registry = InMemoryQueryConverter().with_default_clause_converters()

        projection_1 = MappingProjectionBuilder().with_id("1").build()
        projection_2 = MappingProjectionBuilder().with_id("2").build()
        projection_3 = MappingProjectionBuilder().with_id("3").build()
        projection_4 = MappingProjectionBuilder().with_id("4").build()
        projection_5 = MappingProjectionBuilder().with_id("5").build()

        clause = KeySetPagingClause(
            last_id="2", direction=PagingDirection.FORWARDS, item_count=2
        )

        transformer = registry.convert_clause(clause)

        results = transformer(
            [
                projection_1,
                projection_2,
                projection_3,
                projection_4,
                projection_5,
            ]
        )

        assert results == [projection_3, projection_4]

    def test_key_set_paging_clause_partial_page_paging_forwards(self):
        registry = InMemoryQueryConverter().with_default_clause_converters()

        projection_1 = MappingProjectionBuilder().with_id("1").build()
        projection_2 = MappingProjectionBuilder().with_id("2").build()
        projection_3 = MappingProjectionBuilder().with_id("3").build()

        clause = KeySetPagingClause(
            last_id="2", direction=PagingDirection.FORWARDS, item_count=2
        )

        transformer = registry.convert_clause(clause)

        results = transformer(
            [
                projection_1,
                projection_2,
                projection_3,
            ]
        )

        assert results == [projection_3]

    def test_key_set_paging_clause_after_id_not_present_paging_forwards(self):
        registry = InMemoryQueryConverter().with_default_clause_converters()

        projection_1 = MappingProjectionBuilder().with_id("1").build()
        projection_2 = MappingProjectionBuilder().with_id("2").build()
        projection_3 = MappingProjectionBuilder().with_id("3").build()

        clause = KeySetPagingClause(
            last_id="4", direction=PagingDirection.FORWARDS, item_count=2
        )

        transformer = registry.convert_clause(clause)

        results = transformer(
            [
                projection_1,
                projection_2,
                projection_3,
            ]
        )

        assert results == []

    def test_key_set_paging_clause_after_id_is_last_item_id_paging_forwards(
        self,
    ):
        registry = InMemoryQueryConverter().with_default_clause_converters()

        projection_1 = MappingProjectionBuilder().with_id("1").build()
        projection_2 = MappingProjectionBuilder().with_id("2").build()
        projection_3 = MappingProjectionBuilder().with_id("3").build()

        clause = KeySetPagingClause(
            last_id="3", direction=PagingDirection.FORWARDS, item_count=2
        )

        transformer = registry.convert_clause(clause)

        results = transformer(
            [
                projection_1,
                projection_2,
                projection_3,
            ]
        )

        assert results == []

    def test_key_set_paging_clause_full_page_paging_backwards(self):
        registry = InMemoryQueryConverter().with_default_clause_converters()

        projection_1 = MappingProjectionBuilder().with_id("1").build()
        projection_2 = MappingProjectionBuilder().with_id("2").build()
        projection_3 = MappingProjectionBuilder().with_id("3").build()
        projection_4 = MappingProjectionBuilder().with_id("4").build()
        projection_5 = MappingProjectionBuilder().with_id("5").build()

        clause = KeySetPagingClause(
            last_id="4", direction=PagingDirection.BACKWARDS, item_count=2
        )

        transformer = registry.convert_clause(clause)

        results = transformer(
            [
                projection_1,
                projection_2,
                projection_3,
                projection_4,
                projection_5,
            ]
        )

        assert results == [projection_2, projection_3]

    def test_key_set_paging_clause_partial_page_paging_backwards(self):
        registry = InMemoryQueryConverter().with_default_clause_converters()

        projection_1 = MappingProjectionBuilder().with_id("1").build()
        projection_2 = MappingProjectionBuilder().with_id("2").build()
        projection_3 = MappingProjectionBuilder().with_id("3").build()

        clause = KeySetPagingClause(
            last_id="2", direction=PagingDirection.BACKWARDS, item_count=2
        )

        transformer = registry.convert_clause(clause)

        results = transformer([projection_1, projection_2, projection_3])

        assert results == [projection_1]

    def test_key_set_paging_clause_before_id_not_present_paging_backwards(
        self,
    ):
        registry = InMemoryQueryConverter().with_default_clause_converters()

        projection_1 = MappingProjectionBuilder().with_id("1").build()
        projection_2 = MappingProjectionBuilder().with_id("2").build()
        projection_3 = MappingProjectionBuilder().with_id("3").build()

        clause = KeySetPagingClause(
            last_id="4", direction=PagingDirection.BACKWARDS, item_count=2
        )

        transformer = registry.convert_clause(clause)

        results = transformer(
            [
                projection_1,
                projection_2,
                projection_3,
            ]
        )

        assert results == []

    def test_key_set_paging_clause_before_id_is_first_item_id_paging_backwards(
        self,
    ):
        registry = InMemoryQueryConverter().with_default_clause_converters()

        projection_1 = MappingProjectionBuilder().with_id("1").build()
        projection_2 = MappingProjectionBuilder().with_id("2").build()
        projection_3 = MappingProjectionBuilder().with_id("3").build()

        clause = KeySetPagingClause(
            last_id="1", direction=PagingDirection.BACKWARDS, item_count=2
        )

        transformer = registry.convert_clause(clause)

        results = transformer(
            [
                projection_1,
                projection_2,
                projection_3,
            ]
        )

        assert results == []

    def test_filter_on_value_in_list(self):
        registry = InMemoryQueryConverter().with_default_clause_converters()

        value_to_filter_1 = data.random_ascii_alphanumerics_string(10)
        value_to_filter_2 = data.random_ascii_alphanumerics_string(10)
        other_value = data.random_ascii_alphanumerics_string(10)
        clause = FilterClause(
            Operator.IN,
            Path("state", "value_2"),
            [value_to_filter_1, value_to_filter_2],
        )

        transformer = registry.convert_clause(clause)

        projection_1 = (
            MappingProjectionBuilder()
            .with_state({"value_1": 5, "value_2": value_to_filter_1})
            .build()
        )
        projection_2 = (
            MappingProjectionBuilder()
            .with_state({"value_1": 10, "value_2": value_to_filter_2})
            .build()
        )
        projection_3 = (
            MappingProjectionBuilder()
            .with_state({"value_1": 15, "value_2": other_value})
            .build()
        )

        results = transformer([projection_1, projection_2, projection_3])

        assert results == [projection_1, projection_2]
