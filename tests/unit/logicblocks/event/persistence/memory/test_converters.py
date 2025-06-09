from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Callable, Self, final

import pytest

from logicblocks.event.persistence.memory import (
    ClauseConverter,
    DelegatingQueryConverter,
    FunctionConverter,
    KeySetPagingClauseConverter,
    QueryConverter,
    ResultSetTransformer,
    SortClauseConverter,
    TypeRegistryFunctionConverter,
)
from logicblocks.event.persistence.memory.converters.types import (
    Result,
    ResultSet,
)
from logicblocks.event.query import (
    Clause,
    FilterClause,
    Function,
    KeySetPagingClause,
    Lookup,
    OffsetPagingClause,
    Operator,
    PagingDirection,
    Path,
    Query,
    Search,
    Similarity,
    SortClause,
    SortField,
    SortOrder,
)
from logicblocks.event.testing import (
    BaseProjectionBuilder,
    MappingProjectionBuilder,
    data,
)
from logicblocks.event.types import JsonValue, JsonValueConvertible, Projection


@dataclass
class Thing(JsonValueConvertible):
    value_1: int
    value_2: str

    @classmethod
    def deserialise(
        cls, value: JsonValue, fallback: Callable[[Any, JsonValue], Any]
    ) -> Self:
        if (
            not isinstance(value, Mapping)
            or "value_1" not in value
            or "value_2" not in value
            or not isinstance(value["value_1"], int)
            or not isinstance(value["value_2"], str)
        ):
            return fallback(cls, value)

        return cls(value_1=value["value_1"], value_2=value["value_2"])

    def serialise(self, fallback: Callable[[object], JsonValue]) -> JsonValue:
        return {"value_1": self.value_1, "value_2": self.value_2}


class ThingProjectionBuilder(BaseProjectionBuilder[Thing, Mapping[str, Any]]):
    def default_state_factory(self) -> Thing:
        return Thing(
            value_1=data.random_int(1, 10),
            value_2=data.random_ascii_alphanumerics_string(),
        )

    def default_metadata_factory(self) -> Mapping[str, Any]:
        return {}


class TestDelegatingQueryConverterClauseConverterRegistration:
    def test_registers_clause_converter_and_converts_clause(self):
        converter = DelegatingQueryConverter()

        class TakeClause(Clause):
            value: int = 2

        class TakeClauseConverter(
            ClauseConverter[Projection[JsonValue], TakeClause]
        ):
            def convert(
                self, item: TakeClause
            ) -> ResultSetTransformer[Projection[JsonValue]]:
                def apply_clause(
                    result_set: ResultSet[Projection[JsonValue]],
                ) -> ResultSet[Projection[JsonValue]]:
                    return result_set.with_results(
                        result_set.results[: item.value]
                    )

                return apply_clause

        converter.register_clause_converter(TakeClause, TakeClauseConverter())

        clause = TakeClause()
        transformer = converter.convert_clause(clause)

        projection_1 = MappingProjectionBuilder().build()
        projection_2 = MappingProjectionBuilder().build()
        projection_3 = MappingProjectionBuilder().build()

        result_set = transformer(
            ResultSet.of(projection_1, projection_2, projection_3)
        )

        assert result_set.records == [projection_1, projection_2]

    def test_replaces_existing_clause_converter(self):
        converter = DelegatingQueryConverter()

        class TakeClause(Clause):
            value: int = 2

        class TakeClauseConverter1(
            ClauseConverter[Projection[JsonValue], TakeClause]
        ):
            def convert(
                self, item: TakeClause
            ) -> ResultSetTransformer[Projection[JsonValue]]:
                def apply_clause(
                    result_set: ResultSet[Projection[JsonValue]],
                ) -> ResultSet[Projection[JsonValue]]:
                    return result_set.with_results(
                        result_set.results[: item.value]
                    )

                return apply_clause

        class TakeClauseConverter2(
            ClauseConverter[Projection[JsonValue], TakeClause]
        ):
            def convert(
                self, item: TakeClause
            ) -> ResultSetTransformer[Projection[JsonValue]]:
                def apply_clause(
                    result_set: ResultSet[Projection[JsonValue]],
                ) -> ResultSet[Projection[JsonValue]]:
                    return result_set

                return apply_clause

        converter = converter.register_clause_converter(
            TakeClause, TakeClauseConverter1()
        ).register_clause_converter(TakeClause, TakeClauseConverter2())

        clause = TakeClause()
        transformer = converter.convert_clause(clause)

        projection_1 = MappingProjectionBuilder().build()
        projection_2 = MappingProjectionBuilder().build()
        projection_3 = MappingProjectionBuilder().build()

        result_set = transformer(
            ResultSet.of(projection_1, projection_2, projection_3)
        )

        assert result_set.records == [
            projection_1,
            projection_2,
            projection_3,
        ]


class TestDelegatingQueryConverterClauseConversion:
    def test_raises_for_unregistered_clause_type(self):
        converter = DelegatingQueryConverter()

        class TakeClause(Clause):
            value: int = 2

        clause = TakeClause()

        with pytest.raises(ValueError):
            converter.convert_clause(clause)


class TestDelegatingQueryConverterDefaultClauseConverters:
    def test_filter_top_level_attribute(self):
        converter = DelegatingQueryConverter().with_default_clause_converters()

        clause = FilterClause(Operator.EQUAL, Path("id"), "123")

        transformer = converter.convert_clause(clause)

        projection_1 = MappingProjectionBuilder().with_id("123").build()
        projection_2 = MappingProjectionBuilder().with_id("456").build()

        result_set = transformer(ResultSet.of(projection_1, projection_2))

        assert result_set.records == [projection_1]

    def test_filter_nested_state_attribute(self):
        converter = DelegatingQueryConverter().with_default_clause_converters()

        clause = FilterClause(Operator.EQUAL, Path("state", "value_1"), 5)

        transformer = converter.convert_clause(clause)

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

        result_set = transformer(ResultSet.of(projection_1, projection_2))

        assert result_set.records == [projection_1]

    def test_filter_not_equal(self):
        converter = DelegatingQueryConverter().with_default_clause_converters()

        clause = FilterClause(Operator.NOT_EQUAL, Path("state", "value_1"), 5)

        transformer = converter.convert_clause(clause)

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

        result_set = transformer(ResultSet.of(projection_1, projection_2))

        assert result_set.records == [projection_2]

    def test_filter_greater_than(self):
        converter = DelegatingQueryConverter().with_default_clause_converters()

        clause = FilterClause(
            Operator.GREATER_THAN, Path("state", "value_1"), 5
        )

        transformer = converter.convert_clause(clause)

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

        result_set = transformer(ResultSet.of(projection_1, projection_2))

        assert result_set.records == [projection_2]

    def test_filter_greater_than_or_equal(self):
        converter = DelegatingQueryConverter().with_default_clause_converters()

        clause = FilterClause(
            Operator.GREATER_THAN_OR_EQUAL, Path("state", "value_1"), 6
        )

        transformer = converter.convert_clause(clause)

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

        result_set = transformer(
            ResultSet.of(projection_1, projection_2, projection_3)
        )

        assert result_set.records == [projection_2, projection_3]

    def test_filter_less_than(self):
        converter = DelegatingQueryConverter().with_default_clause_converters()

        clause = FilterClause(Operator.LESS_THAN, Path("state", "value_1"), 10)

        transformer = converter.convert_clause(clause)

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

        result_set = transformer(ResultSet.of(projection_1, projection_2))

        assert result_set.records == [projection_1]

    def test_filter_less_than_or_equal(self):
        converter = DelegatingQueryConverter().with_default_clause_converters()

        clause = FilterClause(
            Operator.LESS_THAN_OR_EQUAL, Path("state", "value_1"), 6
        )

        transformer = converter.convert_clause(clause)

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

        result_set = transformer(
            ResultSet.of(projection_1, projection_2, projection_3)
        )

        assert result_set.records == [projection_1, projection_2]

    def test_filter_on_non_existent_top_level_attribute(self):
        converter = DelegatingQueryConverter().with_default_clause_converters()

        clause = FilterClause(Operator.EQUAL, Path("non_existent"), 10)

        transformer = converter.convert_clause(clause)

        projection_1 = MappingProjectionBuilder().build()
        projection_2 = MappingProjectionBuilder().build()

        with pytest.raises(ValueError) as e:
            transformer(ResultSet.of(projection_1, projection_2))

        assert str(e.value) == f"Invalid projection path: {['non_existent']}."

    def test_filter_on_non_existent_nested_attribute(self):
        converter = DelegatingQueryConverter().with_default_clause_converters()

        clause = FilterClause(Operator.EQUAL, Path("state", "value_3"), 10)

        transformer = converter.convert_clause(clause)

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
            transformer(ResultSet.of(projection_1, projection_2))

        assert (
            str(e.value) == f"Invalid projection path: {['state', 'value_3']}."
        )

    def test_sort_clause_over_single_field_on_top_level_attribute(self):
        converter = DelegatingQueryConverter().with_default_clause_converters()

        clause = SortClause(
            fields=[SortField(field=Path("id"), order=SortOrder.ASC)]
        )

        transformer = converter.convert_clause(clause)

        projection_1 = MappingProjectionBuilder().with_id("456").build()
        projection_2 = MappingProjectionBuilder().with_id("123").build()
        projection_3 = MappingProjectionBuilder().with_id("789").build()

        result_set = transformer(
            ResultSet.of(projection_1, projection_2, projection_3)
        )

        assert result_set.records == [projection_2, projection_1, projection_3]

    def test_sort_clause_over_multiple_fields_on_top_level_attributes(self):
        converter = DelegatingQueryConverter().with_default_clause_converters()

        clause = SortClause(
            fields=[
                SortField(field=Path("name"), order=SortOrder.ASC),
                SortField(field=Path("id"), order=SortOrder.DESC),
            ]
        )

        transformer = converter.convert_clause(clause)

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

        result_set = transformer(
            ResultSet.of(projection_1, projection_2, projection_3)
        )

        assert result_set.records == [projection_3, projection_2, projection_1]

    def test_sort_clause_over_single_field_on_nested_state_attribute(self):
        converter = DelegatingQueryConverter().with_default_clause_converters()

        clause = SortClause(
            fields=[
                SortField(field=Path("state", "value_1"), order=SortOrder.ASC),
            ]
        )

        transformer = converter.convert_clause(clause)

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

        result_set = transformer(
            ResultSet.of(projection_1, projection_2, projection_3)
        )

        assert result_set.records == [projection_1, projection_3, projection_2]

    def test_sort_clause_over_multiple_fields_on_nested_state_attribute(self):
        converter = DelegatingQueryConverter().with_default_clause_converters()

        clause = SortClause(
            fields=[
                SortField(
                    field=Path("state", "value_1"), order=SortOrder.DESC
                ),
                SortField(field=Path("state", "value_2"), order=SortOrder.ASC),
            ]
        )

        transformer = converter.convert_clause(clause)

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

        result_set = transformer(
            ResultSet.of(projection_1, projection_2, projection_3)
        )

        assert result_set.records == [projection_2, projection_3, projection_1]

    def test_offset_paging_clause_first_page(self):
        converter = DelegatingQueryConverter().with_default_clause_converters()

        clause = OffsetPagingClause(item_count=2)

        transformer = converter.convert_clause(clause)

        projection_1 = MappingProjectionBuilder().build()
        projection_2 = MappingProjectionBuilder().build()
        projection_3 = MappingProjectionBuilder().build()

        result_set = transformer(
            ResultSet.of(projection_1, projection_2, projection_3)
        )

        assert result_set.records == [projection_1, projection_2]

    def test_offset_paging_clause_subsequent_page(self):
        converter = DelegatingQueryConverter().with_default_clause_converters()

        clause = OffsetPagingClause(page_number=2, item_count=2)

        transformer = converter.convert_clause(clause)

        projection_1 = MappingProjectionBuilder().build()
        projection_2 = MappingProjectionBuilder().build()
        projection_3 = MappingProjectionBuilder().build()
        projection_4 = MappingProjectionBuilder().build()
        projection_5 = MappingProjectionBuilder().build()

        result_set = transformer(
            ResultSet.of(
                projection_1,
                projection_2,
                projection_3,
                projection_4,
                projection_5,
            )
        )

        assert result_set.records == [projection_3, projection_4]

    def test_offset_paging_clause_empty_page(self):
        converter = DelegatingQueryConverter().with_default_clause_converters()

        clause = OffsetPagingClause(page_number=5, item_count=2)

        transformer = converter.convert_clause(clause)

        projection_1 = MappingProjectionBuilder().build()
        projection_2 = MappingProjectionBuilder().build()
        projection_3 = MappingProjectionBuilder().build()

        result_set = transformer(
            ResultSet.of(projection_1, projection_2, projection_3)
        )

        assert result_set.records == []

    def test_key_set_paging_clause_first_page(self):
        converter = DelegatingQueryConverter().with_default_clause_converters()

        clause = KeySetPagingClause(item_count=2)

        transformer = converter.convert_clause(clause)

        projection_1 = MappingProjectionBuilder().build()
        projection_2 = MappingProjectionBuilder().build()
        projection_3 = MappingProjectionBuilder().build()

        result_set = transformer(
            ResultSet.of(projection_1, projection_2, projection_3)
        )

        assert result_set.records == [projection_1, projection_2]

    def test_key_set_paging_clause_full_page_paging_forwards(self):
        converter = DelegatingQueryConverter().with_default_clause_converters()

        projection_1 = MappingProjectionBuilder().with_id("1").build()
        projection_2 = MappingProjectionBuilder().with_id("2").build()
        projection_3 = MappingProjectionBuilder().with_id("3").build()
        projection_4 = MappingProjectionBuilder().with_id("4").build()
        projection_5 = MappingProjectionBuilder().with_id("5").build()

        clause = KeySetPagingClause(
            last_id="2", direction=PagingDirection.FORWARDS, item_count=2
        )

        transformer = converter.convert_clause(clause)

        result_set = transformer(
            ResultSet.of(
                projection_1,
                projection_2,
                projection_3,
                projection_4,
                projection_5,
            )
        )

        assert result_set.records == [projection_3, projection_4]

    def test_key_set_paging_clause_partial_page_paging_forwards(self):
        converter = DelegatingQueryConverter().with_default_clause_converters()

        projection_1 = MappingProjectionBuilder().with_id("1").build()
        projection_2 = MappingProjectionBuilder().with_id("2").build()
        projection_3 = MappingProjectionBuilder().with_id("3").build()

        clause = KeySetPagingClause(
            last_id="2", direction=PagingDirection.FORWARDS, item_count=2
        )

        transformer = converter.convert_clause(clause)

        result_set = transformer(
            ResultSet.of(
                projection_1,
                projection_2,
                projection_3,
            )
        )

        assert result_set.records == [projection_3]

    def test_key_set_paging_clause_after_id_not_present_paging_forwards(self):
        converter = DelegatingQueryConverter().with_default_clause_converters()

        projection_1 = MappingProjectionBuilder().with_id("1").build()
        projection_2 = MappingProjectionBuilder().with_id("2").build()
        projection_3 = MappingProjectionBuilder().with_id("3").build()

        clause = KeySetPagingClause(
            last_id="4", direction=PagingDirection.FORWARDS, item_count=2
        )

        transformer = converter.convert_clause(clause)

        result_set = transformer(
            ResultSet.of(
                projection_1,
                projection_2,
                projection_3,
            )
        )

        assert result_set.records == []

    def test_key_set_paging_clause_after_id_is_last_item_id_paging_forwards(
        self,
    ):
        converter = DelegatingQueryConverter().with_default_clause_converters()

        projection_1 = MappingProjectionBuilder().with_id("1").build()
        projection_2 = MappingProjectionBuilder().with_id("2").build()
        projection_3 = MappingProjectionBuilder().with_id("3").build()

        clause = KeySetPagingClause(
            last_id="3", direction=PagingDirection.FORWARDS, item_count=2
        )

        transformer = converter.convert_clause(clause)

        result_set = transformer(
            ResultSet.of(
                projection_1,
                projection_2,
                projection_3,
            )
        )

        assert result_set.records == []

    def test_key_set_paging_clause_full_page_paging_backwards(self):
        converter = DelegatingQueryConverter().with_default_clause_converters()

        projection_1 = MappingProjectionBuilder().with_id("1").build()
        projection_2 = MappingProjectionBuilder().with_id("2").build()
        projection_3 = MappingProjectionBuilder().with_id("3").build()
        projection_4 = MappingProjectionBuilder().with_id("4").build()
        projection_5 = MappingProjectionBuilder().with_id("5").build()

        clause = KeySetPagingClause(
            last_id="4", direction=PagingDirection.BACKWARDS, item_count=2
        )

        transformer = converter.convert_clause(clause)

        result_set = transformer(
            ResultSet.of(
                projection_1,
                projection_2,
                projection_3,
                projection_4,
                projection_5,
            )
        )

        assert result_set.records == [projection_2, projection_3]

    def test_key_set_paging_clause_partial_page_paging_backwards(self):
        converter = DelegatingQueryConverter().with_default_clause_converters()

        projection_1 = MappingProjectionBuilder().with_id("1").build()
        projection_2 = MappingProjectionBuilder().with_id("2").build()
        projection_3 = MappingProjectionBuilder().with_id("3").build()

        clause = KeySetPagingClause(
            last_id="2", direction=PagingDirection.BACKWARDS, item_count=2
        )

        transformer = converter.convert_clause(clause)

        result_set = transformer(
            ResultSet.of(projection_1, projection_2, projection_3)
        )

        assert result_set.records == [projection_1]

    def test_key_set_paging_clause_before_id_not_present_paging_backwards(
        self,
    ):
        converter = DelegatingQueryConverter().with_default_clause_converters()

        projection_1 = MappingProjectionBuilder().with_id("1").build()
        projection_2 = MappingProjectionBuilder().with_id("2").build()
        projection_3 = MappingProjectionBuilder().with_id("3").build()

        clause = KeySetPagingClause(
            last_id="4", direction=PagingDirection.BACKWARDS, item_count=2
        )

        transformer = converter.convert_clause(clause)

        result_set = transformer(
            ResultSet.of(
                projection_1,
                projection_2,
                projection_3,
            )
        )

        assert result_set.records == []

    def test_key_set_paging_clause_before_id_is_first_item_id_paging_backwards(
        self,
    ):
        converter = DelegatingQueryConverter().with_default_clause_converters()

        projection_1 = MappingProjectionBuilder().with_id("1").build()
        projection_2 = MappingProjectionBuilder().with_id("2").build()
        projection_3 = MappingProjectionBuilder().with_id("3").build()

        clause = KeySetPagingClause(
            last_id="1", direction=PagingDirection.BACKWARDS, item_count=2
        )

        transformer = converter.convert_clause(clause)

        result_set = transformer(
            ResultSet.of(
                projection_1,
                projection_2,
                projection_3,
            )
        )

        assert result_set.records == []

    def test_filter_on_value_in_list(self):
        converter = DelegatingQueryConverter().with_default_clause_converters()

        value_to_filter_1 = data.random_ascii_alphanumerics_string(10)
        value_to_filter_2 = data.random_ascii_alphanumerics_string(10)
        other_value = data.random_ascii_alphanumerics_string(10)
        clause = FilterClause(
            Operator.IN,
            Path("state", "value_2"),
            [value_to_filter_1, value_to_filter_2],
        )

        transformer = converter.convert_clause(clause)

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

        result_set = transformer(
            ResultSet.of(projection_1, projection_2, projection_3)
        )

        assert result_set.records == [projection_1, projection_2]

    def test_filter_on_list_contains_value(self):
        converter = DelegatingQueryConverter().with_default_clause_converters()

        value_to_filter = data.random_ascii_alphanumerics_string(10)
        other_value1 = data.random_ascii_alphanumerics_string(10)
        other_value2 = data.random_ascii_alphanumerics_string(10)
        clause = FilterClause(
            Operator.CONTAINS,
            Path("state", "value_2"),
            value_to_filter,
        )

        transformer = converter.convert_clause(clause)

        projection_1 = (
            MappingProjectionBuilder()
            .with_state({"value_1": 5, "value_2": [other_value1]})
            .build()
        )
        projection_2 = (
            MappingProjectionBuilder()
            .with_state(
                {"value_1": 15, "value_2": [other_value1, other_value2]}
            )
            .build()
        )
        projection_3 = (
            MappingProjectionBuilder()
            .with_state(
                {"value_1": 25, "value_2": [other_value1, value_to_filter]}
            )
            .build()
        )

        result_set = transformer(
            ResultSet.of(projection_1, projection_2, projection_3)
        )

        assert result_set.records == [projection_3]

    def test_filter_on_regex_matches_value(self):
        converter = DelegatingQueryConverter().with_default_clause_converters()

        matching_text = data.random_ascii_alphanumerics_string(10)
        value_to_filter = (
            data.random_ascii_alphanumerics_string(10)
            + matching_text
            + data.random_ascii_alphanumerics_string(10)
        )

        other_value1 = data.random_ascii_alphanumerics_string(10)
        other_value2 = data.random_ascii_alphanumerics_string(10)

        clause = FilterClause(
            Operator.REGEX_MATCHES,
            Path("state", "value_2"),
            f".*{matching_text}.*",
        )

        transformer = converter.convert_clause(clause)

        projection_1 = (
            MappingProjectionBuilder()
            .with_state({"value_1": 5, "value_2": other_value1})
            .build()
        )
        projection_2 = (
            MappingProjectionBuilder()
            .with_state({"value_1": 15, "value_2": other_value2})
            .build()
        )
        projection_3 = (
            MappingProjectionBuilder()
            .with_state({"value_1": 25, "value_2": value_to_filter})
            .build()
        )

        result_set = transformer(
            ResultSet.of(projection_1, projection_2, projection_3)
        )

        assert result_set.records == [projection_3]


class TestDelegatingQueryConverterQueryConverterRegistration:
    def test_registers_query_converter_and_converts_query(self):
        converter = DelegatingQueryConverter()

        class CustomQuery(Query):
            pass

        class CustomQueryConverter(
            QueryConverter[Projection[JsonValue], CustomQuery]
        ):
            def convert(
                self, item: CustomQuery
            ) -> ResultSetTransformer[Projection[JsonValue]]:
                def apply_clause(
                    result_set: ResultSet[Projection[JsonValue]],
                ) -> ResultSet[Projection[JsonValue]]:
                    return result_set.with_results(result_set.results[0::2])

                return apply_clause

        converter.register_query_converter(CustomQuery, CustomQueryConverter())

        query = CustomQuery()
        transformer = converter.convert_query(query)

        projection_1 = MappingProjectionBuilder().build()
        projection_2 = MappingProjectionBuilder().build()
        projection_3 = MappingProjectionBuilder().build()
        projection_4 = MappingProjectionBuilder().build()

        result_set = transformer(
            ResultSet.of(
                projection_1, projection_2, projection_3, projection_4
            )
        )

        assert result_set.records == [projection_1, projection_3]


class TestDelegatingQueryConverterQueryConversion:
    def test_raises_for_unsupported_query_type(self):
        converter = DelegatingQueryConverter()

        class UnsupportedQuery(Query):
            pass

        query = UnsupportedQuery()

        with pytest.raises(ValueError):
            converter.convert_query(query)


class TestDelegatingQueryConverterDefaultQueryConverters:
    def test_converts_lookup_with_multiple_filter_clauses(self):
        converter = DelegatingQueryConverter().with_default_query_converters()

        class SkipClause(Clause):
            number: int = 2

        class TakeClause(Clause):
            number: int = 1

        class SkipClauseConverter(
            ClauseConverter[Projection[JsonValue], SkipClause]
        ):
            def convert(
                self, item: SkipClause
            ) -> ResultSetTransformer[Projection[JsonValue]]:
                def apply_clause(
                    result_set: ResultSet[Projection[JsonValue]],
                ) -> ResultSet[Projection[JsonValue]]:
                    return result_set.with_results(
                        result_set.results[item.number :]
                    )

                return apply_clause

        class TakeClauseConverter(
            ClauseConverter[Projection[JsonValue], TakeClause]
        ):
            def convert(
                self, item: TakeClause
            ) -> ResultSetTransformer[Projection[JsonValue]]:
                def apply_clause(
                    result_set: ResultSet[Projection[JsonValue]],
                ) -> ResultSet[Projection[JsonValue]]:
                    return result_set.with_results(
                        result_set.results[: item.number]
                    )

                return apply_clause

        converter = converter.register_clause_converter(
            SkipClause, SkipClauseConverter()
        ).register_clause_converter(TakeClause, TakeClauseConverter())

        query = Lookup(
            filters=[
                SkipClause(),
                TakeClause(),
            ]
        )

        transformer = converter.convert_query(query)

        projection_1 = MappingProjectionBuilder().build()
        projection_2 = MappingProjectionBuilder().build()
        projection_3 = MappingProjectionBuilder().build()
        projection_4 = MappingProjectionBuilder().build()
        projection_5 = MappingProjectionBuilder().build()

        result_set = transformer(
            ResultSet.of(
                projection_1,
                projection_2,
                projection_3,
                projection_4,
                projection_5,
            )
        )

        assert result_set.records == [projection_3]

    def test_converts_search_with_sort_clause(self):
        converter = (
            DelegatingQueryConverter()
            .with_default_query_converters()
            .register_clause_converter(
                SortClause,
                SortClauseConverter(TypeRegistryFunctionConverter()),
            )
        )

        transformer = converter.convert_query(
            Search(
                sort=(
                    SortClause(
                        fields=[
                            SortField(field=Path("id"), order=SortOrder.ASC)
                        ]
                    )
                )
            )
        )

        projection_1 = MappingProjectionBuilder().with_id("456").build()
        projection_2 = MappingProjectionBuilder().with_id("123").build()
        projection_3 = MappingProjectionBuilder().with_id("789").build()

        result_set = transformer(
            ResultSet.of(projection_1, projection_2, projection_3)
        )

        assert result_set.records == [projection_2, projection_1, projection_3]

    def test_converts_search_with_paging_clause(self):
        converter = DelegatingQueryConverter().with_default_query_converters()
        converter.register_clause_converter(
            KeySetPagingClause, KeySetPagingClauseConverter()
        )

        transformer = converter.convert_query(
            Search(paging=KeySetPagingClause(item_count=2))
        )

        projection_1 = MappingProjectionBuilder().build()
        projection_2 = MappingProjectionBuilder().build()
        projection_3 = MappingProjectionBuilder().build()

        result_set = transformer(
            ResultSet.of(projection_1, projection_2, projection_3)
        )

        assert result_set.records == [projection_1, projection_2]


class TestDelegatingQueryConverterFunctionConverterRegistration:
    def test_registers_function_converter_and_converts_function(self):
        converter = DelegatingQueryConverter()

        @final
        @dataclass(frozen=True)
        class Sum(Function):
            left: Path
            right: Path

        class SumConverter(FunctionConverter[Projection[JsonValue], Sum]):
            def convert(
                self, item: Sum
            ) -> ResultSetTransformer[Projection[JsonValue]]:
                def apply_function(
                    result_set: ResultSet[Projection[JsonValue]],
                ) -> ResultSet[Projection[JsonValue]]:
                    return result_set.with_results(
                        [
                            result.add_extra(
                                item.alias,
                                result.lookup(item.left)
                                + result.lookup(item.right),
                            )
                            for result in result_set.results
                        ]
                    )

                return apply_function

        converter.register_function_converter(Sum, SumConverter())

        function = Sum(
            left=Path("state", "a"), right=Path("state", "b"), alias="a_b_sum"
        )

        transformer = converter.convert_function(function)

        projection_1 = (
            MappingProjectionBuilder().with_state({"a": 1, "b": 2}).build()
        )
        projection_2 = (
            MappingProjectionBuilder().with_state({"a": 3, "b": 4}).build()
        )
        projection_3 = (
            MappingProjectionBuilder().with_state({"a": 5, "b": 6}).build()
        )

        result_set = transformer(
            ResultSet.of(projection_1, projection_2, projection_3)
        )

        assert result_set.results == [
            Result.of(projection_1, {"a_b_sum": 3}),
            Result.of(projection_2, {"a_b_sum": 7}),
            Result.of(projection_3, {"a_b_sum": 11}),
        ]

    def test_replaces_existing_clause_converter(self):
        converter = DelegatingQueryConverter()

        @final
        @dataclass(frozen=True)
        class MysteriousAction(Function):
            left: Path
            right: Path

        class MysteriousActionConverter1(
            FunctionConverter[Projection[JsonValue], MysteriousAction]
        ):
            def convert(
                self, item: MysteriousAction
            ) -> ResultSetTransformer[Projection[JsonValue]]:
                def apply_function(
                    result_set: ResultSet[Projection[JsonValue]],
                ) -> ResultSet[Projection[JsonValue]]:
                    return result_set.with_results(
                        [
                            result.add_extra(
                                item.alias,
                                result.lookup(item.left)
                                + result.lookup(item.right),
                            )
                            for result in result_set.results
                        ]
                    )

                return apply_function

        class MysteriousActionConverter2(
            FunctionConverter[Projection[JsonValue], MysteriousAction]
        ):
            def convert(
                self, item: MysteriousAction
            ) -> ResultSetTransformer[Projection[JsonValue]]:
                def apply_function(
                    result_set: ResultSet[Projection[JsonValue]],
                ) -> ResultSet[Projection[JsonValue]]:
                    return result_set.with_results(
                        [
                            result.add_extra(
                                item.alias,
                                result.lookup(item.left)
                                * result.lookup(item.right),
                            )
                            for result in result_set.results
                        ]
                    )

                return apply_function

        converter = converter.register_function_converter(
            MysteriousAction, MysteriousActionConverter1()
        ).register_function_converter(
            MysteriousAction, MysteriousActionConverter2()
        )

        function = MysteriousAction(
            left=Path("state", "a"), right=Path("state", "b"), alias="a_b"
        )
        transformer = converter.convert_function(function)

        projection_1 = (
            MappingProjectionBuilder().with_state({"a": 1, "b": 2}).build()
        )
        projection_2 = (
            MappingProjectionBuilder().with_state({"a": 3, "b": 4}).build()
        )
        projection_3 = (
            MappingProjectionBuilder().with_state({"a": 5, "b": 6}).build()
        )

        result_set = transformer(
            ResultSet.of(projection_1, projection_2, projection_3)
        )

        assert result_set.results == [
            Result.of(projection_1, {"a_b": 2}),
            Result.of(projection_2, {"a_b": 12}),
            Result.of(projection_3, {"a_b": 30}),
        ]


class TestDelegatingQueryConverterFunctionConversion:
    def test_raises_for_unregistered_function_type(self):
        converter = DelegatingQueryConverter()

        @dataclass(frozen=True)
        class Constantly(Function):
            value: int

        function = Constantly(value=5, alias="five")

        with pytest.raises(ValueError):
            converter.convert_function(function)


class TestDelegatingQueryConverterDefaultFunctionConverters:
    def test_similarity(self):
        converter = (
            DelegatingQueryConverter().with_default_function_converters()
        )

        function = Similarity(
            left=Path("state", "value"), right="xyz", alias="similarity"
        )

        transformer = converter.convert_function(function)

        projection_1 = (
            MappingProjectionBuilder()
            .with_state({"value": "abcd efgh"})
            .build()
        )
        projection_2 = (
            MappingProjectionBuilder()
            .with_state({"value": "axyz bcdefghijklm"})
            .build()
        )
        projection_3 = (
            MappingProjectionBuilder()
            .with_state({"value": "xyzabxyz"})
            .build()
        )
        projection_4 = (
            MappingProjectionBuilder().with_state({"value": "abx"}).build()
        )
        projection_5 = (
            MappingProjectionBuilder().with_state({"value": "xyz"}).build()
        )

        result_set = transformer(
            ResultSet.of(
                projection_1,
                projection_2,
                projection_3,
                projection_4,
                projection_5,
            )
        )

        assert result_set.results == [
            Result.of(projection_1, {"similarity": 0}),
            Result.of(projection_2, {"similarity": 0.1}),
            Result.of(projection_3, {"similarity": 0.5}),
            Result.of(projection_4, {"similarity": 0}),
            Result.of(projection_5, {"similarity": 1}),
        ]
