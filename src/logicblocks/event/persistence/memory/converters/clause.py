from collections.abc import Callable, Sequence
from dataclasses import dataclass
from functools import reduce
from typing import Any, Self

from logicblocks.event.query import (
    Clause,
    FilterClause,
    Function,
    KeySetPagingClause,
    OffsetPagingClause,
    Operator,
    PagingDirection,
    Path,
    SortClause,
    SortField,
    SortOrder,
)
from logicblocks.event.types import Converter

from ...converter import TypeRegistryConverter
from .helpers import compose_transformers
from .types import (
    ClauseConverter,
    Identifiable,
    Result,
    ResultSet,
    ResultSetTransformer,
)


def make_field_key_function(
    field: Path | Function,
) -> Callable[[Result[Any]], Any]:
    def get_key_for_projection(result: Result[Any]) -> Any:
        if isinstance(field, Function):
            return result.lookup(Path(top_level=field.alias))
        return result.lookup(field)

    return get_key_for_projection


class FilterClauseConverter[R: Identifiable](ClauseConverter[R, FilterClause]):
    @staticmethod
    def _matches(clause: FilterClause, item: Result[R]) -> bool:
        comparison_value = clause.value
        resolved_value = item.lookup(clause.field)

        match clause.operator:
            case Operator.EQUAL:
                return resolved_value == comparison_value
            case Operator.NOT_EQUAL:
                return not resolved_value == comparison_value
            case Operator.GREATER_THAN:
                return resolved_value > comparison_value
            case Operator.GREATER_THAN_OR_EQUAL:
                return resolved_value >= comparison_value
            case Operator.LESS_THAN:
                return resolved_value < comparison_value
            case Operator.LESS_THAN_OR_EQUAL:
                return resolved_value <= comparison_value
            case Operator.IN:
                return resolved_value in comparison_value
            case Operator.CONTAINS:
                return comparison_value in resolved_value
            case _:  # pragma: no cover
                raise ValueError(f"Unknown operator: {clause.operator}.")

    def convert(self, item: FilterClause) -> ResultSetTransformer[R]:
        def handler(result_set: ResultSet[R]) -> ResultSet[R]:
            return result_set.with_results(
                list(
                    result
                    for result in result_set.results
                    if self._matches(item, result)
                )
            )

        return handler


class SortClauseConverter[R: Identifiable](ClauseConverter[R, SortClause]):
    def __init__(
        self,
        function_converter: Converter[Function, ResultSetTransformer[R]],
    ):
        self._function_converter = function_converter

    @staticmethod
    def _accumulator(
        results: Sequence[Result[R]],
        sort_field: SortField,
    ) -> Sequence[Result[R]]:
        result = sorted(
            results,
            key=make_field_key_function(sort_field.field),
            reverse=(sort_field.order == SortOrder.DESC),
        )
        return result

    def convert(self, item: SortClause) -> ResultSetTransformer[R]:
        def handler(
            result_set: ResultSet[R],
        ) -> ResultSet[R]:
            return result_set.with_results(
                reduce(
                    self._accumulator,
                    reversed(item.fields),
                    result_set.results,
                )
            )

        function_transformers = [
            self._function_converter.convert(sort_field.field)
            for sort_field in item.fields
            if isinstance(sort_field.field, Function)
        ]
        sort_clause_transformer = handler

        return compose_transformers(
            [
                *function_transformers,
                sort_clause_transformer,
            ]
        )


@dataclass
class LastIndexNotFound:
    pass


@dataclass
class LastIndexNotProvided:
    pass


@dataclass
class LastIndexFound:
    index: int


class KeySetPagingClauseConverter[R: Identifiable](
    ClauseConverter[R, KeySetPagingClause]
):
    @staticmethod
    def _determine_last_index(
        results: Sequence[Result[R]],
        last_id: str | None,
    ) -> LastIndexFound | LastIndexNotFound | LastIndexNotProvided:
        if last_id is None:
            return LastIndexNotProvided()

        last_indices = [
            index
            for index, result in enumerate(results)
            if result.record.id == last_id
        ]
        if len(last_indices) != 1:
            return LastIndexNotFound()

        return LastIndexFound(last_indices[0])

    def convert(self, item: KeySetPagingClause) -> ResultSetTransformer[R]:
        def handler(
            result_set: ResultSet[R],
        ) -> ResultSet[R]:
            results = result_set.results
            last_index_result = self._determine_last_index(
                results, item.last_id
            )
            direction = item.direction
            item_count = item.item_count

            match (last_index_result, direction):
                case (
                    (LastIndexNotFound(), PagingDirection.FORWARDS)
                    | (LastIndexNotFound(), PagingDirection.BACKWARDS)
                    | (LastIndexNotProvided(), PagingDirection.BACKWARDS)
                ):
                    return result_set.with_results([])
                case (LastIndexNotProvided(), PagingDirection.FORWARDS):
                    return result_set.with_results(results[:item_count])
                case (LastIndexFound(last_index), PagingDirection.FORWARDS):
                    return result_set.with_results(
                        results[last_index + 1 : last_index + 1 + item_count]
                    )
                case (LastIndexFound(last_index), PagingDirection.BACKWARDS):
                    resolved_start_index = max(last_index - item_count, 0)
                    return result_set.with_results(
                        results[resolved_start_index:last_index]
                    )
                case _:  # pragma: no cover
                    raise ValueError("Unreachable state.")

        return handler


class OffsetPagingClauseConverter[R: Identifiable](
    ClauseConverter[R, OffsetPagingClause]
):
    def convert(self, item: OffsetPagingClause) -> ResultSetTransformer[R]:
        offset = item.offset
        item_count = item.item_count

        def handler(result_set: ResultSet[R]) -> ResultSet[R]:
            return result_set.with_results(
                result_set.results[offset : offset + item_count]
            )

        return handler


class TypeRegistryClauseConverter[R: Identifiable](
    TypeRegistryConverter[Clause, ResultSetTransformer[R]]
):
    def register[C: Clause](
        self,
        item_type: type[C],
        converter: Converter[C, ResultSetTransformer[R]],
    ) -> Self:
        return super()._register(item_type, converter)
