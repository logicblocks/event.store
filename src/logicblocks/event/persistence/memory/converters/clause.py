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
from .types import (
    ClauseConverter,
    Identifiable,
    ResultSet,
    ResultSetTransformer,
)


def lookup_path(object: Any, path: Path) -> Any:
    attribute_name = path.top_level
    remaining_path = path.sub_levels

    try:
        attribute = getattr(object, attribute_name)
    except AttributeError:
        raise ValueError(f"Invalid projection path: {path}.")

    value = attribute
    for path_segment in remaining_path:
        try:
            value = value[path_segment]
        except KeyError:
            raise ValueError(f"Invalid projection path: {path}.")

    return value


def make_path_key_function(
    path: Path,
) -> Callable[[Any], Any]:
    def get_key_for_projection(object: Any) -> Any:
        return lookup_path(object, path)

    return get_key_for_projection


# def make_function_key_function(
#         function: Function,
# ) -> Callable[[Projection[JsonValue, JsonValue]], Any]:
#     def get_key_for_projection(
#             projection: Projection[JsonValue, JsonValue]
#     ) -> Any:


class FilterClauseConverter[R: Identifiable](ClauseConverter[R, FilterClause]):
    @staticmethod
    def _matches(clause: FilterClause, item: R) -> bool:
        comparison_value = clause.value
        resolved_value = lookup_path(item, clause.path)

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
        def handler(results: Sequence[R]) -> Sequence[R]:
            return list(
                result for result in results if self._matches(item, result)
            )

        return handler


class SortClauseConverter[R: Identifiable](ClauseConverter[R, SortClause]):
    @staticmethod
    def _accumulator(
        results: Sequence[R],
        field: SortField,
    ) -> Sequence[R]:
        if isinstance(field.path, Function):
            raise ValueError("Function sorting is not supported.")
        result = sorted(
            results,
            key=make_path_key_function(field.path),
            reverse=(field.order == SortOrder.DESC),
        )
        return result

    def convert(self, item: SortClause) -> ResultSetTransformer[R]:
        def handler(
            results: Sequence[R],
        ) -> Sequence[R]:
            return reduce(self._accumulator, reversed(item.fields), results)

        return handler


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
        results: Sequence[R],
        last_id: str | None,
    ) -> LastIndexFound | LastIndexNotFound | LastIndexNotProvided:
        if last_id is None:
            return LastIndexNotProvided()

        last_indices = [
            index
            for index, result in enumerate(results)
            if result.id == last_id
        ]
        if len(last_indices) != 1:
            return LastIndexNotFound()

        return LastIndexFound(last_indices[0])

    def convert(self, item: KeySetPagingClause) -> ResultSetTransformer[R]:
        def handler(
            results: Sequence[R],
        ) -> Sequence[R]:
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
                    return []
                case (LastIndexNotProvided(), PagingDirection.FORWARDS):
                    return results[:item_count]
                case (LastIndexFound(last_index), PagingDirection.FORWARDS):
                    return results[
                        last_index + 1 : last_index + 1 + item_count
                    ]
                case (LastIndexFound(last_index), PagingDirection.BACKWARDS):
                    resolved_start_index = max(last_index - item_count, 0)
                    return results[resolved_start_index:last_index]
                case _:  # pragma: no cover
                    raise ValueError("Unreachable state.")

        return handler


class OffsetPagingClauseConverter[R: Identifiable](
    ClauseConverter[R, OffsetPagingClause]
):
    def convert(self, item: OffsetPagingClause) -> ResultSetTransformer[R]:
        offset = item.offset
        item_count = item.item_count

        def handler(
            results: ResultSet[R],
        ) -> ResultSet[R]:
            return results[offset : offset + item_count]

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
