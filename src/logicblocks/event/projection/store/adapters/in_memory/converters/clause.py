from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping, Sequence
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
from logicblocks.event.types import (
    Converter,
    JsonValue,
    Projection,
)

from ..types import (
    ProjectionResultSet,
    ProjectionResultSetTransformer,
)


class ClauseConverter[C: Clause = Clause](
    Converter[C, ProjectionResultSetTransformer], ABC
):
    @abstractmethod
    def convert(self, item: C) -> ProjectionResultSetTransformer:
        raise NotImplementedError


def lookup_projection_path(
    projection: Projection[JsonValue, JsonValue], path: Path
) -> Any:
    attribute_name = path.top_level
    remaining_path = path.sub_levels

    try:
        attribute = getattr(projection, attribute_name)
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
) -> Callable[[Projection[JsonValue, JsonValue]], Any]:
    def get_key_for_projection(
        projection: Projection[JsonValue, JsonValue],
    ) -> Any:
        return lookup_projection_path(projection, path)

    return get_key_for_projection


# def make_function_key_function(
#         function: Function,
# ) -> Callable[[Projection[JsonValue, JsonValue]], Any]:
#     def get_key_for_projection(
#             projection: Projection[JsonValue, JsonValue]
#     ) -> Any:


class FilterClauseConverter(ClauseConverter[FilterClause]):
    @staticmethod
    def _matches(
        clause: FilterClause,
        projection: Projection[JsonValue, JsonValue],
    ) -> bool:
        comparison_value = clause.value
        resolved_value = lookup_projection_path(projection, clause.path)

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

    def convert(self, item: FilterClause) -> ProjectionResultSetTransformer:
        def handler(
            projections: Sequence[Projection[JsonValue, JsonValue]],
        ) -> Sequence[Projection[JsonValue, JsonValue]]:
            return list(
                projection
                for projection in projections
                if self._matches(item, projection)
            )

        return handler


class SortClauseConverter(ClauseConverter[SortClause]):
    @staticmethod
    def _accumulator(
        projections: Sequence[Projection[JsonValue, JsonValue]],
        field: SortField,
    ) -> Sequence[Projection[JsonValue, JsonValue]]:
        if isinstance(field.path, Function):
            raise ValueError("Function sorting is not supported.")
        result = sorted(
            projections,
            key=make_path_key_function(field.path),
            reverse=(field.order == SortOrder.DESC),
        )
        return result

    def convert(self, item: SortClause) -> ProjectionResultSetTransformer:
        def handler(
            projections: Sequence[Projection[JsonValue, JsonValue]],
        ) -> Sequence[Projection[JsonValue, JsonValue]]:
            return reduce(
                self._accumulator, reversed(item.fields), projections
            )

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


class KeySetPagingClauseConverter(ClauseConverter[KeySetPagingClause]):
    @staticmethod
    def _determine_last_index(
        projections: Sequence[Projection[JsonValue, JsonValue]],
        last_id: str | None,
    ) -> LastIndexFound | LastIndexNotFound | LastIndexNotProvided:
        if last_id is None:
            return LastIndexNotProvided()

        last_indices = [
            index
            for index, projection in enumerate(projections)
            if projection.id == last_id
        ]
        if len(last_indices) != 1:
            return LastIndexNotFound()

        return LastIndexFound(last_indices[0])

    def convert(
        self, item: KeySetPagingClause
    ) -> ProjectionResultSetTransformer:
        def handler(
            projections: Sequence[Projection[JsonValue, JsonValue]],
        ) -> Sequence[Projection[JsonValue, JsonValue]]:
            last_index_result = self._determine_last_index(
                projections, item.last_id
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
                    return projections[:item_count]
                case (LastIndexFound(last_index), PagingDirection.FORWARDS):
                    return projections[
                        last_index + 1 : last_index + 1 + item_count
                    ]
                case (LastIndexFound(last_index), PagingDirection.BACKWARDS):
                    resolved_start_index = max(last_index - item_count, 0)
                    return projections[resolved_start_index:last_index]
                case _:  # pragma: no cover
                    raise ValueError("Unreachable state.")

        return handler


class OffsetPagingClauseConverter(ClauseConverter[OffsetPagingClause]):
    def convert(
        self, item: OffsetPagingClause
    ) -> ProjectionResultSetTransformer:
        offset = item.offset
        item_count = item.item_count

        def handler(
            projections: ProjectionResultSet,
        ) -> ProjectionResultSet:
            return projections[offset : offset + item_count]

        return handler


class TypeRegistryClauseConverter(ClauseConverter):
    def __init__(
        self,
        registry: Mapping[type[Clause], ClauseConverter[Any]] | None = None,
    ):
        self._registry = dict(registry) if registry is not None else {}

    def register[C: Clause](
        self, clause_type: type[C], converter: ClauseConverter[C]
    ) -> Self:
        self._registry[clause_type] = converter
        return self

    def convert(self, item: Clause) -> ProjectionResultSetTransformer:
        if item.__class__ not in self._registry:
            raise ValueError(f"No converter registered for clause: {item}")
        return self._registry[item.__class__].convert(item)
