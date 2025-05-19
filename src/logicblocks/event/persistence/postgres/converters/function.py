from typing import Self

import logicblocks.event.query as genericquery
from logicblocks.event.persistence.postgres.converters.helpers import (
    expression_for_path,
)
from logicblocks.event.types import Converter

from ...converter import TypeRegistryConverter
from .. import query as postgresquery
from .types import (
    FunctionConverter,
    QueryApplier,
)


class SimilarityFunctionQueryApplier(QueryApplier):
    def __init__(self, function: genericquery.Similarity):
        self._function = function

    def apply(self, target: postgresquery.Query) -> postgresquery.Query:
        result_target = postgresquery.ResultTarget(
            expression=postgresquery.FunctionApplication(
                function_name="similarity",
                arguments=[
                    postgresquery.Cast(
                        expression=expression_for_path(self._function.left),
                        typename="text",
                    ),
                    postgresquery.Constant(self._function.right),
                ],
            ),
            label=self._function.alias,
        )

        return target.select(result_target)


class SimilarityFunctionConverter(FunctionConverter[genericquery.Similarity]):
    def convert(self, item: genericquery.Similarity) -> QueryApplier:
        return SimilarityFunctionQueryApplier(function=item)


class TypeRegistryFunctionConverter(
    TypeRegistryConverter[genericquery.Function, QueryApplier]
):
    def register[F: genericquery.Function](
        self,
        item_type: type[F],
        converter: Converter[F, QueryApplier],
    ) -> Self:
        return super()._register(item_type, converter)
