from typing import Self

from logicblocks.event.persistence import TypeRegistryConverter
from logicblocks.event.persistence.postgres import (
    Column,
    Condition,
    Operator,
    Query,
    QueryApplier,
    Value,
)
from logicblocks.event.types import Converter

from ...constraints import (
    QueryConstraint,
    SequenceNumberAfterConstraint,
)


class SequenceNumberAfterConstraintQueryApplier(QueryApplier):
    def __init__(self, sequence_number: int):
        self.sequence_number = sequence_number

    def apply(self, target: Query) -> Query:
        return target.where(
            Condition()
            .left(Column(field="sequence_number"))
            .operator(Operator.GREATER_THAN)
            .right(Value(self.sequence_number))
        )


class SequenceNumberAfterConstraintConverter(
    Converter[SequenceNumberAfterConstraint, QueryApplier]
):
    def convert(self, item: SequenceNumberAfterConstraint) -> QueryApplier:
        return SequenceNumberAfterConstraintQueryApplier(item.sequence_number)


class TypeRegistryConstraintConverter(
    TypeRegistryConverter[QueryConstraint, QueryApplier]
):
    def register[QC: QueryConstraint](
        self,
        item_type: type[QC],
        converter: Converter[QC, QueryApplier],
    ) -> Self:
        return super()._register(item_type, converter)

    def with_default_constraint_converters(self) -> Self:
        return self.register(
            SequenceNumberAfterConstraint,
            SequenceNumberAfterConstraintConverter(),
        )
