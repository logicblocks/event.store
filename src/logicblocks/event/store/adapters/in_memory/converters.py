from collections.abc import Callable
from typing import Any, Self

from logicblocks.event.persistence import TypeRegistryConverter
from logicblocks.event.types import Converter, JsonValue
from logicblocks.event.types.event import StoredEvent

from ...constraints import (
    QueryConstraint,
    SequenceNumberAfterConstraint,
)

type QueryConstraintCheck = Callable[[StoredEvent[str, JsonValue]], bool]


class SequenceNumberAfterConstraintConverter(
    Converter[SequenceNumberAfterConstraint, QueryConstraintCheck]
):
    def convert(
        self, item: SequenceNumberAfterConstraint
    ) -> QueryConstraintCheck:
        def check(event: StoredEvent[Any, Any]) -> bool:
            return event.sequence_number > item.sequence_number

        return check


class TypeRegistryConstraintConverter(
    TypeRegistryConverter[QueryConstraint, QueryConstraintCheck]
):
    def register[QC: QueryConstraint](
        self,
        item_type: type[QC],
        converter: Converter[QC, QueryConstraintCheck],
    ) -> Self:
        return super()._register(item_type, converter)

    def with_default_constraint_converters(self) -> Self:
        return self.register(
            SequenceNumberAfterConstraint,
            SequenceNumberAfterConstraintConverter(),
        )
