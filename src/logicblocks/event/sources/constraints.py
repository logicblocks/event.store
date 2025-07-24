from abc import ABC
from dataclasses import dataclass

from logicblocks.event.types import JsonValue


class QueryConstraint(ABC): ...


@dataclass(frozen=True)
class OrderingIdAfterConstraint(QueryConstraint):
    ordering_id: JsonValue


def ordering_id_after(ordering_id: JsonValue) -> QueryConstraint:
    return OrderingIdAfterConstraint(ordering_id=ordering_id)
