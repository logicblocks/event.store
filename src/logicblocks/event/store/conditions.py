from dataclasses import dataclass
from typing import Generic, TypeVar, Literal

T = TypeVar("T")
type Operator = Literal["equals"]


@dataclass(frozen=True)
class WriteCondition(Generic[T]):
    attribute: str
    operator: Operator
    value: T


def position_is(position: int) -> WriteCondition[int]:
    return WriteCondition(
        attribute="position", operator="equals", value=position
    )
