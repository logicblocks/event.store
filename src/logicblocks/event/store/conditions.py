from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Literal

from logicblocks.event.types import StoredEvent
from logicblocks.event.store.exceptions import UnmetWriteConditionError

type Operator = Literal["equals"]
type Target = Literal["last_event", "stream"]


class WriteCondition(ABC):
    @abstractmethod
    def ensure(self, last_event: StoredEvent | None) -> None:
        raise NotImplementedError()


@dataclass(frozen=True)
class PositionIsCondition(WriteCondition):
    position: int

    def ensure(self, last_event: StoredEvent | None):
        if last_event is None or last_event.position is not self.position:
            raise UnmetWriteConditionError("unexpected stream position")


@dataclass(frozen=True)
class EmptyStreamCondition(WriteCondition):
    def ensure(self, last_event: StoredEvent | None):
        if last_event is not None:
            raise UnmetWriteConditionError("stream is not empty")


def position_is(position: int) -> WriteCondition:
    return PositionIsCondition(position=position)


def stream_is_empty() -> WriteCondition:
    return EmptyStreamCondition()
