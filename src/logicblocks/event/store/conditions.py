from abc import ABC, abstractmethod
from dataclasses import dataclass

from logicblocks.event.store.exceptions import UnmetWriteConditionError
from logicblocks.event.types import StoredEvent


class WriteCondition(ABC):
    @abstractmethod
    def assert_met_by(self, *, last_event: StoredEvent | None) -> None:
        raise NotImplementedError()


@dataclass(frozen=True)
class PositionIsCondition(WriteCondition):
    position: int

    def assert_met_by(self, *, last_event: StoredEvent | None):
        if last_event is None or last_event.position is not self.position:
            raise UnmetWriteConditionError("unexpected stream position")


@dataclass(frozen=True)
class EmptyStreamCondition(WriteCondition):
    def assert_met_by(self, *, last_event: StoredEvent | None):
        if last_event is not None:
            raise UnmetWriteConditionError("stream is not empty")


def position_is(position: int) -> WriteCondition:
    return PositionIsCondition(position=position)


def stream_is_empty() -> WriteCondition:
    return EmptyStreamCondition()
