from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Literal, Self

type Operator = Literal["equals"]
type Target = Literal["last_event", "stream"]


class WriteConditionDescriptor(ABC):
    @abstractmethod
    def target(self, target: Target) -> Self:
        raise NotImplementedError()

    @abstractmethod
    def attribute(self, attribute: str) -> Self:
        raise NotImplementedError()

    @abstractmethod
    def operator(self, operator: Operator) -> Self:
        raise NotImplementedError()

    @abstractmethod
    def value(self, value: object) -> Self:
        raise NotImplementedError()


class WriteCondition(ABC):
    @abstractmethod
    def describe[T: WriteConditionDescriptor](self, descriptor: T) -> T:
        raise NotImplementedError()


@dataclass(frozen=True)
class LastStreamEventAttributeCondition(WriteCondition):
    attribute: str
    operator: Operator
    value: object

    def describe[T: WriteConditionDescriptor](self, descriptor: T) -> T:
        return (
            descriptor.target("last_event")
            .attribute(self.attribute)
            .operator(self.operator)
            .value(self.value)
        )


@dataclass(frozen=True)
class EmptyStreamCondition(WriteCondition):
    def describe[T: WriteConditionDescriptor](self, descriptor: T) -> T:
        return (
            descriptor.target("stream")
            .attribute("length")
            .operator("equals")
            .value(0)
        )


def position_is(position: int) -> WriteCondition:
    return LastStreamEventAttributeCondition(
        attribute="position", operator="equals", value=position
    )


def stream_is_empty() -> WriteCondition:
    return EmptyStreamCondition()
