from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Any, Self

from logicblocks.event.persistence import TypeRegistryConverter
from logicblocks.event.store.conditions import (
    AndCondition,
    EmptyStreamCondition,
    NoCondition,
    OrCondition,
    PositionIsCondition,
    WriteCondition,
)
from logicblocks.event.store.exceptions import UnmetWriteConditionError
from logicblocks.event.types import (
    Converter,
    JsonValue,
    StoredEvent,
    StreamIdentifier,
)

from ...constraints import (
    QueryConstraint,
    SequenceNumberAfterConstraint,
)
from .db import InMemoryEventsDBTransaction
from .types import QueryConstraintCheck


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


class WriteConditionEnforcerContext:
    def __init__(
        self,
        identifier: StreamIdentifier,
        latest_event: StoredEvent[str, JsonValue] | None,
    ):
        self.identifier = identifier
        self.latest_event = latest_event


class WriteConditionEnforcer(ABC):
    @abstractmethod
    def assert_satisfied(
        self,
        context: WriteConditionEnforcerContext,
        transaction: InMemoryEventsDBTransaction,
    ) -> None:
        """Throw an UnmetWriteConditionError if the WriteCondition
        represented/encapsulated by this WriteConditionEnforcer is not
        satisfied.

        Args:
            context: The context of the stream, against which the WriteCondition
            will be checked. This includes the stream identifier and the latest
            event in the stream, if any.
            transaction: The transaction over the in-memory database, which will
            be the same instance used for inserting events, such that
            transactionality can be maintained.

        Raises:
            UnmetWriteConditionError: If the corresponding WriteCondition is
            not satisfied.

        Returns:
            None: If the corresponding WriteCondition is satisfied.
        """
        raise NotImplementedError


class NoConditionEnforcer(WriteConditionEnforcer):
    def assert_satisfied(
        self,
        context: WriteConditionEnforcerContext,
        transaction: InMemoryEventsDBTransaction,
    ):
        return


class NoConditionConverter(Converter[NoCondition, WriteConditionEnforcer]):
    def convert(self, item: NoCondition) -> WriteConditionEnforcer:
        return NoConditionEnforcer()


class PositionIsConditionEnforcer(WriteConditionEnforcer):
    def __init__(self, position: int | None):
        self.position = position

    def assert_satisfied(
        self,
        context: WriteConditionEnforcerContext,
        transaction: InMemoryEventsDBTransaction,
    ) -> None:
        latest_event = context.latest_event
        latest_position = latest_event.position if latest_event else None
        if latest_position != self.position:
            raise UnmetWriteConditionError("unexpected stream position")


class PositionIsConditionConverter(
    Converter[PositionIsCondition, WriteConditionEnforcer]
):
    def convert(self, item: PositionIsCondition) -> WriteConditionEnforcer:
        return PositionIsConditionEnforcer(item.position)


class EmptyStreamConditionEnforcer(WriteConditionEnforcer):
    def assert_satisfied(
        self,
        context: WriteConditionEnforcerContext,
        transaction: InMemoryEventsDBTransaction,
    ) -> None:
        latest_event = context.latest_event
        if latest_event is not None:
            raise UnmetWriteConditionError("stream is not empty")


class EmptyStreamConditionConverter(
    Converter[EmptyStreamCondition, WriteConditionEnforcer]
):
    def convert(self, item: EmptyStreamCondition) -> WriteConditionEnforcer:
        return EmptyStreamConditionEnforcer()


class AndConditionEnforcer(WriteConditionEnforcer):
    def __init__(self, enforcers: Sequence[WriteConditionEnforcer]):
        self.enforcers = enforcers

    def assert_satisfied(
        self,
        context: WriteConditionEnforcerContext,
        transaction: InMemoryEventsDBTransaction,
    ) -> None:
        for enforcer in self.enforcers:
            enforcer.assert_satisfied(context=context, transaction=transaction)


class AndConditionConverter(Converter[AndCondition, WriteConditionEnforcer]):
    def __init__(
        self,
        condition_converter: Converter[WriteCondition, WriteConditionEnforcer],
    ):
        self.condition_converter = condition_converter

    def convert(self, item: AndCondition) -> WriteConditionEnforcer:
        return AndConditionEnforcer(
            enforcers=[
                self.condition_converter.convert(condition)
                for condition in item.conditions
            ]
        )


class OrConditionEnforcer(WriteConditionEnforcer):
    def __init__(self, enforcers: Sequence[WriteConditionEnforcer]):
        self.enforcers = enforcers

    def assert_satisfied(
        self,
        context: WriteConditionEnforcerContext,
        transaction: InMemoryEventsDBTransaction,
    ) -> None:
        first_exception = None
        for enforcer in self.enforcers:
            try:
                enforcer.assert_satisfied(context, transaction)
                return
            except UnmetWriteConditionError as e:
                first_exception = e
        if first_exception is not None:
            raise first_exception


class OrConditionConverter(Converter[OrCondition, WriteConditionEnforcer]):
    def __init__(
        self,
        condition_converter: Converter[WriteCondition, WriteConditionEnforcer],
    ):
        self.condition_converter = condition_converter

    def convert(self, item: OrCondition) -> WriteConditionEnforcer:
        return OrConditionEnforcer(
            enforcers=[
                self.condition_converter.convert(condition)
                for condition in item.conditions
            ]
        )


class TypeRegistryConditionConverter(
    TypeRegistryConverter[WriteCondition, WriteConditionEnforcer]
):
    def register[WC: WriteCondition](
        self,
        item_type: type[WC],
        converter: Converter[WC, WriteConditionEnforcer],
    ) -> Self:
        return super()._register(item_type, converter)

    def with_default_condition_converters(self) -> Self:
        return (
            self.register(NoCondition, NoConditionConverter())
            .register(PositionIsCondition, PositionIsConditionConverter())
            .register(EmptyStreamCondition, EmptyStreamConditionConverter())
            .register(AndCondition, AndConditionConverter(self))
            .register(OrCondition, OrConditionConverter(self))
        )
