from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Self

from psycopg import AsyncConnection

from logicblocks.event.persistence import TypeRegistryConverter
from logicblocks.event.persistence.postgres import (
    Condition,
    Constant,
    Operator,
    Query,
    QueryApplier,
)
from logicblocks.event.persistence.postgres.query import ColumnReference
from logicblocks.event.sources import constraints
from logicblocks.event.types import (
    Converter,
    JsonValue,
    StoredEvent,
    StreamIdentifier,
)

from ...conditions import (
    AndCondition,
    EmptyStreamCondition,
    NoCondition,
    OrCondition,
    PositionIsCondition,
    WriteCondition,
)
from ...exceptions import UnmetWriteConditionError


class SequenceNumberAfterConstraintQueryApplier(QueryApplier):
    """Restricts a query to events after a sequence number.

    Args:
        sequence_number: The exclusive lower bound on sequence number.
    """

    def __init__(self, sequence_number: int):
        self.sequence_number = sequence_number

    def apply(self, target: Query) -> Query:
        return target.where(
            Condition()
            .left(ColumnReference(field="sequence_number"))
            .operator(Operator.GREATER_THAN)
            .right(Constant(self.sequence_number))
        )


class SequenceNumberAfterConstraintConverter(
    Converter[constraints.SequenceNumberAfterConstraint, QueryApplier]
):
    def convert(
        self, item: constraints.SequenceNumberAfterConstraint
    ) -> QueryApplier:
        return SequenceNumberAfterConstraintQueryApplier(item.sequence_number)


class TypeRegistryConstraintConverter(
    TypeRegistryConverter[constraints.QueryConstraint, QueryApplier]
):
    """Converts query constraints into Postgres query appliers by type.

    Register a converter for each supported constraint type, or call
    `with_default_constraint_converters` to register the built-in ones.
    """

    def register[QC: constraints.QueryConstraint](
        self,
        item_type: type[QC],
        converter: Converter[QC, QueryApplier],
    ) -> Self:
        return super()._register(item_type, converter)

    def with_default_constraint_converters(self) -> Self:
        """Register converters for the built-in query constraints.

        Returns:
            this converter, to allow chaining.
        """
        return self.register(
            constraints.SequenceNumberAfterConstraint,
            SequenceNumberAfterConstraintConverter(),
        )


class WriteConditionEnforcerContext:
    """The state of a stream against which a write condition is checked.

    Args:
        identifier: The identifier of the stream being written to.
        latest_event: The latest event in the stream, or `None` if empty.
    """

    def __init__(
        self,
        identifier: StreamIdentifier,
        latest_event: StoredEvent[str, JsonValue, JsonValue] | None,
    ):
        self.identifier = identifier
        self.latest_event = latest_event


class WriteConditionEnforcer(ABC):
    """Checks a write condition against a stream in the Postgres adapter."""

    @abstractmethod
    async def assert_satisfied(
        self,
        context: WriteConditionEnforcerContext,
        connection: AsyncConnection,
    ) -> None:
        """Raise if the write condition this enforcer represents is unmet.

        Args:
            context: The state of the stream against which the condition is
                checked: its identifier and latest event, if any.
            connection: The database connection that will be used to insert
                the events, so that checks and writes are transactional.

        Raises:
            UnmetWriteConditionError: If the condition is not met.
        """
        raise NotImplementedError


class NoConditionEnforcer(WriteConditionEnforcer):
    async def assert_satisfied(
        self,
        context: WriteConditionEnforcerContext,
        connection: AsyncConnection,
    ):
        return


class NoConditionConverter(Converter[NoCondition, WriteConditionEnforcer]):
    def convert(self, item: NoCondition) -> WriteConditionEnforcer:
        return NoConditionEnforcer()


class PositionIsConditionEnforcer(WriteConditionEnforcer):
    def __init__(self, position: int | None):
        self.position = position

    async def assert_satisfied(
        self,
        context: WriteConditionEnforcerContext,
        connection: AsyncConnection,
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
    async def assert_satisfied(
        self,
        context: WriteConditionEnforcerContext,
        connection: AsyncConnection,
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

    async def assert_satisfied(
        self,
        context: WriteConditionEnforcerContext,
        connection: AsyncConnection,
    ) -> None:
        for enforcer in self.enforcers:
            await enforcer.assert_satisfied(
                context=context, connection=connection
            )


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

    async def assert_satisfied(
        self,
        context: WriteConditionEnforcerContext,
        connection: AsyncConnection,
    ) -> None:
        first_exception = None
        for enforcer in self.enforcers:
            try:
                await enforcer.assert_satisfied(context, connection)
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
    """Converts write conditions into Postgres enforcers by type.

    Register a converter for each supported condition type, or call
    `with_default_condition_converters` to register the built-in ones.
    """

    def register[WC: WriteCondition](
        self,
        item_type: type[WC],
        converter: Converter[WC, WriteConditionEnforcer],
    ) -> Self:
        return super()._register(item_type, converter)

    def with_default_condition_converters(self) -> Self:
        """Register converters for the built-in write conditions.

        The `AndCondition` and `OrCondition` converters use this converter to
        convert their constituent conditions.

        Returns:
            this converter, to allow chaining.
        """
        return (
            self.register(NoCondition, NoConditionConverter())
            .register(PositionIsCondition, PositionIsConditionConverter())
            .register(EmptyStreamCondition, EmptyStreamConditionConverter())
            .register(AndCondition, AndConditionConverter(self))
            .register(OrCondition, OrConditionConverter(self))
        )
