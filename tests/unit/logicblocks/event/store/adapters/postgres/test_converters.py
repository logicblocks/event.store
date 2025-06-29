from dataclasses import dataclass
from unittest.mock import AsyncMock

import pytest
from psycopg import AsyncConnection

from logicblocks.event.persistence.postgres import (
    ColumnReference,
    Condition,
    Constant,
    Operator,
    Query,
)
from logicblocks.event.store.adapters.postgres.converters import (
    AndConditionConverter,
    OrConditionConverter,
    SequenceNumberAfterConstraintConverter,
    StreamNamePrefixConstraintConverter,
    TypeRegistryConditionConverter,
    WriteConditionEnforcer,
    WriteConditionEnforcerContext,
)
from logicblocks.event.store.conditions import (
    AndCondition,
    OrCondition,
    WriteCondition,
)
from logicblocks.event.store.constraints import (
    SequenceNumberAfterConstraint,
    StreamNamePrefixConstraint,
)
from logicblocks.event.store.exceptions import UnmetWriteConditionError
from logicblocks.event.testing import StoredEventBuilder, data
from logicblocks.event.types import Converter, JsonValue, StoredEvent
from logicblocks.event.types.identifier import StreamIdentifier


@dataclass(frozen=True)
class EventNameIsCondition(WriteCondition):
    name: str


@dataclass(frozen=True)
class StreamNameIsCondition(WriteCondition):
    stream: str


@dataclass(frozen=True)
class CategoryNameIsCondition(WriteCondition):
    category: str


class EventNameIsConditionEnforcer(WriteConditionEnforcer):
    def __init__(self, name: str):
        self.name = name

    async def assert_satisfied(
        self,
        context: WriteConditionEnforcerContext,
        connection: AsyncConnection,
    ) -> None:
        latest_event = context.latest_event
        if latest_event is None or latest_event.name != self.name:
            raise UnmetWriteConditionError("unexpected event name")


class StreamNameIsConditionEnforcer(WriteConditionEnforcer):
    def __init__(self, stream_name: str):
        self.stream_name = stream_name

    async def assert_satisfied(
        self,
        context: WriteConditionEnforcerContext,
        connection: AsyncConnection,
    ) -> None:
        latest_event = context.latest_event
        if latest_event is None or latest_event.stream != self.stream_name:
            raise UnmetWriteConditionError("unexpected stream name")


class CategoryNameIsConditionEnforcer(WriteConditionEnforcer):
    def __init__(self, category_name: str):
        self.category_name = category_name

    async def assert_satisfied(
        self,
        context: WriteConditionEnforcerContext,
        connection: AsyncConnection,
    ) -> None:
        latest_event = context.latest_event
        if latest_event is None or latest_event.category != self.category_name:
            raise UnmetWriteConditionError("unexpected category name")


class EventNameIsConditionConverter(
    Converter[EventNameIsCondition, EventNameIsConditionEnforcer]
):
    def convert(
        self, item: EventNameIsCondition
    ) -> EventNameIsConditionEnforcer:
        return EventNameIsConditionEnforcer(name=item.name)


class StreamNameIsConditionConverter(
    Converter[StreamNameIsCondition, StreamNameIsConditionEnforcer]
):
    def convert(
        self, item: StreamNameIsCondition
    ) -> StreamNameIsConditionEnforcer:
        return StreamNameIsConditionEnforcer(stream_name=item.stream)


class CategoryNameIsConditionConverter(
    Converter[CategoryNameIsCondition, CategoryNameIsConditionEnforcer]
):
    def convert(
        self, item: CategoryNameIsCondition
    ) -> CategoryNameIsConditionEnforcer:
        return CategoryNameIsConditionEnforcer(category_name=item.category)


def make_test_type_registry_converter() -> TypeRegistryConditionConverter:
    return (
        TypeRegistryConditionConverter()
        .register(EventNameIsCondition, EventNameIsConditionConverter())
        .register(StreamNameIsCondition, StreamNameIsConditionConverter())
        .register(CategoryNameIsCondition, CategoryNameIsConditionConverter())
    )


def make_connection() -> AsyncConnection:
    return AsyncMock(AsyncConnection)


def make_context(
    event: StoredEvent[str, JsonValue],
) -> WriteConditionEnforcerContext:
    return WriteConditionEnforcerContext(
        identifier=StreamIdentifier(event.category, event.stream),
        latest_event=event,
    )


class TestWriteConditionsAnd:
    async def test_met(self):
        event_name = data.random_event_name()
        stream_name = data.random_event_stream_name()

        condition1 = EventNameIsCondition(name=event_name)
        condition2 = StreamNameIsCondition(stream=stream_name)
        combined_condition = condition1 & condition2

        event = StoredEventBuilder(name=event_name, stream=stream_name).build()

        converter = make_test_type_registry_converter()
        converter = converter.register(
            AndCondition, AndConditionConverter(converter)
        )

        enforcer = converter.convert(combined_condition)
        await enforcer.assert_satisfied(
            context=make_context(event), connection=make_connection()
        )

    async def test_first_unmet(self):
        event_name_1 = data.random_event_name()
        event_name_2 = data.random_event_name()
        stream_name = data.random_event_stream_name()

        condition1 = EventNameIsCondition(name=event_name_1)
        condition2 = StreamNameIsCondition(stream=stream_name)
        combined_condition = condition1 & condition2

        event = StoredEventBuilder(
            name=event_name_2, stream=stream_name
        ).build()

        converter = make_test_type_registry_converter()
        converter = converter.register(
            AndCondition, AndConditionConverter(converter)
        )

        enforcer = converter.convert(combined_condition)

        with pytest.raises(UnmetWriteConditionError):
            await enforcer.assert_satisfied(
                context=make_context(event), connection=make_connection()
            )

    async def test_second_unmet(self):
        event_name = data.random_event_name()
        stream_name_1 = data.random_event_stream_name()
        stream_name_2 = data.random_event_stream_name()

        condition1 = EventNameIsCondition(name=event_name)
        condition2 = StreamNameIsCondition(stream=stream_name_1)
        combined_condition = condition1 & condition2

        event = StoredEventBuilder(
            name=event_name, stream=stream_name_2
        ).build()

        converter = make_test_type_registry_converter()
        converter = converter.register(
            AndCondition, AndConditionConverter(converter)
        )

        enforcer = converter.convert(combined_condition)

        with pytest.raises(UnmetWriteConditionError):
            await enforcer.assert_satisfied(
                context=make_context(event), connection=make_connection()
            )

    async def test_both_unmet(self):
        event_name_1 = data.random_event_name()
        event_name_2 = data.random_event_name()
        stream_name_1 = data.random_event_stream_name()
        stream_name_2 = data.random_event_stream_name()

        condition1 = EventNameIsCondition(name=event_name_1)
        condition2 = StreamNameIsCondition(stream=stream_name_1)
        combined_condition = condition1 & condition2

        event = StoredEventBuilder(
            name=event_name_2, stream=stream_name_2
        ).build()

        converter = make_test_type_registry_converter()
        converter = converter.register(
            AndCondition, AndConditionConverter(converter)
        )

        enforcer = converter.convert(combined_condition)

        with pytest.raises(UnmetWriteConditionError):
            await enforcer.assert_satisfied(
                context=make_context(event), connection=make_connection()
            )

    async def test_three_conditions_met(self):
        event_name = data.random_event_name()
        stream_name = data.random_event_stream_name()
        category_name = data.random_event_category_name()

        condition1 = EventNameIsCondition(name=event_name)
        condition2 = StreamNameIsCondition(stream=stream_name)
        condition3 = CategoryNameIsCondition(category=category_name)
        combined_condition = condition1 & condition2 & condition3

        event = StoredEventBuilder(
            name=event_name, stream=stream_name, category=category_name
        ).build()

        converter = make_test_type_registry_converter()
        converter = converter.register(
            AndCondition, AndConditionConverter(converter)
        )

        enforcer = converter.convert(combined_condition)

        await enforcer.assert_satisfied(
            context=make_context(event), connection=make_connection()
        )


class TestWriteConditionsOr:
    async def test_both_met(self):
        event_name = data.random_event_name()
        stream_name = data.random_event_stream_name()

        condition1 = EventNameIsCondition(name=event_name)
        condition2 = StreamNameIsCondition(stream=stream_name)
        combined_condition = condition1 | condition2

        event = StoredEventBuilder(name=event_name, stream=stream_name).build()

        converter = make_test_type_registry_converter()
        converter = converter.register(
            OrCondition, OrConditionConverter(converter)
        )

        enforcer = converter.convert(combined_condition)

        await enforcer.assert_satisfied(
            context=make_context(event), connection=make_connection()
        )

    async def test_first_met(self):
        event_name = data.random_event_name()
        stream_name_1 = data.random_event_stream_name()
        stream_name_2 = data.random_event_stream_name()

        condition1 = EventNameIsCondition(name=event_name)
        condition2 = StreamNameIsCondition(stream=stream_name_1)
        combined_condition = condition1 | condition2

        event = StoredEventBuilder(
            name=event_name, stream=stream_name_2
        ).build()

        converter = make_test_type_registry_converter()
        converter = converter.register(
            OrCondition, OrConditionConverter(converter)
        )

        enforcer = converter.convert(combined_condition)

        await enforcer.assert_satisfied(
            context=make_context(event), connection=make_connection()
        )

    async def test_second_met(self):
        event_name_1 = data.random_event_name()
        event_name_2 = data.random_event_name()
        stream_name = data.random_event_stream_name()

        condition1 = EventNameIsCondition(name=event_name_1)
        condition2 = StreamNameIsCondition(stream=stream_name)
        combined_condition = condition1 | condition2

        event = StoredEventBuilder(
            name=event_name_2, stream=stream_name
        ).build()

        converter = make_test_type_registry_converter()
        converter = converter.register(
            OrCondition, OrConditionConverter(converter)
        )

        enforcer = converter.convert(combined_condition)

        await enforcer.assert_satisfied(
            context=make_context(event), connection=make_connection()
        )

    async def test_both_unmet(self):
        event_name_1 = data.random_event_name()
        event_name_2 = data.random_event_name()
        stream_name_1 = data.random_event_stream_name()
        stream_name_2 = data.random_event_stream_name()

        condition1 = EventNameIsCondition(name=event_name_1)
        condition2 = StreamNameIsCondition(stream=stream_name_1)
        combined_condition = condition1 | condition2

        event = StoredEventBuilder(
            name=event_name_2,
            stream=stream_name_2,
        ).build()

        converter = make_test_type_registry_converter()
        converter = converter.register(
            OrCondition, OrConditionConverter(converter)
        )

        enforcer = converter.convert(combined_condition)

        with pytest.raises(UnmetWriteConditionError):
            await enforcer.assert_satisfied(
                context=make_context(event), connection=make_connection()
            )


class TestSequenceNumberAfterConstraintConversion:
    def test_applies_greater_than_where_clause_to_query(self):
        sequence_number = 100
        constraint = SequenceNumberAfterConstraint(
            sequence_number=sequence_number
        )
        converter = SequenceNumberAfterConstraintConverter()

        query_applier = converter.convert(constraint)
        base_query = Query().select("*").from_table("events")
        result_query = query_applier.apply(base_query)

        expected_query = (
            Query()
            .select("*")
            .from_table("events")
            .where(
                Condition()
                .left(ColumnReference(field="sequence_number"))
                .operator(Operator.GREATER_THAN)
                .right(Constant(sequence_number))
            )
        )

        assert str(result_query) == str(expected_query)


class TestStreamNamePrefixConstraintConversion:
    def test_applies_like_where_clause_to_query(self):
        stream_name_prefix = "test_"
        constraint = StreamNamePrefixConstraint(prefix=stream_name_prefix)
        converter = StreamNamePrefixConstraintConverter()

        query_applier = converter.convert(constraint)
        base_query = Query().select("*").from_table("events")
        result_query = query_applier.apply(base_query)

        expected_query = (
            Query()
            .select("*")
            .from_table("events")
            .where(
                Condition()
                .left(ColumnReference(field="stream"))
                .operator(Operator.LIKE)
                .right(Constant(f"{stream_name_prefix}%"))
            )
        )

        assert str(result_query) == str(expected_query)
