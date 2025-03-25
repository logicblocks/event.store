from dataclasses import dataclass

import pytest

from logicblocks.event.store.conditions import (
    ConditionCombinators,
    EmptyStreamCondition,
    NoCondition,
    PositionIsCondition,
    WriteCondition,
    WriteConditions,
    position_is,
    stream_is_empty,
)
from logicblocks.event.store.exceptions import UnmetWriteConditionError
from logicblocks.event.testing import StoredEventBuilder, data
from logicblocks.event.types import StoredEvent


class TestNoCondition:
    def test_no_condition(self):
        condition = NoCondition

        condition.assert_met_by(last_event=None)
        condition.assert_met_by(last_event=StoredEventBuilder().build())


class TestPositionIsCondition:
    def test_position_is_condition_met(self):
        position = data.random_event_position()
        condition = PositionIsCondition(position=position)

        condition.assert_met_by(
            last_event=StoredEventBuilder().with_position(position).build()
        )

    def test_position_is_condition_unmet(self):
        expected_position = data.random_event_position()
        condition = PositionIsCondition(position=expected_position)

        event = (
            StoredEventBuilder().with_position(expected_position + 1).build()
        )
        with pytest.raises(UnmetWriteConditionError):
            condition.assert_met_by(last_event=event)


class TestEmptyStreamCondition:
    def test_empty_stream_condition_met(self):
        condition = EmptyStreamCondition()
        condition.assert_met_by(last_event=None)

    def test_empty_stream_condition_unmet(self):
        condition = EmptyStreamCondition()

        with pytest.raises(UnmetWriteConditionError):
            condition.assert_met_by(last_event=StoredEventBuilder().build())


@dataclass(frozen=True)
class EventNameIsCondition(WriteCondition):
    name: str

    def assert_met_by(self, *, last_event: StoredEvent | None):
        if last_event is None or last_event.name != self.name:
            raise UnmetWriteConditionError("unexpected event name")


@dataclass(frozen=True)
class StreamNameIsCondition(WriteCondition):
    stream: str

    def assert_met_by(self, *, last_event: StoredEvent | None):
        if last_event is None or last_event.stream != self.stream:
            raise UnmetWriteConditionError("unexpected stream name")


@dataclass(frozen=True)
class CategoryNameIsCondition(WriteCondition):
    category: str

    def assert_met_by(self, *, last_event: StoredEvent | None):
        if last_event is None or last_event.category != self.category:
            raise UnmetWriteConditionError("unexpected category name")


class TestWriteConditions:
    def test_combining_with_and_creates_write_conditions(self):
        event_name = data.random_event_name()
        stream_name = data.random_event_stream_name()
        category_name = data.random_event_category_name()

        condition1 = EventNameIsCondition(name=event_name)
        condition2 = StreamNameIsCondition(stream=stream_name)
        condition3 = CategoryNameIsCondition(category=category_name)
        combined_condition = condition1 & condition2 & condition3

        assert isinstance(combined_condition, WriteConditions)
        assert isinstance(combined_condition, WriteCondition)
        assert combined_condition.conditions == {
            condition1,
            condition2,
            condition3,
        }
        assert combined_condition.combinator == ConditionCombinators.AND

    def test_combining_with_or_creates_write_conditions(self):
        event_name = data.random_event_name()
        stream_name = data.random_event_stream_name()
        category_name = data.random_event_category_name()

        condition1 = EventNameIsCondition(name=event_name)
        condition2 = StreamNameIsCondition(stream=stream_name)
        condition3 = CategoryNameIsCondition(category=category_name)
        combined_condition = condition1 | condition2 | condition3

        assert isinstance(combined_condition, WriteConditions)
        assert isinstance(combined_condition, WriteCondition)
        assert combined_condition.conditions == {
            condition1,
            condition2,
            condition3,
        }
        assert combined_condition.combinator == ConditionCombinators.OR

    class TestWriteConditionOperatorPrecedence:
        def test_and_takes_precedence_second(self):
            event_name = data.random_event_name()
            stream_name = data.random_event_stream_name()
            category_name = data.random_event_category_name()

            condition1 = EventNameIsCondition(name=event_name)
            condition2 = StreamNameIsCondition(stream=stream_name)
            condition3 = CategoryNameIsCondition(category=category_name)
            combined_condition = condition1 | condition2 & condition3

            assert combined_condition == WriteConditions(
                combinator=ConditionCombinators.OR,
                conditions=frozenset(
                    {
                        condition1,
                        WriteConditions(
                            combinator=ConditionCombinators.AND,
                            conditions=frozenset({condition2, condition3}),
                        ),
                    }
                ),
            )

        def test_and_takes_precedence_first(self):
            event_name = data.random_event_name()
            stream_name = data.random_event_stream_name()
            category_name = data.random_event_category_name()

            condition1 = EventNameIsCondition(name=event_name)
            condition2 = StreamNameIsCondition(stream=stream_name)
            condition3 = CategoryNameIsCondition(category=category_name)
            combined_condition = condition1 & condition2 | condition3

            assert combined_condition == WriteConditions(
                combinator=ConditionCombinators.OR,
                conditions=frozenset(
                    {
                        condition3,
                        WriteConditions(
                            combinator=ConditionCombinators.AND,
                            conditions=frozenset({condition1, condition2}),
                        ),
                    }
                ),
            )

        def test_brackets_take_precedence_first(self):
            event_name = data.random_event_name()
            stream_name = data.random_event_stream_name()
            category_name = data.random_event_category_name()

            condition1 = EventNameIsCondition(name=event_name)
            condition2 = StreamNameIsCondition(stream=stream_name)
            condition3 = CategoryNameIsCondition(category=category_name)
            combined_condition = (condition1 | condition2) & condition3

            assert combined_condition == WriteConditions(
                combinator=ConditionCombinators.AND,
                conditions=frozenset(
                    {
                        condition3,
                        WriteConditions(
                            combinator=ConditionCombinators.OR,
                            conditions=frozenset({condition1, condition2}),
                        ),
                    }
                ),
            )

        def test_brackets_take_precedence_second(self):
            event_name = data.random_event_name()
            stream_name = data.random_event_stream_name()
            category_name = data.random_event_category_name()

            condition1 = EventNameIsCondition(name=event_name)
            condition2 = StreamNameIsCondition(stream=stream_name)
            condition3 = CategoryNameIsCondition(category=category_name)
            combined_condition = condition1 & (condition2 | condition3)

            assert combined_condition == WriteConditions(
                combinator=ConditionCombinators.AND,
                conditions=frozenset(
                    {
                        condition1,
                        WriteConditions(
                            combinator=ConditionCombinators.OR,
                            conditions=frozenset({condition2, condition3}),
                        ),
                    }
                ),
            )

    class TestWriteConditionsAnd:
        def test_met(self):
            event_name = data.random_event_name()
            stream_name = data.random_event_stream_name()

            condition1 = EventNameIsCondition(name=event_name)
            condition2 = StreamNameIsCondition(stream=stream_name)
            combined_condition = condition1 & condition2

            event = StoredEventBuilder(
                name=event_name, stream=stream_name
            ).build()
            combined_condition.assert_met_by(last_event=event)

        def test_first_unmet(self):
            event_name = data.random_event_name()
            stream_name = data.random_event_stream_name()

            condition1 = EventNameIsCondition(name=event_name)
            condition2 = StreamNameIsCondition(stream=stream_name)
            combined_condition = condition1 & condition2

            event = StoredEventBuilder(
                name=data.random_event_name(), stream=stream_name
            ).build()
            with pytest.raises(UnmetWriteConditionError):
                combined_condition.assert_met_by(last_event=event)

        def test_second_unmet(self):
            event_name = data.random_event_name()
            stream_name = data.random_event_stream_name()

            condition1 = EventNameIsCondition(name=event_name)
            condition2 = StreamNameIsCondition(stream=stream_name)
            combined_condition = condition1 & condition2

            event = StoredEventBuilder(
                name=event_name, stream=data.random_event_stream_name()
            ).build()
            with pytest.raises(UnmetWriteConditionError):
                combined_condition.assert_met_by(last_event=event)

        def test_both_unmet(self):
            event_name = data.random_event_name()
            stream_name = data.random_event_stream_name()

            condition1 = EventNameIsCondition(name=event_name)
            condition2 = StreamNameIsCondition(stream=stream_name)
            combined_condition = condition1 & condition2

            event = StoredEventBuilder(
                name=data.random_event_name(),
                stream=data.random_event_stream_name(),
            ).build()
            with pytest.raises(UnmetWriteConditionError):
                combined_condition.assert_met_by(last_event=event)

        def test_three_conditions_met(self):
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
            combined_condition.assert_met_by(last_event=event)

    class TestWriteConditionsOr:
        def test_both_met(self):
            event_name = data.random_event_name()
            stream_name = data.random_event_stream_name()

            condition1 = EventNameIsCondition(name=event_name)
            condition2 = StreamNameIsCondition(stream=stream_name)
            combined_condition = condition1 | condition2

            event = StoredEventBuilder(
                name=event_name, stream=stream_name
            ).build()
            combined_condition.assert_met_by(last_event=event)

        def test_first_met(self):
            event_name = data.random_event_name()
            stream_name = data.random_event_stream_name()

            condition1 = EventNameIsCondition(name=event_name)
            condition2 = StreamNameIsCondition(stream=stream_name)
            combined_condition = condition1 | condition2

            event = StoredEventBuilder(
                name=event_name, stream=data.random_event_stream_name()
            ).build()
            combined_condition.assert_met_by(last_event=event)

        def test_second_met(self):
            event_name = data.random_event_name()
            stream_name = data.random_event_stream_name()

            condition1 = EventNameIsCondition(name=event_name)
            condition2 = StreamNameIsCondition(stream=stream_name)
            combined_condition = condition1 | condition2

            event = StoredEventBuilder(
                name=data.random_event_name(), stream=stream_name
            ).build()
            combined_condition.assert_met_by(last_event=event)

        def test_both_unmet(self):
            event_name = data.random_event_name()
            stream_name = data.random_event_stream_name()

            condition1 = EventNameIsCondition(name=event_name)
            condition2 = StreamNameIsCondition(stream=stream_name)
            combined_condition = condition1 | condition2

            event = StoredEventBuilder(
                name=data.random_event_name(),
                stream=data.random_event_stream_name(),
            ).build()
            with pytest.raises(UnmetWriteConditionError):
                combined_condition.assert_met_by(last_event=event)


class TestConvenienceFunctions:
    def test_position_is(self):
        condition = position_is(1)
        assert isinstance(condition, PositionIsCondition)
        assert condition.position == 1

    def test_stream_is_empty(self):
        condition = stream_is_empty()
        assert isinstance(condition, EmptyStreamCondition)
