from dataclasses import dataclass

from logicblocks.event.store.conditions import (
    AndCondition,
    EmptyStreamCondition,
    OrCondition,
    PositionIsCondition,
    WriteCondition,
    position_is,
    stream_is_empty,
)
from logicblocks.event.testing import data


@dataclass(frozen=True)
class EventNameIsCondition(WriteCondition):
    name: str


@dataclass(frozen=True)
class StreamNameIsCondition(WriteCondition):
    stream: str


@dataclass(frozen=True)
class CategoryNameIsCondition(WriteCondition):
    category: str


class TestWriteConditionCombination:
    def test_combining_with_and_creates_and_condition(self):
        event_name = data.random_event_name()
        stream_name = data.random_event_stream_name()
        category_name = data.random_event_category_name()

        condition1 = EventNameIsCondition(name=event_name)
        condition2 = StreamNameIsCondition(stream=stream_name)
        condition3 = CategoryNameIsCondition(category=category_name)
        combined_condition = condition1 & condition2 & condition3

        assert isinstance(combined_condition, AndCondition)
        assert isinstance(combined_condition, WriteCondition)
        assert combined_condition.conditions == {
            condition1,
            condition2,
            condition3,
        }

    def test_combining_with_or_creates_or_condition(self):
        event_name = data.random_event_name()
        stream_name = data.random_event_stream_name()
        category_name = data.random_event_category_name()

        condition1 = EventNameIsCondition(name=event_name)
        condition2 = StreamNameIsCondition(stream=stream_name)
        condition3 = CategoryNameIsCondition(category=category_name)
        combined_condition = condition1 | condition2 | condition3

        assert isinstance(combined_condition, OrCondition)
        assert isinstance(combined_condition, WriteCondition)
        assert combined_condition.conditions == {
            condition1,
            condition2,
            condition3,
        }


class TestWriteConditionOperatorPrecedence:
    def test_and_takes_precedence_second(self):
        event_name = data.random_event_name()
        stream_name = data.random_event_stream_name()
        category_name = data.random_event_category_name()

        condition1 = EventNameIsCondition(name=event_name)
        condition2 = StreamNameIsCondition(stream=stream_name)
        condition3 = CategoryNameIsCondition(category=category_name)
        combined_condition = condition1 | condition2 & condition3

        assert combined_condition == OrCondition.construct(
            condition1,
            AndCondition.construct(condition2, condition3),
        )

    def test_and_takes_precedence_first(self):
        event_name = data.random_event_name()
        stream_name = data.random_event_stream_name()
        category_name = data.random_event_category_name()

        condition1 = EventNameIsCondition(name=event_name)
        condition2 = StreamNameIsCondition(stream=stream_name)
        condition3 = CategoryNameIsCondition(category=category_name)
        combined_condition = condition1 & condition2 | condition3

        assert combined_condition == OrCondition.construct(
            condition3, AndCondition.construct(condition1, condition2)
        )

    def test_brackets_take_precedence_first(self):
        event_name = data.random_event_name()
        stream_name = data.random_event_stream_name()
        category_name = data.random_event_category_name()

        condition1 = EventNameIsCondition(name=event_name)
        condition2 = StreamNameIsCondition(stream=stream_name)
        condition3 = CategoryNameIsCondition(category=category_name)
        combined_condition = (condition1 | condition2) & condition3

        assert combined_condition == AndCondition.construct(
            condition3,
            OrCondition.construct(condition1, condition2),
        )

    def test_brackets_take_precedence_second(self):
        event_name = data.random_event_name()
        stream_name = data.random_event_stream_name()
        category_name = data.random_event_category_name()

        condition1 = EventNameIsCondition(name=event_name)
        condition2 = StreamNameIsCondition(stream=stream_name)
        condition3 = CategoryNameIsCondition(category=category_name)
        combined_condition = condition1 & (condition2 | condition3)

        assert combined_condition == AndCondition.construct(
            condition1, OrCondition.construct(condition2, condition3)
        )


class TestConvenienceFunctions:
    def test_position_is(self):
        condition = position_is(1)
        assert isinstance(condition, PositionIsCondition)
        assert condition.position == 1

    def test_stream_is_empty(self):
        condition = stream_is_empty()
        assert isinstance(condition, EmptyStreamCondition)
