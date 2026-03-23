from collections.abc import AsyncIterator
from dataclasses import dataclass

import pytest

from logicblocks.event.sources import constraints
from logicblocks.event.store.adapters.memory.converters import (
    AndConditionConverter,
    FilteringQueryApplier,
    OffsetPagingConstraintConverter,
    OffsetPagingQueryApplier,
    OrConditionConverter,
    SequenceNumberAfterConstraintConverter,
    TypeRegistryConditionConverter,
    TypeRegistryConstraintConverter,
    WriteConditionEnforcer,
    WriteConditionEnforcerContext,
)
from logicblocks.event.store.adapters.memory.db import (
    InMemoryEventsDB,
    InMemoryEventsDBTransaction,
)
from logicblocks.event.store.conditions import (
    AndCondition,
    OrCondition,
    WriteCondition,
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

    def assert_satisfied(
        self,
        context: WriteConditionEnforcerContext,
        transaction: InMemoryEventsDBTransaction,
    ) -> None:
        latest_event = context.latest_event
        if latest_event is None or latest_event.name != self.name:
            raise UnmetWriteConditionError("unexpected event name")


class StreamNameIsConditionEnforcer(WriteConditionEnforcer):
    def __init__(self, stream_name: str):
        self.stream_name = stream_name

    def assert_satisfied(
        self,
        context: WriteConditionEnforcerContext,
        transaction: InMemoryEventsDBTransaction,
    ) -> None:
        latest_event = context.latest_event
        if latest_event is None or latest_event.stream != self.stream_name:
            raise UnmetWriteConditionError("unexpected stream name")


class CategoryNameIsConditionEnforcer(WriteConditionEnforcer):
    def __init__(self, category_name: str):
        self.category_name = category_name

    def assert_satisfied(
        self,
        context: WriteConditionEnforcerContext,
        transaction: InMemoryEventsDBTransaction,
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


def make_transaction() -> InMemoryEventsDBTransaction:
    return InMemoryEventsDBTransaction(db=InMemoryEventsDB())


def make_context(
    event: StoredEvent[str, JsonValue],
) -> WriteConditionEnforcerContext:
    return WriteConditionEnforcerContext(
        identifier=StreamIdentifier(event.category, event.stream),
        latest_event=event,
    )


class TestAndCondition:
    def test_met(self):
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
        enforcer.assert_satisfied(
            context=make_context(event), transaction=make_transaction()
        )

    def test_first_unmet(self):
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
            enforcer.assert_satisfied(
                context=make_context(event), transaction=make_transaction()
            )

    def test_second_unmet(self):
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
            enforcer.assert_satisfied(
                context=make_context(event), transaction=make_transaction()
            )

    def test_both_unmet(self):
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
            enforcer.assert_satisfied(
                context=make_context(event), transaction=make_transaction()
            )

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

        converter = make_test_type_registry_converter()
        converter = converter.register(
            AndCondition, AndConditionConverter(converter)
        )

        enforcer = converter.convert(combined_condition)

        enforcer.assert_satisfied(
            context=make_context(event), transaction=make_transaction()
        )


class TestWriteConditionsOr:
    def test_both_met(self):
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

        enforcer.assert_satisfied(
            context=make_context(event), transaction=make_transaction()
        )

    def test_first_met(self):
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

        enforcer.assert_satisfied(
            context=make_context(event), transaction=make_transaction()
        )

    def test_second_met(self):
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

        enforcer.assert_satisfied(
            context=make_context(event), transaction=make_transaction()
        )

    def test_both_unmet(self):
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
            enforcer.assert_satisfied(
                context=make_context(event), transaction=make_transaction()
            )


async def _collect[E](iterator: AsyncIterator[E]) -> list[E]:
    return [item async for item in iterator]


async def _async_iter[E](*items: E) -> AsyncIterator[E]:
    for item in items:
        yield item


class TestFilteringQueryApplier:
    async def test_yields_events_matching_predicate(self):
        event_1 = StoredEventBuilder().with_sequence_number(0).build()
        event_2 = StoredEventBuilder().with_sequence_number(1).build()
        event_3 = StoredEventBuilder().with_sequence_number(2).build()

        applier = FilteringQueryApplier(check=lambda e: e.sequence_number > 0)
        result = await _collect(
            applier.apply(_async_iter(event_1, event_2, event_3))
        )

        assert result == [event_2, event_3]

    async def test_yields_nothing_when_no_events_match(self):
        event_1 = StoredEventBuilder().with_sequence_number(0).build()

        applier = FilteringQueryApplier(check=lambda e: e.sequence_number > 5)
        result = await _collect(applier.apply(_async_iter(event_1)))

        assert result == []

    async def test_yields_all_events_when_all_match(self):
        event_1 = StoredEventBuilder().build()
        event_2 = StoredEventBuilder().build()

        applier = FilteringQueryApplier(check=lambda e: True)
        result = await _collect(applier.apply(_async_iter(event_1, event_2)))

        assert result == [event_1, event_2]

    async def test_has_filter_order(self):
        applier = FilteringQueryApplier(check=lambda e: True)

        assert applier.order == 0


class TestOffsetPagingQueryApplier:
    async def test_yields_first_page(self):
        event_1 = StoredEventBuilder().build()
        event_2 = StoredEventBuilder().build()
        event_3 = StoredEventBuilder().build()

        applier = OffsetPagingQueryApplier(offset=0, limit=2)
        result = await _collect(
            applier.apply(_async_iter(event_1, event_2, event_3))
        )

        assert result == [event_1, event_2]

    async def test_yields_second_page(self):
        event_1 = StoredEventBuilder().build()
        event_2 = StoredEventBuilder().build()
        event_3 = StoredEventBuilder().build()
        event_4 = StoredEventBuilder().build()

        applier = OffsetPagingQueryApplier(offset=2, limit=2)
        result = await _collect(
            applier.apply(_async_iter(event_1, event_2, event_3, event_4))
        )

        assert result == [event_3, event_4]

    async def test_yields_partial_last_page(self):
        event_1 = StoredEventBuilder().build()
        event_2 = StoredEventBuilder().build()
        event_3 = StoredEventBuilder().build()

        applier = OffsetPagingQueryApplier(offset=2, limit=2)
        result = await _collect(
            applier.apply(_async_iter(event_1, event_2, event_3))
        )

        assert result == [event_3]

    async def test_yields_nothing_when_offset_beyond_events(self):
        event_1 = StoredEventBuilder().build()

        applier = OffsetPagingQueryApplier(offset=10, limit=2)
        result = await _collect(applier.apply(_async_iter(event_1)))

        assert result == []

    async def test_has_window_order(self):
        applier = OffsetPagingQueryApplier(offset=0, limit=10)

        assert applier.order == 1


class TestSequenceNumberAfterConstraintConverter:
    async def test_produces_filtering_applier(self):
        converter = SequenceNumberAfterConstraintConverter()
        constraint = constraints.SequenceNumberAfterConstraint(
            sequence_number=1
        )

        applier = converter.convert(constraint)

        assert isinstance(applier, FilteringQueryApplier)

    async def test_applier_filters_by_sequence_number(self):
        converter = SequenceNumberAfterConstraintConverter()
        constraint = constraints.SequenceNumberAfterConstraint(
            sequence_number=1
        )

        event_1 = StoredEventBuilder().with_sequence_number(0).build()
        event_2 = StoredEventBuilder().with_sequence_number(1).build()
        event_3 = StoredEventBuilder().with_sequence_number(2).build()

        applier = converter.convert(constraint)
        result = await _collect(
            applier.apply(_async_iter(event_1, event_2, event_3))
        )

        assert result == [event_3]


class TestOffsetPagingConstraintConverter:
    async def test_produces_paging_applier(self):
        converter = OffsetPagingConstraintConverter()
        constraint = constraints.OffsetPagingConstraint(
            page_number=2, item_count=3
        )

        applier = converter.convert(constraint)

        assert isinstance(applier, OffsetPagingQueryApplier)

    async def test_applier_pages_events(self):
        converter = OffsetPagingConstraintConverter()
        constraint = constraints.OffsetPagingConstraint(
            page_number=2, item_count=2
        )

        event_1 = StoredEventBuilder().build()
        event_2 = StoredEventBuilder().build()
        event_3 = StoredEventBuilder().build()
        event_4 = StoredEventBuilder().build()

        applier = converter.convert(constraint)
        result = await _collect(
            applier.apply(_async_iter(event_1, event_2, event_3, event_4))
        )

        assert result == [event_3, event_4]


class TestTypeRegistryConstraintConverterWithAppliers:
    async def test_converts_sequence_number_constraint(self):
        converter = TypeRegistryConstraintConverter().with_default_constraint_converters()
        constraint = constraints.SequenceNumberAfterConstraint(
            sequence_number=1
        )

        applier = converter.convert(constraint)

        assert isinstance(applier, FilteringQueryApplier)

    async def test_converts_paging_constraint(self):
        converter = TypeRegistryConstraintConverter().with_default_constraint_converters()
        constraint = constraints.OffsetPagingConstraint(
            page_number=1, item_count=10
        )

        applier = converter.convert(constraint)

        assert isinstance(applier, OffsetPagingQueryApplier)
