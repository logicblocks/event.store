from logicblocks.event.sources import (
    ConstrainedEventSource,
    constraints,
)
from logicblocks.event.sources.constraints import (
    QueryConstraint,
    SequenceNumberAfterConstraint,
)
from logicblocks.event.store import InMemoryStoredEventSource
from logicblocks.event.store.adapters.memory import (
    InMemoryQueryConstraintCheck,
    InMemoryTypeRegistryConstraintConverter,
)
from logicblocks.event.testing import data
from logicblocks.event.testing.builders import StoredEventBuilder
from logicblocks.event.types import (
    Converter,
    JsonValue,
    StoredEvent,
    StreamIdentifier,
)


class TestConstrainedEventSource:
    async def test_returns_identifier_from_underlying_source(self):
        identifier = StreamIdentifier(
            category=data.random_event_category_name(),
            stream=data.random_event_stream_name(),
        )

        event_1 = StoredEventBuilder().with_sequence_number(1).build()
        event_2 = StoredEventBuilder().with_sequence_number(2).build()
        event_3 = StoredEventBuilder().with_sequence_number(3).build()

        delegate = InMemoryStoredEventSource(
            events=[event_1, event_2, event_3], identifier=identifier
        )
        constrained = ConstrainedEventSource(
            delegate=delegate, constraints={SequenceNumberAfterConstraint(1)}
        )

        assert constrained.identifier == identifier

    async def test_returns_latest_from_underlying_source(self):
        identifier = StreamIdentifier(
            category=data.random_event_category_name(),
            stream=data.random_event_stream_name(),
        )

        event_1 = StoredEventBuilder().with_sequence_number(1).build()
        event_2 = StoredEventBuilder().with_sequence_number(2).build()
        event_3 = StoredEventBuilder().with_sequence_number(3).build()

        delegate = InMemoryStoredEventSource(
            events=[event_1, event_2, event_3], identifier=identifier
        )
        constrained = ConstrainedEventSource(
            delegate=delegate, constraints={SequenceNumberAfterConstraint(1)}
        )

        assert await constrained.latest() == event_3

    async def test_constrains_iteration_of_underlying_source(self):
        identifier = StreamIdentifier(
            category=data.random_event_category_name(),
            stream=data.random_event_stream_name(),
        )

        event_1 = StoredEventBuilder().with_sequence_number(1).build()
        event_2 = StoredEventBuilder().with_sequence_number(2).build()
        event_3 = StoredEventBuilder().with_sequence_number(3).build()

        delegate = InMemoryStoredEventSource(
            events=[event_1, event_2, event_3], identifier=identifier
        )
        constrained = ConstrainedEventSource(
            delegate=delegate, constraints={SequenceNumberAfterConstraint(1)}
        )

        events = [event async for event in constrained.iterate()]

        assert events == [event_2, event_3]

    async def test_constrains_iteration_with_additional_constraints(self):
        identifier = StreamIdentifier(
            category=data.random_event_category_name(),
            stream=data.random_event_stream_name(),
        )

        event_1 = (
            StoredEventBuilder()
            .with_name("first")
            .with_sequence_number(1)
            .build()
        )
        event_2 = (
            StoredEventBuilder()
            .with_name("second")
            .with_sequence_number(2)
            .build()
        )
        event_3 = (
            StoredEventBuilder()
            .with_name("third")
            .with_sequence_number(3)
            .build()
        )

        class NotNameConstraint(QueryConstraint):
            def __init__(self, name: str):
                self.name = name

            def met_by(self, *, event: StoredEvent) -> bool:
                return event.name != self.name

        class NotNameConstraintQueryConstraintCheck:
            def __init__(self, constraint: NotNameConstraint):
                self.constraint = constraint

            def __call__(self, event: StoredEvent[str, JsonValue]) -> bool:
                return event.name != self.constraint.name

        class NotNameConstraintConverter(
            Converter[NotNameConstraint, InMemoryQueryConstraintCheck]
        ):
            def convert(
                self, item: NotNameConstraint
            ) -> InMemoryQueryConstraintCheck:
                return NotNameConstraintQueryConstraintCheck(item)

        constraint_converter = (
            InMemoryTypeRegistryConstraintConverter()
            .with_default_constraint_converters()
            .register(NotNameConstraint, NotNameConstraintConverter())
        )

        delegate = InMemoryStoredEventSource(
            events=[event_1, event_2, event_3],
            identifier=identifier,
            constraint_converter=constraint_converter,
        )
        constrained = ConstrainedEventSource(
            delegate=delegate, constraints={SequenceNumberAfterConstraint(1)}
        )

        events = [
            event
            async for event in constrained.iterate(
                constraints={NotNameConstraint("second")}
            )
        ]

        assert events == [event_3]

    async def test_constrains_aiter_of_underlying_source(self):
        identifier = StreamIdentifier(
            category=data.random_event_category_name(),
            stream=data.random_event_stream_name(),
        )

        event_1 = StoredEventBuilder().with_sequence_number(1).build()
        event_2 = StoredEventBuilder().with_sequence_number(2).build()
        event_3 = StoredEventBuilder().with_sequence_number(3).build()

        delegate = InMemoryStoredEventSource(
            events=[event_1, event_2, event_3], identifier=identifier
        )
        constrained = ConstrainedEventSource(
            delegate=delegate, constraints={SequenceNumberAfterConstraint(1)}
        )

        events = [event async for event in constrained]

        assert events == [event_2, event_3]

    async def test_constrains_read_of_underlying_source(self):
        identifier = StreamIdentifier(
            category=data.random_event_category_name(),
            stream=data.random_event_stream_name(),
        )

        event_1 = StoredEventBuilder().with_sequence_number(1).build()
        event_2 = StoredEventBuilder().with_sequence_number(2).build()
        event_3 = StoredEventBuilder().with_sequence_number(3).build()

        delegate = InMemoryStoredEventSource(
            events=[event_1, event_2, event_3], identifier=identifier
        )
        constrained = ConstrainedEventSource(
            delegate=delegate,
            constraints={constraints.sequence_number_after(1)},
        )

        events = await constrained.read()

        assert events == [event_2, event_3]
