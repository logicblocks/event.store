import pytest

from logicblocks.event.processing.broker import (
    EventSubscriptionSourceMapping,
    InMemoryEventSubscriptionSourceMappingStore,
)
from logicblocks.event.testing import data
from logicblocks.event.types.identifier import (
    EventSequenceIdentifier,
    StreamIdentifier,
)


def random_event_sequence_identifier() -> EventSequenceIdentifier:
    return StreamIdentifier(
        category=data.random_event_category_name(),
        stream=data.random_event_stream_name(),
    )


class TestInMemoryEventSubscriptionSourceMappingStore:
    async def test_adds_event_sources_for_single_subscriber_group(self):
        subscriber_group = data.random_subscriber_group()

        event_sequence_identifier_1 = random_event_sequence_identifier()
        event_sequence_identifier_2 = random_event_sequence_identifier()

        store = InMemoryEventSubscriptionSourceMappingStore()

        await store.add(
            subscriber_group=subscriber_group,
            event_sources=(
                event_sequence_identifier_1,
                event_sequence_identifier_2,
            ),
        )

        mappings = await store.list()

        assert mappings == [
            EventSubscriptionSourceMapping(
                subscriber_group=subscriber_group,
                event_sources=(
                    event_sequence_identifier_1,
                    event_sequence_identifier_2,
                ),
            )
        ]

    async def test_adds_event_sources_for_multiple_subscriber_groups(self):
        subscriber_group_1 = data.random_subscriber_group()
        subscriber_group_2 = data.random_subscriber_group()

        event_sequence_identifier_1 = random_event_sequence_identifier()
        event_sequence_identifier_2 = random_event_sequence_identifier()
        event_sequence_identifier_3 = random_event_sequence_identifier()
        event_sequence_identifier_4 = random_event_sequence_identifier()

        store = InMemoryEventSubscriptionSourceMappingStore()

        await store.add(
            subscriber_group=subscriber_group_1,
            event_sources=(
                event_sequence_identifier_1,
                event_sequence_identifier_2,
            ),
        )
        await store.add(
            subscriber_group=subscriber_group_2,
            event_sources=(
                event_sequence_identifier_3,
                event_sequence_identifier_4,
            ),
        )

        mappings = await store.list()

        assert set(mappings) == {
            EventSubscriptionSourceMapping(
                subscriber_group=subscriber_group_1,
                event_sources=(
                    event_sequence_identifier_1,
                    event_sequence_identifier_2,
                ),
            ),
            EventSubscriptionSourceMapping(
                subscriber_group=subscriber_group_2,
                event_sources=(
                    event_sequence_identifier_3,
                    event_sequence_identifier_4,
                ),
            ),
        }

    async def test_removes_event_sources_for_subscriber_group(self):
        subscriber_group_1 = data.random_subscriber_group()
        subscriber_group_2 = data.random_subscriber_group()

        event_sequence_identifier_1 = random_event_sequence_identifier()
        event_sequence_identifier_2 = random_event_sequence_identifier()
        event_sequence_identifier_3 = random_event_sequence_identifier()
        event_sequence_identifier_4 = random_event_sequence_identifier()

        store = InMemoryEventSubscriptionSourceMappingStore()

        await store.add(
            subscriber_group=subscriber_group_1,
            event_sources=(
                event_sequence_identifier_1,
                event_sequence_identifier_2,
            ),
        )
        await store.add(
            subscriber_group=subscriber_group_2,
            event_sources=(
                event_sequence_identifier_3,
                event_sequence_identifier_4,
            ),
        )

        await store.remove(subscriber_group=subscriber_group_2)

        mappings = await store.list()

        assert set(mappings) == {
            EventSubscriptionSourceMapping(
                subscriber_group=subscriber_group_1,
                event_sources=(
                    event_sequence_identifier_1,
                    event_sequence_identifier_2,
                ),
            )
        }

    async def test_raises_if_removing_event_sources_for_missing_subscriber_group(
        self,
    ):
        subscriber_group = data.random_subscriber_group()

        store = InMemoryEventSubscriptionSourceMappingStore()

        with pytest.raises(ValueError) as error:
            await store.remove(subscriber_group=subscriber_group)

        assert error.value.args == (
            "Can't remove event sources for missing subscriber group.",
        )

    async def test_raises_if_adding_event_sources_for_already_present_subscriber_group(
        self,
    ):
        subscriber_group = data.random_subscriber_group()

        event_sequence_identifier_1 = random_event_sequence_identifier()
        event_sequence_identifier_2 = random_event_sequence_identifier()

        store = InMemoryEventSubscriptionSourceMappingStore()

        await store.add(
            subscriber_group=subscriber_group,
            event_sources=(
                event_sequence_identifier_1,
                event_sequence_identifier_2,
            ),
        )

        with pytest.raises(ValueError) as error:
            await store.add(
                subscriber_group=subscriber_group,
                event_sources=(
                    event_sequence_identifier_1,
                    event_sequence_identifier_2,
                ),
            )

        assert error.value.args == (
            "Can't add event sources for existing subscription.",
        )
