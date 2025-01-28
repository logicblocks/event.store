import pytest

from logicblocks.event.processing.broker import (
    EventSubscriptionChange,
    EventSubscriptionChangeType,
    EventSubscriptionState,
    InMemoryEventSubscriptionStore,
)
from logicblocks.event.testing import data
from logicblocks.event.types.identifier import (
    CategoryIdentifier,
    StreamIdentifier,
)


class TestInMemoryEventSubscriptionStore:
    async def test_adds_single_subscription(self):
        store = InMemoryEventSubscriptionStore()

        subscriber_name = data.random_subscriber_name()
        subscriber_id = data.random_subscriber_id()

        category_name = data.random_event_category_name()

        change = EventSubscriptionChange(
            type=EventSubscriptionChangeType.ADD,
            state=EventSubscriptionState(
                name=subscriber_name,
                id=subscriber_id,
                event_sources=[CategoryIdentifier(category_name)],
            ),
        )

        await store.apply(changes=[change])

        states = await store.list()

        assert states == [
            EventSubscriptionState(
                name=subscriber_name,
                id=subscriber_id,
                event_sources=[CategoryIdentifier(category_name)],
            )
        ]

    async def test_adds_many_subscriptions(self):
        store = InMemoryEventSubscriptionStore()

        subscriber_1_name = data.random_subscriber_name()
        subscriber_1_id = data.random_subscriber_id()

        subscriber_2_name = data.random_subscriber_name()
        subscriber_2_id = data.random_subscriber_id()

        category_name = data.random_event_category_name()

        change_1 = EventSubscriptionChange(
            type=EventSubscriptionChangeType.ADD,
            state=EventSubscriptionState(
                name=subscriber_1_name,
                id=subscriber_1_id,
                event_sources=[CategoryIdentifier(category_name)],
            ),
        )
        change_2 = EventSubscriptionChange(
            type=EventSubscriptionChangeType.ADD,
            state=EventSubscriptionState(
                name=subscriber_2_name,
                id=subscriber_2_id,
                event_sources=[CategoryIdentifier(category_name)],
            ),
        )

        await store.apply(changes=[change_1, change_2])

        states = await store.list()

        assert states == [
            EventSubscriptionState(
                name=subscriber_1_name,
                id=subscriber_1_id,
                event_sources=[CategoryIdentifier(category_name)],
            ),
            EventSubscriptionState(
                name=subscriber_2_name,
                id=subscriber_2_id,
                event_sources=[CategoryIdentifier(category_name)],
            ),
        ]

    async def test_adds_subscriptions_to_existing_subscriptions(self):
        store = InMemoryEventSubscriptionStore()

        subscriber_name_1 = data.random_subscriber_name()
        subscriber_name_2 = data.random_subscriber_name()

        subscriber_1_id = data.random_subscriber_id()
        subscriber_2_id = data.random_subscriber_id()
        subscriber_3_id = data.random_subscriber_id()

        category_1_name = data.random_event_category_name()
        category_2_name = data.random_event_category_name()

        change_1 = EventSubscriptionChange(
            type=EventSubscriptionChangeType.ADD,
            state=EventSubscriptionState(
                name=subscriber_name_1,
                id=subscriber_1_id,
                event_sources=[CategoryIdentifier(category_1_name)],
            ),
        )

        await store.apply(changes=[change_1])

        change_2 = EventSubscriptionChange(
            type=EventSubscriptionChangeType.ADD,
            state=EventSubscriptionState(
                name=subscriber_name_1,
                id=subscriber_2_id,
                event_sources=[CategoryIdentifier(category_1_name)],
            ),
        )
        change_3 = EventSubscriptionChange(
            type=EventSubscriptionChangeType.ADD,
            state=EventSubscriptionState(
                name=subscriber_name_2,
                id=subscriber_3_id,
                event_sources=[CategoryIdentifier(category_2_name)],
            ),
        )

        await store.apply(changes=[change_2, change_3])

        states = await store.list()

        assert states == [
            EventSubscriptionState(
                name=subscriber_name_1,
                id=subscriber_1_id,
                event_sources=[CategoryIdentifier(category_1_name)],
            ),
            EventSubscriptionState(
                name=subscriber_name_1,
                id=subscriber_2_id,
                event_sources=[CategoryIdentifier(category_1_name)],
            ),
            EventSubscriptionState(
                name=subscriber_name_2,
                id=subscriber_3_id,
                event_sources=[CategoryIdentifier(category_2_name)],
            ),
        ]

    async def test_raises_if_adding_existing_subscription(self):
        store = InMemoryEventSubscriptionStore()

        subscriber_name = data.random_subscriber_name()
        subscriber_id = data.random_subscriber_id()

        category_name = data.random_event_category_name()

        change = EventSubscriptionChange(
            type=EventSubscriptionChangeType.ADD,
            state=EventSubscriptionState(
                name=subscriber_name,
                id=subscriber_id,
                event_sources=[CategoryIdentifier(category_name)],
            ),
        )

        await store.apply(changes=[change])

        with pytest.raises(ValueError):
            await store.apply(changes=[change])

    async def test_removes_single_subscription(self):
        store = InMemoryEventSubscriptionStore()

        subscriber_1_name = data.random_subscriber_name()
        subscriber_1_id = data.random_subscriber_id()

        subscriber_2_name = data.random_subscriber_name()
        subscriber_2_id = data.random_subscriber_id()

        category_name = data.random_event_category_name()

        subscription_1 = EventSubscriptionState(
            name=subscriber_1_name,
            id=subscriber_1_id,
            event_sources=[CategoryIdentifier(category_name)],
        )
        subscription_2 = EventSubscriptionState(
            name=subscriber_2_name,
            id=subscriber_2_id,
            event_sources=[CategoryIdentifier(category_name)],
        )

        add_change_1 = EventSubscriptionChange(
            type=EventSubscriptionChangeType.ADD,
            state=subscription_1,
        )
        add_change_2 = EventSubscriptionChange(
            type=EventSubscriptionChangeType.ADD,
            state=subscription_2,
        )

        await store.apply(changes=[add_change_1, add_change_2])

        remove_change = EventSubscriptionChange(
            type=EventSubscriptionChangeType.REMOVE,
            state=subscription_1,
        )

        await store.apply(changes=[remove_change])

        states = await store.list()

        assert states == [
            subscription_2,
        ]

    async def test_removes_many_subscriptions(self):
        store = InMemoryEventSubscriptionStore()

        subscriber_1_name = data.random_subscriber_name()
        subscriber_1_id = data.random_subscriber_id()

        subscriber_2_name = data.random_subscriber_name()
        subscriber_2_id = data.random_subscriber_id()

        category_name = data.random_event_category_name()

        subscription_1 = EventSubscriptionState(
            name=subscriber_1_name,
            id=subscriber_1_id,
            event_sources=[CategoryIdentifier(category_name)],
        )
        subscription_2 = EventSubscriptionState(
            name=subscriber_2_name,
            id=subscriber_2_id,
            event_sources=[CategoryIdentifier(category_name)],
        )

        add_change_1 = EventSubscriptionChange(
            type=EventSubscriptionChangeType.ADD,
            state=subscription_1,
        )
        add_change_2 = EventSubscriptionChange(
            type=EventSubscriptionChangeType.ADD,
            state=subscription_2,
        )

        await store.apply(changes=[add_change_1, add_change_2])

        remove_change_1 = EventSubscriptionChange(
            type=EventSubscriptionChangeType.REMOVE,
            state=subscription_1,
        )
        remove_change_2 = EventSubscriptionChange(
            type=EventSubscriptionChangeType.REMOVE,
            state=subscription_2,
        )

        await store.apply(changes=[remove_change_1, remove_change_2])

        states = await store.list()

        assert states == []

    async def test_adds_and_removes_subscriptions_in_same_apply(self):
        store = InMemoryEventSubscriptionStore()

        subscriber_name_1 = data.random_subscriber_name()
        subscriber_name_2 = data.random_subscriber_name()

        subscriber_1_id = data.random_subscriber_id()
        subscriber_2_id = data.random_subscriber_id()
        subscriber_3_id = data.random_subscriber_id()

        category_name_1 = data.random_event_category_name()
        category_name_2 = data.random_event_category_name()

        subscription_1 = EventSubscriptionState(
            name=subscriber_name_1,
            id=subscriber_1_id,
            event_sources=[CategoryIdentifier(category_name_1)],
        )
        subscription_2 = EventSubscriptionState(
            name=subscriber_name_2,
            id=subscriber_2_id,
            event_sources=[CategoryIdentifier(category_name_2)],
        )
        subscription_3 = EventSubscriptionState(
            name=subscriber_name_2,
            id=subscriber_3_id,
            event_sources=[CategoryIdentifier(category_name_2)],
        )

        add_change_1 = EventSubscriptionChange(
            type=EventSubscriptionChangeType.ADD,
            state=subscription_1,
        )
        add_change_2 = EventSubscriptionChange(
            type=EventSubscriptionChangeType.ADD,
            state=subscription_2,
        )

        await store.apply(changes=[add_change_1, add_change_2])

        add_change_3 = EventSubscriptionChange(
            type=EventSubscriptionChangeType.ADD,
            state=subscription_3,
        )
        remove_change_1 = EventSubscriptionChange(
            type=EventSubscriptionChangeType.REMOVE,
            state=subscription_1,
        )

        await store.apply(changes=[remove_change_1, add_change_3])

        states = await store.list()

        assert states == [
            subscription_2,
            subscription_3,
        ]

    async def test_removes_before_adding(self):
        store = InMemoryEventSubscriptionStore()

        subscriber_name = data.random_subscriber_name()
        subscriber_id = data.random_subscriber_id()

        category_name = data.random_event_category_name()
        stream_1_name = data.random_event_stream_name()
        stream_2_name = data.random_event_stream_name()
        stream_3_name = data.random_event_stream_name()

        add_change_1 = EventSubscriptionChange(
            type=EventSubscriptionChangeType.ADD,
            state=EventSubscriptionState(
                name=subscriber_name,
                id=subscriber_id,
                event_sources=[
                    StreamIdentifier(
                        category=category_name,
                        stream=stream_1_name,
                    ),
                    StreamIdentifier(
                        category=category_name,
                        stream=stream_2_name,
                    ),
                ],
            ),
        )

        await store.apply(changes=[add_change_1])

        remove_change_1 = EventSubscriptionChange(
            type=EventSubscriptionChangeType.REMOVE,
            state=EventSubscriptionState(
                name=subscriber_name,
                id=subscriber_id,
                event_sources=[
                    StreamIdentifier(
                        category=category_name,
                        stream=stream_1_name,
                    ),
                    StreamIdentifier(
                        category=category_name,
                        stream=stream_2_name,
                    ),
                ],
            ),
        )
        add_change_1 = EventSubscriptionChange(
            type=EventSubscriptionChangeType.ADD,
            state=EventSubscriptionState(
                name=subscriber_name,
                id=subscriber_id,
                event_sources=[
                    StreamIdentifier(
                        category=category_name,
                        stream=stream_1_name,
                    ),
                    StreamIdentifier(
                        category=category_name,
                        stream=stream_3_name,
                    ),
                ],
            ),
        )

        await store.apply(changes=[add_change_1, remove_change_1])

        states = await store.list()

        assert states == [
            EventSubscriptionState(
                name=subscriber_name,
                id=subscriber_id,
                event_sources=[
                    StreamIdentifier(
                        category=category_name,
                        stream=stream_1_name,
                    ),
                    StreamIdentifier(
                        category=category_name,
                        stream=stream_3_name,
                    ),
                ],
            )
        ]
