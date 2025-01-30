import pytest

from logicblocks.event.processing.broker import (
    EventSubscriptionChange,
    EventSubscriptionChangeType,
    EventSubscriptionKey,
    EventSubscriptionState,
    InMemoryEventSubscriptionStore,
)
from logicblocks.event.testing import data
from logicblocks.event.types.identifier import (
    CategoryIdentifier,
)


class TestInMemoryEventSubscriptionStore:
    async def test_adds_single_subscription(self):
        store = InMemoryEventSubscriptionStore()

        subscriber_group = data.random_subscriber_group()
        subscriber_id = data.random_subscriber_id()

        category_name = data.random_event_category_name()

        change = EventSubscriptionChange(
            type=EventSubscriptionChangeType.ADD,
            state=EventSubscriptionState(
                group=subscriber_group,
                id=subscriber_id,
                event_sources=[CategoryIdentifier(category_name)],
            ),
        )

        await store.apply(changes=[change])

        states = await store.list()

        assert states == [
            EventSubscriptionState(
                group=subscriber_group,
                id=subscriber_id,
                event_sources=[CategoryIdentifier(category_name)],
            )
        ]

    async def test_adds_many_subscriptions(self):
        store = InMemoryEventSubscriptionStore()

        subscriber_1_group = data.random_subscriber_group()
        subscriber_1_id = data.random_subscriber_id()

        subscriber_2_group = data.random_subscriber_group()
        subscriber_2_id = data.random_subscriber_id()

        category_name = data.random_event_category_name()

        change_1 = EventSubscriptionChange(
            type=EventSubscriptionChangeType.ADD,
            state=EventSubscriptionState(
                group=subscriber_1_group,
                id=subscriber_1_id,
                event_sources=[CategoryIdentifier(category_name)],
            ),
        )
        change_2 = EventSubscriptionChange(
            type=EventSubscriptionChangeType.ADD,
            state=EventSubscriptionState(
                group=subscriber_2_group,
                id=subscriber_2_id,
                event_sources=[CategoryIdentifier(category_name)],
            ),
        )

        await store.apply(changes=[change_1, change_2])

        states = await store.list()

        assert states == [
            EventSubscriptionState(
                group=subscriber_1_group,
                id=subscriber_1_id,
                event_sources=[CategoryIdentifier(category_name)],
            ),
            EventSubscriptionState(
                group=subscriber_2_group,
                id=subscriber_2_id,
                event_sources=[CategoryIdentifier(category_name)],
            ),
        ]

    async def test_adds_subscriptions_to_existing_subscriptions(self):
        store = InMemoryEventSubscriptionStore()

        subscriber_group_1 = data.random_subscriber_group()
        subscriber_group_2 = data.random_subscriber_group()

        subscriber_1_id = data.random_subscriber_id()
        subscriber_2_id = data.random_subscriber_id()
        subscriber_3_id = data.random_subscriber_id()

        category_1_name = data.random_event_category_name()
        category_2_name = data.random_event_category_name()

        change_1 = EventSubscriptionChange(
            type=EventSubscriptionChangeType.ADD,
            state=EventSubscriptionState(
                group=subscriber_group_1,
                id=subscriber_1_id,
                event_sources=[CategoryIdentifier(category_1_name)],
            ),
        )

        await store.apply(changes=[change_1])

        change_2 = EventSubscriptionChange(
            type=EventSubscriptionChangeType.ADD,
            state=EventSubscriptionState(
                group=subscriber_group_1,
                id=subscriber_2_id,
                event_sources=[CategoryIdentifier(category_1_name)],
            ),
        )
        change_3 = EventSubscriptionChange(
            type=EventSubscriptionChangeType.ADD,
            state=EventSubscriptionState(
                group=subscriber_group_2,
                id=subscriber_3_id,
                event_sources=[CategoryIdentifier(category_2_name)],
            ),
        )

        await store.apply(changes=[change_2, change_3])

        states = await store.list()

        assert states == [
            EventSubscriptionState(
                group=subscriber_group_1,
                id=subscriber_1_id,
                event_sources=[CategoryIdentifier(category_1_name)],
            ),
            EventSubscriptionState(
                group=subscriber_group_1,
                id=subscriber_2_id,
                event_sources=[CategoryIdentifier(category_1_name)],
            ),
            EventSubscriptionState(
                group=subscriber_group_2,
                id=subscriber_3_id,
                event_sources=[CategoryIdentifier(category_2_name)],
            ),
        ]

    async def test_raises_if_adding_existing_subscription(self):
        store = InMemoryEventSubscriptionStore()

        subscriber_group = data.random_subscriber_group()
        subscriber_id = data.random_subscriber_id()

        category_name = data.random_event_category_name()

        change = EventSubscriptionChange(
            type=EventSubscriptionChangeType.ADD,
            state=EventSubscriptionState(
                group=subscriber_group,
                id=subscriber_id,
                event_sources=[CategoryIdentifier(category_name)],
            ),
        )

        await store.apply(changes=[change])

        with pytest.raises(ValueError) as error:
            await store.apply(changes=[change])

        assert error.value.args == tuple(["Can't add existing subscription."])

    async def test_replaces_single_existing_subscription(self):
        store = InMemoryEventSubscriptionStore()

        subscriber_group = data.random_subscriber_group()
        subscriber_id = data.random_subscriber_id()

        category_1_name = data.random_event_category_name()
        category_2_name = data.random_event_category_name()

        add_change = EventSubscriptionChange(
            type=EventSubscriptionChangeType.ADD,
            state=EventSubscriptionState(
                group=subscriber_group,
                id=subscriber_id,
                event_sources=[CategoryIdentifier(category_1_name)],
            ),
        )

        await store.apply(changes=[add_change])

        update_change = EventSubscriptionChange(
            type=EventSubscriptionChangeType.REPLACE,
            state=EventSubscriptionState(
                group=subscriber_group,
                id=subscriber_id,
                event_sources=[
                    CategoryIdentifier(category_1_name),
                    CategoryIdentifier(category_2_name),
                ],
            ),
        )

        await store.apply(changes=[update_change])

        states = await store.list()

        assert states == [
            EventSubscriptionState(
                group=subscriber_group,
                id=subscriber_id,
                event_sources=[
                    CategoryIdentifier(category_1_name),
                    CategoryIdentifier(category_2_name),
                ],
            ),
        ]

    async def test_replaces_many_existing_subscriptions(self):
        store = InMemoryEventSubscriptionStore()

        subscriber_1_group = data.random_subscriber_group()
        subscriber_1_id = data.random_subscriber_id()

        subscriber_2_group = data.random_subscriber_group()
        subscriber_2_id = data.random_subscriber_id()

        category_1_name = data.random_event_category_name()
        category_2_name = data.random_event_category_name()
        category_3_name = data.random_event_category_name()

        add_change_1 = EventSubscriptionChange(
            type=EventSubscriptionChangeType.ADD,
            state=EventSubscriptionState(
                group=subscriber_1_group,
                id=subscriber_1_id,
                event_sources=[CategoryIdentifier(category_1_name)],
            ),
        )
        add_change_2 = EventSubscriptionChange(
            type=EventSubscriptionChangeType.ADD,
            state=EventSubscriptionState(
                group=subscriber_2_group,
                id=subscriber_2_id,
                event_sources=[CategoryIdentifier(category_1_name)],
            ),
        )

        await store.apply(changes=[add_change_1, add_change_2])

        update_change_1 = EventSubscriptionChange(
            type=EventSubscriptionChangeType.REPLACE,
            state=EventSubscriptionState(
                group=subscriber_1_group,
                id=subscriber_1_id,
                event_sources=[
                    CategoryIdentifier(category_1_name),
                    CategoryIdentifier(category_2_name),
                ],
            ),
        )
        update_change_2 = EventSubscriptionChange(
            type=EventSubscriptionChangeType.REPLACE,
            state=EventSubscriptionState(
                group=subscriber_2_group,
                id=subscriber_2_id,
                event_sources=[
                    CategoryIdentifier(category_1_name),
                    CategoryIdentifier(category_3_name),
                ],
            ),
        )

        await store.apply(changes=[update_change_1, update_change_2])

        states = await store.list()

        subscriber_1_subscription = next(
            state
            for state in states
            if state.key
            == EventSubscriptionKey(
                group=subscriber_1_group, id=subscriber_1_id
            )
        )
        subscriber_2_subscription = next(
            state
            for state in states
            if state.key
            == EventSubscriptionKey(
                group=subscriber_2_group, id=subscriber_2_id
            )
        )

        assert len(states) == 2
        assert subscriber_1_subscription == EventSubscriptionState(
            group=subscriber_1_group,
            id=subscriber_1_id,
            event_sources=[
                CategoryIdentifier(category_1_name),
                CategoryIdentifier(category_2_name),
            ],
        )
        assert subscriber_2_subscription == EventSubscriptionState(
            group=subscriber_2_group,
            id=subscriber_2_id,
            event_sources=[
                CategoryIdentifier(category_1_name),
                CategoryIdentifier(category_3_name),
            ],
        )

    async def test_raises_if_replacing_missing_subscription(self):
        store = InMemoryEventSubscriptionStore()

        subscriber_group = data.random_subscriber_group()
        subscriber_id = data.random_subscriber_id()

        category_name = data.random_event_category_name()

        change = EventSubscriptionChange(
            type=EventSubscriptionChangeType.REPLACE,
            state=EventSubscriptionState(
                group=subscriber_group,
                id=subscriber_id,
                event_sources=[CategoryIdentifier(category_name)],
            ),
        )

        with pytest.raises(ValueError) as error:
            await store.apply(changes=[change])

        assert error.value.args == tuple(
            ["Can't replace missing subscription."]
        )

    async def test_removes_single_subscription(self):
        store = InMemoryEventSubscriptionStore()

        subscriber_1_group = data.random_subscriber_group()
        subscriber_1_id = data.random_subscriber_id()

        subscriber_2_group = data.random_subscriber_group()
        subscriber_2_id = data.random_subscriber_id()

        category_name = data.random_event_category_name()

        subscription_1 = EventSubscriptionState(
            group=subscriber_1_group,
            id=subscriber_1_id,
            event_sources=[CategoryIdentifier(category_name)],
        )
        subscription_2 = EventSubscriptionState(
            group=subscriber_2_group,
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

        subscriber_1_group = data.random_subscriber_group()
        subscriber_1_id = data.random_subscriber_id()

        subscriber_2_group = data.random_subscriber_group()
        subscriber_2_id = data.random_subscriber_id()

        category_name = data.random_event_category_name()

        subscription_1 = EventSubscriptionState(
            group=subscriber_1_group,
            id=subscriber_1_id,
            event_sources=[CategoryIdentifier(category_name)],
        )
        subscription_2 = EventSubscriptionState(
            group=subscriber_2_group,
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

    async def test_raises_if_removing_missing_subscription(self):
        store = InMemoryEventSubscriptionStore()

        subscriber_group = data.random_subscriber_group()
        subscriber_id = data.random_subscriber_id()

        category_name = data.random_event_category_name()

        change = EventSubscriptionChange(
            type=EventSubscriptionChangeType.REMOVE,
            state=EventSubscriptionState(
                group=subscriber_group,
                id=subscriber_id,
                event_sources=[CategoryIdentifier(category_name)],
            ),
        )

        with pytest.raises(ValueError) as error:
            await store.apply(changes=[change])

        assert error.value.args == tuple(
            ["Can't remove missing subscription."]
        )

    async def test_raises_if_multiple_changes_for_same_subscription_key(self):
        store = InMemoryEventSubscriptionStore()

        subscriber_group = data.random_subscriber_group()
        subscriber_id = data.random_subscriber_id()

        category_name_1 = data.random_event_category_name()
        category_name_2 = data.random_event_category_name()

        add_change = EventSubscriptionChange(
            type=EventSubscriptionChangeType.ADD,
            state=EventSubscriptionState(
                group=subscriber_group,
                id=subscriber_id,
                event_sources=[CategoryIdentifier(category_name_1)],
            ),
        )

        await store.apply(changes=[add_change])

        replace_change = EventSubscriptionChange(
            type=EventSubscriptionChangeType.REPLACE,
            state=EventSubscriptionState(
                group=subscriber_group,
                id=subscriber_id,
                event_sources=[
                    CategoryIdentifier(category_name_1),
                    CategoryIdentifier(category_name_2),
                ],
            ),
        )
        remove_change = EventSubscriptionChange(
            type=EventSubscriptionChangeType.REMOVE,
            state=EventSubscriptionState(
                group=subscriber_group,
                id=subscriber_id,
                event_sources=[CategoryIdentifier(category_name_1)],
            ),
        )

        with pytest.raises(ValueError) as error:
            await store.apply(changes=[replace_change, remove_change])

        assert error.value.args == tuple(
            ["Multiple changes present for same subscription key."]
        )
