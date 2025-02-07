from dataclasses import dataclass
from typing import Sequence

import pytest

from logicblocks.event.processing.broker import (
    EventSubscriberHealth,
    InMemoryEventSubscriberStore,
)
from logicblocks.event.processing.broker.types import (
    EventSubscriber,
    EventSubscriberKey,
)
from logicblocks.event.store import EventSource
from logicblocks.event.testing import data
from logicblocks.event.types import EventSequenceIdentifier


@dataclass(frozen=True)
class DummyEventSubscriber(EventSubscriber):
    _group: str
    _id: str

    @property
    def group(self) -> str:
        return self._group

    @property
    def id(self) -> str:
        return self._id

    @property
    def sequences(self) -> Sequence[EventSequenceIdentifier]:
        return []

    def health(self) -> EventSubscriberHealth:
        return EventSubscriberHealth.HEALTHY

    async def accept(self, source: EventSource) -> None:
        pass

    async def withdraw(self, source: EventSource) -> None:
        pass


class TestInMemoryEventSubscriberStore:
    async def test_manages_single_event_subscriber_instance(self):
        stored = DummyEventSubscriber(
            data.random_subscriber_group(),
            data.random_subscriber_id(),
        )

        store = InMemoryEventSubscriberStore()
        await store.add(stored)

        found = await store.get(stored.key)

        assert found == stored

    async def test_manages_many_event_subscriber_instances(self):
        stored_1 = DummyEventSubscriber(
            data.random_subscriber_group(),
            data.random_subscriber_id(),
        )
        stored_2 = DummyEventSubscriber(
            data.random_subscriber_group(),
            data.random_subscriber_id(),
        )
        stored_3 = DummyEventSubscriber(
            data.random_subscriber_group(),
            data.random_subscriber_id(),
        )

        store = InMemoryEventSubscriberStore()

        await store.add(stored_1)
        await store.add(stored_2)
        await store.add(stored_3)

        found_1 = await store.get(stored_1.key)
        found_2 = await store.get(stored_2.key)
        found_3 = await store.get(stored_3.key)

        assert (found_1, found_2, found_3) == (stored_1, stored_2, stored_3)

    async def test_replaces_if_adding_subscriber_instance_for_existing_key(
        self,
    ):
        subscriber_group = data.random_subscriber_group()
        subscriber_id = data.random_subscriber_id()

        subscriber_1 = DummyEventSubscriber(subscriber_group, subscriber_id)
        subscriber_2 = DummyEventSubscriber(subscriber_group, subscriber_id)

        store = InMemoryEventSubscriberStore()

        await store.add(subscriber_1)
        await store.add(subscriber_2)

        found = await store.get(
            EventSubscriberKey(subscriber_group, subscriber_id)
        )

        assert found == subscriber_2

    async def test_removes_subscriber_instance(self):
        stored_and_removed = DummyEventSubscriber(
            data.random_subscriber_group(),
            data.random_subscriber_id(),
        )

        store = InMemoryEventSubscriberStore()

        await store.add(stored_and_removed)
        await store.remove(stored_and_removed)

        found = await store.get(stored_and_removed.key)

        assert found is None

    async def test_raises_when_removing_missing_subscriber_instance(self):
        subscriber = DummyEventSubscriber(
            data.random_subscriber_group(),
            data.random_subscriber_id(),
        )

        store = InMemoryEventSubscriberStore()

        with pytest.raises(ValueError):
            await store.remove(subscriber)

    async def test_list_returns_all_subscriber_instances(self):
        stored_1 = DummyEventSubscriber(
            data.random_subscriber_group(),
            data.random_subscriber_id(),
        )
        stored_2 = DummyEventSubscriber(
            data.random_subscriber_group(),
            data.random_subscriber_id(),
        )
        stored_3 = DummyEventSubscriber(
            data.random_subscriber_group(),
            data.random_subscriber_id(),
        )

        store = InMemoryEventSubscriberStore()

        await store.add(stored_1)
        await store.add(stored_2)
        await store.add(stored_3)

        found = await store.list()

        assert set(found) == {stored_1, stored_2, stored_3}
