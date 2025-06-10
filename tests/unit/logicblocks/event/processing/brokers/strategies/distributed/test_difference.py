from collections.abc import Sequence

from logicblocks.event.processing.broker.strategies.distributed import (
    EventSubscriptionChange,
    EventSubscriptionChangeset,
    EventSubscriptionDifference,
    EventSubscriptionState,
)
from logicblocks.event.testing import data
from logicblocks.event.types import (
    CategoryIdentifier,
    StreamIdentifier,
)


class TestEventSubscriptionDifference:
    async def test_allocates_single_new_subscription_single_source(self):
        node_id = data.random_node_id()

        subscriber_group = data.random_subscriber_group()
        subscriber_id = data.random_subscriber_id()

        category_name = data.random_event_category_name()

        event_sequence_identifier = CategoryIdentifier(category=category_name)

        existing: Sequence[EventSubscriptionState] = []
        updated: Sequence[EventSubscriptionState] = [
            (
                EventSubscriptionState(
                    group=subscriber_group,
                    id=subscriber_id,
                    node_id=node_id,
                    event_sources=[event_sequence_identifier],
                )
            )
        ]

        difference = EventSubscriptionDifference()

        changeset = difference.diff(existing, updated)

        assert changeset == EventSubscriptionChangeset(
            allocations={
                EventSubscriptionChange(
                    group=subscriber_group,
                    id=subscriber_id,
                    event_source=event_sequence_identifier,
                )
            },
            revocations=set(),
        )

    async def test_allocates_single_new_subscription_many_sources(self):
        node_id = data.random_node_id()

        subscriber_group = data.random_subscriber_group()
        subscriber_id = data.random_subscriber_id()

        category_name = data.random_event_category_name()

        event_sequence_identifier_1 = StreamIdentifier(
            category=category_name, stream=data.random_event_stream_name()
        )
        event_sequence_identifier_2 = StreamIdentifier(
            category=category_name, stream=data.random_event_stream_name()
        )

        existing: Sequence[EventSubscriptionState] = []
        updated: Sequence[EventSubscriptionState] = [
            (
                EventSubscriptionState(
                    group=subscriber_group,
                    id=subscriber_id,
                    node_id=node_id,
                    event_sources=[
                        event_sequence_identifier_1,
                        event_sequence_identifier_2,
                    ],
                )
            )
        ]

        difference = EventSubscriptionDifference()

        changeset = difference.diff(existing, updated)

        assert changeset == EventSubscriptionChangeset(
            allocations={
                EventSubscriptionChange(
                    group=subscriber_group,
                    id=subscriber_id,
                    event_source=event_sequence_identifier_1,
                ),
                EventSubscriptionChange(
                    group=subscriber_group,
                    id=subscriber_id,
                    event_source=event_sequence_identifier_2,
                ),
            },
            revocations=set(),
        )

    async def test_allocates_many_new_subscriptions_many_sources(self):
        node_id = data.random_node_id()

        subscriber_group_1 = data.random_subscriber_group()
        subscriber_id_1 = data.random_subscriber_id()

        subscriber_group_2 = data.random_subscriber_group()
        subscriber_id_2 = data.random_subscriber_id()

        category_name_1 = data.random_event_category_name()
        category_name_2 = data.random_event_category_name()

        event_sequence_identifier_1 = StreamIdentifier(
            category=category_name_1, stream=data.random_event_stream_name()
        )
        event_sequence_identifier_2 = StreamIdentifier(
            category=category_name_1, stream=data.random_event_stream_name()
        )
        event_sequence_identifier_3 = StreamIdentifier(
            category=category_name_2, stream=data.random_event_stream_name()
        )
        event_sequence_identifier_4 = StreamIdentifier(
            category=category_name_2, stream=data.random_event_stream_name()
        )

        existing: Sequence[EventSubscriptionState] = []
        updated: Sequence[EventSubscriptionState] = [
            EventSubscriptionState(
                group=subscriber_group_1,
                id=subscriber_id_1,
                node_id=node_id,
                event_sources=[
                    event_sequence_identifier_1,
                    event_sequence_identifier_2,
                ],
            ),
            EventSubscriptionState(
                group=subscriber_group_2,
                id=subscriber_id_2,
                node_id=node_id,
                event_sources=[
                    event_sequence_identifier_3,
                    event_sequence_identifier_4,
                ],
            ),
        ]

        difference = EventSubscriptionDifference()

        changeset = difference.diff(existing, updated)

        assert changeset == EventSubscriptionChangeset(
            allocations={
                EventSubscriptionChange(
                    group=subscriber_group_1,
                    id=subscriber_id_1,
                    event_source=event_sequence_identifier_1,
                ),
                EventSubscriptionChange(
                    group=subscriber_group_1,
                    id=subscriber_id_1,
                    event_source=event_sequence_identifier_2,
                ),
                EventSubscriptionChange(
                    group=subscriber_group_2,
                    id=subscriber_id_2,
                    event_source=event_sequence_identifier_3,
                ),
                EventSubscriptionChange(
                    group=subscriber_group_2,
                    id=subscriber_id_2,
                    event_source=event_sequence_identifier_4,
                ),
            },
            revocations=set(),
        )

    async def test_revokes_single_old_subscription_single_source(self):
        node_id = data.random_node_id()

        subscriber_group = data.random_subscriber_group()
        subscriber_id = data.random_subscriber_id()

        category_name = data.random_event_category_name()

        event_sequence_identifier = CategoryIdentifier(category=category_name)

        existing: Sequence[EventSubscriptionState] = [
            EventSubscriptionState(
                group=subscriber_group,
                id=subscriber_id,
                node_id=node_id,
                event_sources=[event_sequence_identifier],
            )
        ]
        updated: Sequence[EventSubscriptionState] = []

        difference = EventSubscriptionDifference()

        changeset = difference.diff(existing, updated)

        assert changeset == EventSubscriptionChangeset(
            allocations=set(),
            revocations={
                EventSubscriptionChange(
                    group=subscriber_group,
                    id=subscriber_id,
                    event_source=event_sequence_identifier,
                )
            },
        )

    async def test_revokes_single_old_subscription_many_sources(self):
        node_id = data.random_node_id()

        subscriber_group = data.random_subscriber_group()
        subscriber_id = data.random_subscriber_id()

        category_name = data.random_event_category_name()

        event_sequence_identifier_1 = StreamIdentifier(
            category=category_name, stream=data.random_event_stream_name()
        )
        event_sequence_identifier_2 = StreamIdentifier(
            category=category_name, stream=data.random_event_stream_name()
        )

        existing: Sequence[EventSubscriptionState] = [
            (
                EventSubscriptionState(
                    group=subscriber_group,
                    id=subscriber_id,
                    node_id=node_id,
                    event_sources=[
                        event_sequence_identifier_1,
                        event_sequence_identifier_2,
                    ],
                )
            )
        ]
        updated: Sequence[EventSubscriptionState] = []

        difference = EventSubscriptionDifference()

        changeset = difference.diff(existing, updated)

        assert changeset == EventSubscriptionChangeset(
            allocations=set(),
            revocations={
                EventSubscriptionChange(
                    group=subscriber_group,
                    id=subscriber_id,
                    event_source=event_sequence_identifier_1,
                ),
                EventSubscriptionChange(
                    group=subscriber_group,
                    id=subscriber_id,
                    event_source=event_sequence_identifier_2,
                ),
            },
        )

    async def test_revokes_many_old_subscriptions_many_sources(self):
        node_id = data.random_node_id()

        subscriber_group_1 = data.random_subscriber_group()
        subscriber_id_1 = data.random_subscriber_id()

        subscriber_group_2 = data.random_subscriber_group()
        subscriber_id_2 = data.random_subscriber_id()

        category_name_1 = data.random_event_category_name()
        category_name_2 = data.random_event_category_name()

        event_sequence_identifier_1 = StreamIdentifier(
            category=category_name_1, stream=data.random_event_stream_name()
        )
        event_sequence_identifier_2 = StreamIdentifier(
            category=category_name_1, stream=data.random_event_stream_name()
        )
        event_sequence_identifier_3 = StreamIdentifier(
            category=category_name_2, stream=data.random_event_stream_name()
        )
        event_sequence_identifier_4 = StreamIdentifier(
            category=category_name_2, stream=data.random_event_stream_name()
        )

        existing: Sequence[EventSubscriptionState] = [
            EventSubscriptionState(
                group=subscriber_group_1,
                id=subscriber_id_1,
                node_id=node_id,
                event_sources=[
                    event_sequence_identifier_1,
                    event_sequence_identifier_2,
                ],
            ),
            EventSubscriptionState(
                group=subscriber_group_2,
                id=subscriber_id_2,
                node_id=node_id,
                event_sources=[
                    event_sequence_identifier_3,
                    event_sequence_identifier_4,
                ],
            ),
        ]
        updated: Sequence[EventSubscriptionState] = []

        difference = EventSubscriptionDifference()

        changeset = difference.diff(existing, updated)

        assert changeset == EventSubscriptionChangeset(
            allocations=set(),
            revocations={
                EventSubscriptionChange(
                    group=subscriber_group_1,
                    id=subscriber_id_1,
                    event_source=event_sequence_identifier_1,
                ),
                EventSubscriptionChange(
                    group=subscriber_group_1,
                    id=subscriber_id_1,
                    event_source=event_sequence_identifier_2,
                ),
                EventSubscriptionChange(
                    group=subscriber_group_2,
                    id=subscriber_id_2,
                    event_source=event_sequence_identifier_3,
                ),
                EventSubscriptionChange(
                    group=subscriber_group_2,
                    id=subscriber_id_2,
                    event_source=event_sequence_identifier_4,
                ),
            },
        )

    async def test_allocates_single_new_source_for_existing_subscription(self):
        node_id = data.random_node_id()

        subscriber_group = data.random_subscriber_group()
        subscriber_id = data.random_subscriber_id()

        category_name = data.random_event_category_name()

        event_sequence_identifier_1 = StreamIdentifier(
            category=category_name, stream=data.random_event_stream_name()
        )
        event_sequence_identifier_2 = StreamIdentifier(
            category=category_name, stream=data.random_event_stream_name()
        )

        existing: Sequence[EventSubscriptionState] = [
            EventSubscriptionState(
                group=subscriber_group,
                id=subscriber_id,
                node_id=node_id,
                event_sources=[
                    event_sequence_identifier_1,
                ],
            )
        ]
        updated: Sequence[EventSubscriptionState] = [
            (
                EventSubscriptionState(
                    group=subscriber_group,
                    id=subscriber_id,
                    node_id=node_id,
                    event_sources=[
                        event_sequence_identifier_1,
                        event_sequence_identifier_2,
                    ],
                )
            )
        ]

        difference = EventSubscriptionDifference()

        changeset = difference.diff(existing, updated)

        assert changeset == EventSubscriptionChangeset(
            allocations={
                EventSubscriptionChange(
                    group=subscriber_group,
                    id=subscriber_id,
                    event_source=event_sequence_identifier_2,
                )
            },
            revocations=set(),
        )

    async def test_allocates_many_new_sources_for_existing_subscription(self):
        node_id = data.random_node_id()

        subscriber_group = data.random_subscriber_group()
        subscriber_id = data.random_subscriber_id()

        category_name = data.random_event_category_name()

        event_sequence_identifier_1 = StreamIdentifier(
            category=category_name, stream=data.random_event_stream_name()
        )
        event_sequence_identifier_2 = StreamIdentifier(
            category=category_name, stream=data.random_event_stream_name()
        )
        event_sequence_identifier_3 = StreamIdentifier(
            category=category_name, stream=data.random_event_stream_name()
        )

        existing: Sequence[EventSubscriptionState] = [
            EventSubscriptionState(
                group=subscriber_group,
                id=subscriber_id,
                node_id=node_id,
                event_sources=[
                    event_sequence_identifier_1,
                ],
            )
        ]
        updated: Sequence[EventSubscriptionState] = [
            (
                EventSubscriptionState(
                    group=subscriber_group,
                    id=subscriber_id,
                    node_id=node_id,
                    event_sources=[
                        event_sequence_identifier_1,
                        event_sequence_identifier_2,
                        event_sequence_identifier_3,
                    ],
                )
            )
        ]

        difference = EventSubscriptionDifference()

        changeset = difference.diff(existing, updated)

        assert changeset == EventSubscriptionChangeset(
            allocations={
                EventSubscriptionChange(
                    group=subscriber_group,
                    id=subscriber_id,
                    event_source=event_sequence_identifier_2,
                ),
                EventSubscriptionChange(
                    group=subscriber_group,
                    id=subscriber_id,
                    event_source=event_sequence_identifier_3,
                ),
            },
            revocations=set(),
        )

    async def test_revokes_single_old_source_for_existing_subscription(self):
        node_id = data.random_node_id()

        subscriber_group = data.random_subscriber_group()
        subscriber_id = data.random_subscriber_id()

        category_name = data.random_event_category_name()

        event_sequence_identifier_1 = StreamIdentifier(
            category=category_name, stream=data.random_event_stream_name()
        )
        event_sequence_identifier_2 = StreamIdentifier(
            category=category_name, stream=data.random_event_stream_name()
        )

        existing: Sequence[EventSubscriptionState] = [
            (
                EventSubscriptionState(
                    group=subscriber_group,
                    id=subscriber_id,
                    node_id=node_id,
                    event_sources=[
                        event_sequence_identifier_1,
                        event_sequence_identifier_2,
                    ],
                )
            )
        ]
        updated: Sequence[EventSubscriptionState] = [
            EventSubscriptionState(
                group=subscriber_group,
                id=subscriber_id,
                node_id=node_id,
                event_sources=[
                    event_sequence_identifier_1,
                ],
            )
        ]

        difference = EventSubscriptionDifference()

        changeset = difference.diff(existing, updated)

        assert changeset == EventSubscriptionChangeset(
            allocations=set(),
            revocations={
                EventSubscriptionChange(
                    group=subscriber_group,
                    id=subscriber_id,
                    event_source=event_sequence_identifier_2,
                )
            },
        )

    async def test_revokes_many_old_sources_for_existing_subscription(self):
        node_id = data.random_node_id()

        subscriber_group = data.random_subscriber_group()
        subscriber_id = data.random_subscriber_id()

        category_name = data.random_event_category_name()

        event_sequence_identifier_1 = StreamIdentifier(
            category=category_name, stream=data.random_event_stream_name()
        )
        event_sequence_identifier_2 = StreamIdentifier(
            category=category_name, stream=data.random_event_stream_name()
        )
        event_sequence_identifier_3 = StreamIdentifier(
            category=category_name, stream=data.random_event_stream_name()
        )

        existing: Sequence[EventSubscriptionState] = [
            (
                EventSubscriptionState(
                    group=subscriber_group,
                    id=subscriber_id,
                    node_id=node_id,
                    event_sources=[
                        event_sequence_identifier_1,
                        event_sequence_identifier_2,
                        event_sequence_identifier_3,
                    ],
                )
            )
        ]
        updated: Sequence[EventSubscriptionState] = [
            EventSubscriptionState(
                group=subscriber_group,
                id=subscriber_id,
                node_id=node_id,
                event_sources=[
                    event_sequence_identifier_1,
                ],
            )
        ]

        difference = EventSubscriptionDifference()

        changeset = difference.diff(existing, updated)

        assert changeset == EventSubscriptionChangeset(
            allocations=set(),
            revocations={
                EventSubscriptionChange(
                    group=subscriber_group,
                    id=subscriber_id,
                    event_source=event_sequence_identifier_2,
                ),
                EventSubscriptionChange(
                    group=subscriber_group,
                    id=subscriber_id,
                    event_source=event_sequence_identifier_3,
                ),
            },
        )
