import unittest

from datetime import datetime

import logicblocks.event.store.testing.data as data
from logicblocks.event.store import EventStore
from logicblocks.event.store.adapters import InMemoryStorageAdapter
from logicblocks.event.store.testing import NewEventBuilder
from logicblocks.event.store.testing.builders import StoredEventBuilder
from logicblocks.event.store.types import NewEvent, StoredEvent


class TestEventStoreStreamBasics(unittest.TestCase):
    def test_has_no_events_in_stream_initially(self):
        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()

        store = EventStore(adapter=InMemoryStorageAdapter())
        stream = store.stream(category=category_name, stream=stream_name)

        events = stream.read()

        self.assertEqual(events, [])

    def test_reads_single_published_event(self):
        now = datetime.now()
        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()
        event_name = data.random_event_name()
        payload = data.random_event_payload()

        store = EventStore(adapter=InMemoryStorageAdapter())
        stream = store.stream(category=category_name, stream=stream_name)

        stream.publish(
            events=[
                NewEvent(
                    name=event_name,
                    payload=payload,
                    observed_at=now,
                    occurred_at=now,
                )
            ],
        )

        events = stream.read()

        self.assertEqual(
            events,
            [
                StoredEvent(
                    name=event_name,
                    category=category_name,
                    stream=stream_name,
                    payload=payload,
                    position=0,
                    occurred_at=now,
                    observed_at=now,
                )
            ],
        )

    def test_reads_multiple_published_events(self):
        now = datetime.now()
        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()
        new_events = [
            NewEventBuilder()
            .with_observed_at(now)
            .with_occurred_at(now)
            .build()
            for _ in range(10)
        ]
        stored_events = [
            StoredEvent(
                name=event.name,
                category=category_name,
                stream=stream_name,
                payload=event.payload,
                position=position,
                occurred_at=event.occurred_at,
                observed_at=event.observed_at,
            )
            for event, position in zip(new_events, range(len(new_events)))
        ]

        store = EventStore(adapter=InMemoryStorageAdapter())
        stream = store.stream(category=category_name, stream=stream_name)

        stream.publish(events=new_events)

        events = stream.read()

        self.assertEqual(events, stored_events)

    def test_reads_events_in_multiple_streams_in_same_category(self):
        category_name = data.random_event_category_name()
        stream_1_name = data.random_event_stream_name()
        stream_2_name = data.random_event_stream_name()

        stream_1_new_events = [NewEventBuilder().build() for _ in range(5)]
        stream_2_new_events = [NewEventBuilder().build() for _ in range(5)]

        store = EventStore(adapter=InMemoryStorageAdapter())
        stream_1 = store.stream(category=category_name, stream=stream_1_name)
        stream_2 = store.stream(category=category_name, stream=stream_2_name)

        stream_1.publish(events=stream_1_new_events)
        stream_2.publish(events=stream_2_new_events)

        stream_1_events = stream_1.read()
        stream_2_events = stream_2.read()

        self.assertEqual(
            {
                "stream_1": [
                    (event.name, stream_1_name, category_name)
                    for event in stream_1_new_events
                ],
                "stream_2": [
                    (event.name, stream_2_name, category_name)
                    for event in stream_2_new_events
                ],
            },
            {
                "stream_1": [
                    (event.name, event.stream, event.category)
                    for event in stream_1_events
                ],
                "stream_2": [
                    (event.name, event.stream, event.category)
                    for event in stream_2_events
                ],
            },
        )

    def test_reads_events_in_streams_in_different_categories(self):
        category_1_name = data.random_event_category_name()
        category_2_name = data.random_event_category_name()
        category_1_stream_name = data.random_event_stream_name()
        category_2_stream_name = data.random_event_stream_name()

        category_1_stream_new_events = [
            NewEventBuilder().build() for _ in range(5)
        ]
        category_2_stream_new_events = [
            NewEventBuilder().build() for _ in range(5)
        ]

        store = EventStore(adapter=InMemoryStorageAdapter())
        category_1_stream = store.stream(
            category=category_1_name, stream=category_1_stream_name
        )
        category_2_stream = store.stream(
            category=category_2_name, stream=category_2_stream_name
        )

        category_1_stream.publish(events=category_1_stream_new_events)
        category_2_stream.publish(events=category_2_stream_new_events)

        category_1_stream_events = category_1_stream.read()
        category_2_stream_events = category_2_stream.read()

        self.assertEqual(
            {
                "stream_1": [
                    (event.name, category_1_stream_name, category_1_name)
                    for event in category_1_stream_new_events
                ],
                "stream_2": [
                    (event.name, category_2_stream_name, category_2_name)
                    for event in category_2_stream_new_events
                ],
            },
            {
                "stream_1": [
                    (event.name, event.stream, event.category)
                    for event in category_1_stream_events
                ],
                "stream_2": [
                    (event.name, event.stream, event.category)
                    for event in category_2_stream_events
                ],
            },
        )


class TestEventStoreStreamIteration(unittest.TestCase):
    def test_iterates_over_all_events_in_stream(self):
        now = datetime.now()
        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()
        new_events = [
            NewEventBuilder()
            .with_observed_at(now)
            .with_occurred_at(now)
            .build()
            for _ in range(10)
        ]

        store = EventStore(adapter=InMemoryStorageAdapter())
        stream = store.stream(category=category_name, stream=stream_name)

        stream.publish(events=new_events)

        new_event_keys = [
            (event.name, stream_name, category_name) for event in new_events
        ]
        stream_event_keys = [
            (event.name, event.stream, event.category) for event in stream
        ]

        self.assertEqual(stream_event_keys, new_event_keys)


class TestEventStoreCategoryBasics(unittest.TestCase):
    def test_has_no_events_in_category_initially(self):
        category_name = data.random_event_category_name()

        store = EventStore(adapter=InMemoryStorageAdapter())
        category = store.category(category=category_name)

        events = category.read()

        self.assertEqual(events, [])

    def test_reads_single_published_event_for_single_stream_in_category(self):
        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()
        new_event = NewEventBuilder().build()

        store = EventStore(adapter=InMemoryStorageAdapter())
        category = store.category(category=category_name)
        stream = category.stream(stream=stream_name)

        stream.publish(events=[new_event])

        events = category.read()

        self.assertEqual(
            events,
            [
                StoredEvent(
                    name=new_event.name,
                    category=category_name,
                    stream=stream_name,
                    payload=new_event.payload,
                    position=0,
                    occurred_at=new_event.occurred_at,
                    observed_at=new_event.observed_at,
                )
            ],
        )

    def test_reads_multiple_published_events_for_single_stream_in_category(
        self,
    ):
        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()
        new_events = [NewEventBuilder().build() for _ in range(10)]
        stored_events = [
            StoredEvent(
                name=event.name,
                category=category_name,
                stream=stream_name,
                payload=event.payload,
                position=position,
                occurred_at=event.occurred_at,
                observed_at=event.observed_at,
            )
            for event, position in zip(new_events, range(len(new_events)))
        ]

        store = EventStore(adapter=InMemoryStorageAdapter())
        category = store.category(category=category_name)
        stream = category.stream(stream=stream_name)

        stream.publish(events=new_events)

        events = category.read()

        self.assertEqual(events, stored_events)

    def test_reads_for_different_streams_in_category_in_publish_order(
        self,
    ):
        category_name = data.random_event_category_name()
        stream_1_name = data.random_event_stream_name()
        stream_2_name = data.random_event_stream_name()

        stream_1_new_event_1 = NewEventBuilder().build()
        stream_1_new_event_2 = NewEventBuilder().build()
        stream_2_new_event_1 = NewEventBuilder().build()
        stream_2_new_event_2 = NewEventBuilder().build()

        store = EventStore(adapter=InMemoryStorageAdapter())
        category = store.category(category=category_name)
        stream_1 = category.stream(stream=stream_1_name)
        stream_2 = category.stream(stream=stream_2_name)

        stream_1.publish(events=[stream_1_new_event_1])
        stream_2.publish(events=[stream_2_new_event_1])
        stream_2.publish(events=[stream_2_new_event_2])
        stream_1.publish(events=[stream_1_new_event_2])

        events = category.read()

        stream_1_stored_event_builder = (
            StoredEventBuilder()
            .with_category(category_name)
            .with_stream(stream_1_name)
        )
        stream_2_stored_event_builder = (
            StoredEventBuilder()
            .with_category(category_name)
            .with_stream(stream_2_name)
        )

        stream_1_stored_event_1 = (
            stream_1_stored_event_builder.from_new_event(stream_1_new_event_1)
            .with_position(0)
            .build()
        )
        stream_2_stored_event_1 = (
            stream_2_stored_event_builder.from_new_event(stream_2_new_event_1)
            .with_position(0)
            .build()
        )
        stream_2_stored_event_2 = (
            stream_2_stored_event_builder.from_new_event(stream_2_new_event_2)
            .with_position(1)
            .build()
        )
        stream_1_stored_event_2 = (
            stream_1_stored_event_builder.from_new_event(stream_1_new_event_2)
            .with_position(1)
            .build()
        )

        self.assertEqual(
            events,
            [
                stream_1_stored_event_1,
                stream_2_stored_event_1,
                stream_2_stored_event_2,
                stream_1_stored_event_2,
            ],
        )

    def test_ignores_events_in_other_categories(self):
        category = data.random_event_category_name()
        stream = data.random_event_stream_name()
        new_event = NewEventBuilder().build()

        store = EventStore(adapter=InMemoryStorageAdapter())
        store.stream(category=category, stream=stream).publish(
            events=[new_event]
        )
        store.stream(
            category=(data.random_event_category_name()),
            stream=(data.random_event_stream_name()),
        ).publish(events=[(NewEventBuilder().build())])

        category_to_read = store.category(category=category)

        category_events = category_to_read.read()

        expected_stored_event = (
            StoredEventBuilder()
            .from_new_event(new_event)
            .with_category(category)
            .with_stream(stream)
            .with_position(0)
            .build()
        )

        self.assertEqual(
            category_events,
            [expected_stored_event],
        )


if __name__ == "__main__":
    unittest.main()
