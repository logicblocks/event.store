import unittest

from datetime import datetime

import logicblocks.event.store.testing.data as data
from logicblocks.event.store import EventStore
from logicblocks.event.store.adapters import InMemoryStorageAdapter
from logicblocks.event.store.testing import NewEventBuilder
from logicblocks.event.store.types import NewEvent, StoredEvent


class TestEventStore(unittest.TestCase):
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
                position=0,
                occurred_at=event.occurred_at,
                observed_at=event.observed_at,
            )
            for event in new_events
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


if __name__ == "__main__":
    unittest.main()
