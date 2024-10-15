import unittest

from datetime import datetime

from logicblocks.event.store import EventStore
from logicblocks.event.store.adapters import InMemoryStorageAdapter
from logicblocks.event.store.types import NewEvent


class TestEventStore(unittest.TestCase):
    def test_fetches_published_events(self):
        now = datetime.now()
        store = EventStore(adapter=InMemoryStorageAdapter())

        store.publish(
            category="some-category",
            stream="some-stream",
            events=[
                NewEvent(
                    name="something-happened",
                    payload={"foo": "bar"},
                    observed_at=now,
                    occurred_at=now,
                )
            ],
        )


if __name__ == "__main__":
    unittest.main()
