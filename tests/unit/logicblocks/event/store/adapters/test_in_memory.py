import unittest

from logicblocks.event.store.types import Event
from logicblocks.event.store.adapters import InMemoryEventStoreAdapter


class TestInMemoryAdapter(unittest.TestCase):
    def test_publishes_single_event(self):
        adapter = InMemoryEventStoreAdapter()
        event = Event(name="something-happened", payload={"foo": "bar"})
        adapter.publish([event])

        self.assertEqual(adapter._events, [event])


if __name__ == "__main__":
    unittest.main()
