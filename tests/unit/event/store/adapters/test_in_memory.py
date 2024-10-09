import unittest

from event.types import Event
from event.store.adapters import InMemoryEventStoreAdapter

class TestInMemoryAdapter(unittest.TestCase):
    def test_saves_single_event(self):
        adapter = InMemoryEventStoreAdapter()
        event = Event(name='something-happened', payload={'foo': 'bar'})
        adapter.publish([event])

        self.assertEqual(adapter._events, [event])
