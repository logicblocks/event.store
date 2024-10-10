class InMemoryEventStoreAdapter(object):
    def __init__(self):
        self._events = []

    def publish(self, events):
        self._events.extend(events)
