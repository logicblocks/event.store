from logicblocks.event.processing.broker import EventBroker, EventSubscriber
from logicblocks.event.processing.services import Service


class PostgresEventBroker(EventBroker, Service):
    def __init__(self):
        self.consumers: list[EventSubscriber] = []

    async def register(self, subscriber: EventSubscriber) -> None:
        pass

    def execute(self):
        while True:
            # try to become leader
            # register and allocate work
            # ---
            # provide consumers with their event sources
            # revoke them when no longer allowed
            pass
