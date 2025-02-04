from logicblocks.event.processing.services import Service

from .types import EventSubscriber


class EventBroker(Service):
    def __init__(self):
        self.consumers: list[EventSubscriber] = []

    async def register(self, subscriber: EventSubscriber) -> None:
        pass

    def execute(self):
        while True:
            # process to heartbeat node - PARTIALLY DONE
            # process to register and healthcheck subscribers -
            # process to coordinate and distribute subscriptions - DONE
            # process to observe and synchronise subscriptions - DONE
            pass
