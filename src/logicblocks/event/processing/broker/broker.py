from logicblocks.event.processing.services import Service

from .types import EventSubscriber


class EventBroker(Service):
    def __init__(self):
        self.consumers: list[EventSubscriber] = []

    async def register(self, subscriber: EventSubscriber) -> None:
        pass

    def execute(self):
        while True:
            # register node and set up heartbeating
            # add all subscriber instances to store
            # add all subscribers to the state store and healthcheck
            # start coordinator process
            #   - if wins lock, manage subscription table
            #   - else sit and wait
            # start observer process
            #   - monitor subscription table and replay event sources onto
            #     subscriber instances

            # process to heartbeat node - PARTIALLY DONE
            # process to register and healthcheck subscribers -
            # process to coordinate and distribute subscriptions - DONE
            # process to observe and synchronise subscriptions - DONE
            pass
