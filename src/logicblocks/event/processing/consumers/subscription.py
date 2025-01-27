from .types import EventConsumer


class EventSubscriptionConsumer(EventConsumer):
    async def consume_all(self) -> None:
        pass

    async def execute(self) -> None:
        pass
