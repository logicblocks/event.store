from .difference import EventSubscriptionDifference
from .subscribers import EventSubscriberStore
from .subscriptions import EventSubscriptionStateStore


class EventSubscriptionObserver:
    def __init__(
        self,
        subscription_store: EventSubscriptionStateStore,
        subscriber_store: EventSubscriberStore,
        subscription_difference: EventSubscriptionDifference,
    ):
        self.subscriber_store = subscriber_store
        self.subscription_store = subscription_store
        self.subscription_difference = subscription_difference

    async def observe(self):
        pass

    async def synchronise(self):
        pass
