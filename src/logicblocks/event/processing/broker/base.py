from abc import ABC, abstractmethod
from types import NoneType

from logicblocks.event.types import Event

from ..process import Process
from ..services import Service
from .types import EventSubscriber


class EventBroker[E: Event](Service[NoneType], Process, ABC):
    """A long-running service that allocates event sources to subscribers.

    Subscribers are registered with the broker, which then hands each of
    them the event sources they requested, via `EventSubscriber.accept`,
    while it executes. Brokers expose a `ProcessStatus` so their health can
    be monitored.

    Use `make_event_broker` to construct a broker of the required type.
    """

    @abstractmethod
    async def register(self, subscriber: EventSubscriber[E]) -> None:
        """Register a subscriber to receive event sources from the broker.

        Subscribers must be registered before the broker is executed, as
        the distributed broker never allocates sources to later ones.

        Args:
            subscriber: The subscriber to register.
        """
        raise NotImplementedError
