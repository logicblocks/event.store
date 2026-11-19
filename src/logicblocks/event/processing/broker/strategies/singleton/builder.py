from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import timedelta
from types import NoneType
from typing import Self, TypedDict

from logicblocks.event.store import EventStoreEventSourceFactory
from logicblocks.event.types import StoredEvent

from ....services import ErrorHandler, RetryErrorHandler
from ...base import EventBroker
from ...subscribers import (
    InMemoryEventSubscriberStore,
)
from .broker import SingletonEventBroker


@dataclass(frozen=True)
class SingletonEventBrokerSettings:
    """Settings for a singleton event broker.

    Attributes:
        distribution_interval: The time to wait between rounds of passing
            event sources to subscribers.
    """

    distribution_interval: timedelta = timedelta(seconds=60)


class SingletonEventBrokerDependencies(TypedDict):
    """The storage dependent dependencies of a singleton event broker."""

    event_source_factory: EventStoreEventSourceFactory


class SingletonEventBrokerBuilder[**P = ...](ABC):
    """Base builder for singleton event brokers.

    Subclasses provide the storage dependent dependencies via
    `dependencies`; `prepare` must be called before `build`.

    Args:
        node_id: The ID of the node on which the broker will run.
    """

    node_id: str

    event_source_factory: EventStoreEventSourceFactory

    def __init__(self, node_id: str):
        self.node_id = node_id

    @abstractmethod
    def dependencies(
        self, *args: P.args, **kwargs: P.kwargs
    ) -> SingletonEventBrokerDependencies:
        """Create the dependencies for the broker from the given arguments."""
        pass

    def prepare(self, *args: P.args, **kwargs: P.kwargs) -> Self:
        """Resolve and store the broker's dependencies.

        Returns:
            this builder, for chaining.
        """
        prepare = self.dependencies(*args, **kwargs)
        self.event_source_factory = prepare["event_source_factory"]
        return self

    def build(
        self,
        settings: SingletonEventBrokerSettings,
        error_handler: ErrorHandler[NoneType] = RetryErrorHandler(),
    ) -> EventBroker[StoredEvent]:
        """Build a singleton event broker from the prepared dependencies.

        Args:
            settings: The settings for the broker.
            error_handler: The handler for errors raised during execution.

        Returns:
            the constructed event broker.
        """
        event_subscriber_store = InMemoryEventSubscriberStore[StoredEvent]()

        return SingletonEventBroker[StoredEvent](
            node_id=self.node_id,
            event_subscriber_store=event_subscriber_store,
            event_source_factory=self.event_source_factory,
            error_handler=error_handler,
            distribution_interval=settings.distribution_interval,
        )
