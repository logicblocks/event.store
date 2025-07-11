from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import timedelta
from typing import Self, TypedDict

from logicblocks.event.sources import EventStoreEventSourceFactory
from logicblocks.event.types import StoredEvent

from ...base import EventBroker
from ...subscribers import (
    InMemoryEventSubscriberStore,
)
from .broker import SingletonEventBroker


@dataclass(frozen=True)
class SingletonEventBrokerSettings:
    distribution_interval: timedelta = timedelta(seconds=60)


class SingletonEventBrokerDependencies(TypedDict):
    event_source_factory: EventStoreEventSourceFactory


class SingletonEventBrokerBuilder[**P = ...](ABC):
    node_id: str

    event_source_factory: EventStoreEventSourceFactory

    def __init__(self, node_id: str):
        self.node_id = node_id

    @abstractmethod
    def dependencies(
        self, *args: P.args, **kwargs: P.kwargs
    ) -> SingletonEventBrokerDependencies:
        pass

    def prepare(self, *args: P.args, **kwargs: P.kwargs) -> Self:
        prepare = self.dependencies(*args, **kwargs)
        self.event_source_factory = prepare["event_source_factory"]
        return self

    def build(
        self,
        settings: SingletonEventBrokerSettings,
    ) -> EventBroker[StoredEvent]:
        event_subscriber_store = InMemoryEventSubscriberStore[StoredEvent]()

        return SingletonEventBroker[StoredEvent](
            node_id=self.node_id,
            event_subscriber_store=event_subscriber_store,
            event_source_factory=self.event_source_factory,
            distribution_interval=settings.distribution_interval,
        )
