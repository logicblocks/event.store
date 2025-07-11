### Changed

- The constraints module has moved from `logicblocks.event.store` to `logicblocks.event.sources`
- The `EventSource` class has moved from `logicblocks.event.store` to `logicblocks.event.sources`
- The following classes are now generic over the event type they support, with a bound of the new `BaseEvent` protocol
  - `EventSource`
  - `ConstrainedEventSource`
  - `InMemoryEventSource`
  - `EventSubscriber`
  - `EventBroker`
  - `EventSubscriptionObserver`
  - `EventSubscriberManager`
  - `EventSubscriberStore`
  - `EventSourceFactory`
  - `EventProcessor`
  - `EventSourceConsumer`
  - `EventSubscriptionConsumer`
  - `QueryConstraintCheck`
