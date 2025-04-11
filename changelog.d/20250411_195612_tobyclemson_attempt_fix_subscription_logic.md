### Fixed

- From time to time, the active `EventSubscriptionCoordinator` inside a
  `CoordinatorObserverEventBroker` would fail due to being unable to locate the
  subscription source mapping for a subscriber group that existed in the 
  centralised subscriber state store. Upon failing, the `EventBroker` would
  become irrecoverable such that over time, all instances would fail and event
  processing would cease. To fix this, the sources requested by subscribers are
  now stored alongside the subscriber state in the centralised store.

### Removed

- The `NodeManager`, `NodeStateStore`, `InMemoryNodeStateStore` and 
  `PostgresNodeStateStore` classes have been removed as they weren't required
  by the subscription coordination algorithm.
- The `SubscriptionSourceMappingStore` and 
  `InMemorySubscriptionSourceMappingStore` classes have been removed as they are 
  no longer required by the subscription coordination algorithm.

-->
<!--
### Added

- A bullet item for the Added category.

-->
<!--
### Changed

- A bullet item for the Changed category.

-->
<!--
### Deprecated

- A bullet item for the Deprecated category.

-->
<!--

-->
<!--
### Security

- A bullet item for the Security category.

-->
