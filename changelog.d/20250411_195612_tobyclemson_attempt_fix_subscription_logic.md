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
  by the subscription coordination algorithm. If you are creating `EventBroker`
  instances using the `make_*_event_broker` factory functions, this will be
  invisible to you.
- The `SubscriptionSourceMappingStore` and 
  `InMemorySubscriptionSourceMappingStore` classes have been removed as they are 
  no longer required by the subscription coordination algorithm.

### Changed

- The changes to how the `CoordinatorObserverEventBroker` operates have resulted
  in breaking changes to the supporting table structure. Upgrading to this
  version requires migrations to drop the `nodes` table and to modify the 
  `subscribers` table to have a JSONB `subscription_requests` column. See 
  `sql/create_subscribers_table.sql` for the latest table structure.
