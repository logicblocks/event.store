### Added
 - `InMemoryEventSource` is now generic over the event type it supports
 - `EventConsumerStateStore` is now generic over the event type it supports
 - `EventConsumerStateStore` and `make_subscriber` supports a `save_state_after_consumption` flag that disables automatic state saving after consuming a batch of events
 - `EventConsumerStateStore` requires a `EventConsumerStateConverter`, which converts events to state and state into QueryConstraints

### Changed
 - The `Event` protocol no longer requires a `sequence_number`
 - `EventConsumerStateStore` no longer stores `last_sequence_number` at the top level. This is now handled in `StoredEventEventConsumerStateConverter`.
   - This change is backwards compatible
 - `InMemoryEventSource` has been renamed `InMemoryStoredEventSource` and has moved from `logicblocks.event.sources` to `logicblocks.event.store`
 - The following have moved:
   - `EventStoreEventSourceFactory` has moved from `logicblocks.event.sources` to `logicblocks.event.store`
