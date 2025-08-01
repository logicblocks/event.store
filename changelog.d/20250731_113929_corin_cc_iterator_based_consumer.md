### Added
- Create new `EventProcessor` types:
- `EventProcessor`: This is the same as the previous implementation - a single function that takes a single event and processes it, the processor state is tracked and stored automatically.
- `AutoCommitEventIteratorProcessor`: Receives an iterator of events, after each iteration, the processor state is updated and stored automatically.
- `ManagedEventIteratorProcessor`: Receives an iterator of events and a `StateStoreEventProcessorManager` for managing the processor state.
  - The processor state is not automatically tracked and stored.
  - To indicate that an event or events have been processed you must call `processor_manager.acknowledge(events)` after processing the events.
  - To persist the processor state, you must call `processor_manager.commit()` after processing the events.
  - Persistence still respects the `persistence_interval` of `EventConsumerStateStore`.
  - Persistence can be forced by calling `processor_manager.commit(force=True)`.

### Changed
- `EventSourceConsumer` now supports different types of `EventProcessor` (see above) .
- `StoredEventEventConsumerStateConverter` has moved from `logicblocks.event.store` to `logicblocks.event.processing`, this is to remove circular dependencies.
- `EventConsumerStateStore` changes:
  - No longer implicitly calls `save` inside `record_processed`.
  - Add `save_if_needed` method, which saves the processor state if the `persistence_interval` has been reached.
  - `record_processed` is no longer async.
  - Supports a `persistence_interval` of `None` to disable persistence of the processor state unless forced.
