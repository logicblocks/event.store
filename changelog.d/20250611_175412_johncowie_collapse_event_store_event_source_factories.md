### Removed 

- `PostgresEventStoreEventSourceFactory` and `InMemoryEventStoreEventSourceFactory` - usages of both can be replaced with `EventStoreEventSourceFactory`

### Changed

- `EventStorageAdapter` adapter argument can now optionally be provided to factories for building postgres event-brokers, in the case where you want to build a postgres backed event-broker but use an event-store with a different persistence backend.
