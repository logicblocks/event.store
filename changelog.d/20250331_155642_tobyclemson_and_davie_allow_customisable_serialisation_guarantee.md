### Added

- When reading from the `EventStore`, readers need to see events in a consistent
  order and should not skip any events being written concurrently. This requires
  serialising writes to the log such that all readers see the same event 
  ordering all the time. However, sometimes it's tolerable to only have 
  consistent order without skips at category or stream level, e.g., when reading
  of events is only done at category or stream level respectively. To support
  this, the `PostgresStorageAdapter` and `InMemoryStorageAdapter` now allow the
  serialisation guarantee to be customised, by passing a 
  `EventSerialisationGuarantee` enum value (`LOG`, `CATEGORY`, or `STREAM`) at 
  construction time.
