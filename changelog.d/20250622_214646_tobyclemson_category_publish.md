### Added

- Added category-level publish functionality allowing atomic publishing of 
  events to multiple streams within a category via `EventCategory.publish()`.
- Added `StreamPublishDefinition` TypedDict for type-safe stream definitions 
  in category publishes.
- Added `stream_publish_definition` factory function to simplify creation of 
  stream definitions for category-level publishes.
- Added `MultiLock` utility class for coordinating multiple async locks in 
  memory adapter.
- Added comprehensive test coverage for category publish functionality across 
  all adapter implementations.

### Changed

- Enhanced `EventStorageAdapter` base class with category-level save method 
  supporting atomic multi-stream operations.
- Implemented category-level save in `InMemoryEventStorageAdapter` and 
  `PostgresEventStorageAdapter` ensuring atomicity and serialisation guarantee
  through appropriate locking.
