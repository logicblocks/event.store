### Added

- Added `EventStore.log()` that will return an `EventLog` class which can read the entire log of events, unfiltered by category or stream. `EventLog` inherits from `EventSource` just like `EventCategory` and `EventStream`, so it can be used in the same way as those classes, without the `category` and `stream` parameters.
