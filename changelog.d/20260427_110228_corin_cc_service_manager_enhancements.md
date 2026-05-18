### Added

- Services registered in `ServiceManager` now include a name
- Optional `name` parameter on `ServiceManager.register` for explicit service naming
- Automatic UUID-based service name generation in `ServiceManager` when no name is provided
- `ManagedServiceState` class for accessing the current state of a service that has been registered in `ServiceManager`
- `ServiceManager.services` property exposing registered service states as a name-keyed mapping
- `ServiceManager.service` method for retrieving a single service's state by name
- `DeferredFuture` class providing a stable awaitable handle that delegates to the real future once resolved, raising `RuntimeError` if awaited before scheduling. Returned by `ManagedServiceState.future`

### Changed

- `ServiceManager.start` and `async with ServiceManager: ...` return `Self` instead of a list of futures; futures are now accessed via `ManagedServiceState.future`
- Added `ProcessStatus.UNKNOWN` to enum
