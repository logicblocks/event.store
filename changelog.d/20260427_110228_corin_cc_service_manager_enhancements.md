### Added

- `ServiceManager.services` property exposing registered service states as a name-keyed mapping
- `ServiceManager.get_service_state` method for retrieving a single service's state by name
- `ManagedServiceState.name` property for accessing the service name
- `ManagedServiceState.service_status` property for accessing `ProcessStatus` on status-aware services
- Optional `name` parameter on `ServiceManager.register` for explicit service naming
- Automatic UUID-based service name generation in `ServiceManager` when no name is provided

### Changed

- `ServiceDefinition` renamed to `ManagedServiceState`
- Service naming is now a responsibility of `ServiceManager` rather than `Service`
- `ServiceManager` stores managed service states in a name-keyed dictionary instead of a list
- `ServiceManager.start` and `ServiceManager.__aenter__` returns a name-keyed Mapping of futures instead of a list of futures
