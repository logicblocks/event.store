### Added

- `ServiceManager.services` property exposing registered service definitions as a name-keyed mapping
- `ServiceManager.get_status_of_services` method exposing the statuses of registered services
- `ServiceDefinition.name` property for accessing the service name
- `ServiceDefinition.service_status` property for accessing `ProcessStatus` on status-aware services
- Optional `name` parameter on `ServiceManager.register` for explicit service naming
- Automatic service name generation and deduplication in `ServiceManager` when no name is provided

### Changed

- Service naming is now a responsibility of `ServiceManager` rather than `Service`
- `ServiceManager` stores service definitions in a name-keyed dictionary instead of a list
