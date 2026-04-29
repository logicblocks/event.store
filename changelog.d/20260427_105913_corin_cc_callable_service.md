### Added

- `CallableService` for wrapping async callables as `Service` instances
- `ServiceLike` type alias unifying `Service` and async callables
- `make_callable_service` adapter function for converting `ServiceLike` to `Service`
- `Service.name` base class property derived from class name
