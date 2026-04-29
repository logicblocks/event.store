### Changed

- `SingletonEventBroker` and `DistributedEventBroker` now compose `ErrorHandlingService` instead of inheriting from `ErrorHandlingServiceMixin`

### Removed

- `ErrorHandlingServiceMixin` has been removed in favour of `ErrorHandlingService` composition
