### Changed

- `SingletonEventBroker` and `DistributedEventBroker` now compose `ErrorHandlingService` instead of inheriting from `ErrorHandlingServiceMixin`

### Deprecated

- `ErrorHandlingServiceMixin` has been deprecated in favour of `ErrorHandlingService` composition
