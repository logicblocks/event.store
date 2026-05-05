### Changed

- `ErrorHandlingService` now uses composition instead of inheritance, accepting an instance of `Service` via the `service` parameter
- `ErrorHandlingService.apply_error_handling` is now a reusable class method

### Deprecated

- `callable` keyword argument on `ErrorHandlingService` in favour of `service`
- Passing in a callable as a positional argument on `ErrorHandlingService`, an instance of `Service` should be used instead

### Removed

- `ErrorHandlingService` no longer supports passing in an `error_handler` as a positional argument
