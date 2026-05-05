### Changed

- `PollingService` now uses composition instead of inheritance, accepting an instance of `Service` via the `service` parameter

### Deprecated

- `callable` keyword argument on `PollingService` in favour of `service`
- Passing in a callable as a positional argument on `PollingService`, an instance of `Service` should be used instead

### Removed

- `PollingService` no longer supports passing in a `poll_interval` as a positional argument
