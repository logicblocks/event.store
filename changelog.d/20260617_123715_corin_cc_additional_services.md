### Added

- Added `RetryStrategy` support to the error handling service, allowing
  configurable retry behaviour including `ConstantRetryStrategy`,
  `IncludeExceptionsRetryStrategy`, and `ExcludeExceptionsRetryStrategy`.
- Added `RetryStrategyDecision` types (`WaitRetryStrategyDecision`,
  `RetryImmediatelyDecision`, `OverrideRetryStrategyDecision`) for controlling
  retry behaviour from strategies.
- Added `ErrorHandler` hierarchy (`ContinueErrorHandler`, `ExitErrorHandler`,
  `RaiseErrorHandler`, `RetryErrorHandler`) as standalone classes.
- Added `ErrorHandlerDecision` types for representing error handling outcomes.


### Changed

- Reorganised `processing.services.error` into a subpackage with separate
  modules for `ErrorHandler`, `ErrorHandlerDecision`, `RetryStrategy`, and
  `RetryStrategyDecision`.
- Renamed `WaitStrategy` to `RetryStrategy` throughout the error handling
  service.
