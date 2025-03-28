### Added

- Add a `@retry_on_unmet_condition_error` and `@ignore_on_unmet_condition_error`
  decorators for handling common event processing scenarios.
- Add more general `@retry_on_error` decorator retrying a function on a given 
  error.
- Add a `@event_store_transaction` decorator that wraps a function with 
  arbitrary handling.
