### Changed

- **BREAKING:** The `error_handler_type_mapping` function has been removed and
  replaced with four specialized functions for each error handling strategy:
  - `exit_fatally_type_mapping(types, exit_code_factory, callback)`
  - `raise_exception_type_mapping(types, exception_factory, callback)`
  - `continue_execution_type_mapping(types, value_factory, callback)`
  - `retry_execution_type_mapping(types, callback)`

  **Migration:** Replace calls to `error_handler_type_mapping(types=...,
  callback=...)` with the appropriate specialized function for the error
  handling strategy being configured. For example:
  ```python
  # Before
  error_handler_type_mappings(
      retry_execution=error_handler_type_mapping(
          types=[MyException],
          callback=my_callback,
      )
  )

  # After
  error_handler_type_mappings(
      retry_execution=retry_execution_type_mapping(
          types=[MyException],
          callback=my_callback,
      )
  )
  ```

- **BREAKING:** The `ExitErrorHandler` constructor now takes an
  `exit_code_factory: Callable[[BaseException], int]` parameter instead of
  `exit_code: int`. This allows dynamic exit code determination based on the
  exception.

  **Migration:** Replace `ExitErrorHandler(exit_code=N)` with
  `ExitErrorHandler(exit_code_factory=lambda _: N)`.

- **BREAKING:** The `ContinueErrorHandler` constructor now requires a
  `value_factory: Callable[[BaseException], T]` parameter (previously optional
  with a default returning `None`).

  **Migration:** Replace `ContinueErrorHandler()` with
  `ContinueErrorHandler(value_factory=lambda _: None)` or provide a meaningful
  value factory.

- **BREAKING:** The `ErrorHandlerDecision.continue_execution` static method now
  requires the `value` parameter (previously optional with default `None`).

  **Migration:** Replace `ErrorHandlerDecision.continue_execution()` with
  `ErrorHandlerDecision.continue_execution(value=None)` or provide a meaningful
  value.

- **BREAKING:** The `TypeMappingErrorHandler` constructor parameter
  `default_decision_factory` has been renamed to `default_error_handler` and now
  accepts an `ErrorHandler[T]` instance instead of a decision factory callable.

  **Migration:** Replace:
  ```python
  # Before
  TypeMappingErrorHandler(
      default_decision_factory=lambda _: ErrorHandlerDecision.retry_execution()
  )

  # After
  TypeMappingErrorHandler(
      default_error_handler=RetryErrorHandler()
  )
  ```

- **BREAKING:** The `TypeMappingErrorHandler` constructor parameter
  `type_mappings` now expects a `TypeMappings[T]` (a mapping from exception
  types to `ErrorHandlingDefinition[T]`) instead of `TypeMappingsDict`. Use the
  `error_handler_type_mappings` function to construct this value.

- **BREAKING:** The `continue_execution` parameter of `error_handler_type_mappings`
  no longer accepts a simple sequence of exception types. It now requires a
  `ContinueExecutionTypeMappingDict` with an explicit `value_factory`.

  **Migration:** Replace:
  ```python
  # Before
  error_handler_type_mappings(
      continue_execution=[MyException1, MyException2]
  )

  # After
  error_handler_type_mappings(
      continue_execution=continue_execution_type_mapping(
          types=[MyException1, MyException2],
          value_factory=lambda _: None,
      )
  )
  ```

- **BREAKING:** The `make_subscriber` function now requires the
  `subscriber_state_converter` parameter (previously optional with a default
  `StoredEventEventConsumerStateConverter`).

  **Migration:** Either:
  1. Use the new `make_event_store_subscriber` function which provides the
     default converter for `StoredEvent` types, or
  2. Explicitly pass `subscriber_state_converter=StoredEventEventConsumerStateConverter()`
     to `make_subscriber`.

### Added

- New `make_event_store_subscriber` function as a convenience wrapper around
  `make_subscriber` that provides a default `StoredEventEventConsumerStateConverter`
  for event store subscribers. Import from
  `logicblocks.event.processing.consumers.subscription`.

- New specialized type mapping functions (`exit_fatally_type_mapping`,
  `raise_exception_type_mapping`, `continue_execution_type_mapping`,
  `retry_execution_type_mapping`) providing better type safety and clearer API
  for configuring error handling strategies.

- `exit_fatally_type_mapping` and `raise_exception_type_mapping` now support
  factory functions (`exit_code_factory` and `exception_factory` respectively)
  allowing dynamic determination of exit codes and exceptions based on the
  caught exception.
