from logicblocks.event.processing import error_handler_type_mappingfrom logicblocks.event.processing import error_handler_type_mappings

### Added

- A new `ErrorHandler` abstraction has been introduced, with `ExitErrorHandler`,
  `RaiseErrorHandler`, `ContinueErrorHandler`, `RetryErrorHandler` and 
  `TypeMappingErrorHandler` implementations. The existing `ErrorHandlingService`
  has been extended to use this new abstraction, adding support for the various
  strategies for any delegate service.
  - `ExitErrorHandler` raises a `SystemExit` exception to exit the process. By
    default, it will exit with code 1, but this can be configured via the 
    `exit_code` constructor parameter.
  - `RaiseErrorHandler`, by default, raises the exception that was passed to it.
    This can be configured to raise a different exception via the 
    `exception_factory` constructor parameter.
  - `ContinueErrorHandler` request that execution continues from the point that
    the error was handled. The decision included a value that can be used as the
    return value of the calling method if required. By default, the value is
    `None`, but this can be configured via the `value_factory` constructor.
  - `RetryErrorHandler` requests that the operation that caused the error be 
    retried.
  - `TypeMappingErrorHandler` allows different error handling strategies for
    different exception types or their subclasses to be defined via the 
    `type_mappings` constructor parameter. By default, it will re-raise
    the handled exception, but this can be configured via the
    `default_decision_factory`. An example definition is:
    ```python
    handler = TypeMappingErrorHandler(
        type_mappings=error_handler_type_mappings(
            exit_fatally=error_handler_type_mapping(
              types=[CompletelyFatalError],
              callback=log_exception
            ),
            retry_execution=error_handler_type_mapping(
              types=[RetyableError],
            )
        )
    )
    ```
- A new `ErrorHandlingServiceMixin` has been introduced to allow error handling
  to be mixed in to `Service` classes, rather than using the 
  `ErrorHandlingService` delegating approach.
- `DistributedEventBroker` now automatically restarts on non-fatal exception.
  The error handling strategy can be configured via a new `error_handler`
  constructor parameter, accepting any `ErrorHandler` implementation.
- A new `SingletonEventBroker` has been introduced, which is a much simplified
  event broker that assumes it is the only broker in the system and does no
  coordination or work distribution. In this model, the infrastructure is 
  responsible for ensuring that only one instance of the broker is running at a
  time.
- A new `make_event_broker` function has been added to the 
  `logicblocks.event.processing` package, which allows creation of different
  types of event brokers using different storage types.

### Changed

- The `EventBrokerSettings` class has been renamed to 
  `DistributedEventBrokerSettings` to allow for other event broker 
  implementations.
- The `CoordinatorObserverEventBroker` class has been renamed to 
  `DistributedEventBroker`. 
- `ErrorHandlingService` has been improved to allow for more error handling 
  strategies such as exiting the process or retrying the operation. However,
  as part of this, a new `ErrorHandler` type has been introduced, which is
  expected to return a decision rather than raising or continuing directly.
- The `DistributedEventBroker` has undergone a number of changes in an effort to
  increase its robustness and reliability, specifically:
  - All long-running processes employed internally by the broker are now 
    executed within an `asyncio.TaskGroup` to ensure each is correctly cancelled
    in case of an exception being thrown by any of them. The same approach has
    been used in those processes directly where they manage their own group of
    long-running processes.
  - The approach to acquiring the coordinator lock has been changed such that
    now, rather than waiting for the lock to become available, the coordinator
    attempts to acquire the lock and if it fails, it will retry on a schedule
    controlled by the `coordinator_leadership_attempt_interval` setting on 
    `DistributedEventBrokerSettings`.
  - Rather than the leading coordinator continuing as leader until an error 
    occurs or the node is shut down, now the coordinator will abdicate after
    some period of time, allowing another node to take over as leader. This
    period is controlled by the `coordinator_leadership_max_duration` setting on
    `DistributedEventBrokerSettings`.
  - As mentioned in the "Added" section, the broker now automatically
    restarts on non-fatal exceptions, allowing it to recover from transient
    issues without manual intervention.
- The `make_postgres_event_broker` and `make_in_memory_event_broker` functions
  have been deprecated in favour of the new `make_event_broker` function.
