<a id='changelog-0.1.5a25'></a>
# 0.1.6 — 2025-06-11

## Removed

- The `json` methods exposed by some types have been removed since they weren't
  being used within the library.
- The `NodeManager`, `NodeStateStore`, `InMemoryNodeStateStore` and
  `PostgresNodeStateStore` classes have been removed as they weren't required
  by the subscription coordination algorithm. If you are creating `EventBroker`
  instances using the `make_*_event_broker` factory functions, this will be
  invisible to you.
- The `SubscriptionSourceMappingStore` and
  `InMemorySubscriptionSourceMappingStore` classes have been removed as they are
  no longer required by the subscription coordination algorithm.
- Both `PostgresEventStoreEventSourceFactory` and 
  `InMemoryEventStoreEventSourceFactory` have been removed - usages of both can 
  be replaced with `EventStoreEventSourceFactory`.

## Added

- A `JsonValue` type alias type has been introduced which represents a datatype
  that can be converted to and from JSON.
- Supporting `JsonObject`, `JsonArray` and `JsonPrimitive` type alias types have
  been added for use when a subset of `JsonValue` is required.
- A `JsonValueType` type has been introduced for situations where a `type` is
  required, rather than a type alias type.
- Type predicate functions, `is_json_value`, `is_json_object`, `is_json_array`
  and `is_json_primitive` have been added for use in type checking, with each
  function narrowing the type of its argument if the argument is of the
  requisite type.
- Add a `@retry_on_unmet_condition_error` and `@ignore_on_unmet_condition_error`
  decorators for handling common event processing scenarios.
- Add more general `@retry_on_error` decorator retrying a function on a given
  error.
- Add a `@event_store_transaction` decorator that wraps a function with
  arbitrary handling.
- A new `StringPersistable` type has been added, along with corresponding
  `StringSerialisable`, `StringDeserialisable` and `StringConvertible` protocols
  representing the ability to serialise, deserialise and convert between custom
  types and strings.
- Utility `serialise_to_string` and `deserialise_from_string` functions have
  been added for types adhering to the `StringPersistable` type.
- `EventStore` can now publish events into streams that have a `JsonPersistable`
  payload and a `StringPersistable` name. This allows using custom types for
  each of these event fields so long as they can be appropriately serialised and
  deserialised. As yet, only writes support these types, with the primitive
  types being returned on read. However, a future change will add support for
  event schemas to allow typed reads.
- When reading from the `EventStore`, readers need to see events in a consistent
  order and should not skip any events being written concurrently. This requires
  serialising writes to the log such that all readers see the same event
  ordering all the time. However, sometimes it's tolerable to only have
  consistent order without skips at category or stream level, e.g., when reading
  of events is only done at category or stream level respectively. To support
  this, the `PostgresStorageAdapter` and `InMemoryStorageAdapter` now allow the
  serialisation guarantee to be customised, by passing a
  `EventSerialisationGuarantee` enum value (`LOG`, `CATEGORY`, or `STREAM`) at
  construction time.
- Added `ErrorHandlingService` to allow for injection of error handling
  strategies in services.
- Added `EventStore.log()` that will return an `EventLog` class which can read 
  the entire log of events, unfiltered by category or stream. `EventLog` 
  inherits from `EventSource` just like `EventCategory` and `EventStream`, so 
  it can be used in the same way as those classes, without the `category` and 
  `stream` parameters.
- The general purpose query package now supports `Function` types, currently
  only as part of `SortClause`, but with the intention that these could
  additionally be supported in `FilterClause` as well. The only function
  currently shipped with the library is `Similarity`, which has been implemented
  using trigram matching.
- As part of adding support for `Function`s in the general purpose query
  language, the PostgreSQL and in-memory query conversion capabilities have been
  extended to support conversion of functions, allowing arbitrary `Function`
  types to be supported by the consumers of the conversion capability.
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
- Query now supports a `REGEX_MATCHES` operator, allowing regex matching on 
  string fields. This uses the postgres '~' operator for regex matching.

## Changed

- The `Codec` protocol has been split into `JsonValueSerialisable` and
  `JsonValueDeserialisable`, with the composition of those being renamed to
  `JsonValueConvertible`.
- The `CodecOrMapping` type alias has been renamed to `Persistable` since that
  is what it is meant to represent. Similarly, a `Loggable` type alias type has
  been introduced for those things that can be logged.
- Rather than using `Mapping[str, Any]` as the persistable type in
  `ProjectionStore` and `EventStore`, `JsonValue` is now used, which means
  implementations of the `JsonValueConvertible` (previously `Codec`) protocol
  must be changed to accept and produce a `JsonValue` instead.
- Types that previously supported a `dict` method (e.g., `*Event`,
  `*Identifier`) for rendering themselves to a `Mapping[str, Any] now have a
  `serialise` method which renders them to a `JsonValue`.
- Types that previously supported an `enveloper` method (e.g., `*Event`,
  `Projection`) for rendering themselves to a `Mapping[str, Any] now have a
  `summarise` method which renders them to a `JsonValue`.
- The `NewEvent` and `StoredEvent` classes are now generic over their payload
  type, defaulting to `JsonValue`, rather than `Mapping[str, Any]`.
- The `serialise` and `deserialise` methods, which are largely an internal
  detail of the library, but can be used by, for example, adapter implementers,
  have been made less strict in terms of the types they accept.
- The `serialise` and `deserialise` methods now allow a fallback conversion
  callable to be provided in the case that they are unable to handle the
  provided values. By default, the fallback callables raise `ValueError`.
- WriteConditions can now be combined using the `&` and `|` operators
  - These can be chained together to create complex conditions, for example: 
    `condition1 & condition2 | (condition3 & condition4)`
- Publish no longer takes a set of conditions, instead it accepts a single 
  WriteCondition.
- "No conditions" is now represented by the `NoCondition` singleton rather than 
  an empty set
- The type parameters on `Projector` have been reordered to better reflect the
  transformation taking place. So rather than
  `Projector[State, Identifier, Metadata]` where identifier identifies the
  source of the events, the type signature is now
  `Projector[Identifier, State, Metadata]`
- `NewEvent` and `StoredEvent` are now generic over their name type, in addition
  to the payload, i.e., `NewEvent[Name, Payload]` and
  `StoredEvent[Name, Payload]`. This change is to allow methods/functions to
  target specific events, for example, as
  `StoredEvent[Literal["profile-created"], ProfilePayload]`. Previously, if two
  events had the same payload type but different names, it wasn't possible to
  restrict a method/function signature to only one of them.
- The `Persistable` type has been renamed to `JsonPersistable` for parity with
  the addition of `StringPersistable`.
- The `Loggable` type has been renamed to `JsonLoggable` for parity with the
  other JSON-related types.
- The utility `serialise` and `deserialise` functions have been renamed to
  `serialise_to_json_value` and `deserialise_from_json_value` respectively due
  to the addition of `serialise_to_string` and `deserialise_from_string`.
- The changes to how the `CoordinatorObserverEventBroker` operates have resulted
  in breaking changes to the supporting table structure. Upgrading to this
  version requires migrations to drop the `nodes` table and to modify the
  `subscribers` table to have a JSONB `subscription_requests` column. See
  `sql/create_subscribers_table.sql` for the latest table structure.
- As part of adding support for `Function`s in the general purpose query types,
  `SortField` and `FilterClause` have had their `path` attribute renamed to
  `field` since it now accepts, in the case of `SortField` initially, either a
  `Path` or a `Function`.
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
- `EventStorageAdapter` adapter argument can now optionally be provided to 
  factories for building postgres event-brokers, in the case where you want to 
  build a postgres backed event-broker but use an event-store with a different 
  persistence backend.

### Package Reorganisation

- The query types previously exported by the `logicblocks.event.projection`
  package have been moved to a dedicated package called
  `logicblocks.event.query` since they are also used within the `EventBroker`
  implementation. This affects the following types:
  - `Query`
  - `Clause`
  - `Path`
  - `Operator`
  - `SortField`
  - `SortOrder`
  - `Lookup`
  - `Search`
  - `FilterClause`
  - `SortClause`
  - `OffsetPagingClause`
  - `KeySetPagingClause`
- The `logicblocks.event.db` package has been renamed to
  `logicblocks.event.persistence` since it now houses persistence related
  abstractions for in-memory storage in addition to database-backed storage.
- The `Postgres...` prefixed types exported by `logicblocks.event.db` are now
  accessible without the `Postgres...` prefix from
  `logicblocks.event.persistence.postgres`. This affects the following types:
  - `PostgresConnectionSettings`
  - `PostgresConnectionSource`
  - `PostgresParameterisedQuery`
  - `PostgresParameterisedQueryFragment`
  - `PostgresQuery`
  - `PostgresCondition`
  - `PostgresSqlFragment`
- Some `Postgres...` and `InMemory...` prefixed types exported by
  `logicblocks.projection.store.adapters` are now accessible without the
  prefixes from `logicblocks.event.persistence.postgres` and
  `logicblocks.event.persistence.memory` respectively. This affects the
  following types:
  - `PostgresParameterisedQuery`
  - `PostgresQuery`
  - `PostgresQueryConverter`
  - `PostgresTableSettings`
  - `InMemoryQueryConverter`
  - `InMemoryClauseConverter`
  - `InMemoryProjectionResultSetTransformer`
- `PostgresTableSettings` has been generalised and renamed to `TableSettings`
  and moved from `logicblocks.event.store.adapters`,
  `logicblocks.projection.store.adapters`,
  `logicblocks.processing.broker.subscribers.stores.state.postgres` and
  `logicblocks.processing.broker.subscriptions.stores.state.postres` into
  `logicblocks.event.persistence.postgres` since it is now used in more
  contexts.

### Behavioural Changes

- The `NoCondition` value previously available in
  `logicblocks.event.store.conditions` has been changed to a class, such that it
  must be instantiated at the point of use.

### Converter Abstraction

- The `PostgresQueryConverter` abstraction previously utilised in the PostgreSQL
  projection storage adapter has been extracted and abstracted under the
  `logicblocks.event.persistence` package. This approach was also copied in to
  the `logicblocks.event.processing.broker` package multiple times, and these
  copies have now been removed in favour of using the generalised abstraction.
- The `QueryConverter` abstraction has also been extended to the in-memory
  projection / subscriber / subscription storage cases for parity and to allow
  for further extension.
- The `...Converter` approach to converting between generalised concepts and
  adapter-specific implementations has been also extended to the implementations
  of `QueryContstraint`s and `WriteCondition`s in the `logicblocks.event.store`
  package.
- Previously, `QueryConstraint`s used a combination of a method on the
  constraint itself for in-memory storage adapter usage and a `singledispatch`
  function, declared in the `logicblocks.event.store.adapters.postgres`
  module, for PostgreSQL storage adapter usage. The disparity by adapter type
  was confusing and inflexible and as such, the `query_constraint_to_sql`
  `singledispatch` function and the `met_by` method on the constraint have been
  removed and a constraint converter has been introduced instead. The constraint
  converter is adapter-specific, allowing constraints to be converted into an
  appropriate form based on the nature of the storage mechanism. See
  `logicblocks.event.store.adapters.postgres.converters` and
  `logicblocks.event.store.adapters.memory.converters` for example
  implementations and extension points.
- Similarly, `WriteCondition`s used a method on each condition which was
  passed the latest event in the stream, if it existed, to make an
  assessment of whether the condition was satisfied. Whilst this allowed for
  simplistic implementation of the condition check, it was not flexible enough
  to accommodate conditions which needed to be aware of properties of the whole
  stream or category. Instead, a condition converter has been introduced. Again,
  the condition converter is adapter-specific. For the PostgreSQL
  implementation, the condition enforcer receives the currently open connection,
  allowing for transactional checks to be performed against the database. For
  the in-memory implementation, the condition enforcer receives the currently
  open in-memory transaction allowing transaction checks against the in-memory
  database. See `logicblocks.event.store.adapters.postgres.converters` and
  `logicblocks.event.store.adapters.memory.converters` for example
  implementations and extension points.

## Fixed

- Fix race condition in coordinator.
- Fixed an issue in `make_subscriber` so that a new subscriber ID is created
  by default each time `make_subscriber` is called, if a value for
  `subscriber_id` is not provided as an argument.
- From time to time, the active `EventSubscriptionCoordinator` inside a
  `CoordinatorObserverEventBroker` would fail due to being unable to locate the
  subscription source mapping for a subscriber group that existed in the
  centralised subscriber state store. Upon failing, the `EventBroker` would
  become irrecoverable such that over time, all instances would fail and event
  processing would cease. To fix this, the sources requested by subscribers are
  now stored alongside the subscriber state in the centralised store.

## DevEx

- Allow for running `library:test:unit|integration|component` with filter
  options to run only tests matching a specific pattern. Example usage:
  `go "library:test:unit[TestAllTestsInFile]"` or
  `go "library:test:component[test_a_specific_test]"`.
