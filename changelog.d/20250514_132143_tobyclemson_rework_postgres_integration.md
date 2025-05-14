### Changed

#### Package Reorganisation

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

#### Behavioural Changes

- The `NoCondition` value previously available in 
  `logicblocks.event.store.conditions` has been changed to a class, such that it
  must be instantiated at the point of use.

#### Converter Abstraction

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
