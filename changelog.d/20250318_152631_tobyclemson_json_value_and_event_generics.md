### Removed

- The `json` methods exposed by some types have been removed since they weren't
  being used within the library.

### Added

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

### Changed

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