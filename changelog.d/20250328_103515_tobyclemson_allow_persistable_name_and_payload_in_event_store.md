### Added

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

### Changed

- The `Persistable` type has been renamed to `JsonPersistable` for parity with
  the addition of `StringPersistable`.
- The `Loggable` type has been renamed to `JsonLoggable` for parity with the 
  other JSON-related types.
- The utility `serialise` and `deserialise` functions have been renamed to
  `serialise_to_json_value` and `deserialise_from_json_value` respectively due
  to the addition of `serialise_to_string` and `deserialise_from_string`.
