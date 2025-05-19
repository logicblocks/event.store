### Added

- The general purpose query package now supports `Function` types, currently
  only as part of `SortClause`, but with the intention that these could 
  additionally be supported in `FilterClause` as well. The only function 
  currently shipped with the library is `Similarity`, which has been implemented
  using trigram matching.
- As part of adding support for `Function`s in the general purpose query 
  language, the PostgreSQL and in-memory query conversion capabilities have been
  extended to support conversion of functions, allowing arbitrary `Function`
  types to be supported by the consumers of the conversion capability.

### Changed

- As part of adding support for `Function`s in the general purpose query types,
  `SortField` and `FilterClause` have had their `path` attribute renamed to 
  `field` since it now accepts, in the case of `SortField` initially, either a
  `Path` or a `Function`.
