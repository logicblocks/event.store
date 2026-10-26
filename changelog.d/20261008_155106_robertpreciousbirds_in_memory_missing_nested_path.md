### Fixed

- Filter clauses on a nested path missing from a projection no longer raise in
  the in-memory adapter; they do not match, as in the Postgres adapter
