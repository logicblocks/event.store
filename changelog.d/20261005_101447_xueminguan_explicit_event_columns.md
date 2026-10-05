### Fixed

- `PostgresEventStorageAdapter` now names the event columns explicitly
  in every read and in `RETURNING`, instead of using `*`. An `events`
  table with columns the installed `StoredEvent` does not know no
  longer breaks reads or writes with
  `TypeError: StoredEvent.__init__() got an unexpected keyword argument`.
  - This makes additive schema changes safe for the *running* version
    during a rolling deploy, from this release onwards. Services on an
    earlier release still break on an extra column.
  - A new `NOT NULL` column still needs a default while old and new
    versions overlap, because the old `INSERT` does not name it. Drop
    the default only after all writers supply the column and the
    rollback window requiring that default has closed.
  - A release that adds a field still needs its migration applied
    before it runs.
