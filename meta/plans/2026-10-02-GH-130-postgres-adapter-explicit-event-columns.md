---
type: plan
id: "2026-10-02-GH-130-postgres-adapter-explicit-event-columns"
title: "Postgres Adapter Explicit Event Columns Implementation Plan"
date: "2026-10-02T14:56:13+00:00"
author: "Xuemin Guan"
producer: create-plan
status: draft
tags: [postgres, event-store, schema-evolution, rolling-deploy]
revision: "a525967ba61efb6eac03f0915d2723d138b5b849"
repository: "event.store"
last_updated: "2026-10-02T16:07:28+00:00"
last_updated_by: "Xuemin Guan"
schema_version: 1
---

# Postgres Adapter Explicit Event Columns Implementation Plan

## Overview

`PostgresEventStorageAdapter` reads events with `*` and builds each row
with `class_row(StoredEvent[...])`. psycopg calls `StoredEvent(**row)`,
so any column the installed `StoredEvent` does not know becomes an
unexpected keyword argument:

```
TypeError: StoredEvent.__init__() got an unexpected keyword argument 'metadata'
```

This turns additive schema changes into read-time breaks, and forced a
read shim onto both sides of the 0.1.11 → 0.1.12a3 upgrade (see
`chl-email-service` COMMS-144, steps 3a and 3c).

We fix it by naming the event columns explicitly in every query whose
rows reach `class_row`. The column list comes from one place: the fields
of `StoredEvent`.

Issue: https://github.com/logicblocks/event.store/issues/130

## Current State Analysis

All four `class_row(StoredEvent[str, JsonValue, JsonValue])` cursors
(`adapter.py:747,805,894,912`) are fed by queries that use `*`:

| Query builder | Line | SQL | Reached by |
| --- | --- | --- | --- |
| `insert_batch_query` | `adapter.py:453` | `RETURNING *` | every `save` |
| `read_last_query` | `adapter.py:309` | `SELECT *` | `latest`, `save` to a stream |
| `scan_query` | `adapter.py:185` | `Query().select_all()` | `scan` |
| `read_last_category_batch_query` | `adapter.py:363` | `SELECT DISTINCT ON (category, stream ) *` | `save` to a category |

The issue names the first-column `SELECT *`, the `class_row` cursors and
`Star`. It misses `RETURNING *` and `DISTINCT ON ... *`. Both must
change, or writes still break.

The other Postgres stores already tolerate extra columns. The projection
store (`projection/store/adapters/postgres.py:133,172`) and the
subscriber and subscription state stores
(`.../subscribers/stores/state/postgres.py:197`,
`.../subscriptions/stores/state/postgres.py:182`) use `dict_row` and
pick columns by name. The events adapter is the odd one out.

## Desired End State

- The events adapter never selects or returns `*`. Every query whose
  rows reach `class_row` names exactly the fields of `StoredEvent`.
- An `events` table with an extra column the library does not know
  works for `latest`, `scan`, `save` to a stream and `save` to a
  category.
- A changelog fragment states the guarantee and its limits.
- `mise run` is green.

### Key Discoveries

- `StoredEvent` is a `@dataclass(frozen=True)`
  (`types/event.py:83`), so `dataclasses.fields(StoredEvent)` gives the
  ten field names. `class_row` already relies on field names matching
  column names, so deriving the column list from the fields adds no new
  coupling.
- `Query.select(*target_list: str | ...)` (`persistence/postgres/query.py:533`)
  turns each string into a quoted `ColumnReference`. `scan_query` can
  use it in place of `select_all()` with no builder change.
- Scan constraint appliers only add `WHERE` and `ORDER BY` clauses, so
  changing the select list does not affect them.
- `insert_batch` (`adapter.py:559`) uses the returned rows only for
  `id`, `stream`, `category`, `position`, `sequence_number`,
  `observed_at` and `occurred_at`. It takes `name`, `payload` and
  `metadata` from the `NewEvent`. Returning the full `StoredEvent`
  column list keeps `class_row` working unchanged.
- The test helper `read_events_query`
  (`tests/integration/.../store/adapters/test_postgres.py:57`) also uses
  `SELECT *` into `class_row(StoredEvent)`. It breaks the same way, so
  the new tests cannot use it as it is.
- `test_batch_insert_query_builds_correct_sql`
  (`tests/unit/.../postgres/test_adapter.py:248`) pins `RETURNING *` in
  its expected SQL.
- `Star` (`persistence/postgres/query.py:109`) stays. The generic query
  converters and paging clauses use it over projections and subqueries,
  and none of those rows reach `StoredEvent`.

## What We're NOT Doing

- Not ignoring unknown columns in a custom row factory. That silently
  drops data and hides column typos. Explicit columns are clearer.
- Not changing `Star`, `select_all()`, or the generic query converters.
- Not changing the projection, subscriber or subscription stores. They
  already tolerate extra columns.
- Not changing the `INSERT` column list. It is already explicit.
- Not making the *new* version tolerate a *missing* column. A release
  that adds a field still needs its migration applied first.
- Not removing the need for a default during an additive `NOT NULL`
  migration. The old `INSERT` does not name the new column, so it still
  needs a default for the overlap. Drop it in a second migration only
  after all writers supply the column and the rollback window requiring
  that default has closed.

## Implementation Approach

One PR, built test-first, one test at a time.

Each step adds an integration test against a real table that carries an
extra column. Each test fails first with the `TypeError` above, then
passes after the smallest change to one query builder. The order is
chosen so each new test fails for exactly one reason:

1. `save` to a new stream — fails only on `RETURNING *`, because
   `read_last` finds no row and never calls `class_row`.
2. `latest`, and `save` to an existing stream — both fail only on
   `read_last_query`.
3. `scan` — fails only on `scan_query`.
4. `save` to a category with an existing stream — fails only on
   `DISTINCT ON ... *`, once `RETURNING` is fixed.

Tests that need existing events seed them **before** adding the extra
column, using the adapter as it is. This keeps the seed step independent
of the fix. The new-stream test starts with an empty table.

---

## Phase 1: Tolerate unknown columns in the adapter

### Overview

Add the shared column list and fix all four queries, one red-green cycle
per query.

### Changes Required

#### 1. Test support: an extra column and a tolerant reader

**File**: `tests/integration/logicblocks/event/store/adapters/test_postgres.py`

Add a helper that adds a column the library does not know:

```python
async def add_unknown_column(
    pool: AsyncConnectionPool[AsyncConnection], table: str
) -> None:
    async with pool.connection() as connection:
        await connection.execute(
            sql.SQL(
                "ALTER TABLE {0} "
                "ADD COLUMN unknown_column TEXT NOT NULL DEFAULT 'unknown'"
            ).format(sql.Identifier(table))
        )
```

Change `read_events_query` to select the `StoredEvent` fields by name,
so `read_events` works on the widened table:

```python
from dataclasses import fields


def read_events_query(table: str) -> abc.Query:
    columns = sql.SQL(", ").join(
        sql.Identifier(field.name) for field in fields(StoredEvent)
    )
    return sql.SQL("SELECT {0} FROM {1} ORDER BY sequence_number").format(
        columns, sql.Identifier(table)
    )
```

This is a test helper only. It does not depend on the production
change, so existing tests keep passing before and after.

Add a test class, following the fixture layout of
`TestPostgresStorageAdapterScanPaging` (`test_postgres.py:227`): a
separate `store_connection_pool` fixture, and
`shutdown_async_generators`, because in the red step the scan generator
dies mid-iteration:

```python
class TestPostgresStorageAdapterUnknownColumns:
    pool: AsyncConnectionPool[AsyncConnection]

    @pytest_asyncio.fixture(autouse=True)
    async def store_connection_pool(self, open_connection_pool):
        self.pool = open_connection_pool

    @pytest_asyncio.fixture(autouse=True)
    async def reinitialise_storage(self, open_connection_pool):
        await drop_table(open_connection_pool, "events")
        await create_table(open_connection_pool, "events")

    @pytest_asyncio.fixture(autouse=True)
    async def shutdown_async_generators(self):
        yield

        await asyncio.get_event_loop().shutdown_asyncgens()
```

#### 2. Step 1 — `RETURNING`

**Test (red)**: `test_saves_to_new_stream_when_table_has_unknown_column`.
Add the unknown column, save one event to a new stream, and assert the
returned events equal `read_events(pool, "events")`.

**Unit test (red)**: in
`tests/unit/logicblocks/event/store/adapters/postgres/test_adapter.py`,
change the expected SQL in `test_batch_insert_query_builds_correct_sql`
from `RETURNING *;` to the explicit list:

```
RETURNING "id", "name", "stream", "category", "position",
          "sequence_number", "payload", "metadata", "observed_at",
          "occurred_at";
```

**File**: `src/logicblocks/event/store/adapters/postgres/adapter.py`

**Change (green)**: add the shared column list near the top of the
module, and use it in `insert_batch_query`:

```python
from dataclasses import fields

EVENT_COLUMNS = tuple(field.name for field in fields(StoredEvent))


def event_columns() -> sql.Composable:
    return sql.SQL(", ").join(
        sql.Identifier(column) for column in EVENT_COLUMNS
    )
```

```python
                VALUES
                    {1}
                    RETURNING {2};
                """).format(
            sql.Identifier(table_settings.table_name),
            rows_expression,
            event_columns(),
        ),
```

#### 3. Step 2 — `latest`

**Test (red)**: `test_reads_latest_when_table_has_unknown_column`,
parameterised over a log, category and stream target. Seed two events
in one stream, add the unknown column, and assert `adapter.latest(...)`
equals the last seeded event.

**Test (red)**: `test_saves_to_existing_stream_when_table_has_unknown_column`.
Seed one event in a stream, add the unknown column, save one more event
to the same stream, and assert the returned sequence equals the final
one-element slice from `read_events(pool, "events")`. Saving to an existing
stream calls `read_last`, which finds a row, so `class_row` runs. Without this test
the `save` to a stream path is only covered for a new stream.

**Change (green)**: in `read_last_query`, which fixes both tests:

```python
    select_clause = sql.SQL("SELECT {columns}").format(
        columns=event_columns()
    )
```

#### 4. Step 3 — `scan`

**Test (red)**: `test_scans_when_table_has_unknown_column`,
parameterised over a log, category and stream target. Seed events, add
the unknown column, and assert the scanned list equals the seeded
events.

**Change (green)**: in `scan_query`:

```python
    builder = (
        Query().select(*EVENT_COLUMNS).from_table(table_settings.table_name)
    )
```

#### 5. Step 4 — `save` to a category

**Test (red)**:
`test_saves_to_existing_streams_in_category_when_table_has_unknown_column`.
Seed one event in each of two streams of a category, add the unknown
column, save one event to each stream through a `CategoryIdentifier`
target, and read all events back with `read_events`. Exclude the seeded
event IDs, group the remaining newly appended events by stream name, and
assert that this mapping equals the returned mapping. Assert that each
stream has exactly one newly appended event. The seed makes
`read_last_category_batch` return rows, so `class_row` runs.

**Change (green)**: in `read_last_category_batch_query`:

```python
    select_clause = sql.SQL(
        "SELECT DISTINCT ON (category, stream) {columns}"
    ).format(columns=event_columns())
```

#### 6. Refactor

With all tests green:

- Confirm no `*` remains in the events adapter:
  `grep -n '\*' src/logicblocks/event/store/adapters/postgres/adapter.py`
  should show only non-SQL uses.
- Check whether `event_columns()` reads better as a module constant
  (`EVENT_COLUMNS_SQL = sql.SQL(", ").join(...)`). Keep whichever the
  type checker accepts cleanly.
- Fold the four new tests into fewer parameterised tests only if it
  reads more clearly. Do not force it.

### Success Criteria

#### Automated Verification

- [ ] Each new integration test fails with
      `TypeError: StoredEvent.__init__() got an unexpected keyword argument 'unknown_column'`
      before its query change, and passes after it
- [ ] The updated `test_batch_insert_query_builds_correct_sql` fails
      before the `RETURNING` change and passes after it
- [ ] Provision the local database before targeted integration runs:
      `mise run database:test:provision`
- [ ] New tests pass:
      `mise exec -- invoke test.integration --test-args="-k TestPostgresStorageAdapterUnknownColumns"`
- [ ] Shared adapter cases still pass:
      `mise exec -- invoke test.integration --test-args="-k TestPostgresEventStorageAdapterCommonCases"`
- [ ] All unit tests pass: `mise run test:unit`
- [ ] All integration tests pass: `mise run test:integration`
- [ ] Type checking passes: `mise run types:check`
- [ ] Lint and format pass: `mise run lint:fix` and `mise run format:fix`
      leave no diff

#### Manual Verification

- [ ] Reproduce the issue end to end once against a local database:
      save events, `ALTER TABLE events ADD COLUMN unknown_column TEXT`,
      then `latest` and `scan` succeed and the rows match
- [ ] The custom table name path still works with an extra column (spot
      check with `TableSettings(table_name="event_log")`)

---

## Phase 2: Changelog

### Overview

Tell users what changed and what it does not cover. Correct the existing
metadata migration and rollback guidance in the same PR so the release
does not ship contradictory instructions.

### Changes Required

**File**: new fragment from `mise run changelog:fragment:create`

```markdown
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
    the default only after all writers supply the column and the rollback
    window requiring that default has closed.
  - A release that adds a field still needs its migration applied
    before it runs.
```

**File**: `changelog.d/20260626_135547_bivav.satyal_event_metadata.md`

Correct the migration and rollback advice:

- Remove the claim that the previous version ignores the extra column.
  Releases before this fix, including 0.1.11, fail when wildcard query
  results include `metadata`. An application-only rollback to 0.1.11
  therefore does not work while that column remains.
- Split adding the column and dropping its default into separate migration
  steps. Retain `DEFAULT 'null'::jsonb` while any running writer omits
  `metadata`; drop it only after all writers supply the column and the
  rollback window requiring that default has closed.
- Explain that retaining the default protects inserts that omit the
  column, but does not fix the old adapter's wildcard reads or `RETURNING`.
  This fix cannot retroactively make the 0.1.11 metadata upgrade safe for
  rolling deployment; that transition still needs a compatibility fix or
  a coordinated migration and application cutover.
- Preserve the warning that dropping `metadata` permanently discards its
  data. Do not present dropping the column as a routine rollback step.
- Keep the requirement to apply the metadata migration before running
  code that requires the column.

### Success Criteria

#### Automated Verification

- [ ] The fragment exists in `changelog.d/`
- [ ] The existing metadata fragment is updated in the same PR
- [ ] Full build passes: `mise run`

#### Manual Verification

- [ ] The fragment reads clearly to someone planning their next
      upgrade
- [ ] Both fragments agree on the limits for releases before this fix,
      defaults during version overlap, and application-only rollback
- [ ] The metadata migration example does not drop the default in the
      initial add-column step

---

## Testing Strategy

### Unit Tests

- `test_batch_insert_query_builds_correct_sql` asserts the explicit
  `RETURNING` list. No new unit tests: query text alone does not prove
  the behaviour, and the integration tests do.

### Integration Tests

- `TestPostgresStorageAdapterUnknownColumns` covers every path that
  reaches `class_row`: `save` to a new stream, `save` to an existing
  stream, `latest` (log, category,
  stream), `scan` (log, category, stream) and `save` to a category with
  existing streams.
- `TestPostgresEventStorageAdapterCommonCases` guards against
  regressions on the normal schema.

### Manual Testing Steps

1. Start the local database and create the `events` table.
2. Save a few events through the adapter.
3. Add a column the library does not know.
4. Call `latest`, `scan`, and `save` to both a stream and a category.
   All succeed, and the returned events match the stored rows.

## Performance Considerations

- None of note. The query plans do not change. Naming columns can only
  reduce the data read, because an extra column is no longer fetched.

## Migration Notes

- No schema change and no API change.
- The benefit starts with the *next* additive column. To gain it, a
  service must already run this release when that column's migration
  lands.

## References

- Issue: https://github.com/logicblocks/event.store/issues/130
- Motivating upgrade: `2026-09-25-COMMS-144-structlog-and-logicblocks-upgrade.md`
  (chl-email-service), phases 3a and 3c
- Adapter: `src/logicblocks/event/store/adapters/postgres/adapter.py:185,309,363,453`
- Column-tolerant precedent: `src/logicblocks/event/projection/store/adapters/postgres.py:133,172`
- Query builder `select`: `src/logicblocks/event/persistence/postgres/query.py:533`
- Metadata migration guidance: `changelog.d/20260626_135547_bivav.satyal_event_metadata.md`
