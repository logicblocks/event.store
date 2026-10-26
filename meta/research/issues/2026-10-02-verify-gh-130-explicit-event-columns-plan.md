---
type: issue-research
id: "2026-10-02-verify-gh-130-explicit-event-columns-plan"
title: "Investigation: Verify the GH-130 explicit event columns plan"
date: "2026-10-02T15:05:20+00:00"
author: "Xuemin Guan"
producer: research-issue
status: complete
topic: "Check the GH-130 plan's claims against the code and against a real database"
tags: [research, debugging, postgres, event-store, storage-adapter, schema-evolution]
revision: "63ee8e4454d97325e6c211c6a269a7b5ebaf02b4"
repository: "event.store"
last_updated: "2026-10-02T15:05:20+00:00"
last_updated_by: "Xuemin Guan"
schema_version: 1
---

# Investigation: Verify the GH-130 explicit event columns plan

**Date**: 2026-10-02 15:05 UTC
**Author**: Xuemin Guan
**Git Commit**: 63ee8e4454d97325e6c211c6a269a7b5ebaf02b4 (parent `1f6c448`, 0.1.12a5)
**Branch**: main
**Repository**: event.store

## Issue Description

Verify the plan
`meta/plans/2026-10-02-GH-130-postgres-adapter-explicit-event-columns.md`.
Its background is the chl-email-service upgrade plan
`2026-09-25-COMMS-144-structlog-and-logicblocks-upgrade.md`.

The reported failure:

```
TypeError: StoredEvent.__init__() got an unexpected keyword argument 'metadata'
```

`PostgresEventStorageAdapter` reads with `*` into
`class_row(StoredEvent[...])`. Any column that `StoredEvent` does not
know becomes an unexpected keyword. In COMMS-144 this forced a monkey
patch on `StoredEvent.__init__` (phase 3a) and a strict same-commit
removal (phase 3c).

## Input Classification

Mixed. One exact error message, plus a written plan whose claims need
checking one by one.

## Affected Components

- `src/logicblocks/event/store/adapters/postgres/adapter.py:185` — `scan_query`, `select_all()`
- `src/logicblocks/event/store/adapters/postgres/adapter.py:309` — `read_last_query`, `SELECT *`
- `src/logicblocks/event/store/adapters/postgres/adapter.py:363` — `read_last_category_batch_query`, `DISTINCT ON (...) *`
- `src/logicblocks/event/store/adapters/postgres/adapter.py:453` — `insert_batch_query`, `RETURNING *`
- `src/logicblocks/event/store/adapters/postgres/adapter.py:747,805,894,912` — the four `class_row` cursors
- `src/logicblocks/event/types/event.py:83` — `StoredEvent`, frozen dataclass, ten fields
- `src/logicblocks/event/persistence/postgres/query.py:533` — `Query.select`
- `tests/integration/logicblocks/event/store/adapters/test_postgres.py:57` — test helper `read_events_query`, `SELECT *`
- `tests/unit/logicblocks/event/store/adapters/postgres/test_adapter.py:248,286` — pins `RETURNING *`
- `changelog.d/20260626_135547_bivav.satyal_event_metadata.md` — unreleased fragment with a wrong rollback claim

## Timeline / Reproduction

Reproduced against the local test database (`mise run database:test:provision`).
A scratch script seeded two streams, then ran
`ALTER TABLE events ADD COLUMN unknown_column TEXT NOT NULL DEFAULT 'unknown'`,
then called each adapter path.

| Path | Current code | Plan applied |
| --- | --- | --- |
| `save` to a new stream | `TypeError` | OK |
| `save` to an existing stream | `TypeError` | OK |
| `latest` (log) | `TypeError` | OK |
| `latest` (stream) | `TypeError` | OK |
| `save` to a category | `TypeError` | OK |
| `scan` (log) | `TypeError` | OK |

Then the four fixes were applied one at a time, in the plan's order:

| Fixed so far | Still failing |
| --- | --- |
| none | all six |
| `RETURNING` | existing stream, latest ×2, category, scan |
| + `read_last_query` | category, scan |
| + `scan_query` | category |
| + `DISTINCT ON` | none |

Each step clears only its own paths. The plan's claim that "each new test
fails for exactly one reason" holds.

## Hypotheses

The "hypotheses" here are the plan's claims. Each one was tested.

### Hypothesis 1: The four queries listed are the only ones that reach `class_row`

- **Evidence for**: `grep` finds `class_row` only at `adapter.py:747,805,894,912`.
  Those cursors execute exactly `read_last_query`, `read_last_category_batch_query`,
  `insert_batch_query`, `scan_query` and `obtain_write_locks_query`. The
  lock query is `SELECT pg_advisory_xact_lock(...)` and is never fetched.
  The write condition enforcers (`converters.py:84-232`) use only the
  `latest_event` passed in and run no SQL.
- **Evidence against**: None found.
- **Verdict**: Confirmed.

### Hypothesis 2: Deriving columns from `fields(StoredEvent)` gives the right list

- **Evidence for**: `fields(StoredEvent)` returns
  `id, name, stream, category, position, sequence_number, payload, metadata, observed_at, occurred_at`.
  This matches the plan's expected `RETURNING` list and the table DDL
  (`sql/create_events_table.sql`). The base classes add no dataclass fields.
- **Evidence against**: None.
- **Verdict**: Confirmed.

### Hypothesis 3: `Query().select(*EVENT_COLUMNS)` is a drop-in for `select_all()`

- **Evidence for**: `Query.select` (`query.py:533`) turns each string into a
  quoted identifier. `Query().select('id','name').from_table('events').build()`
  yields `SELECT "id", "name" FROM "events"`. The only built-in applier,
  `SequenceNumberAfterConstraintQueryApplier` (`converters.py:35`), adds a
  `WHERE`. The custom applier test (`test_postgres.py:647`) also only adds a
  `WHERE`.
- **Evidence against**: None for built-in code.
- **Verdict**: Confirmed.

### Hypothesis 4: `insert_batch` still works when `RETURNING` names the columns

- **Evidence for**: `insert_batch` (`adapter.py:595-608`) reads `id`, `stream`,
  `category`, `position`, `sequence_number`, `observed_at` and `occurred_at`
  from the returned row. It takes `name`, `payload` and `metadata` from the
  `NewEvent`. Full reproduction passes.
- **Evidence against**: None.
- **Verdict**: Confirmed.

### Hypothesis 5: The unit test change will match

- **Evidence for**: `test_batch_insert_query_builds_correct_sql` compares with
  `normalize_whitespace` (`test_adapter.py:137,288`). `sql.Identifier`
  renders as `"id"`, joined by `", "`, so the plan's multi-line expected
  string normalises to the same text.
- **Evidence against**: None.
- **Verdict**: Confirmed.

### Hypothesis 6: The plan and its changelog are consistent with the rest of the repo

- **Evidence for**: The new fragment correctly says an earlier release still
  breaks on an extra column, and that a `NOT NULL` column needs a default
  during the overlap.
- **Evidence against**: The unreleased fragment
  `changelog.d/20260626_135547_bivav.satyal_event_metadata.md` says
  "Rollback is non-destructive: the previous version ignores the extra
  column". COMMS-144 measured this as false: 0.1.11 fails 65 reads with
  `TypeError`, and its recommended migration (which drops the default at
  once) fails 43 writes with `NotNullViolation`. That fragment is not yet
  in `CHANGELOG.md`, so it can still be fixed. The plan leaves it alone,
  so the next release would ship two fragments that disagree.
- **Verdict**: Confirmed gap.

## Root Cause

The plan's diagnosis is correct. psycopg's `class_row` calls
`cls(**dict(zip(names, values)))`, and `names` comes from the query's
result columns. Four queries in the events adapter return `*`
(`adapter.py:185,309,363,453`), so every table column becomes a keyword
for `StoredEvent`. An extra column raises `TypeError` on every read and
on every write, because writes end in `RETURNING *`.

## Causal Chain

1. A migration adds a column to `events` that the running `StoredEvent` does not have.
2. Any adapter query returns `*`, so the result includes that column.
3. `class_row` passes it to `StoredEvent.__init__` as a keyword.
4. The frozen dataclass rejects it with `TypeError`.
5. In a service, `backoff_polling` logs this at warning level and retries forever (COMMS-144, phase 3a findings).

## Contributing Factors

- `RETURNING *` and `DISTINCT ON ... *` are easy to miss. The GitHub issue names
  neither. The plan does catch both.
- The test helper `read_events_query` uses the same `SELECT *` pattern, so tests
  could not run against a widened table.
- The metadata changelog fragment told users the old version ignores the extra
  column. That made the risk look smaller than it was.

## Fix Options

The plan already chose its fix. These are the changes I suggest to the plan itself.

| Option | Description | Risk | Effort |
|--------|-------------|------|--------|
| A | Accept the plan as written | Low | — |
| B | A, plus correct the rollback and migration advice in the unreleased metadata fragment | Low | Low |
| C | B, plus a test for `save` to an existing stream, plus two small test-code fixes | Low | Low |

## Recommended Fix

Option C. The plan is sound and its core claims are all proven. Add these:

1. **Fix the metadata fragment (Phase 2).** In
   `changelog.d/20260626_135547_bivav.satyal_event_metadata.md`, replace
   "the previous version ignores the extra column" with the truth: releases
   before this fix fail on an extra column, so an app-only rollback to
   0.1.11 breaks while the column exists. Also say that keeping the default
   during the overlap protects the old `INSERT`. Otherwise the release ships
   two fragments that contradict each other.
2. **Test `save` to an existing stream.** "Desired End State" lists `save`
   to a stream, but the only stream-save test uses a *new* stream. Saving to
   an existing stream goes through `read_last` and fails today (proved
   above). Add it in Step 2, next to `latest`, since the same
   `read_last_query` change fixes both.
3. **Small test-code fixes.**
   - The `read_events_query` snippet needs `from dataclasses import fields`.
   - The proposed fixture puts `self.pool = ...` inside `reinitialise_storage`.
     Sibling classes (`TestPostgresStorageAdapterScanPaging`, `test_postgres.py:227`)
     use a separate `store_connection_pool` fixture. Match that.
   - For the scan tests, copy the `shutdown_async_generators` fixture from
     `TestPostgresStorageAdapterScanPaging`. In the red step the scan
     generator dies mid-iteration.

## Prevention

- After this change, an integration test with an extra column guards all four
  paths. Any new query that returns `*` into `class_row` will fail it.
- Never use `*` in a query whose rows reach `class_row`. The plan's refactor
  step (`grep` for `*` in the adapter) is a good check to keep in review.

## Recent Changes

- `jj log` on `adapter.py` shows one recent relevant change:
  `78b9b535` "Make serialised StoredEvent Metadata type parameter explicit".
  The `metadata` column and field were added in the 0.1.12 alpha line
  (fragment dated 2026-06-26), which is what exposed the `*` problem.

## Open Questions

- The COMMS-144 document is staged at this repo's root
  (`2026-09-25-COMMS-144-structlog-and-logicblocks-upgrade.md`). It belongs
  to chl-email-service. Should it be committed here, moved under `meta/`,
  or left out of the PR?
- Users with custom `QueryApplier`s that join other tables could see
  ambiguous column names with unqualified identifiers. With `*` they would
  have got duplicate keywords, which also fails, so this is not a
  regression. No built-in code does this.
