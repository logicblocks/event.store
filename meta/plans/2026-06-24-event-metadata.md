---
type: plan
id: "2026-06-24-event-metadata"
title: "Event Metadata Implementation Plan"
date: "2026-06-24T10:04:40+00:00"
author: "bivav-ebury"
producer: create-plan
status: draft
derived_from: []
relates_to: []
tags: ["metadata", "events", "schema-migration"]
revision: "366dcb6ca5b3a97cd3bf13628ace29c3db31bbc1"
repository: "event.store"
last_updated: "2026-06-24T10:04:40+00:00"
last_updated_by: "bivav-ebury"
schema_version: 1
---

# Event Metadata Implementation Plan

## Overview

Add an optional, domain-agnostic `metadata` bag to events, persisted in a
dedicated `metadata JSONB NOT NULL` column on the `events` table. This mirrors
the existing `projections.metadata` column and the `Projection[State, Metadata]`
generic. Consumers who supply no metadata see no behavioural change (it defaults
to `{}`); consumers who need cross-cutting context (e.g. "who was the actor")
place a convention inside the bag. The `payload` column stays byte-identical to
today.

The design is specified and approved in
`meta/specs/2026-06-11-event-metadata-design.md`. This plan operationalises that
spec, grounded in the current code, and is implemented **clean from `main`** —
any earlier fold-into-payload work is discarded, not adapted.

## Current State Analysis

Events today carry only `name`, `payload`, `observed_at`, `occurred_at`
(`src/logicblocks/event/types/event.py:18,77`). There is no place for
cross-cutting context.

The `Projection` type already solves this exact problem and is the template to
mirror:

- `Projection[State = JsonValue, Metadata = JsonValue]`
  (`src/logicblocks/event/types/projection.py:18`) carries a `metadata` field,
  includes it in `serialise()`, `__repr__`, and in `serialise_projection()`.
- The `projections` table has `metadata JSONB NOT NULL` with no DB default
  (`sql/create_projections_table.sql`) — the application always supplies a
  value.
- `BaseProjectionBuilder` (`src/logicblocks/event/testing/builders.py:249`)
  carries a `Metadata` generic, a `with_metadata(...)` method, and a default
  metadata factory.

### Key Discoveries

- **Read path is free in Postgres.** Both adapters read rows via
  `class_row(StoredEvent[str, JsonValue])`
  (`postgres/adapter.py:718,773,859,877`). Once `StoredEvent` has a `metadata`
  field matching the column name, psycopg maps it automatically — no custom row
  factory is needed.
- **Write path is two narrow edits.** `insert_batch_query`
  (`postgres/adapter.py:404`) builds a fixed 8-placeholder row
  `"(%s, %s, %s, %s, %s, %s, %s, %s)"` and an explicit `INSERT INTO ... (id,
  name, stream, category, position, payload, observed_at, occurred_at)` column
  list. Both grow by one for `metadata`.
- **RETURNING reconstruction is explicit.** `insert_batch`
  (`postgres/adapter.py:583`) rebuilds caller-facing `StoredEvent[Name, Payload]`
  from the original `NewEvent`, carrying `payload=event.payload`; it must
  likewise carry `metadata=event.metadata` (the DB-returned metadata is already
  serialised, so use the original event's value to preserve the caller's type,
  exactly as payload is handled).
- **Memory adapter has two construction blocks.** `_save_to_stream`
  (`memory/adapter.py:201`) and `_save_to_category` (`memory/adapter.py:282`)
  each build a `StoredEvent[Name, Payload]` (caller-returned) and a
  `StoredEvent[str, JsonValue]` (serialised, stored in the in-memory DB). All
  four sites carry `metadata` through, serialising it for the stored copy via
  `serialise_to_json_value`.
- **Generic chain is deep.** `Name, Payload` flows through `publish`
  (`store/store.py:56,201`), `save` (`store/adapters/base.py:82,93,102`),
  `StreamPublishDefinition` (`store/types.py:14`), `StreamInsertDefinition`
  (`postgres/adapter.py:66`), `insert_batch` / `insert_batch_query`, and
  `_save_to_stream` / `_save_to_category` in both adapters. Per the decision
  below, `Metadata = JsonValue` is added to every one of these.
- **Migration precedent exists.** The
  `events_category_stream_sequence_number_index` addition shipped as a canonical
  DDL change plus a changelog fragment stating a migration is required
  (`changelog.d/20260126_110222_daviemoston_add_category_stream_sequence_number_index.md`).
  No migration framework — raw `sql/` DDL + changelog announcement.
- **Builders mirror the projection builder.** `NewEventBuilder` /
  `StoredEventBuilder` (`testing/builders.py:42,110`) follow the same `_clone` /
  `with_*` / `build` shape that `BaseProjectionBuilder` already uses for
  `metadata`.

## Desired End State

`NewEvent`, `StoredEvent`, and their builders carry a `Metadata = JsonValue`
generic and a `metadata` field defaulting to `{}`. The `events` table has a
`metadata JSONB NOT NULL` column. An event published with metadata round-trips
through both the memory and Postgres adapters; an event published without
metadata round-trips as `{}`. The `payload` column is byte-identical to today in
all cases. A changelog fragment documents the addition and the required
migration.

**Verification:** `mise run` (full build) is green — unit, integration, and
component tests, type checking, linting, and formatting all pass — and a new
shared adapter test proves metadata round-trips identically in both adapters.

## Decisions (resolved before planning)

- **Public-surface-only generics.** The `Metadata` generic is added to the
  public types (`NewEvent`, `StoredEvent`), the builders, and the adapter
  construction sites that build those types. It is **not** threaded through the
  intermediate `publish` / `save` / `StreamPublishDefinition` /
  `StreamInsertDefinition` / `insert_batch` signatures, which stay at
  `[Name, Payload]`. Metadata still round-trips end-to-end as a plain field;
  only the static generic parameterisation of the returned `StoredEvent` is
  narrower than full propagation. This matches the deleted plan's deliberate
  scope. If `mise run types:check` flags the third generic anywhere it is
  reactively required, add it there only — don't pre-emptively thread it.
- **Verification via `mise run`.** Per project `CLAUDE.md`, all automated
  criteria use `mise run ...` targets, not `make`. Targeted runs use the
  `--test-args` switch: `mise run test:unit --test-args='-k <expr>'` (and the
  same for `test:integration`). Do **not** use `mise exec -- invoke ...` — it
  bypasses the task-dependency chain (e.g. DB provisioning). The build brings up
  the test database itself; never pass `DB_PORT`.
- **`summarise()` excludes metadata** *(deviation from spec, per reviewer
  feedback on PR #116)*. `summarise()` output is used in log statements, and
  metadata may carry sensitive context (e.g. actor identifiers). To avoid
  leaking it into logs, metadata is **omitted** from `summarise()`. It is still
  included in `serialise()` for full round-trip. The spec said to include it in
  both; this narrows that to `serialise()` only.
- **`StoredEvent.metadata` has no default** *(deviation from spec, per reviewer
  feedback on PR #116)*. The spec proposed adding the field last with a
  `default_factory` of `{}`. That is type-unsound: if `Metadata` is, say,
  `list[str]`, `{}` is not a valid default. Instead, `metadata` is a **required**
  field on `StoredEvent`, and every construction site passes it explicitly.
  `NewEvent.metadata` keeps its `{}` default because `NewEvent` has a custom
  `__init__` where the default is a deliberate convenience for callers.

## What We're NOT Doing

- No first-class `actor`, `causation`, or `correlation` types. The library
  provides a generic bag only.
- No querying/filtering on metadata contents. The column makes it possible
  later; no query support is added now.
- No reserved `__metadata` payload key, no fold/split helpers, no custom row
  factory, no payload-shape detection, no write guards. The dedicated column
  removes all of these concerns (and the superseded fold-based approach).
- No migration framework. Raw `sql/` DDL + changelog fragment only.
- No DB-level default on the column — the application always supplies at least
  `{}`.
- No metadata-specific adapter tests where an existing full-field test can be
  extended instead (per reviewer feedback — see Phase 7).
- Do **not** commit the assembled `CHANGELOG.md`; only the fragment under
  `changelog.d/` is committed (the human/CI assembles).

## Implementation Approach

Strict TDD (red-green-refactor), one test at a time, following the spec's test
ordering. Build inner-out: types first (the foundation every other layer
depends on), then builders (used by the adapter tests), then the canonical SQL
and adapters, then the changelog fragment, and finally the shared cross-adapter
round-trip coverage. Shared adapter test cases are extended once so memory and
Postgres receive identical coverage. Expected results are built as complete
objects and asserted by equality (the codebase's preferred style).

---

## Phase 1: Core Types

### Overview

Add the `Metadata` generic, field, and serialisation to `NewEvent` and
`StoredEvent`, and carry it through `serialise_stored_event`. This is the
foundation; all later phases depend on it.

### Changes Required

#### 1. `NewEvent`

**File**: `src/logicblocks/event/types/event.py`
**Changes**: Add `Metadata = JsonValue` generic and `metadata: Metadata` field.
Default `metadata` to `{}` in `__init__` (mirroring how `observed_at` defaults).
Include `metadata` in `serialise()` and `__repr__`. Do **not** add it to
`summarise()` (see Decisions — summarise feeds logs).

```python
@dataclass(frozen=True)
class NewEvent[Name = str, Payload = JsonValue, Metadata = JsonValue](
    JsonValueSerialisable
):
    name: Name
    payload: Payload
    metadata: Metadata
    observed_at: datetime
    occurred_at: datetime

    def __init__(
        self,
        *,
        name: Name,
        payload: Payload,
        metadata: Metadata | None = None,
        observed_at: datetime | None = None,
        occurred_at: datetime | None = None,
        clock: Clock = SystemClock(),
    ):
        ...
        object.__setattr__(
            self, "metadata", metadata if metadata is not None else {}
        )
```

`serialise()` gains `"metadata": serialise_to_json_value(self.metadata, fallback)`;
`__repr__` gains `metadata={repr(self.metadata)}`. `summarise()` is left
unchanged (no metadata).

#### 2. `StoredEvent`

**File**: `src/logicblocks/event/types/event.py`
**Changes**: Add `Metadata = JsonValue` generic and a **required** `metadata:
Metadata` field (no default — see Decisions; a `{}` default factory is
type-unsound for non-mapping `Metadata`). Include `metadata` in `serialise()`
and `__repr__`; leave `summarise()` unchanged. Because the field is required,
**every** `StoredEvent(...)` construction site must pass `metadata` — these are
enumerated in Phases 4–5 and the builder in Phase 2. (Construction sites:
`serialise_stored_event` here, both adapters, the `StoredEventBuilder.build`,
plus any in `store.py` / `projector.py` / adapter `db.py` surfaced by
`mise run types:check`.)

#### 3. `serialise_stored_event`

**File**: `src/logicblocks/event/types/event.py`
**Changes**: Carry `metadata=serialise_to_json_value(event.metadata, fallback)`
through, mirroring `payload`.

### Success Criteria

#### Automated Verification:

- [ ] New type unit tests pass: `mise run test:unit --test-args='-k "NewEvent or StoredEvent"'`
- [ ] All unit tests pass: `mise run test:unit`
- [ ] Type checking passes: `mise run types:check`
- [ ] Linting passes: `mise run lint:fix`
- [ ] Formatting passes: `mise run format:fix`

#### Manual Verification:

- [ ] `NewEvent(name=..., payload=...)` with no metadata reports `metadata == {}`
- [ ] `serialise()` / `repr()` of an event with metadata show it; `summarise()`
      does **not** contain metadata

---

## Phase 2: Builders

### Overview

Extend `NewEventBuilder` and `StoredEventBuilder` to mirror how the builders
already handle `payload` — **including a random populated default** (per reviewer
feedback: the builders generate a random `payload`, so metadata should be
random-populated too, not `{}`).

### Changes Required

#### 1. Random metadata data helper

**File**: `src/logicblocks/event/testing/data.py`
**Changes**: Add `random_event_metadata()` mirroring `random_event_payload()`
(line 73) / `random_projection_metadata()` (line 99) — a random mapping of
string keys to string values.

```python
def random_event_metadata() -> Mapping[str, Any]:
    return {
        random_lowercase_ascii_alphabetics_string(
            length=10
        ): random_ascii_alphanumerics_string(length=20)
        for _ in range(random_int(1, 10))
    }
```

#### 2. `NewEventBuilder` and `StoredEventBuilder`

**File**: `src/logicblocks/event/testing/builders.py`
**Changes**: Add `Metadata = JsonValue` generic to each builder and its
`*Params` TypedDict. Add a `metadata` field that defaults to
`random_event_metadata()` in `__init__` (mirroring how `payload` defaults to
`random_event_payload()` via `payload or random_event_payload()`), thread it
through `_clone`, add a `with_metadata(...)` method, and pass it to the `build()`
constructor. `StoredEventBuilder.from_new_event` propagates
`metadata=event.metadata`.

```python
def with_metadata(self, metadata: Metadata):
    return self._clone(metadata=metadata)
```

Do **not** touch the `ProjectionBuilder` family — it already has its own
metadata handling.

### Success Criteria

#### Automated Verification:

- [ ] Builder unit tests pass: `mise run test:unit --test-args='-k "Builder and Metadata"'`
- [ ] All unit tests pass: `mise run test:unit`
- [ ] Type checking passes: `mise run types:check`

#### Manual Verification:

- [ ] `NewEventBuilder().build().metadata` is a random non-empty mapping by
      default (parity with `payload`)
- [ ] `with_metadata({"actor": "user-123"})` sets it; `from_new_event`
      propagates it

---

## Phase 3: Canonical Schema

### Overview

Add the `metadata` column to the canonical events DDL so fresh installs are
correct.

### Changes Required

#### 1. Events table DDL

**File**: `sql/create_events_table.sql`
**Changes**: Add `metadata JSONB NOT NULL` (no DB default, matching
`projections`).

```sql
CREATE TABLE events (
    id TEXT NOT NULL,
    name TEXT NOT NULL,
    stream TEXT NOT NULL,
    category TEXT NOT NULL,
    position INT NOT NULL,
    payload JSONB NOT NULL,
    metadata JSONB NOT NULL,
    sequence_number BIGSERIAL NOT NULL,
    observed_at TIMESTAMP WITH TIME ZONE NOT NULL,
    occurred_at TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (id),
    UNIQUE (id)
);
```

### Success Criteria

#### Automated Verification:

- [ ] Integration test schema bootstrap succeeds (the harness that creates the
      `events` table from this DDL runs without error): `mise run test:integration`

#### Manual Verification:

- [ ] A fresh Postgres install created from this DDL has a non-nullable
      `metadata` column with no default

---

## Phase 4: Postgres Adapter

### Overview

Persist and return `metadata`. The read path needs no change beyond
`StoredEvent` having the field; the intermediate signatures stay at
`[Name, Payload]` (public-surface-only scope).

### Changes Required

#### 1. Insert query

**File**: `src/logicblocks/event/store/adapters/postgres/adapter.py`
**Changes**: In `insert_batch_query` (line 404): add `metadata` to the INSERT
column list (after `payload`), add a ninth `%s` to the row template, and bind
`Jsonb(serialise_to_json_value(event.metadata))` immediately after the existing
`payload` bind. The `values` element type already includes `Jsonb`.

#### 2. RETURNING reconstruction

**File**: `src/logicblocks/event/store/adapters/postgres/adapter.py`
**Changes**: In `insert_batch` (line 583), the `StoredEvent[Name, Payload]`
rebuilt from each original event carries `metadata=event.metadata` (from the
original event, same treatment as `payload=event.payload`).

#### 3. Generics — unchanged

The `StreamInsertDefinition`, `insert_batch_query`, `insert_batch`,
`_save_to_stream`, `_save_to_category`, and `save` signatures stay at
`[Name, Payload]`. Do **not** add a third generic here. `StoredEvent`'s
`Metadata` *type parameter* still defaults to `JsonValue`, so the
`StoredEvent[Name, Payload]` parameterisation remains valid; only the `metadata`
*constructor argument* is now required (passed in step 2).

### Success Criteria

#### Automated Verification:

- [ ] Integration tests pass: `mise run test:integration`
- [ ] Type checking passes: `mise run types:check`
- [ ] Linting passes: `mise run lint:fix`

#### Manual Verification:

- [ ] An event saved with `metadata={"actor": "user-123"}` reads back identically
- [ ] An event saved with no metadata reads back as `metadata={}`
- [ ] The stored `payload` column value is byte-identical to today

---

## Phase 5: Memory Adapter

### Overview

Store and return `metadata` as a plain field on both the caller-returned and
serialised stored events. `latest` / `scan` need no change — they return the
stored events (which now carry metadata) directly.

### Changes Required

#### 1. Construction sites

**File**: `src/logicblocks/event/store/adapters/memory/adapter.py`
**Changes**: In both `_save_to_stream` (line 201) and `_save_to_category` (line
282): the caller-facing `StoredEvent[Name, Payload]` carries
`metadata=new_event.metadata`; the serialised `StoredEvent[str, JsonValue]`
stored in the in-memory DB carries
`metadata=serialise_to_json_value(new_stored_event.metadata)`.

#### 2. Generics — unchanged

The `_determine_required_locks`, `save`, `_save_to_stream`, and
`_save_to_category` signatures stay at `[Name, Payload]`. Do **not** add a third
generic here (public-surface-only scope).

### Success Criteria

#### Automated Verification:

- [ ] Memory adapter shared tests pass: `mise run test:unit --test-args='-k "Memory"'`
- [ ] All unit tests pass: `mise run test:unit`
- [ ] Type checking passes: `mise run types:check`

#### Manual Verification:

- [ ] Memory adapter round-trips metadata identically to Postgres

---

## Phase 6: Changelog Fragment

### Overview

Document the addition and the required breaking migration.

### Changes Required

#### 1. Changelog fragment

**File**: `changelog.d/<generated>.md` (via `mise run changelog:fragment:create`)
**Changes**: Describe the new `metadata` field/column and state that a migration
is required for existing deployments, including the recommended SQL inline:

```sql
ALTER TABLE events ADD COLUMN metadata JSONB NOT NULL DEFAULT '{}';
ALTER TABLE events ALTER COLUMN metadata DROP DEFAULT;
```

The transient `DEFAULT '{}'` backfills existing rows during the add, then is
dropped so the column's final state matches the no-default canonical DDL.

**Important:** commit only the new fragment under `changelog.d/`. Do **not** run
`changelog:assemble` and commit the resulting `CHANGELOG.md` — assembly is the
human's/CI's job (per reviewer feedback).

### Success Criteria

#### Automated Verification:

- [ ] Fragment file exists under `changelog.d/` with the expected
      `YYYYMMDD_HHMMSS_<author>_<slug>.md` naming (matching sibling fragments)

#### Manual Verification:

- [ ] Fragment clearly states the migration is required and is a breaking change
- [ ] No assembled `CHANGELOG.md` change is staged for commit

---

## Phase 7: Shared Adapter Round-Trip Coverage

### Overview

Prove metadata round-trips identically in both memory and Postgres. Per reviewer
feedback, **extend the existing full-field equality tests** rather than adding
metadata-specific ones. Because `NewEventBuilder` now produces random metadata by
default (Phase 2), the existing tests' expected `StoredEvent` objects must gain a
`metadata=new_event.metadata` field, and they then assert metadata round-trips
for free.

### Changes Required

#### 1. Extend existing shared cases

**File**: `tests/shared/logicblocks/event/testcases/store/adapters.py`
**Changes**: In `test_stores_single_event_for_later_retrieval` (line 77) and the
sibling full-field equality tests (e.g. `test_stores_multiple_events_in_same_stream`),
add `metadata=new_event.metadata` to each expected `StoredEvent(...)`. Since the
builder default is now a random populated map, this asserts metadata is persisted
and retrieved unchanged — no separate metadata test needed.

#### 2. Returned-event assertion

**File**: `tests/shared/logicblocks/event/testcases/store/adapters.py`
**Changes**: Where a test asserts on the `save()` *return value* (the
caller-facing `StoredEvent`), assert **all** fields including `metadata`
(mirroring the full-field style of the retrieval test) rather than asserting on
`metadata` alone. Per reviewer feedback, a returned-event test should validate
the whole event, not be metadata-specific.

#### 3. No empty-default-on-retrieval test

Do **not** add a test asserting retrieval defaults metadata to `{}` — the
reviewer rejected that as not-the-intended-behaviour. The `{}` default belongs to
`NewEvent` construction (Phase 1), and is covered by unit tests there; the
adapter layer simply round-trips whatever was provided.

### Success Criteria

#### Automated Verification:

- [ ] Extended shared cases pass against memory: `mise run test:unit`
- [ ] Extended shared cases pass against Postgres: `mise run test:integration`
- [ ] Component tests pass: `mise run test:component`
- [ ] Full build is green: `mise run`

#### Manual Verification:

- [ ] Both adapters produce byte-identical metadata round-trips

---

## Testing Strategy

Following the spec's TDD ordering (one failing test at a time, red-green-refactor):

### Unit Tests

- `NewEvent` defaults `metadata` to `{}` when omitted; `StoredEvent` requires it.
- `serialise()` and `__repr__` include metadata; `summarise()` does **not**.
- `serialise_stored_event()` carries metadata through.
- Builders: `with_metadata` sets it; default is a random populated map (parity
  with `payload`); `from_new_event` propagates it.

### Integration / Shared (memory + Postgres)

- The existing full-field equality tests in the shared adapter cases are
  extended to include `metadata` (random by builder default), so both adapters
  assert metadata is persisted and retrieved unchanged. No metadata-specific
  adapter test and no empty-default-on-retrieval test (per reviewer feedback).

No guard tests and no legacy-row tests — a dedicated column removes both
concerns.

### Manual Testing Steps

1. Save an event with `metadata={"actor": "user-123"}` via the Postgres adapter;
   confirm retrieval returns it unchanged and `payload` is byte-identical to
   today.
2. Save an event built with `NewEventBuilder()` (random metadata) and confirm it
   round-trips unchanged.
3. Repeat (1)–(2) against the memory adapter and confirm parity.

## Performance Considerations

Negligible. One additional JSONB column per row, always small in the primary use
case (a handful of traceability keys). No new indexes. No query-path changes.

## Migration Notes

This is a **breaking schema change** for existing deployments, shipped exactly as
the `events_category_stream_sequence_number_index` addition was: canonical DDL
updated for fresh installs, plus a changelog fragment announcing the migration
with the recommended SQL inline (add column with transient `DEFAULT '{}'` to
backfill, then drop the default). No migration framework is introduced.

## References

- Design spec: `meta/specs/2026-06-11-event-metadata-design.md`
- Prior-plan review feedback incorporated here: PR #116
  (https://github.com/logicblocks/event.store/pull/116) — reviewer `tobyclemson`.
  Key points folded in: `mise run` task forms (not `mise exec`/`DB_PORT`),
  exclude metadata from `summarise()`, no default on `StoredEvent.metadata`,
  random builder metadata, extend existing full-field adapter tests, commit only
  the changelog fragment.
- Builder random-data helpers to mirror: `random_event_payload()`
  (`src/logicblocks/event/testing/data.py:73`),
  `random_projection_metadata()` (`...data.py:99`)
- Existing full-field adapter test to extend:
  `tests/shared/logicblocks/event/testcases/store/adapters.py:77`
  (`test_stores_single_event_for_later_retrieval`)
- Pattern to mirror — `Projection[State, Metadata]`:
  `src/logicblocks/event/types/projection.py:18`
- Projection metadata column: `sql/create_projections_table.sql`
- Migration precedent:
  `changelog.d/20260126_110222_daviemoston_add_category_stream_sequence_number_index.md`
- Postgres insert query: `src/logicblocks/event/store/adapters/postgres/adapter.py:404`
- Postgres RETURNING reconstruction: `src/logicblocks/event/store/adapters/postgres/adapter.py:583`
- Memory adapter construction sites: `src/logicblocks/event/store/adapters/memory/adapter.py:201,282`
- Builders: `src/logicblocks/event/testing/builders.py:42,110,249`
