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
  scope. If `type:check` flags the third generic anywhere it is reactively
  required, add it there only — do not pre-emptively thread it everywhere.
- **Verification via `mise run`.** Per project `CLAUDE.md`, all automated
  criteria use `mise run ...` targets, not `make`.

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

Add the `Metadata` generic, field, default, and serialisation to `NewEvent` and
`StoredEvent`, and carry it through `serialise_stored_event`. This is the
foundation; all later phases depend on it.

### Changes Required

#### 1. `NewEvent`

**File**: `src/logicblocks/event/types/event.py`
**Changes**: Add `Metadata = JsonValue` generic and `metadata: Metadata` field.
Default `metadata` to `{}` in `__init__` (mirroring how `observed_at` defaults).
Include `metadata` in `serialise()`, `summarise()`, and `__repr__`.

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
`summarise()` gains `"metadata": self.metadata`; `__repr__` gains
`metadata={repr(self.metadata)}`.

#### 2. `StoredEvent`

**File**: `src/logicblocks/event/types/event.py`
**Changes**: Add `Metadata = JsonValue` generic and `metadata: Metadata` field.
Include `metadata` in `serialise()`, `summarise()`, and `__repr__`. Per the
spec, add the field **last** so existing keyword construction keeps working.

#### 3. `serialise_stored_event`

**File**: `src/logicblocks/event/types/event.py`
**Changes**: Carry `metadata=serialise_to_json_value(event.metadata, fallback)`
through, mirroring `payload`.

### Success Criteria

#### Automated Verification:

- [ ] New type unit tests pass: `mise run test:unit[NewEventTestCase]` and the
      `StoredEvent` test class
- [ ] All unit tests pass: `mise run test:unit`
- [ ] Type checking passes: `mise run type:check`
- [ ] Linting passes: `mise run lint:fix`
- [ ] Formatting passes: `mise run format:fix`

#### Manual Verification:

- [ ] `NewEvent(name=..., payload=...)` with no metadata reports `metadata == {}`
- [ ] `serialise()` / `summarise()` / `repr()` of an event with metadata show it

---

## Phase 2: Builders

### Overview

Extend `NewEventBuilder` and `StoredEventBuilder` to mirror how
`BaseProjectionBuilder` handles metadata.

### Changes Required

#### 1. `NewEventBuilder` and `StoredEventBuilder`

**File**: `src/logicblocks/event/testing/builders.py`
**Changes**: Add `Metadata = JsonValue` generic to each builder and its
`*Params` TypedDict. Add a `metadata` field defaulting to `{}` in `__init__`,
thread it through `_clone`, add a `with_metadata(...)` method, and pass it to the
`build()` constructor. `StoredEventBuilder.from_new_event` propagates
`metadata=event.metadata`.

```python
def with_metadata(self, metadata: Metadata):
    return self._clone(metadata=metadata)
```

### Success Criteria

#### Automated Verification:

- [ ] Builder unit tests pass: `mise run test:unit[NewEventBuilderTestCase]` and
      the `StoredEventBuilder` test class (names per existing
      `tests/unit/logicblocks/event/testing/test_builders.py`)
- [ ] All unit tests pass: `mise run test:unit`
- [ ] Type checking passes: `mise run type:check`

#### Manual Verification:

- [ ] `NewEventBuilder().build().metadata == {}` by default
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
`[Name, Payload]`. Do **not** add a third generic here. `StoredEvent` defaults
its `Metadata` parameter, so `StoredEvent[Name, Payload]` remains valid and
metadata still flows through as a field value.

### Success Criteria

#### Automated Verification:

- [ ] Integration tests pass: `mise run test:integration`
- [ ] Type checking passes: `mise run type:check`
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

- [ ] Memory adapter unit tests pass: `mise run test:unit[InMemoryEventStorageAdapter...]`
      (the existing memory adapter test classes)
- [ ] All unit tests pass: `mise run test:unit`
- [ ] Type checking passes: `mise run type:check`

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

### Success Criteria

#### Automated Verification:

- [ ] Changelog assembles cleanly: `mise run changelog:assemble` (or the project's
      changelog lint step within `mise run`)

#### Manual Verification:

- [ ] Fragment clearly states the migration is required and is a breaking change

---

## Phase 7: Shared Adapter Round-Trip Tests

### Overview

Prove, via the shared adapter test cases, that metadata round-trips identically
in both memory and Postgres. (Written test-first per phase in TDD; this phase
ensures the cross-adapter equality coverage the spec calls for exists.)

### Changes Required

#### 1. Shared adapter cases

**File**: `tests/shared/logicblocks/event/testcases/store/adapters.py`
**Changes**: Add cases that (a) save an event with
`metadata={"actor": "user-123"}` and assert the retrieved `StoredEvent` equals a
fully-built expected `StoredEvent` (including metadata), and (b) save an event
with no metadata and assert it round-trips as `metadata={}`. Use
`NewEventBuilder().with_metadata(...)` and build the complete expected
`StoredEvent` for equality assertion.

### Success Criteria

#### Automated Verification:

- [ ] New shared cases pass against memory: `mise run test:unit`
- [ ] New shared cases pass against Postgres: `mise run test:integration`
- [ ] Component tests pass: `mise run test:component`
- [ ] Full build is green: `mise run`

#### Manual Verification:

- [ ] Both adapters produce byte-identical metadata round-trips

---

## Testing Strategy

Following the spec's TDD ordering (one failing test at a time, red-green-refactor):

### Unit Tests

- `NewEvent` / `StoredEvent` default `metadata` to `{}` when omitted.
- `serialise()` / `summarise()` / `__repr__` include metadata.
- `serialise_stored_event()` carries metadata through.
- Builders: `with_metadata` sets it; default is `{}`; `from_new_event`
  propagates it.

### Integration Tests

- Postgres adapter: event with metadata round-trips through save and retrieval;
  default `{}` round-trips. Built as full-equality assertions against an expected
  `StoredEvent`.

### Shared (memory + Postgres)

- The above round-trip cases live in the shared adapter test cases so both
  adapters receive identical coverage.

No guard tests and no legacy-row tests — a dedicated column removes both
concerns.

### Manual Testing Steps

1. Save an event with `metadata={"actor": "user-123"}` via the Postgres adapter;
   confirm retrieval returns it unchanged and `payload` is byte-identical to
   today.
2. Save an event with no metadata; confirm it reads back as `metadata={}`.
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
- Pattern to mirror — `Projection[State, Metadata]`:
  `src/logicblocks/event/types/projection.py:18`
- Projection metadata column: `sql/create_projections_table.sql`
- Migration precedent:
  `changelog.d/20260126_110222_daviemoston_add_category_stream_sequence_number_index.md`
- Postgres insert query: `src/logicblocks/event/store/adapters/postgres/adapter.py:404`
- Postgres RETURNING reconstruction: `src/logicblocks/event/store/adapters/postgres/adapter.py:583`
- Memory adapter construction sites: `src/logicblocks/event/store/adapters/memory/adapter.py:201,282`
- Builders: `src/logicblocks/event/testing/builders.py:42,110,249`
