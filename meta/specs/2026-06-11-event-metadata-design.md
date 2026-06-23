# Event Metadata — Design

**Date:** 2026-06-11 (revised 2026-06-12)
**Status:** Approved (pending implementation)

## Problem

Events in the store currently carry only `name`, `payload`, `observed_at`,
and `occurred_at`. There is no place to record cross-cutting context about an
event — most pressingly, *who* the actor was that caused it (e.g. which user
assigned a role). Downstream services (role service and similar) need this for
traceability: who did what.

The store is a general-purpose library. Not every consumer needs actor
tracking, so the mechanism must be optional and must not force a domain notion
of "actor" onto the library.

## Goals

- **Optional.** Consumers who don't supply metadata see no API change; metadata
  defaults to an empty mapping.
- **Domain-agnostic.** The library provides a generic metadata *bag*, not an
  `actor` type. "Actor" is a convention a consumer places inside metadata.
- **Consistent with existing patterns.** Persist metadata in a dedicated
  `metadata JSONB NOT NULL` column on the `events` table, exactly mirroring the
  existing `projections.metadata` column. Mirror the `Projection[State,
  Metadata]` generic on the event types. Introduce no new conventions.
- **Clean separation.** The `payload` column stays pristine — metadata lives in
  its own column, is independently queryable, and never collides with payload
  contents.

## Non-Goals

- No first-class `actor`, `causation`, or `correlation` types in the library.
- No querying/filtering on metadata contents in this change (the column makes
  it possible later, but no query support is added now).

## Revision Note

An earlier revision of this design folded metadata into the existing `payload`
JSONB under a reserved `__metadata` key, to avoid a schema migration. That
approach has been **superseded**. The requirement changed: metadata must live
in its own column, and a breaking schema change is acceptable. The dedicated
column is simpler (no reserved-key collisions, no payload-shape detection, no
custom row factory, no write guards) and keeps `payload` untouched. This design
is implemented **clean from `main`** — any uncommitted fold-based work is
discarded, not adapted.

## Storage Model

Metadata is stored in a dedicated column on the `events` table:

```sql
metadata JSONB NOT NULL
```

This matches the existing `projections.metadata` column (`sql/
create_projections_table.sql`): every JSONB column in the schema is `NOT NULL`
with no DB-level default — the application always supplies a value. Since
`NewEvent.metadata` defaults to an empty mapping, every insert naturally
provides at least `{}`; the database never needs a default.

| Case | Stored `payload` | Stored `metadata` | Read result |
|---|---|---|---|
| No metadata (default) | `{prop1, prop2}` | `{}` | `payload={prop1, prop2}`, `metadata={}` |
| Metadata present | `{prop1, prop2}` | `{"actor": "user-123"}` | `payload={prop1, prop2}`, `metadata={"actor": "user-123"}` |

The `payload` column is byte-identical to today in all cases. There is no
fold/split, no reserved key, and no read-path ambiguity. Any payload shape
(object, array, or scalar) coexists with metadata, so no write guards are
needed.

## Migration

This is a **breaking schema change** for existing deployments. It is shipped
the same way the `events_category_stream_sequence_number_index` addition was
handled:

1. **Canonical DDL updated** — `sql/create_events_table.sql` gains the
   `metadata JSONB NOT NULL` column, so fresh installs are correct.
2. **Changelog fragment** states that a migration is required and includes the
   recommended SQL inline:

   ```sql
   ALTER TABLE events ADD COLUMN metadata JSONB NOT NULL DEFAULT '{}';
   ALTER TABLE events ALTER COLUMN metadata DROP DEFAULT;
   ```

   The transient `DEFAULT '{}'` backfills existing rows during the add, then is
   dropped so the column's final state matches the no-default canonical DDL
   (identical to a fresh install).

No migration framework is introduced; the project ships raw `sql/` DDL and
announces migrations via the changelog.

## API Surface

### Types (`src/logicblocks/event/types/event.py`)

`NewEvent` gains a third generic parameter and an optional constructor kwarg,
following the existing `Projection[State, Metadata]` precedent:

```python
@dataclass(frozen=True)
class NewEvent[Name = str, Payload = JsonValue, Metadata = JsonValue]:
    name: Name
    payload: Payload
    metadata: Metadata          # defaults to {} when omitted
    observed_at: datetime
    occurred_at: datetime

    def __init__(self, *, name, payload, metadata=None,
                 observed_at=None, occurred_at=None, clock=SystemClock()):
        ...
        object.__setattr__(
            self, "metadata", metadata if metadata is not None else {}
        )
```

`StoredEvent` likewise gains `metadata: Metadata` as a generic parameter and a
field. Because `StoredEvent` uses a dataclass-generated `__init__` and is
constructed positionally/by-keyword in adapters, the `metadata` field is added
**last** with a default factory so existing construction sites keep working.

### Serialisation

- `serialise()` includes `"metadata"` (full round-trip).
- `summarise()` includes `metadata` (small, and central to the traceability
  use case).
- `serialise_stored_event()` carries `metadata` through.

### Builders (`src/logicblocks/event/testing/builders.py`)

`NewEventBuilder` and `StoredEventBuilder` gain a `Metadata` generic, a
`with_metadata(...)` method, and a default of `{}`, mirroring how `payload` is
handled.

### Publish API

No signature change. `EventStream.publish(events=[...])` and
`EventCategory.publish(...)` already accept `NewEvent` objects, so metadata
rides along on the event:

```python
NewEvent(name="role.assigned", payload={...}, metadata={"actor": "user-123"})
```

The generic propagates: `publish[Name, Payload]` becomes
`publish[Name, Payload, Metadata]`, returning `StoredEvent[Name, Payload, Metadata]`.

## Adapter Changes

Metadata is a plain field persisted to / read from its own column. No shared
fold/split helpers, no custom row factory, no guards.

### Postgres (`src/logicblocks/event/store/adapters/postgres/adapter.py`)

- **Write** — add `metadata` to the INSERT column list and bind
  `Jsonb(serialise_to_json_value(event.metadata))` alongside the existing
  `payload` value.
- **Read** — `metadata` is a real column, so the existing
  `class_row(StoredEvent[str, JsonValue])` row factory maps it automatically
  once `StoredEvent` has the field. No custom row factory is needed.
- **RETURNING mapping** — the loop that builds caller-facing
  `StoredEvent[Name, Payload]` from the original events carries
  `metadata=event.metadata` through (same as it carries `payload`).

### Memory (`src/logicblocks/event/store/adapters/memory/adapter.py`)

- Store and return `metadata` as a plain field on both the caller-returned
  `StoredEvent` and the serialised stored event. No folding, no split helper.

## Testing (TDD)

Red-green-refactor, one test at a time, extending the shared adapter test cases
so memory and Postgres receive identical coverage. Build complete expected
results and assert equality (the codebase's preferred style).

1. `NewEvent` / `StoredEvent` default `metadata` to `{}` when omitted.
2. `serialise()` / `summarise()` / `__repr__` include metadata.
3. `serialise_stored_event()` carries metadata through.
4. Builders: `with_metadata` sets it; default is `{}`;
   `from_new_event` propagates it.
5. Shared adapter (memory + Postgres): event with metadata round-trips through
   save and retrieval; default `{}` round-trips. Built as full-equality
   assertions against an expected `StoredEvent`.
6. Generic typing propagates through `publish` → `StoredEvent`.

No guard tests and no legacy-row tests — a dedicated column removes both
concerns.

A changelog fragment is added (`mise run changelog:fragment:create`) describing
the addition and the required migration.
