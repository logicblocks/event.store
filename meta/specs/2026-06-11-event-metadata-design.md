# Event Metadata — Design

**Date:** 2026-06-11
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

- **Optional.** Consumers who don't use metadata see no behavioural change.
- **Non-breaking.** Existing SDK call sites compile and behave identically.
  Existing database rows continue to read correctly. The stored `payload`
  JSONB shape is unchanged for any event that carries no metadata.
- **Domain-agnostic.** The library provides a generic metadata *bag*, not an
  `actor` type. "Actor" is a convention a consumer places inside metadata.
- **Consistent with existing patterns.** Mirror the existing
  `Projection[State, Metadata]` generic, the frozen-dataclass `__init__` style
  using `object.__setattr__`, and the existing `serialise`/`summarise`
  structure. Introduce no new conventions.

## Non-Goals

- No first-class `actor`, `causation`, or `correlation` types in the library.
- No new database column and no data migration.
- No querying/filtering on metadata contents (out of scope for this change).

## Storage Model

Metadata is folded into the existing `payload` JSONB column as a reserved
sibling key `__metadata`. There is **no schema migration and no new column**.

| Case | Stored JSONB | Read result |
|---|---|---|
| No metadata (default) | `{prop1, prop2}` *(unchanged from today)* | `payload={prop1, prop2}`, `metadata={}` |
| Metadata present | `{"__metadata": {...}, prop1, prop2}` | `payload={prop1, prop2}`, `metadata={...}` |
| Legacy row (pre-feature) | `{prop1, prop2}` | `payload={prop1, prop2}`, `metadata={}` |

### Write rule

- If metadata is empty/omitted → store the payload object verbatim (identical
  to today's behaviour — zero change, zero risk).
- If metadata is present → store `{**payload, "__metadata": metadata}`.

### Read rule (detect-on-read)

- If the stored object contains a `__metadata` key → pop it; that value is
  `metadata`, the remaining object is `payload`.
- Otherwise → `metadata = {}` and the whole stored object is `payload`.

New no-metadata rows and legacy pre-feature rows are byte-identical, so a
single read path covers both. No migration is required.

### Write guards (raise on `publish`)

Both guards trigger **only** when metadata is supplied; a no-metadata publish
of any payload shape is never rejected.

1. **Non-object payload + metadata supplied → error.** Sibling-key injection
   requires an object payload; JSON arrays/scalars cannot host a sibling key.
2. **Payload already contains `__metadata` + metadata supplied → error.** The
   `__metadata` key is reserved.

Guards raise using the library's existing exception style.

### Accepted risk

A legacy raw payload that itself happens to contain a top-level `__metadata`
key would be misread as metadata. This is considered vanishingly unlikely and
is accepted rather than guarded on the read path.

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

`StoredEvent` likewise gains `metadata: Metadata` as a generic parameter and
field.

### Serialisation

- `serialise()` includes `"metadata"` (full round-trip).
- `summarise()` includes `metadata` (small, and central to the traceability
  use case).
- `serialise_stored_event()` carries `metadata` through.

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

The fold-in / strip-out logic lives in the adapters so both backends behave
identically and the type layer stays pure. The wrap/unwrap + guard functions
are pure helpers placed alongside the existing serialisation helpers (in the
types layer) so both adapters call the same code (DRY).

### Postgres
(`src/logicblocks/event/store/adapters/postgres/adapter.py`, `converters.py`)

- **Write** — at the insert site (~line 426), the value placed into the
  `payload` JSONB is computed by the shared helper: payload as-is when metadata
  is empty, else `{**payload, "__metadata": metadata}` after applying the two
  guards.
- **Read** — wherever rows become `StoredEvent` (the `RETURNING *` mapping and
  the scan/latest converters), the shared helper splits the stored object into
  `(payload, metadata)` via the detect-on-read rule.

### Memory
(`src/logicblocks/event/store/adapters/memory/`)

- The same two helpers are applied on the save and scan paths so the in-memory
  and Postgres adapters are behaviourally identical.

## Testing (TDD)

Red-green-refactor, one test at a time, extending the shared adapter test cases
so memory and Postgres receive identical coverage. Build complete expected
results and assert equality (the codebase's preferred style).

1. Event with no metadata → stored raw, read back `metadata={}`
   (byte-identical to today).
2. Event with metadata → stored with `__metadata` sibling, read back split
   correctly.
3. Round-trip equality against a fully-built expected `StoredEvent`.
4. Legacy raw row (inserted without `__metadata`) → reads back `metadata={}`.
5. Guard: metadata + non-object payload → raises.
6. Guard: payload already contains `__metadata` → raises.
7. `serialise()` / `summarise()` include metadata.
8. Generic typing propagates through `publish` → `StoredEvent`.

A changelog fragment is added (`mise run changelog:fragment:create`) since this
is a user-facing addition.
