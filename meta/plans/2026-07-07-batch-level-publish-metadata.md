---
type: plan
id: "2026-07-07-batch-level-publish-metadata"
title: "Batch-Level Publish Metadata Implementation Plan"
date: "2026-07-07T13:45:01+00:00"
author: "John Cowie Del Corral"
producer: create-plan
status: draft
work_item_id: ""
parent: ""
reviewer: ""
tags: ["metadata", "events", "publish"]
revision: "e5d599c59aed0f151a0d999c237c398d2eb4b63e"
repository: "event.store"
last_updated: "2026-07-07T13:45:01+00:00"
last_updated_by: "John Cowie Del Corral"
schema_version: 1
---

# Batch-Level Publish Metadata Implementation Plan

## Overview

Allow callers to supply a single `metadata` mapping **alongside** `events` when
publishing, and have it applied to every event in the batch. The per-event
`metadata` field (shipped in PR #116) stays authoritative: batch metadata is a
**default** that fills only the events whose `metadata is None`. Events that
carry their own metadata are left untouched.

Resolution is a **pure transformation at the store edge** (`EventStream.publish`
/ `EventCategory.publish`) that rewrites each `NewEvent`'s `metadata` before the
existing `save()` call. There are **no adapter, storage-port, schema, or
migration changes** — the resolved value flows into the existing per-event
`metadata` JSONB column exactly as any per-event metadata does today.

## Current State Analysis

- Events carry a required `metadata` field on `NewEvent` / `StoredEvent`, with a
  `Metadata` generic already threaded end-to-end through `publish` → `save` →
  both adapters (`src/logicblocks/event/types/event.py:18,84`; PR #116, plan
  `meta/plans/2026-06-24-event-metadata.md`).
- `EventStream.publish(*, events, condition)`
  (`src/logicblocks/event/store/store.py:56`) delegates verbatim to
  `adapter.save(target, events, condition)`. There is no batch-level metadata
  parameter.
- `EventCategory.publish(*, streams)`
  (`src/logicblocks/event/store/store.py:205`) takes a
  `Mapping[str, StreamPublishDefinition[Name, Payload, Metadata]]` and delegates
  verbatim to `adapter.save(target, streams)`.
- `StreamPublishDefinition` is a `TypedDict` with `events` and optional
  `condition` (`src/logicblocks/event/store/types.py:14`); the
  `stream_publish_definition(...)` helper builds it
  (`src/logicblocks/event/store/types.py:23`).
- "No metadata" is represented as `None` (stored as JSON `null`), established as
  a load-bearing convention by PR #116 — callers pass `metadata=None`
  explicitly. This is exactly the sentinel the batch-fill logic keys off.
- The only non-test internal `.publish(...)` call site is
  `src/logicblocks/event/processing/consumers/state/base.py:166`; it does not
  pass metadata and is unaffected (the new parameter defaults to `None`).

### Key Discoveries

- **The `Metadata` generic is already everywhere.** No signature needs a new
  type parameter — `publish`, `save`, `StreamPublishDefinition`, and both
  adapters already carry `Metadata` (`store.py:59,208`; `types.py:14`;
  `adapters/base.py:85`). Adding the feature is additive, not a re-threading.
- **`None` is the fill sentinel.** Per-event metadata absence is already
  canonicalised to `None` (`types/event.py:23`, README:69). "Fill None only" maps
  directly onto this: `event.metadata if event.metadata is not None else batch`.
- **Adapters need no changes.** Because resolution happens before `save`, the
  adapters receive fully-formed `NewEvent`s and persist their `metadata` as they
  already do (`memory/adapter.py:216`; `postgres/adapter.py:404,583`).
- **`NewEvent` is frozen; rebuild, don't mutate.** `NewEvent` is a frozen
  dataclass (`types/event.py:17`). The resolver must construct a **new**
  `NewEvent` preserving all other fields — critically `observed_at` and
  `occurred_at`, which `__init__` would otherwise re-derive from the clock. Pass
  them through explicitly.
- **Builders already support metadata.** `NewEventBuilder.with_metadata(...)`
  and a random-metadata default already exist
  (`testing/builders.py:97`) — no builder changes are needed; tests can construct
  `metadata=None` events directly.
- **Existing tests pin the delegation shape.** `test_store.py` already has
  `test_publishes_events_with_metadata_...` for both stream (line 625) and
  category (line 1113); new tests sit beside them.

## Desired End State

```python
# Stream: metadata sibling to events, applied to the whole batch
await stream.publish(
    events=[
        NewEvent(name="a", payload={...}, metadata=None),          # -> gets batch
        NewEvent(name="b", payload={...}, metadata={"actor": "x"}), # -> keeps own
    ],
    metadata={"actor": "svc", "tenant": "acme"},
)
# a.metadata == {"actor": "svc", "tenant": "acme"}
# b.metadata == {"actor": "x"}

# Category: metadata sibling to events, per stream definition
await category.publish(streams={
    "s1": stream_publish_definition(events=[...], metadata={"tenant": "acme"}),
})
```

- `EventStream.publish` accepts an optional `metadata` keyword; when supplied,
  every event whose `metadata is None` is published with the batch metadata,
  others unchanged. Omitting it preserves today's behaviour exactly.
- `StreamPublishDefinition` accepts an optional `metadata` key, and
  `stream_publish_definition(...)` accepts a `metadata` argument;
  `EventCategory.publish` applies it per stream.
- No schema change, no migration, no adapter change. The returned
  `StoredEvent`s carry the resolved metadata.

**Verification:** `mise run` (full build) is green; new unit tests prove the
fills-None-only semantics for stream and category publish, and existing tests
remain green (backwards compatible).

## Decisions (resolved before planning)

- **Merge rule: fill `None` only.** For each event,
  `event.metadata if event.metadata is not None else batch_metadata`. No shallow
  merge of keys; an event either keeps its own metadata wholesale or takes the
  batch metadata wholesale. (Chosen by the user over merge/exclusive variants.)
- **Scope: stream and category.** Both `EventStream.publish` and
  `StreamPublishDefinition` (hence `EventCategory.publish`) get the option, for a
  consistent sibling-to-`events` surface.
- **Storage: existing per-event column.** Resolution happens at publish time and
  writes into the existing `metadata` column. No new column, no DDL, no
  migration.
- **Batch metadata default is `None` (feature off).** `publish(...)` with no
  `metadata` argument, and a `StreamPublishDefinition` with no `metadata` key,
  behave exactly as today — the resolver is a no-op when batch metadata is
  `None`. This keeps the change fully backwards compatible.
- **`None` batch + `None` event stays `None`.** If neither is supplied, the
  event's metadata remains `None` (stored as JSON `null`) — unchanged from today.
- **`None` is the only "off" value; `{}` is a real value.** The feature is off
  only when batch metadata `is None`. An empty mapping `metadata={}` is a
  distinct, fillable value that overwrites `None` events with `{}`. Every guard
  — the `stream_publish_definition` helper, the resolver's fast path, and the
  category `.get("metadata")` read — MUST use `is not None`, **never** truthiness
  (`if metadata:`), or `{}` would be silently dropped. This mirrors PR #116's
  per-event convention where only `None` is special.
- **Null-collision is accepted (not supported).** Mixing a deliberately-null
  event with batch-filled siblings in a single call is not possible; see "What
  We're NOT Doing". Confirmed acceptable by the user.
- **Filled events share the batch reference (accepted risk).** All events filled
  in one call receive the *same* `batch_metadata` object. Under the memory
  adapter (which keeps Python references rather than round-tripping through
  JSONB), mutating one filled event's metadata would bleed into the others. It is
  **not** defensively copied: `Metadata` is generic (a `dict(...)` copy is
  type-unsound when it is e.g. a `list` or a frozen value type), the events are
  frozen dataclasses, and `payload` already has this exact aliasing property
  today without issue. Accepted risk.
- **Pure resolver, injected nothing.** The resolver is a pure function over
  `(events, batch_metadata)`; no I/O, no clock, no global state. It lives beside
  the store types and is unit-tested in isolation (test pyramid: behaviour
  proven at the unit level, not via adapters).
- **Verification via `mise run`.** Per project `CLAUDE.md`, automated criteria
  use `mise run ...` targets. Targeted unit runs use
  `mise run test:unit --test-args='-k <expr>'`.

## What We're NOT Doing

- **No key-level merging.** We do not shallow-merge batch and per-event keys; the
  rule is whole-value fill-if-`None`. (If per-key merge is wanted later, it is a
  separate change to the same resolver.)
- **No adapter, port, or `save()` signature changes.** Resolution is entirely in
  the store layer.
- **No schema change and no migration.** Batch metadata reuses the existing
  `metadata` column.
- **No new `Metadata` type parameter anywhere** — the generic is already
  threaded through every relevant signature.
- **No change to the "required per-event metadata" rule** on `NewEvent` /
  `StoredEvent`. Callers still pass `metadata=None` on events that have none;
  batch metadata then fills them.
- **No mutation of `NewEvent`.** The resolver returns new frozen instances.
- **No support for mixing deliberately-null and batch-filled events in one
  call.** Because `metadata is None` is the fill sentinel, supplying batch
  metadata means **every** metadata-less event in that batch takes it — there is
  no way to keep an event's metadata as `None` *and* apply batch metadata to its
  siblings in the same `publish(...)`. This is the natural reading of "batch
  default" and is an accepted limitation (see Decisions). Callers who need a
  null-metadata event alongside batch-filled ones must publish them separately or
  set that event's metadata explicitly.
- **No category-wide metadata layer.** Metadata is a sibling to the `events` it
  governs: a top-level argument on `EventStream.publish`, and a per-definition
  key on `EventCategory.publish`. There is deliberately **no** single
  `category.publish(streams=..., metadata=...)` that fills across all streams;
  callers repeat per-definition metadata if they want it on multiple streams. A
  category-wide layer would introduce a three-level precedence chain
  (category → definition → event) for marginal benefit and can be an additive
  follow-up if ever needed.
- Do **not** commit the assembled `CHANGELOG.md`; only the fragment under
  `changelog.d/`.

## Implementation Approach

Strict TDD (red-green-refactor), one test at a time. Build inner-out: the pure
resolver first (the whole feature's logic lives here and is cheapest to test),
then the `StreamPublishDefinition` surface, then wire it into `EventStream.publish`
and `EventCategory.publish`, then docs and changelog. Because the logic is a pure
function, the store-method phases are thin wiring with a couple of behaviour
tests each; the edge cases are exhausted at the resolver unit level.

---

## Phase 1: Pure Batch-Metadata Resolver

### Overview

A pure function that, given a sequence of `NewEvent`s and an optional batch
metadata value, returns a new sequence where events with `metadata is None`
receive the batch metadata and all other fields (including `observed_at` /
`occurred_at`) are preserved.

### Changes Required

#### 1. Resolver function

**File**: `src/logicblocks/event/store/types.py` (co-located with
`StreamPublishDefinition`, which is where the store-level publish shapes live)
**Changes**: Add a pure helper. When `batch_metadata is None` it returns the
events unchanged (fast path / no-op). Otherwise it rebuilds each event whose
`metadata is None`.

```python
def resolve_batch_metadata[
    Name: StringPersistable,
    Payload: JsonPersistable,
    Metadata: JsonPersistable,
](
    events: Sequence[NewEvent[Name, Payload, Metadata]],
    batch_metadata: Metadata | None,
) -> Sequence[NewEvent[Name, Payload, Metadata]]:
    if batch_metadata is None:
        return events
    return [
        event
        if event.metadata is not None
        else NewEvent[Name, Payload, Metadata](
            name=event.name,
            payload=event.payload,
            metadata=batch_metadata,
            observed_at=event.observed_at,
            occurred_at=event.occurred_at,
        )
        for event in events
    ]
```

> **Why pass `observed_at` / `occurred_at` explicitly:** `NewEvent.__init__`
> defaults these from the clock when omitted (`types/event.py:37-40`); rebuilding
> without them would change the timestamps. This is the one non-obvious gotcha in
> the phase.

### Success Criteria

#### Automated Verification:

- [x] Resolver unit tests pass: `mise run test:unit --test-args='-k ResolveBatchMetadata'`
- [x] All unit tests pass: `mise run test:unit`
- [x] Type checking passes: `mise run types:check`
- [x] Linting passes: `mise run lint:fix`
- [x] Formatting passes: `mise run format:fix`

#### Manual Verification:

- [ ] N/A (pure function; covered by unit tests)

### Tests (write first, one at a time)

**File**: `tests/unit/logicblocks/event/store/test_types.py`

1. `batch_metadata=None` returns the events unchanged (identity / no rebuild).
2. Event with `metadata=None` and a batch value → returned event has
   `metadata == batch`, and `name`/`payload`/`observed_at`/`occurred_at`
   unchanged (assert full-object equality against an expected `NewEvent`).
3. Event with its own `metadata` and a batch value → returned event is unchanged
   (keeps its own metadata).
4. Mixed batch: one `None` event and one metadata-carrying event → the `None` one
   is filled, the other untouched (single assertion on the resulting list).
5. Empty `events` with a batch value → returns `[]`.
6. `batch_metadata={}` (empty mapping, not `None`) → `None` events are filled
   with `{}`; proves `{}` is a real fillable value and the guard uses `is not
   None`, not truthiness.

---

## Phase 2: `StreamPublishDefinition` Metadata Key

### Overview

Expose batch metadata on the category-publish surface by adding an optional
`metadata` key to `StreamPublishDefinition` and a `metadata` argument to
`stream_publish_definition(...)`.

### Changes Required

#### 1. TypedDict field

**File**: `src/logicblocks/event/store/types.py`
**Changes**: Add `metadata: NotRequired[Metadata]` to `StreamPublishDefinition`
(mirroring how `condition` is `NotRequired`).

```python
class StreamPublishDefinition[
    Name: StringPersistable = str,
    Payload: JsonPersistable = JsonValue,
    Metadata: JsonPersistable = JsonValue,
](TypedDict):
    events: Sequence[NewEvent[Name, Payload, Metadata]]
    condition: NotRequired[WriteCondition]
    metadata: NotRequired[Metadata]
```

#### 2. Helper argument

**File**: `src/logicblocks/event/store/types.py`
**Changes**: `stream_publish_definition(..., metadata: Metadata | None = None)`
sets the key only when provided (mirroring the existing `condition` handling).

```python
def stream_publish_definition[...](
    *,
    events: Sequence[NewEvent[Name, Payload, Metadata]],
    condition: WriteCondition | None = None,
    metadata: Metadata | None = None,
) -> StreamPublishDefinition[Name, Payload, Metadata]:
    definition: StreamPublishDefinition[Name, Payload, Metadata] = {
        "events": events
    }
    if condition is not None:
        definition["condition"] = condition
    if metadata is not None:
        definition["metadata"] = metadata
    return definition
```

> **Note on the `None` sentinel:** because `metadata=None` means "no batch
> metadata", the key is omitted rather than stored as `None`. Phase 4 reads it
> with `definition.get("metadata")`, which yields `None` when absent — the exact
> value `resolve_batch_metadata` treats as a no-op.

### Success Criteria

#### Automated Verification:

- [x] `stream_publish_definition` unit tests pass: `mise run test:unit --test-args='-k StreamPublishDefinition'`
- [x] All unit tests pass: `mise run test:unit`
- [x] Type checking passes: `mise run types:check`
- [x] Linting passes: `mise run lint:fix`

#### Manual Verification:

- [ ] N/A (covered by unit tests)

### Tests (write first)

**File**: `tests/unit/logicblocks/event/store/test_types.py`

1. `stream_publish_definition(events=[...])` → no `metadata` key present.
2. `stream_publish_definition(events=[...], metadata={"a": "b"})` → definition
   equals `{"events": [...], "metadata": {"a": "b"}}`.
3. `stream_publish_definition(events=[...], condition=..., metadata=...)` → both
   keys present (assert full-dict equality).

---

## Phase 3: `EventStream.publish` Metadata Parameter

### Overview

Add the optional `metadata` keyword to `EventStream.publish` and apply the
resolver before delegating to `save`.

### Changes Required

#### 1. Publish signature + wiring

**File**: `src/logicblocks/event/store/store.py`
**Changes**: Add `metadata: Metadata | None = None` to `EventStream.publish`
(line 56). Before `self._adapter.save(...)`, resolve:
`events = resolve_batch_metadata(events, metadata)`. Everything downstream
(logging, `save`, return) is unchanged — it already operates on the resolved
`events`.

```python
async def publish[
    Name: StringPersistable,
    Payload: JsonPersistable,
    Metadata: JsonPersistable,
](
    self,
    *,
    events: Sequence[NewEvent[Name, Payload, Metadata]],
    condition: WriteCondition = NoCondition(),
    metadata: Metadata | None = None,
) -> Sequence[StoredEvent[Name, Payload, Metadata]]:
    events = resolve_batch_metadata(events, metadata)
    ...
```

Import `resolve_batch_metadata` from `.types`.

> **Logging note:** the pre-publish debug log (`store.py:67`) serialises
> `events`. Resolving *before* it means the log reflects the metadata that will
> actually be stored — the desired behaviour. Confirm the existing
> `test_logs_event_and_conditions_pre_publish` still passes (it publishes without
> batch metadata, so events are unchanged and the assertion holds).

### Success Criteria

#### Automated Verification:

- [x] New stream-publish batch-metadata test passes: `mise run test:unit --test-args='-k TestStreamPublishing'`
- [x] All store unit tests pass: `mise run test:unit --test-args='-k TestStream'`
- [x] All unit tests pass: `mise run test:unit`
- [x] Type checking passes: `mise run types:check`

#### Manual Verification:

- [ ] N/A (covered by unit tests)

### Tests (write first)

**File**: `tests/unit/logicblocks/event/store/test_store.py` (in
`TestStreamPublishing`)

1. Publish two events — one `metadata=None`, one with its own metadata — with a
   batch `metadata`; assert returned `stored_events[0].metadata == batch` and
   `stored_events[1].metadata == own` (single list-equality assertion on the
   metadata of the read-back events, following the existing full-object style).
2. Publish with no `metadata` argument → events with `metadata=None` remain
   `None` (backwards-compatibility guard).

---

## Phase 4: `EventCategory.publish` Metadata

### Overview

Apply per-stream batch metadata in `EventCategory.publish` by resolving each
definition's `events` against its `metadata` key before delegating to `save`.

### Changes Required

#### 1. Per-stream resolution

**File**: `src/logicblocks/event/store/store.py`
**Changes**: In `EventCategory.publish` (line 205), rebuild the `streams`
mapping, resolving each definition's events against its `metadata` before calling
`save`. Construct each resolved definition **explicitly** (do not spread the
original dict) so the `StreamPublishDefinition` type is preserved and the
consumed `metadata` key is dropped. Preserve `condition` only when present, so
absence still defers to the adapter's own `NoCondition()` default.

```python
async def publish[...](
    self,
    *,
    streams: Mapping[str, StreamPublishDefinition[Name, Payload, Metadata]],
) -> Mapping[str, Sequence[StoredEvent[Name, Payload, Metadata]]]:
    def _resolve(
        definition: StreamPublishDefinition[Name, Payload, Metadata],
    ) -> StreamPublishDefinition[Name, Payload, Metadata]:
        resolved: StreamPublishDefinition[Name, Payload, Metadata] = {
            "events": resolve_batch_metadata(
                definition["events"], definition.get("metadata")
            ),
        }
        if "condition" in definition:
            resolved["condition"] = definition["condition"]
        return resolved

    resolved_streams = {
        name: _resolve(definition) for name, definition in streams.items()
    }
    return await self._adapter.save(
        target=self._identifier, streams=resolved_streams
    )
```

> **Why explicit construction, not a dict spread:** the project type-checks with
> **pyrefly**, and spreading a `TypedDict` with `NotRequired` keys then
> overriding one can widen the inferred type to `dict[str, object]`, breaking
> `save`'s `Mapping[str, StreamPublishDefinition[...]]` parameter. Explicit
> construction keeps the type exact.
>
> **Why drop the `metadata` key:** once resolved into each event, a lingering
> batch `metadata` on the definition is dead data that could confuse a future
> reader (or a hypothetical re-resolution). The explicit form naturally omits it.
>
> **Why preserve `condition` only when present:** the adapters apply their own
> `NoCondition()` default via `stream_request.get("condition", NoCondition())`
> (`memory/adapter.py`). Introducing a `condition` key where there wasn't one
> would change the shape the adapter sees; `if "condition" in definition` keeps
> the four combinations (neither / condition-only / metadata-only / both)
> behaving exactly as today.

### Success Criteria

#### Automated Verification:

- [x] New category-publish batch-metadata test passes: `mise run test:unit --test-args='-k TestCategoryPublish'`
- [x] All category unit tests pass: `mise run test:unit --test-args='-k TestCategory'`
- [x] All unit tests pass: `mise run test:unit`
- [x] Type checking passes: `mise run types:check`
- [x] Integration tests pass (Postgres adapter round-trips resolved metadata): `mise run test:integration`

#### Manual Verification:

- [ ] N/A (covered by unit + integration tests)

### Tests (write first)

**File**: `tests/unit/logicblocks/event/store/test_store.py` (in
`TestCategoryPublish`)

1. Publish to one stream via `stream_publish_definition(events=[none_event,
   own_event], metadata=batch)`; assert the returned stored events for that
   stream carry `[batch, own]` metadata respectively.
2. Publish to two streams, only one carrying batch `metadata`; assert the
   metadata-less stream's `None` events stay `None` and the other stream's are
   filled (per-stream isolation).
3. Publish with a definition that has no `metadata` key → unchanged behaviour
   (backwards-compatibility guard).
4. Publish a definition carrying **both** `condition` and `metadata`
   (`stream_publish_definition(events=[none_event], condition=stream_is_empty(),
   metadata=batch)`) into a **non-empty** stream → asserts the condition still
   fires (`UnmetWriteConditionError` raised), proving `_resolve` preserves
   `condition` while filling metadata. Pair with a happy-path variant (condition
   met) asserting the returned event carries the batch metadata — this exercises
   the metadata-and-condition combination end-to-end through the category path.

---

## Phase 5: Documentation & Changelog

### Overview

Document the new option and record an additive, non-breaking changelog fragment.

### Changes Required

#### 1. README / docs example

**File**: `README.md` and `docs/index.md`
**Changes**: Add a short example under the existing publish usage showing
`stream.publish(events=[...], metadata={"actor": "..."})` and noting the
"fill-None-only" rule and that per-event metadata takes precedence.

#### 2. Changelog fragment

**File**: `changelog.d/<generated>.md` (via `mise run changelog:fragment:create`)
**Changes**: Announce the new optional batch-level `metadata` argument on
`EventStream.publish` and the `metadata` key on `StreamPublishDefinition` /
`stream_publish_definition`. State explicitly that it is **additive and
backwards compatible** (no migration, no breaking change) and that per-event
metadata takes precedence over batch metadata.

**Important:** commit only the new fragment under `changelog.d/`. Do **not**
commit an assembled `CHANGELOG.md`.

### Success Criteria

#### Automated Verification:

- [x] Fragment file exists under `changelog.d/` with the expected
      `YYYYMMDD_HHMMSS_<author>_<slug>.md` naming.
- [x] Full build is green: `mise run`

#### Manual Verification:

- [x] README / docs example renders correctly and states the precedence rule.
- [x] Fragment clearly states the change is additive / non-breaking.

---

## Testing Strategy

### Unit Tests (the bulk — test pyramid)

- **Resolver (`resolve_batch_metadata`)** — exhaustive edge cases: no batch
  metadata (no-op), fill a `None` event, preserve an own-metadata event, mixed
  batch, empty batch, timestamp preservation, and `{}` as a distinct fillable
  value (guard uses `is not None`, not truthiness).
- **`stream_publish_definition`** — metadata key present / absent / with
  condition.
- **`EventStream.publish`** — batch fills `None` events, per-event metadata wins,
  omitting the argument is unchanged.
- **`EventCategory.publish`** — per-stream fill, per-stream isolation, no-metadata
  definition unchanged, and `condition` + `metadata` together (condition still
  fires while metadata is filled).

### Integration Tests

- The existing Postgres shared adapter cases already round-trip per-event
  metadata; a category-publish batch-metadata unit test plus the existing
  integration suite confirm the resolved metadata persists. No new
  adapter-specific test is required (the adapters are unchanged).

### Manual Testing Steps

1. `stream.publish(events=[NewEvent(..., metadata=None), NewEvent(...,
   metadata={"actor": "x"})], metadata={"actor": "svc"})`; read back and confirm
   the first event has `{"actor": "svc"}` and the second `{"actor": "x"}`.
2. Repeat via `category.publish` with `stream_publish_definition(..., metadata=...)`.
3. Publish without `metadata` and confirm behaviour is identical to today.

## Performance Considerations

Negligible. The resolver is an O(n) pass building at most one new frozen
`NewEvent` per `None` event; when `batch_metadata is None` it is a zero-copy
no-op returning the original sequence.

## Migration Notes

None. This is a purely additive, backwards-compatible API change: no schema
change, no data migration, and existing `publish(...)` / `stream_publish_definition(...)`
call sites are unaffected (the new parameter/key defaults to "off").

## References

- Prior metadata work (per-event field + generic threading):
  `meta/plans/2026-06-24-event-metadata.md`, PR #116
- `EventStream.publish`: `src/logicblocks/event/store/store.py:56`
- `EventCategory.publish`: `src/logicblocks/event/store/store.py:205`
- `StreamPublishDefinition` / `stream_publish_definition`:
  `src/logicblocks/event/store/types.py:14,23`
- `NewEvent` (frozen; clock-derived timestamps):
  `src/logicblocks/event/types/event.py:17,37`
- Existing metadata publish tests: `tests/unit/logicblocks/event/store/test_store.py:625,1113`
- Builders (`with_metadata`): `src/logicblocks/event/testing/builders.py:97`