---
type: plan
id: "2026-09-25-projector-finalise-state"
title: "Projector finalise_state Implementation Plan"
date: "2026-09-25T11:52:53+00:00"
author: "Paul Svalin"
producer: create-plan
status: draft
tags: ["projection", "projector", "performance"]
revision: "73008206b31d2257dd361ff44f1fbb2b4d05bce5"
repository: "event.store"
last_updated: "2026-09-25T12:22:17+00:00"
last_updated_by: "Paul Svalin"
schema_version: 1
---

# Projector finalise_state Implementation Plan

## Overview

Add an overridable `finalise_state(state) -> State` method to `Projector`.
It's called once at the end of every `project()` call, after all events have
been applied and before the `Projection` is built. The default returns the
state unchanged.

This lets projectors use cheap, unvalidated state transitions per event and
do expensive work (validation, normalisation, derived fields) **once per
`project()` call** instead of once per event. The benefit comes from
multi-event folds (rebuilds, replays). `ProjectionEventProcessor` projects one
event per call, so there it still runs once per processed event.

**Motivating case.** compliance-journey-service rebuilds journeys whose
Pydantic state is ~200–250KB. Its handlers fully re-validate the state
around twice per event, which is 26–38% of the service's CPU in production-like
profiles. Copying without validation per event and validating once at the
end cuts a 76-event rebuild from ~105–125ms to ~3–3.5ms, with identical
output. Today the service can only do this by overriding `project()`, which
means copying its signature and rebuilding the `Projection` by hand. That's
brittle if `project()` changes.

## Current State Analysis

- `Projector` (`src/logicblocks/event/projection/projector.py:31`) already
  uses overridable methods for customisation: abstract
  `initial_state_factory`, `initial_metadata_factory` and `id_factory`;
  optional `update_metadata` (default returns metadata unchanged); plus the
  `name` and `missing_handler_behaviour` class attributes.
- `project()` (`:71–91`):
  1. resolves the initial state and metadata;
  2. applies each event via `apply()` and `update_metadata()`;
  3. then builds `Projection(id=self.id_factory(state, source.identifier), ..., state=state, ...)`.
- `apply()` (`:63–69`) applies a single event and is public. It is not a
  projection boundary.
- Within the library, the only caller of `project()` is
  `ProjectionEventProcessor.process_event`
  (`src/logicblocks/event/processing/consumers/projection.py:45`). It calls
  `project()` with a single-event `InMemoryStoredEventSource` and saves the
  result.
- Nothing else in `src` folds events outside `project()`.
- Tests: `tests/unit/logicblocks/event/projection/test_projector.py` has
  `TestProjectorEventApplication` (`:124`) and `TestProjectorProjection`
  (`:312`), including the existing `update_metadata` override tests
  (`:451–690`), which are the pattern to follow. Processor tests are in
  `tests/unit/logicblocks/event/processing/consumers/test_projection.py`.
- Changelog: scriv fragments in `changelog.d/`, created with
  `mise run changelog:fragment:create`.

## Desired End State

- `Projector.finalise_state(self, state: State) -> State` exists and returns
  `state` unchanged by default.
- `project()` calls it exactly once per call:
  - after the event loop, including when the source has no events;
  - before `id_factory`, so the id is derived from the finalised state;
  - its result is the `Projection.state`.
- `apply()` never calls it; `apply()` returns unfinalised state.
- `update_metadata` keeps receiving unfinalised, per-event states.
- Exceptions raised by `finalise_state` propagate from `project()` unwrapped,
  consistent with handler exceptions. `ProjectionEventProcessor` then saves
  nothing.
- `ProjectionEventProcessor` saves finalised state, with no change to the
  processor itself.
- Fully backwards compatible: projectors that don't define `finalise_state`
  and don't receive an event named `finalise-state` (or equivalent) behave
  exactly as before.
- Released in the next `0.1.12` prerelease from `main`, with a changelog
  entry.

### The `finalise_state` contract

`project()` is resumable: callers may pass `state=`, and
`ProjectionEventProcessor` always passes the previously saved, already
finalised state. So finalised state flows back in as input. A single call
computes `f(h_b(h_a(s)))`; resuming computes `f(h_b(f(h_a(s))))`. For the two
to agree, overrides must:

- be transparent to later processing: handlers, `update_metadata` and
  `id_factory` produce the same result whether given a finalised or an
  unfinalised state, i.e. `f(h(f(s))) == f(h(s))`. This implies idempotency
  (`f(f(s)) == f(s)`), but idempotency alone isn't enough. An idempotent clamp
  to at most 10 with events `+15` then `-10` gives 5 in one call and 0 when
  resumed;
- round-trip through the projection store's state type unchanged;
- not change fields that `id_factory` depends on for projections that are
  already persisted. Stores upsert on `(name, id)`, so a changed id creates a
  duplicate projection; changing id-relevant fields needs a rebuild.

Pure validation and recomputing derived fields from source fields satisfy
this. Lossy normalisation (clamping, truncation, dropping data) does not.
With a conforming override, projecting `[a, b]` in one call gives the same
`Projection` as projecting `[a]` and then resuming with `[b]`.

Handlers must not rely on validation-time coercion or defaults when
validation is deferred to `finalise_state`.

`finalise_state` becomes a reserved name on `Projector` subclasses. Handlers
are resolved by `getattr(self, to_snake_case(event.name))`, so an event named
`finalise-state` now resolves to it. Subclasses that override `project()`
don't get finalisation unless they call `self.finalise_state(state)`
themselves.

### Key Discoveries:

- Overridable methods are the library's way of customising a projector, not
  constructor callbacks. `update_metadata` is the direct precedent
  (`projector.py:54`).
- `update_metadata` is called inside the loop with each intermediate state.
  It keeps receiving unfinalised states. Finalising is a projection-boundary
  concern.
- `ProjectionEventProcessor` gets the behaviour through `project()` with no
  change of its own.

## What We're NOT Doing

- A constructor argument or registered callback for post-projection work.
- A per-event method (that's what handlers and `apply()` are for).
- Passing `metadata` or `source` to `finalise_state`. Add them later if a
  real need appears, accepting that it will break existing overrides.
- Wrapping `finalise_state` exceptions in a library error.
- Renaming to `normalise_state` or `on_projected`. `finalise_state` is kept
  for consistency with the codebase's naming; the docs state it runs once per
  `project()` call.
- Updating `docs/index.md`. Its example already uses an outdated
  constructor-style API; fixing it is a separate change.
- Making `finalise_state` async. It's synchronous, like the other
  customisation methods.
- Calling `finalise_state` from `apply()`.
- Upgrading compliance-journey-service to `0.1.12` (with the event-metadata
  migration). That's planned in the service repo, and the service uses a
  `project()` override until then.

## Implementation Approach

Test-first, one behaviour at a time (red, green, refactor), following the
existing `update_metadata` tests. The change is a single method plus one
call in `project()`.

## Phase 1: `finalise_state` on `Projector`

### Overview

Add the method, call it from `project()`, and cover every behaviour with
unit tests.

### Changes Required:

#### 1. Unit tests (write first, one at a time)

**File**: `tests/unit/logicblocks/event/projection/test_projector.py`
**Changes**: add to `TestProjectorProjection`, using the existing
`EventStore(adapter=InMemoryEventStorageAdapter())` and `NewEventBuilder`
patterns, and asserting complete expected `Projection`s.

Fixtures: use a recording projector whose `finalise_state` appends each
argument to a list and returns a clearly distinguishable, non-idempotent copy
(e.g. increments a `finalised_count` field). Assert the full recorded list and
the complete expected `Projection`, so calls inside the loop, double calls, or
calls with the pre-fold state all fail. The fixture is non-idempotent only to
make extra calls visible; it isn't an example of a valid override.

- `test_uses_finalised_state_as_projection_state`: a projector whose
  `finalise_state` returns a transformed state produces a `Projection` with
  that transformed state.
- `test_finalises_state_once_after_all_events_applied`: across several
  events, the recorded arguments equal exactly `[state_after_last_event]`.
- `test_finalises_initial_state_when_source_has_no_events`: with an empty
  source, `finalise_state` receives the state from `initial_state_factory`.
- `test_finalises_provided_state_when_source_has_no_events`: with an empty
  source, `finalise_state` receives the provided `state=`.
- `test_derives_projection_id_from_finalised_state`: `id_factory` derives the
  id from a field only `finalise_state` sets, and the expected `Projection`
  carries that id. Existing fixtures ignore state in `id_factory`, so this
  test needs its own.
- `test_updates_metadata_with_unfinalised_states`: `update_metadata` keeps
  receiving the per-event, unfinalised states.
- `test_leaves_state_unchanged_by_default`: without an override, the
  projection state equals the folded state. Existing tests already imply
  this; the explicit test documents the default.
- `test_resumed_projection_matches_single_projection`: with a conforming,
  visible override (e.g. recomputing a derived `total` from source fields),
  projecting `[a, b]` equals projecting `[a]` and then resuming from its
  state with `[b]`. Assert both against a complete expected `Projection` that
  includes the derived field, so an identity or skipped finalisation fails.
- `test_raises_when_finalise_state_raises`: an exception from
  `finalise_state` propagates from `project()` unwrapped
  (`pytest.raises` with the specific type).

Add to `TestProjectorEventApplication`:

- `test_does_not_finalise_state_when_applying_single_event`: `apply()`
  doesn't call `finalise_state`.

**File**: `tests/unit/logicblocks/event/processing/consumers/test_projection.py`
**Changes**:

Processor tests read results back through the projection store, and the
existing `State` fixture only serialises `value`. Make the finalise transform
act on a serialised field (e.g. transform `value` itself) or extend
`State.serialise`/`deserialise`, then assert the full loaded state (e.g.
seeded 5, event 10, finalised once to a specific value).

- `test_saves_finalised_state`: `ProjectionEventProcessor.process_event`
  saves the state returned by the projector's `finalise_state`.
- `test_updates_existing_projection_with_finalised_state`: mirrors
  `test_updates_existing_projection`. Seed an existing projection, process
  one event with a projector whose `finalise_state` applies a distinguishable
  transform, and assert the full saved state: finalisation ran once on the
  loaded state plus the new event.
- `test_does_not_save_when_finalise_state_raises`: seed an existing
  projection, process an event with a projector whose `finalise_state`
  raises, and assert the error propagates and the stored projection is
  unchanged.

#### 2. Implementation

**File**: `src/logicblocks/event/projection/projector.py`
**Changes**:

```python
def finalise_state(self, state: State) -> State:
    return state

async def project(
    self,
    *,
    source: EventSource[Identifier, StoredEvent],
    state: State | None = None,
    metadata: Metadata | None = None,
) -> Projection[State, Metadata]:
    state = self._resolve_state(state)
    metadata = self._resolve_metadata(metadata)

    async for event in source:
        state = self.apply(state=state, event=event)
        metadata = self.update_metadata(state, metadata, event)

    finalised_state = self.finalise_state(state)

    return Projection[State, Metadata](
        id=self.id_factory(finalised_state, source.identifier),
        name=self.projection_name,
        source=source.identifier,
        state=finalised_state,
        metadata=metadata,
    )
```

Binding to `finalised_state` keeps the finalised/unfinalised boundary visible
without a comment.

Place `finalise_state` next to `update_metadata`, among the other
overridable methods.

### Success Criteria:

#### Automated Verification:

- [x] New projector tests pass: `mise run test:unit[TestProjectorProjection]`
      and `mise run test:unit[TestProjectorEventApplication]`
- [x] Processor test passes: `mise run test:unit` (consumers suite)
- [x] All unit tests pass: `mise run test:unit`
- [x] Integration tests pass: `mise run test:integration`
- [x] Component tests pass: `mise run test:component`
- [x] Type checking passes: `mise run types:check`
- [x] Lint and formatting pass: `mise run lint:fix` and `mise run format:fix`
      (no unexpected diffs)
- [x] Full build passes: `mise run` (locally verified as its parts: lint, types, format, build, unit, and integration/component with `DB_PORT=5433` because 5432 was occupied)

#### Manual Verification:

- [x] The diff to `projector.py` is limited to the new method and the one
      call in `project()` (plus binding `finalised_state`)

---

## Phase 2: Documentation, changelog and release

### Overview

Document the method, add a changelog fragment, and release from `main`.

### Changes Required:

#### 1. README

**File**: `README.md`
**Changes**: after the basic `ProfileProjector` example, add a short
subsection showing the motivating pattern: cheap, unvalidated per-event
handlers with one validation or normalisation in `finalise_state`. Keep it to
a minimal example consistent with the existing `dict[str, str]` one: e.g.
raise if a required key such as `email` is missing, and strip surrounding
whitespace from `email`. Neither touches id fields or changes what later
handlers compute. Next to it, state briefly:

- it runs once per `project()` call; with `ProjectionEventProcessor` that is
  once per processed event, and it also runs when the source has no events;
- its output may be passed back in as input (`state=`, the processor), so it
  must not change what later handlers, `update_metadata` or `id_factory`
  compute (validation and recomputing derived fields are fine; lossy
  normalisation isn't), must round-trip through the store, and must not
  change fields `id_factory` relies on;
- `update_metadata` and `apply()` see unfinalised state;
- handlers must not rely on validation-time coercion or defaults;
- `finalise_state` is a reserved name, so no event may be named
  `finalise-state`;
- subclasses overriding `project()` must call it themselves.

#### 2. Changelog fragment

**File**: `changelog.d/<timestamp>_<author>_projector_finalise_state.md`
(created via `mise run changelog:fragment:create`)
**Changes**:

```markdown
### Added

- `Projector` now has an overridable `finalise_state(state)` method, called
  once at the end of each `project()` call, after all events are applied and
  before the projection id is derived. The default returns the state
  unchanged. Use it for expensive work, such as validation, instead of
  repeating it in every event handler. See the README for the full contract.
  - The saving comes from multi-event folds. `ProjectionEventProcessor`
    still calls it once per processed event.
  - Finalised state is passed back in as the starting state when resuming,
    so an override must not change what later handlers compute. Validation
    and recomputing derived fields are fine; lossy normalisation isn't.
  - Overrides must not change fields `id_factory` uses for already-persisted
    projections; if they do, rebuild those projections.
  - `update_metadata` and `apply()` still see unfinalised state.
  - `finalise_state` is now a reserved name on `Projector` subclasses.
```

Fragments accumulate in `changelog.d/` across prereleases and are assembled
into `CHANGELOG.md` at a later release, not by the `main` workflow. Don't
commit an assembled `CHANGELOG.md` with this change.

#### 3. Release

Merge to `main`. The `main` workflow publishes the next `0.1.12` prerelease
and bumps the version.

### Success Criteria:

#### Automated Verification:

- [x] Fragment exists in `changelog.d/` and assembles cleanly:
      `mise run changelog:assemble` run locally, with the resulting
      `CHANGELOG.md` and fragment deletions discarded
- [ ] PR checks pass (`pr-checks.yaml`)
- [ ] The `main` workflow publishes the new prerelease and pushes the version
      bump

#### Manual Verification:

- [x] The README example reads correctly and matches the implementation
- [ ] The published package on the index contains `Projector.finalise_state`

---

## Testing Strategy

### Unit Tests:

- Finalised state becomes the projection state.
- Called exactly once per `project()`, after all events.
- Called when there are no events (both initial-state paths, tested
  separately).
- `id_factory` sees the finalised state.
- `update_metadata` sees unfinalised states.
- Default behaviour is unchanged.
- `apply()` never finalises.
- Resuming a projection matches a single projection for a conforming,
  visible override.
- Exceptions from `finalise_state` propagate unwrapped.
- `ProjectionEventProcessor` saves finalised state, for both new and existing
  projections, and saves nothing when `finalise_state` raises.

### Integration Tests:

- The existing integration and component suites confirm there are no
  regressions for projectors that don't override the method.

### Manual Testing Steps:

1. Smoke check: install the published prerelease in a scratch environment
   and confirm `Projector.finalise_state` exists. Behaviour is covered by the
   unit tests.

## Performance Considerations

- The default method adds one trivial call per `project()`.
- Overriding projectors can move repeated per-event work to a single call.
  For `ProjectionEventProcessor`, that means once per processed event.

## Migration Notes

- No migration. No change for projectors that don't define `finalise_state`
  and have no event named `finalise-state` (or equivalent) that would now
  resolve to it.
- Subclasses that override `project()` don't call `finalise_state`. When
  moving from such an override to the hook, remove the override or call
  `self.finalise_state(state)` from it.
- Adding an override to a projector with persisted projections must not
  change fields `id_factory` uses; otherwise rebuild the projections.
- **Downstream note:** compliance-journey-service pins `0.1.10a29`. Adopting
  this means upgrading across `0.1.12a1`'s breaking event-metadata change
  (required `metadata` on `NewEvent` and `StoredEvent`, plus the
  `events.metadata` column migration). That's planned in the service repo,
  and the service uses a `project()` override until then.

## References

- Projector: `src/logicblocks/event/projection/projector.py:31`
- Existing overridable-method precedent: `update_metadata` (`projector.py:54`)
  and its tests (`tests/unit/logicblocks/event/projection/test_projector.py:451`)
- Projection processor: `src/logicblocks/event/processing/consumers/projection.py:45`
- Motivating research, in the compliance-journey-service repository:
  `compliance-journey-service/meta/research/codebase/2026-09-24-endpoint-latency-after-co-1326.md` §8
- Consuming plan, in the compliance-journey-service repository:
  `compliance-journey-service/meta/plans/2026-09-25-CO-1326-fast-journey-projection.md`
