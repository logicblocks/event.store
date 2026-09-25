---
type: plan-review
id: "2026-09-25-projector-finalise-state-review-1"
title: "Plan Review: Projector finalise_state Implementation Plan"
date: "2026-09-25T12:11:08+00:00"
author: "Paul Svalin"
producer: review-plan
status: complete
target: "plan:2026-09-25-projector-finalise-state"
reviewer: "Paul Svalin"
verdict: COMMENT
lenses: [architecture, code-quality, test-coverage, correctness, compatibility, usability, documentation]
review_number: 1
review_pass: 2
tags: ["projection", "projector"]
last_updated: "2026-09-25T12:22:17+00:00"
last_updated_by: "Paul Svalin"
schema_version: 1
---

## Plan Review: Projector finalise_state Implementation Plan

**Verdict:** REVISE

The plan is small and well-scoped. It follows the library's overridable-method precedent (`update_metadata`), puts the hook at a clear projection boundary, and maps every promised behaviour to a named unit test. Every lens found the same gap: `project()` can resume from existing state, and `ProjectionEventProcessor` feeds already-finalised state back in once per event. So `finalise_state` needs a stated contract: idempotency, handlers accepting finalised input, and id stability. The plan never states it, and no test covers it. The remaining gaps are the processor update path and the finalise failure path in tests, plus tighter user-facing docs.

### Cross-Cutting Themes

- **Undefined contract under resumed/incremental projection** (flagged by: architecture, code-quality, correctness, usability, compatibility) — The processor loads finalised state, applies one event, and finalises again. Overrides must therefore be idempotent (`f(f(s)) == f(s)`), must produce state that handlers can take as input, must survive the store's save/reload round trip, and must not change fields that `id_factory` depends on. If they do, the upsert on `(name, id)` creates duplicate projections.
- **"Once per projection" framing is misleading** (flagged by: architecture, code-quality, usability, documentation) — The hook runs once per `project()` call. In `ProjectionEventProcessor` that means once per processed event. The performance benefit only applies to multi-event folds such as rebuilds and replays, but only the plan's Performance section says so. Nothing user-facing does.
- **Finalised and unfinalised views are not documented** (flagged by: architecture, correctness, usability, documentation) — `update_metadata` and public `apply()` both see unfinalised state. The README and changelog should say so.
- **New reserved name collides with handler lookup** (flagged by: usability, compatibility, documentation) — Handlers are resolved with `getattr(self, to_snake_case(event.name))`. As a result, an event named `finalise-state`, or an existing subclass member with that name, now behaves differently. "Existing projectors are unaffected" is slightly overstated.
- **Finalise failures carry no context** (flagged by: code-quality, usability, test-coverage) — The main use case is validation, so raising is an expected outcome. The plan should decide whether exceptions propagate unchanged, and test that behaviour.

### Tradeoff Analysis

- **Name clarity vs consistency**: Code-quality suggests a name that doesn't imply finality (`normalise_state`, `on_projected`). Usability likes that `finalise_state` matches the codebase's British spelling. Recommendation: keep `finalise_state` and state clearly that it runs per `project()` call.
- **Error wrapping vs simplicity**: Wrapping in a `ProjectionFinalisationError` adds diagnostic context. Leaving exceptions unwrapped matches how handler exceptions behave today. Recommendation: propagate unwrapped, record that as a decision, and test it.
- **Idempotent contract vs detectable test fixtures**: The contract asks overrides to be idempotent. Test-coverage wants non-idempotent fixtures, such as a counter, so that repeated or misplaced calls show up. These don't conflict: the fixtures are instruments for spotting extra calls, not examples to copy. The README example should be idempotent.
- **Future extension vs YAGNI**: Deferring `metadata`/`source` means adding them later breaks existing overrides. Recommendation: accept the deferral, but note the future breaking-change cost in "What We're NOT Doing".

### Findings

#### Critical

None.

#### Major

- 🟡 **Architecture / Code Quality / Correctness / Usability / Compatibility**: Idempotency, handler-input and round-trip contract for resumed projection is unstated
  **Location**: Desired End State / Key Discoveries / Phase 2 (README and changelog)
  `ProjectionEventProcessor` passes previously finalised state back into `project()`, so `finalise_state` runs repeatedly on its own output, and handlers receive finalised state. A non-idempotent override makes a full rebuild and incremental processing give different results. If the override changes id-relevant fields, it also creates duplicate `(name, id)` projection rows.
- 🟡 **Test Coverage**: The processor's update-existing-projection path with `finalise_state` is untested
  **Location**: Phase 1 — `tests/unit/.../consumers/test_projection.py`
  The only planned processor test covers the save-new path. The load → re-project → re-finalise → save path is the processor's real behaviour, and nothing checks it.
- 🟡 **Test Coverage**: No test for `finalise_state` raising (the validation failure path)
  **Location**: Testing Strategy: Unit Tests
  Validation failure is the expected outcome of the motivating use case. Nothing checks that `project()` propagates the error, or that the processor saves nothing when it happens.

#### Minor

- 🔵 **Architecture / Documentation / Code Quality**: "Once per projection" wording doesn't hold for the processor
  **Location**: Overview / Performance Considerations / Changelog fragment
  In the processor flow, finalisation runs once per processed event. Say "once per `project()` call", and say that the benefit comes from multi-event folds.
- 🔵 **Correctness / Architecture / Documentation**: Metadata derived from unfinalised state can disagree with the finalised state
  **Location**: Key Discoveries / `test_updates_metadata_with_unfinalised_states`
  Keep the design, but document that `update_metadata` never sees finalised or derived fields.
- 🔵 **Usability / Documentation**: Public `apply()` returns unfinalised state without saying so
  **Location**: Desired End State / What We're NOT Doing
  Manual folds with `apply()` skip validation or normalisation without any warning. Add one README sentence.
- 🔵 **Usability / Compatibility / Documentation**: The new method name joins the event-handler lookup namespace, so "unaffected" is overstated
  **Location**: Phase 1 Implementation / Migration Notes
  An event named `finalise-state` would now raise `TypeError`. Note in the changelog that the name is now reserved.
- 🔵 **Compatibility**: Adopting an override can change the ids of already-stored projections
  **Location**: Phase 1 Implementation (id_factory receives finalised state)
  If an override normalises a field that `id_factory` uses, persisted projections would be re-keyed. Document that this needs a rebuild.
- 🔵 **Code Quality / Usability**: Errors from `finalise_state` carry no projection context
  **Location**: Phase 1 Implementation
  Decide explicitly whether exceptions propagate unwrapped or are wrapped with source and name context.
- 🔵 **Code Quality**: The name `finalise_state` implies a terminal, once-per-lifetime step
  **Location**: Overview / Desired End State
  Either rename it or make the per-call semantics explicit in the docs.
- 🔵 **Architecture**: Deferring `metadata`/`source` makes adding them later a breaking change for overriders
  **Location**: What We're NOT Doing
  Accept this and record it, or call the hook with keyword arguments so it's easier to extend later.
- 🔵 **Test Coverage**: Test fixtures need distinguishable, non-idempotent transforms to catch mutations
  **Location**: Phase 1 — once/after-all-events tests
  Record every argument the hook receives and assert the full list. A counter field makes a double call visible.
- 🔵 **Test Coverage**: The `id_factory` test must use a state-derived id
  **Location**: Phase 1 — `test_derives_projection_id_from_finalised_state`
  The existing fixtures ignore state in `id_factory`, so reusing them would leave the ordering guarantee unprotected.

#### Suggestions

- 🔵 **Code Quality**: Bind the result to a separate `finalised_state` variable so the boundary is visible in `project()`.
- 🔵 **Correctness**: Caveat that deferring validation means handlers can't rely on validation-time coercion or defaults.
- 🔵 **Usability**: The README example should show cheap per-event handlers paired with a single validation.
- 🔵 **Compatibility**: Note that subclasses overriding `project()` won't call `finalise_state` automatically.
- 🔵 **Documentation**: `docs/index.md` has a stale constructor-style example. Update it, or mark it out of scope.
- 🔵 **Documentation**: Clarify the changelog assembly step, and prefix the cross-repo references with the repository name.
- 🔵 **Test Coverage**: Split or parameterise the two empty-source initial-state paths.
- 🔵 **Test Coverage**: Reduce the manual scratch-project testing to a smoke check that the published package has the method.

### Strengths

- ✅ Follows the established overridable-method pattern (`update_metadata` precedent) instead of constructor callbacks, so the public surface and the processor are unchanged.
- ✅ The hook sits at a clear point: after the fold, before `id_factory`, and outside `apply()`. It also runs for empty sources, so `id` and `state` always come from the same value.
- ✅ Minimal signature and scope: sync, state only, and a deliberate YAGNI on `metadata`/`source`/async with stated reasons.
- ✅ The default returns state unchanged, so the change is additive and existing projectors and `ProjectionEventProcessor` need no changes.
- ✅ Removes the brittle downstream workaround of overriding `project()`.
- ✅ Every Desired End State item traces to a named unit test, including ordering tests against `id_factory` and `update_metadata` and a negative test for `apply()`.
- ✅ Claims checked against the code: `project()` is the only fold in `src`, and the processor is its only caller.
- ✅ Follows project norms: no docstrings, one README example, a scriv `### Added` fragment, and a prerelease that fits current versioning. The downstream pin and upgrade constraint are recorded openly.

### Recommended Changes

1. **Define the `finalise_state` contract explicitly** (addresses: idempotency/round-trip contract; once-per-projection wording; id changes on stored projections; name implies terminal step)
   Add it to Desired End State, and mirror it in the README subsection and the changelog. The rules: it runs once per `project()` call, which is once per event under `ProjectionEventProcessor`. Its output may come back as input via `state=`. It must be idempotent and survive the store round trip. It must not change fields that `id_factory` depends on without a rebuild. Make the README example idempotent.
2. **Add processor update-path and resume-equivalence tests** (addresses: processor update path untested; contract)
   Add `test_updates_existing_projection_with_finalised_state`, mirroring `test_updates_existing_projection`. Optionally add a projector test showing that projecting `[a, b]` equals projecting `[a]` and then resuming with `[b]` when the override is idempotent.
3. **Decide on and test finalise failure behaviour** (addresses: no failure-path test; errors carry no context)
   Record "exceptions propagate unwrapped" as a decision. Add a test that `project()` raises, and a processor test that the stored projection stays unchanged.
4. **Tighten the test fixture design** (addresses: fixtures need distinguishable transforms; state-derived id; split empty-source test)
   Use a projector that records every argument and returns a transformed copy. Use an `id_factory` derived from a field that only finalisation sets. Split or parameterise the two empty-source paths.
5. **Document the finalised and unfinalised views and the reserved name** (addresses: metadata from unfinalised state; `apply()` unfinalised; handler-namespace collision; "unaffected" overstated; `project()` overriders)
   Add one README/changelog sentence each: `update_metadata` and `apply()` see unfinalised state; `finalise_state` is a reserved name on `Projector`; subclasses that override `project()` must call it themselves. Soften the Migration Notes wording.
6. **Minor plan hygiene** (addresses: deferred params become breaking; docs/index.md; changelog assembly; cross-repo refs; manual testing)
   Note the future breaking cost of adding parameters. Scope `docs/index.md` in or out. Clarify whether changelog assembly happens at release. Prefix the compliance-journey-service references. Reduce the manual testing to a smoke check.

## Per-Lens Results

### Architecture

**Summary**: The plan follows the existing overridable-method extension pattern (like `update_metadata`) and is contained to one method and one call site. The main architectural gap is the method's contract under incremental projection. Because `project()` can resume from stored state, `finalise_state`'s output becomes the next call's input. That creates idempotency and handler-input obligations the plan doesn't state, and it means the per-event cost saving mostly disappears on the processor path.

**Strengths**:
- Follows the established way of customising a projector: overridable methods, not constructor callbacks.
- The hook sits at a clear point: after the fold, before `id_factory`, outside `apply()`.
- Minimal signature with deliberate deferral of `metadata`/`source`/async.
- Removes the brittle `project()` override downstream.
- A side-effect-free transformation inside the functional core, so the processor stays a thin shell.

**Findings**:
- 🟡 **major / high** — *Contract under resumed projection (idempotency, handler input) is unspecified* (Desired End State / Key Discoveries). The processor passes previously saved, already-finalised state back in, so handlers receive finalised states and `finalise_state` runs over its own output. Non-idempotent overrides break the rule that folding events in chunks gives the same result as folding them all at once. Suggestion: define the contract (`f(f(s)) == f(s)`, handlers accept finalised states) and add a chunked-versus-whole test.
- 🔵 **minor / high** — *"Once per projection" framing doesn't hold for the library's own incremental consumer* (Overview / Performance Considerations). The processor calls `project()` per event. Reword to "once per `project()` call" and say that the benefit comes from multi-event folds.
- 🔵 **minor / medium** — *Deferring metadata/source parameters makes later extension a breaking change for overriders* (What We're NOT Doing). Accept this and say so, or call the hook with keyword arguments now.
- 🔵 **suggestion / medium** — *update_metadata sees unfinalised state: tradeoff worth stating for adopters* (Key Discoveries). Tell adopters in the README to derive metadata only from fields that finalisation doesn't change.

### Code Quality

**Summary**: A small, well-scoped Template Method hook that follows the `update_metadata` precedent. The main maintainability risk is that the name and the implied contract are unclear, given that the processor calls `project()` once per event with already-finalised state passed back in. The plan never tells implementers that the hook has to be idempotent.

**Strengths**:
- Reuses the existing extension mechanism with no new abstraction.
- YAGNI applied explicitly, with the reasons given.
- A one-line change in `project()` with no added complexity.
- A clear split between per-event and per-projection responsibilities.
- Removes the brittle `project()` override downstream.
- Easy to test on its own through a subclass.

**Findings**:
- 🟡 **major / high** — *Hook contract (idempotency, re-entry of finalised state) is unstated* (Phase 1: Implementation / Key Discoveries). A non-idempotent override silently drifts under the processor. Suggestion: document the contract at the extension point (a short docstring is justified as non-obvious, risky behaviour) and in the README. Optionally add a test that feeds `project()` output back in.
- 🔵 **minor / medium** — *Name 'finalise_state' implies a terminal, once-per-lifetime step* (Overview / Desired End State). Consider `on_projected`, `complete_state` or `normalise_state`, or make the per-call semantics explicit.
- 🔵 **minor / medium** — *Error propagation from finalise_state lacks projection context* (Phase 1: Implementation). Either say explicitly that exceptions propagate unchanged, or wrap them in a `ProjectionFinalisationError` chained with `from`.
- 🔵 **suggestion / low** — *Reassigning `state` hides the finalised/unfinalised distinction* (Phase 1: Implementation). Bind the result to `finalised_state`.

### Test Coverage

**Summary**: Test-first and sized to the change, with a named unit test for every promised behaviour, following the `update_metadata` test pattern. The gaps are the processor update-existing path (finalised state loaded, re-finalised and saved), the `finalise_state` failure path, and fixture design that determines whether the tests would catch mistakes.

**Strengths**:
- The Desired End State and the tests trace one-to-one.
- Ordering against `id_factory` and `update_metadata` is tested explicitly.
- Both empty-source initial-state paths are covered.
- A negative test covers `apply()`.
- Follows the existing test conventions.
- The tests stay at unit level, which fits the change.

**Findings**:
- 🟡 **major / high** — *Processor update-existing-projection path with finalise_state is untested* (Phase 1, consumers tests). Add `test_updates_existing_projection_with_finalised_state` using a distinguishable transform and asserting the full loaded state.
- 🟡 **major / medium** — *No test for finalise_state raising* (Testing Strategy: Unit Tests). Add a `project()` raise test and a processor test asserting that the error propagates and the stored projection is unchanged.
- 🔵 **minor / high** — *Test fixtures need non-idempotent, distinguishable transforms to catch mutations* (once/after-all-events tests). Use a recording projector, assert the full recorded argument list, and use a counter field.
- 🔵 **minor / medium** — *id_factory test must use a state-derived id, unlike existing fixtures* (`test_derives_projection_id_from_finalised_state`). Derive the id from a field that only finalisation sets.
- 🔵 **suggestion / medium** — *Split or parameterise the two empty-source initial-state paths* (`test_finalises_state_when_source_has_no_events`).
- 🔵 **suggestion / low** — *Manual scratch-project check duplicates automated coverage* (Manual Testing Steps). Reduce it to a smoke check of the published package.

### Correctness

**Summary**: The core change is logically sound. The claims check out against the code: `project()` is the only fold, the processor is its only caller, and default behaviour stays exactly as it is. The main gap is the unstated contract under resumable `project()`: `finalise_state` must be idempotent and its output must be valid handler input. This matters most because the id is now derived from finalised state. A smaller issue: metadata computed before finalisation can disagree with the saved state.

**Strengths**:
- The placement after the loop and before `id_factory` means the empty-source path is finalised too, and `id`/`state` always come from the same value.
- The default is an identity function, so non-overriding projectors and the `(name, id)` upserts are unchanged.
- The claims that `project()` is the only fold and the processor is its only caller are confirmed by grep.
- `apply()` keeps its single-event semantics.

**Findings**:
- 🟡 **major / high** — *finalise_state must be idempotent and closed over handler input, but the plan never says so* (Desired End State / Phase 1). A non-idempotent change to id-relevant fields alters the projection id between incremental runs. The adapters upsert on `(name, id)`, so that produces duplicate records. Document the contract, make the README example idempotent, and check that full-rebuild state equals incremental state.
- 🔵 **minor / medium** — *Metadata derived from state can disagree with the finalised state* (Key Discoveries / metadata test). Document that `update_metadata` never sees finalised or derived fields.
- 🔵 **suggestion / medium** — *Deferred validation shifts failures to the end of the projection and allows invalid intermediate state* (Overview / Performance Considerations). Add a README caveat about validation-time coercion and defaults.

### Compatibility

**Summary**: The change is purely additive, and the 0.1.12 prerelease fits how the project already versions. The risks are in the new extension point's contract: the processor saves finalised state and loads it back, so overrides must be idempotent, survive the round trip, and keep the id stable. There is also a small name-collision risk from the name-based handler lookup.

**Strengths**:
- Additive change only, with no signature or format changes.
- Follows the overridable-method pattern, so subclass constructors are untouched.
- Keeps `apply()` and `update_metadata` semantics, with tests locking this in.
- Versioning and changelog practice match the project.
- The downstream pin and upgrade constraint are recorded openly, with an interim path.

**Findings**:
- 🟡 **major / high** — *Undocumented idempotency and round-trip contract for incremental projections* (Phase 2 README/changelog). Document it, and add a test comparing sequential single-event processing with a single multi-event `project()`.
- 🔵 **minor / medium** — *Adopting an override can change the ids of already-stored projections* (Phase 1 id_factory). The Postgres adapter upserts on `(name, id)` while the processor locates by `(source, name)`, so a changed id means duplicate rows. Document that a rebuild or migration is needed.
- 🔵 **minor / medium** — *New method name joins the event-handler lookup namespace* (Phase 1). Say in the changelog that the name is reserved, and grep known downstream projectors for it.
- 🔵 **suggestion / medium** — *Note that subclasses overriding project() won't get finalisation automatically* (Migration Notes).

### Usability

**Summary**: Good developer experience: one small hook that follows `update_metadata`, a sensible identity default, and British spelling that matches the codebase. The friction is the unstated contract: it runs once per event in the processor and must be idempotent. There are two smaller least-surprise gaps: `apply()` returns unfinalised state, and the name can collide with handler lookup.

**Strengths**:
- Easy to guess how it works if you know `update_metadata`.
- No setup cost with the defaults.
- A minimal state-in, state-out signature.
- Removes the brittle `project()` override.
- Clearly defined ordering.
- Naming matches the codebase's spelling conventions.

**Findings**:
- 🟡 **major / high** — *Idempotency and per-event behaviour in the processor flow are not documented* (Phase 2 README and changelog). State the contract, and consider a two-call `process_event` test.
- 🔵 **minor / medium** — *Public apply() returns unfinalised state without saying so* (Desired End State / What We're NOT Doing). Add a README sentence.
- 🔵 **minor / medium** — *New method name can be matched as an event handler* (Phase 1). As a follow-up, `_resolve_handler` could exclude base-class members and raise a clear error.
- 🔵 **suggestion / medium** — *README example should show the motivating cheap-per-event, validate-once pattern* (Phase 2 README).
- 🔵 **suggestion / low** — *Exceptions from finalise_state carry no projection context* (Phase 1). Fine to defer for consistency with handlers.

### Documentation

**Summary**: Fits the project's documentation habits: no docstrings, one README example, scriv fragments. The gaps are in what the user-facing docs will say. They leave out contract details (update_metadata unfinalised, runs on empty sources, apply() skips it) and could be read as promising "once per rebuild" for processor users. The "existing projectors unaffected" claim is slightly overstated.

**Strengths**:
- Follows project norms and the no-comments rule.
- A concrete, user-focused changelog fragment.
- A README subsection next to the existing example, with a check that the docs match the implementation.
- The plan is easy to follow and well referenced.

**Findings**:
- 🔵 **minor / high** — *User-facing docs omit parts of the finalise_state contract* (Phase 2 README and changelog). Add sentences on `update_metadata`, `apply()` and empty sources.
- 🔵 **minor / medium** — *'Once per projection' could be read as once per rebuild for processor users* (Changelog / Performance Considerations).
- 🔵 **minor / medium** — *'Existing projectors are unaffected' claim is slightly overstated* (Desired End State / Migration Notes). Existing `finalise_state` members or events with that name would now behave differently.
- 🔵 **suggestion / medium** — *Docs site example is stale and not considered* (Phase 2). `docs/index.md` uses an outdated constructor-style API.
- 🔵 **suggestion / medium** — *Ambiguous changelog assembly step and cross-repo references* (Phase 2 Success Criteria / References).

---
*Review generated by /accelerator:review-plan*

## Re-Review (Pass 2) — 2026-09-25T12:22:17+00:00

**Verdict:** COMMENT

### Previously Identified Issues

- 🟡 **Architecture / Code Quality / Correctness / Usability / Compatibility**: Idempotency, handler-input and round-trip contract for resumed projection is unstated — Partially resolved (contract section added, but the resume-equivalence claim built on idempotency is logically wrong; see new issues)
- 🟡 **Test Coverage**: Processor update-existing-projection path with finalise_state is untested — Resolved
- 🟡 **Test Coverage**: No test for finalise_state raising — Resolved
- 🔵 **Architecture / Documentation / Code Quality**: "Once per projection" wording doesn't hold for the processor — Resolved
- 🔵 **Correctness / Architecture / Documentation**: Metadata derived from unfinalised state can disagree with finalised state — Resolved
- 🔵 **Usability / Documentation**: Public `apply()` returns unfinalised state without saying so — Resolved
- 🔵 **Usability / Compatibility / Documentation**: New method name joins the event-handler lookup namespace; "unaffected" overstated — Partially resolved (not in README bullets; Desired End State bullet still overstates)
- 🔵 **Compatibility**: Adopting an override can change the ids of already-stored projections — Partially resolved (not mentioned in changelog fragment)
- 🔵 **Code Quality / Usability**: Errors from `finalise_state` carry no projection context — Resolved (recorded decision: propagate unwrapped, tested)
- 🔵 **Code Quality**: Name implies a terminal step — Resolved (per-call semantics explicit)
- 🔵 **Architecture**: Deferring `metadata`/`source` makes later extension breaking — Resolved
- 🔵 **Test Coverage**: Fixtures need distinguishable, non-idempotent transforms — Resolved
- 🔵 **Test Coverage**: `id_factory` test must use a state-derived id — Resolved
- 🔵 **Test Coverage**: Split empty-source paths — Resolved
- 🔵 **Test Coverage**: Manual check duplicates automated coverage — Resolved
- 🔵 **Code Quality**: Reassigning `state` hides the distinction — Resolved
- 🔵 **Correctness**: Deferred validation / coercion caveat — Resolved
- 🔵 **Usability**: README example should show the motivating pattern — Resolved
- 🔵 **Compatibility**: `project()` overriders won't get finalisation — Resolved
- 🔵 **Documentation**: `docs/index.md` stale — Resolved (explicitly out of scope)
- 🔵 **Documentation**: Changelog assembly and cross-repo refs — Resolved

### New Issues Introduced

- 🟡 **Correctness**: Idempotency alone does not make resumed projection equivalent to a single fold — a single call computes `f(h_b(h_a(s0)))`, resuming computes `f(h_b(f(h_a(s0))))`. Equivalence needs finalisation to be transparent to later handlers (`f(h(f(s))) == f(h(s))`) and to `update_metadata`/`id_factory`. Counterexample: an idempotent clamp to ≤10 with +15 then −10 gives 5 vs 0. Pure validation and recomputing derived fields from source fields satisfy it; lossy normalisation (clamping, truncation) does not.
- 🔵 **Test Coverage**: Resume-equivalence test can pass vacuously with an identity or no-op override — use a visible, contract-satisfying transform and assert the complete expected Projection.
- 🔵 **Test Coverage**: Processor finalise fixtures must survive the store's serialise/deserialise round trip — the existing `State` only serialises `value`; transform `value` or extend serialisation.
- 🔵 **Code Quality / Documentation**: Release bullet stranded inside the contract subsection — move it back into the Desired End State list.
- 🔵 **Code Quality**: Decision to keep the name `finalise_state` is not recorded.
- 🔵 **Documentation**: Changelog fragment is one dense bullet — split into a short "Added" bullet plus caveat sub-bullets, including id stability.
- 🔵 **Documentation**: README example content is unspecified for a dict-based state — name a concrete transform that satisfies the contract and doesn't touch id fields.

### Assessment

The revision resolved all three original major findings' test gaps and nearly every minor. One new major remains: the contract is stated as idempotency, but the property the plan actually relies on (resume equals single fold) needs the stronger "finalisation is transparent to later handlers and metadata" rule. Fixing that wording in the contract, README and changelog, and choosing a resume-test override that meets it, is a small edit. The rest are minor tidy-ups. The plan is acceptable to implement once the contract wording is corrected.
