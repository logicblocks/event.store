---
type: pr-review
id: "116-review-1"
title: "Add optional metadata bag to events"
date: "2026-06-26T14:13:08+00:00"
author: "bivav-ebury"
producer: review-pr
status: complete
target: "pr:116"
reviewer: "bivav-ebury"
verdict: "COMMENT"
lenses: ["architecture", "code-quality", "correctness", "test-coverage", "database", "compatibility", "documentation"]
review_number: 1
pr_number: 116
tags: ["metadata", "events", "schema-migration"]
last_updated: "2026-06-26T14:13:08+00:00"
last_updated_by: "bivav-ebury"
schema_version: 1
---

## Code Review: #116 - Add optional metadata bag to events

**Verdict:** COMMENT

This is a clean, well-scoped, and highly consistent change: it threads a new `Metadata` type parameter through the event types, builders, both storage adapters, and the public `publish`/`save` API, faithfully mirroring the established `Projection[State, Metadata]` template and the `projections.metadata JSONB NOT NULL` column. The functional-core/imperative-shell separation is preserved, the migration is genuinely safe on large tables, and test coverage follows the codebase's full-object-equality conventions. No critical issues were found. The concerns worth addressing before merge cluster around: a falsy-coalescing builder bug, the deliberately-undocumented breaking nature of the required `StoredEvent.metadata` constructor argument, and a gap in adapter-level coverage of the central `None`/JSON-null round-trip claim.

### Cross-Cutting Themes

- **`None` vs `{}` representation of "no metadata"** (flagged by: correctness, database, compatibility, documentation) — The app models omitted metadata as `None` → JSON `null`, but the migration backfills existing rows with `'{}'`. Post-migration, historical rows read back as `{}` while new no-metadata rows read back as `None` — two in-database representations of "absent" that deserialise to different Python values. The semantics are also undocumented at the call sites consumers read.
- **Type-soundness of the nullable-by-default `metadata` field** (flagged by: architecture, code-quality) — `NewEvent.metadata: Metadata` is declared non-optional but `__init__` stores `None` when omitted, so the declared type can be violated at runtime. A deliberate divergence from `Projection` (which requires `metadata`), acknowledged in the plan but not in the code.
- **Breaking deploy-ordering dependency** (flagged by: architecture, compatibility) — The read path (`SELECT *` → `class_row(StoredEvent)`) now requires the `metadata` column to exist; an un-migrated deployment running the new code breaks on **reads as well as writes**, making the migration strictly migrate-before-deploy.
- **Weakened Postgres unit assertion** (flagged by: correctness, test-coverage) — The metadata bind is asserted only by type (`isinstance(Jsonb)`), not by value, while payload is still value-asserted.

### Tradeoff Analysis

- **Type soundness vs ergonomics**: A generic `metadata` field cannot carry a `{}` default (unsound when `Metadata` is e.g. `list[str]`), so the PR chose `None`-when-omitted. This keeps the no-arg constructor ergonomic at the cost of a field whose declared type doesn't admit `None`. The chosen tradeoff is reasonable; the recommendation is to make it *explicit* (declare `metadata: Metadata | None` or document it) rather than leave it implicit.

### Strengths

- ✅ Faithfully mirrors the existing `Projection[State, Metadata]` generic and `projections.metadata` column, keeping the schema and type surface consistent and lowering maintenance cost.
- ✅ Purely additive generic propagation — `Metadata = JsonValue` default means existing `NewEvent[Name, Payload]` / `StoredEvent[Name, Payload]` parameterisations and `NewEvent(name=..., payload=...)` call sites keep working unchanged.
- ✅ Strong functional-core/imperative-shell separation: round-trip logic lives in pure type methods (`serialise`, `serialise_stored_event`) reused identically by both adapters, so behaviour is provably symmetric across backends.
- ✅ The recommended migration is genuinely safe and effectively zero-downtime: in Postgres 11+, `ADD COLUMN ... NOT NULL DEFAULT '{}'` with a constant default is a catalog-only operation (no table rewrite, no scan for NOT NULL validation); `DROP DEFAULT` is likewise catalog-only.
- ✅ Storing JSON `null` (from `None`) in a `NOT NULL` JSONB column is correct and intentional — SQL NULL ≠ JSON null — and `__hash__`/`__eq__` stay consistent after adding metadata to `__repr__`.
- ✅ The changelog fragment accurately frames the breaking change and inlines correct, complete migration SQL that leaves the column byte-identical to the canonical DDL.
- ✅ Deliberate exclusion of metadata from `summarise()` (which feeds logs) thoughtfully avoids leaking potentially sensitive context.

### General Findings

- 🟡 **Test Coverage**: The plan's central claim — omitted metadata stays `None` and round-trips through the `NOT NULL` JSONB column as JSON `null` — is **never exercised against Postgres**. The shared adapter cases all use `NewEventBuilder().build()` (now random-populated metadata), and the only `None`-metadata assertions run against the in-memory adapter, which cannot reveal a `NOT NULL` violation or SQL-NULL-vs-JSON-null mismatch. Add a shared adapter test that round-trips omitted/`None` metadata so it runs against both adapters.
- 🔵 **Test Coverage**: Edge cases untested — empty `{}` (notably, the value every migrated row gets), non-mapping metadata (list/scalar, which `JsonValue` permits), and large bodies.
- 🔵 **Test Coverage**: The two new dedicated `test_publishes_events_with_metadata...` tests use single-field assertions (`stored_events[0].metadata == metadata`), diverging from the project's full-object-equality convention and the plan's own Phase 8 guidance.
- 🔵 **Database**: No down-migration/rollback note — `DROP COLUMN metadata` is destructive; recommend documenting that an app-only rollback can leave the column in place (old code ignores it).
- 🔵 **Documentation**: The README already advertises "events can have arbitrary payloads and metadata" but no example shows the now-real field; `publish` docstrings and `NewEvent` carry no note of the metadata parameter or its `None` semantics.
- 🔵 **Code Quality / Architecture**: The four near-identical `StoredEvent(...)` construction blocks in the memory adapter (caller-facing + serialised, ×2 methods) must be kept in lockstep; a follow-up could extract a shared helper. The implicit by-name coupling between `StoredEvent`'s field set and the `events` column set is pre-existing but reinforced here.
- 🔵 **Documentation**: The committed plan references a "separate design spec" that was deleted (per commit history), leaving a dangling reference.

### Additional Findings

- 🔵 `src/logicblocks/event/types/event.py:32` — `None` vs `{}` metadata semantics undocumented at the type (documentation)
- 🔵 `README.md:104` — advertised metadata feature has no worked example (documentation)
- 🔵 `meta/plans/2026-06-24-event-metadata.md` — dangling reference to deleted design spec (documentation)

## Inline Comments

### `src/logicblocks/event/testing/builders.py:64` — Builder discards falsy-but-valid metadata/payload (`or` coalescing)
**Severity**: major | **Confidence**: high | **Lens**: correctness

🔴 **Correctness**

`metadata or random_event_metadata()` replaces any *falsy-but-valid* metadata with a random map. Because `with_metadata(...)` routes back through `__init__`, calling `NewEventBuilder().with_metadata({})` (or `[]`, `0`, `False`) silently discards the caller's value and substitutes a random populated mapping. The same defect exists for `payload or random_event_payload()` on the line above (pre-existing).

**Impact**: A test that builds an event with empty/zero metadata to exercise edge-case behaviour will not get the value it asked for, masking real bugs and making `with_metadata({})` impossible — which directly undermines the missing `{}` edge-case coverage noted elsewhere in this review.

**Suggestion**: Use an explicit sentinel/`None` check rather than truthiness, e.g. `metadata if metadata is not None else random_event_metadata()` (and likewise for `payload`).

---

### `src/logicblocks/event/testing/builders.py:188` — StoredEventBuilder discards falsy-but-valid metadata/payload
**Severity**: major | **Confidence**: high | **Lens**: correctness

🔴 **Correctness**

`StoredEventBuilder.__init__` uses `metadata or random_event_metadata()`, which replaces falsy-but-valid metadata (`{}`, `[]`, `0`, `False`) with a random map. Since `with_metadata(...)` and `from_new_event(...)` clone through `__init__`, propagating a `NewEvent` whose metadata is an empty dict yields a random map in the `StoredEvent` instead, breaking round-trip fidelity. The adjacent `payload or random_event_payload()` has the same issue.

**Impact**: `from_new_event` no longer faithfully mirrors an event with empty-but-present metadata, and `with_metadata({})` cannot produce an empty-metadata stored event.

**Suggestion**: Replace the `or` with an explicit `is not None` check for both `metadata` and `payload`.

---

### `tests/unit/logicblocks/event/store/adapters/postgres/test_adapter.py:301` — Metadata bind asserted by type only, not value
**Severity**: major | **Confidence**: high | **Lens**: correctness, test-coverage

🟡 **Correctness / Test Coverage**

The metadata binding is now asserted only by type (`isinstance(params[6], Jsonb)`, and again at `params[15]`), not by value — and the event under test is built with `NewEventBuilder(...)` which produces *random* metadata, so the actual serialised value cannot be checked here. The payload binding immediately above still asserts the concrete value (`params[5].obj == {"data": "value1"}`).

**Impact**: A regression that bound the wrong value into the metadata slot (e.g. payload-as-metadata, a stale/empty value, or a column-ordering bug) would still satisfy `isinstance(..., Jsonb)` and pass this unit test. This is the strongest guard against a wrong-field/ordering bug in `insert_batch_query`, and it has been weakened.

**Suggestion**: Build the test events with explicit metadata via `NewEventBuilder(...).with_metadata({"meta": "m1"}).build()` and assert `params[6].obj == {"meta": "m1"}`, matching the payload assertion strength.

---

### `sql/create_events_table.sql:8` — Read path now requires the metadata column; un-migrated deployments break on read
**Severity**: major | **Confidence**: high | **Lens**: architecture, compatibility

🟡 **Architecture / Compatibility**

Adding `metadata JSONB NOT NULL` with no DB default makes this a breaking schema change with a hard deploy-ordering dependency. The Postgres read path issues `SELECT *` into `class_row(StoredEvent[str, JsonValue])`, and `StoredEvent.metadata` is now a required constructor argument — so any running instance of the new code against an un-migrated `events` table fails on **both reads and writes**, not just writes, until the migration completes.

**Impact**: During a rolling deploy there is a window where new code is live but the column may not yet exist; without coordinated migrate-before-deploy ordering this is a system-wide failure, not graceful degradation.

**Suggestion**: The changelog correctly documents the migration. Make the release notes state explicitly that the migration must be applied **before** (or atomically with) rolling out the new library version, since both read and write paths now depend on the column.

---

### `src/logicblocks/event/types/event.py:92` — StoredEvent.metadata is a required constructor argument (breaking)
**Severity**: major | **Confidence**: high | **Lens**: compatibility

🟡 **Compatibility**

Unlike `NewEvent` (which keeps `metadata` optional via a custom `__init__` defaulting to `None`), `StoredEvent` gains a `metadata: Metadata` field with no default. Any consumer that constructs `StoredEvent(...)` directly — tests, fixtures, custom adapters, projection harnesses — will now raise `TypeError: missing required keyword-only argument 'metadata'`.

**Impact**: A hard backward-incompatible break for every direct `StoredEvent(...)` call site outside this library, surfacing at runtime/type-check after upgrade. (No such sites exist inside `src/` beyond those this PR updates.)

**Suggestion**: Acceptable for a 0.x library if clearly flagged. The changelog currently lists only the schema migration under "Breaking change" — add an explicit note that `StoredEvent.metadata` is now a required constructor argument.

---

### `src/logicblocks/event/types/event.py:112` — serialise() output gains a new `metadata` key
**Severity**: major | **Confidence**: medium | **Lens**: compatibility

🟡 **Compatibility**

`StoredEvent.serialise()` (and `NewEvent.serialise()`) now always emit a new `"metadata"` key. Consumers that persist serialised events as snapshots/projections, or deserialise this output with strict schema validation (no `additionalProperties`, exhaustive key checks, or round-trip equality assertions), will see the new key.

**Impact**: Reading back an *old* serialised snapshot now finds the `metadata` key absent; a strict deserialiser of *new* output may reject the unexpected key — either direction can break consumers who snapshot `serialise()` output.

**Suggestion**: Call out in the changelog that `serialise()` output now contains a `metadata` key, and confirm whether any stored projection/snapshot format depends on the exact serialised shape.

---

### `src/logicblocks/event/types/event.py:32` — `metadata` field type does not reflect that it can be `None`
**Severity**: minor | **Confidence**: high | **Lens**: architecture, code-quality

🔵 **Architecture / Code Quality**

This is a deliberate divergence from the mirrored `Projection[State, Metadata]` template: `Projection.__init__` requires `metadata`, whereas `NewEvent.__init__` makes it `metadata: Metadata | None = None` and stores `None` as-is. The declared field type `metadata: Metadata` can therefore be violated at runtime — when `Metadata` is bound to e.g. `list[str]`, an omitted value yields `metadata is None`, which the field type does not admit. Downstream code narrowing `Metadata` gets no warning that `None` is possible.

**Impact**: Erodes the type-soundness the generic propagation is meant to provide, and creates an inconsistency between the two parallel metadata-carrying types.

**Suggestion**: The plan justifies the tradeoff (a generic field cannot carry a `{}` default), so it's acknowledged — but make it explicit: declare the field as `metadata: Metadata | None` to mirror the constructor reality, or add a short note on `NewEvent` documenting the omitted-equals-`None` contract.

---

### `changelog.d/20260626_135547_bivav.satyal_event_metadata.md:16` — Migration backfills `'{}'` but the app stores JSON `null` for omitted metadata
**Severity**: minor | **Confidence**: high | **Lens**: database, compatibility

🔵 **Database / Compatibility**

The migration backfills existing rows with `DEFAULT '{}'`, but the application persists omitted metadata as JSON `null` (`NewEvent.metadata` stays `None`). After migrating an existing deployment, pre-existing rows hold `{}` while newly written no-metadata rows hold `null` — two in-database representations of "no metadata" that deserialise to different Python values (`{}` vs `None`).

**Impact**: Consumers comparing or branching on metadata see an inconsistency between historical (`{}`) and new (`None`) rows for the absent case; round-trip equality on old rows won't match the `None` default.

**Suggestion**: Choose one canonical representation. If JSON `null` is canonical (matching the app), backfill with `DEFAULT 'null'::jsonb` instead of `'{}'`; if `{}` is canonical, coerce `None` → `{}` at the type layer. Document the chosen representation in the changelog.

---

### `changelog.d/20260626_135547_bivav.satyal_event_metadata.md:16` — Add `lock_timeout` guidance for the ALTER TABLE
**Severity**: minor | **Confidence**: medium | **Lens**: database

🔵 **Database**

`ALTER TABLE events ADD COLUMN` takes a brief `ACCESS EXCLUSIVE` lock. The operation itself is catalog-only (fast) in Postgres 11+, but the lock request queues behind any long-running transaction on `events` and, while queued, blocks all subsequent queries on that table.

**Impact**: On a busy events table, running the migration during a long transaction can cause a short stall (lock convoy) despite the DDL being fast once the lock is acquired.

**Suggestion**: Recommend running the migration with a bounded `lock_timeout` (e.g. `SET lock_timeout = '2s';`) and a retry, and note that the migration must run before/atomically with the new-version rollout.

---

### `src/logicblocks/event/store/store.py:66` — `publish` docstring not updated for the new metadata field
**Severity**: minor | **Confidence**: high | **Lens**: documentation

🔵 **Documentation**

The `publish` docstring still reads "Publish a sequence of events into the stream." with no mention that events now carry an optional `metadata` bag, nor that omitting it leaves `metadata` as `None` (stored as JSON null) rather than `{}`. This is the primary public API surface a consumer reads.

**Impact**: A consumer reading the docstring alone will not discover the new capability or its `None`-vs-empty semantics, and must read the source or changelog.

**Suggestion**: Add a sentence to the `publish` docstring (and the category overload at ~line 216) noting that each event may include arbitrary serialisable `metadata`, and that omitted metadata stays `None`.

---

## Per-Lens Results

### Architecture

**Summary**: Threads a new `Metadata` type parameter through the event types, builders, both storage adapters, and the public publish/save API, closely mirroring the established `Projection[State, Metadata]` template. Structurally sound, consistent, purely additive at the type level, and preserves functional-core/imperative-shell separation. Main tensions: deliberate divergence from the `Projection` template (nullable-by-default) and a deploy-ordering coupling introduced by the breaking schema change.

**Strengths**: Faithful mirror of `Projection`/`projections.metadata`; purely additive generic propagation via `JsonValue` default; strong functional-core separation so behaviour is symmetric across adapters; read path needs no change (auto-mapped by `class_row`); deliberate `summarise()` exclusion.

**Comments**: `event.py:32` (nullable-by-default divergence, minor); `sql/create_events_table.sql:8` (deploy-ordering break, major).

**General Findings**: Implicit by-name coupling between `StoredEvent` field set and `events` columns (reinforced, pre-existing); `Metadata` generic stops at the write boundary — no symmetric read-side typing (acknowledged scope boundary).

### Code Quality

**Summary**: Mechanical, highly consistent change mirroring `Payload` exactly; well covered by tests. Main maintainability concern is the type-soundness gap (non-optional field that stores `None`).

**Strengths**: Consistent mirroring of `Payload` and `Projection`; uniform builder threading; full-field-equality test style; thoughtful `summarise()` exclusion.

**Comments**: `event.py:32` (field type doesn't reflect `None`, minor).

**General Findings**: Repeated three-line metadata serialisation block across the memory adapter's four construction sites (pre-existing duplication, candidate for a shared helper); wide repetitive generic signatures (faithful to convention, but boilerplate scales with each new generic).

### Correctness

**Summary**: Metadata threaded cleanly and symmetrically; the `None`-defaulting / JSON-null round-trip is logically sound across both adapters. Genuine defect: the builders' `metadata or ...` (and pre-existing `payload or ...`) discards falsy-but-valid values. The Postgres unit test no longer asserts the serialised metadata value, only its type.

**Strengths**: `None` round-trips correctly (JSON null in NOT NULL column); `__hash__`/`__eq__` stay consistent after `__repr__` change; `summarise()` correctly excludes metadata; read path needs no change.

**Comments**: `builders.py:64` and `builders.py:188` (falsy `or` coalescing, major ×2); `test_adapter.py:301` (type-only assertion, minor→merged major).

**General Findings**: Existing persisted rows depend on the changelog migration to satisfy `NOT NULL`; migrated `{}` rows vs application `None` differ — confirm intended.

### Test Coverage

**Summary**: Shared adapter equality tests extended so both adapters assert the random-populated metadata round-trips; focused builder/type/data unit tests added. But the central contract (omitted metadata stays `None`, round-trips as JSON null through the NOT NULL column) is only ever exercised against the in-memory adapter, never Postgres. A weakened Postgres unit assertion and missing edge cases reduce confidence relative to the schema-migration risk.

**Strengths**: Shared cases give memory + Postgres identical coverage; type-level tests cover serialise/repr/equality and `summarise()` exclusion; builder/data tests verify random-population parity and overrides.

**Comments**: `test_adapter.py:301` (assertion weakened from value to `isinstance`, major).

**General Findings**: `None`/omitted metadata never round-tripped through Postgres (major); edge cases `{}`, non-mapping, large bodies untested (minor); new dedicated store tests use single-field assertions, diverging from full-object-equality convention (minor).

### Database

**Summary**: Schema change is sound and the recommended migration is genuinely safe on large tables (catalog-only ADD COLUMN with constant default in PG 11+; catalog-only DROP DEFAULT). INSERT column list and value ordering match; storing JSON null in a NOT NULL JSONB column is correct. Main concern: data-semantics divergence between migrated `{}` rows and the app's `None`/JSON-null model.

**Strengths**: Safe, effectively zero-downtime migration; correct positional column/value ordering and 8→9 placeholder growth; JSON null in NOT NULL JSONB is correct and intentional; mirrors `projections.metadata`; follows the prior index-migration precedent.

**Comments**: `changelog:16` (backfill `{}` vs app `null`, minor); `changelog:16` (lock_timeout + ordering guidance, minor).

**General Findings**: No down-migration/rollback statement — `DROP COLUMN` is destructive; recommend documenting that app-only rollback can leave the column in place.

### Compatibility

**Summary**: Pre-1.0 library (0.1.11) whose changelog already frames the change as breaking. The additive third generic (`Metadata = JsonValue`) preserves existing parameterisations, but two genuine breaks exist: `StoredEvent.metadata` is a required constructor argument, and `serialise()` output gains a `metadata` key. The DB change is breaking for existing deployments but correctly announced.

**Strengths**: `Metadata` default preserves two-param parameterisations; `NewEvent(...)` call sites unaffected; migration announced with correct SQL; generic propagation mirrors `Payload` exactly.

**Comments**: `event.py:92` (required `StoredEvent.metadata`, major); `event.py:112` (new `serialise()` key, major); `sql/create_events_table.sql:8` (read path requires column, major); `changelog:16` (backfill `{}` vs `null`, minor).

**General Findings**: 0.x semver — a minor bump (e.g. 0.2.0), not a patch, is appropriate given multiple breaking changes; three-param generic widens the type surface but stays backward-compatible.

### Documentation

**Summary**: The changelog fragment is the strongest documentation here — accurate, with correct/complete migration SQL. Main gaps: the user-facing `None`-vs-`{}` semantics and the new `metadata` parameter aren't documented at the call sites consumers read (`publish` docstrings, `NewEvent`), and the README — which already advertised metadata — gains no example. The committed plan correctly lives under `meta/plans/` per project convention.

**Strengths**: Accurate changelog with correct, complete migration SQL; clearly states `None`/JSON-null semantics for upgraders; plan respects the `meta/` convention; `with_metadata` lacking a docstring is consistent with sibling `with_*` builder methods.

**Comments**: `store.py:66` (publish docstring not updated, minor); `event.py:32` (`None` semantics undocumented at the type, minor).

**General Findings**: README advertises metadata but shows no worked example; the plan references a deleted design spec (dangling reference).

---
*Review generated by /accelerator:review-pr*
