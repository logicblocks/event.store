---
type: plan-review
id: "2026-10-05-planner-friendly-postgres-query-rendering-review-1"
title: "Plan Review: Planner-Friendly Postgres Query Rendering Implementation Plan"
date: "2026-10-05T13:45:25+00:00"
author: "Paul Svalin"
producer: review-plan
status: complete
target: "plan:2026-10-05-planner-friendly-postgres-query-rendering"
reviewer: "Paul Svalin"
verdict: REVISE
lenses: [architecture, code-quality, test-coverage, correctness, database, performance, compatibility, documentation]
review_number: 1
review_pass: 1
tags: ["postgres", "projection", "query", "converters", "indexes", "performance"]
last_updated: "2026-10-05T13:45:25+00:00"
last_updated_by: "Paul Svalin"
schema_version: 1
---

## Plan Review: Planner-Friendly Postgres Query Rendering Implementation Plan

**Verdict:** REVISE

The plan's diagnosis of Postgres planner behaviour is accurate. It works through existing seams:
`Literal` sits next to `Constant`, the `name` policy lives in the projection adapter, and filters
are overridden by type in the registry. The four-PR split is clean and each PR can be reviewed on
its own. Two verified correctness bugs need fixing before implementation:
- inline literals containing `%` break psycopg's placeholder parsing;
- list values never reach the PR 4 `Jsonb` branch whole.

The generic-plan integration tests are also not yet built to prove what they claim. And PR 1's README
advice is incomplete on its own: it omits the generic-plan caveat and several production hazards of
creating indexes.

### Cross-Cutting Themes

- **No reusable projection default converter** (flagged by: architecture, code-quality, compatibility,
  documentation). The `name` override is built inline in the adapter. A caller who supplies a custom
  converter must rebuild it by hand, and the README snippet has no import path. `FilterClauseConverter`
  is not currently exported from `persistence.postgres`.
- **The decision to render a value as a literal bypasses `value_for_path`** (flagged by: code-quality,
  test-coverage, correctness). This splits value rendering between two places. It also skips the
  `has_value()`/`empty` handling, so `name` equal to `None` would render `"name" IS NULL NULL`.
- **Plan-shape tests can pass vacuously or flake** (flagged by: test-coverage, database). There are
  three causes:
  - the test asserts only on the index name;
  - the `(name, k)` index is usable on `name` alone;
  - the table is empty and unanalysed.

  `PREPARE` with `%s` placeholders is also underspecified.
- **Inline literals are unsafe for special characters** (flagged by: correctness, test-coverage). `%`
  breaks placeholder parsing (verified), and no tests cover quotes, backslashes or `%`.
- **Rendering changes aren't flagged as breaking** (flagged by: compatibility, documentation). The repo
  has a `### Breaking changes` precedent. `Jsonb` params have no `__eq__`, so downstream tests that
  compare params need rewriting, not just new expected values.
- **Index guidance omits the `jsonb_extract_path_text` operators** (flagged by: documentation,
  database). Regex and IS [NOT] NULL render `_text`, but the README says "never `_text`".
- **The `is_json_native` design** (flagged by: architecture, code-quality, correctness,
  compatibility). It duplicates `types/json.py` guards and accepts `Mapping`/`Sequence` types that
  `json.dumps` can't serialise. It also matches subclasses, so consumer-registered dumpers are
  bypassed.
- **Postgres-internals claims rest on a private research doc** (flagged by: documentation, database).

### Tradeoff Analysis

- **Narrow API vs extensibility.** Architecture would like `literal_value_paths` to become a
  bind/inline policy object. Code-quality and the plan's own scope favour the minimal knob. My
  recommendation: keep the knob, and record nested inlining as out of scope.
- **Determinism vs realism in plan tests.** `enable_seqscan=off` on an empty table is deterministic
  but cost-fragile. Seeding data and running ANALYZE is realistic but slower. My recommendation: seed
  a small skewed dataset, run ANALYZE, and assert on `Index Cond` via `EXPLAIN (GENERIC_PLAN, FORMAT JSON)`.

### Findings

#### Major

- 🟡 **Correctness + Test Coverage**: Inline literals containing `%` are re-parsed as placeholders, and quoting is untested
  **Location**: PR 2 §1 Literal node; PR 3 §1
  `sql.Literal('50%x')` raises `ProgrammingError` once params are passed, and `'a%%b'` silently becomes `'a%b'` (verified locally). Keys and `name` values are now inlined, so `%` must be escaped to `%%`. There are also no tests for quotes, backslashes or `%`.
- 🟡 **Correctness**: CONTAINS with a list is split element by element, so `Jsonb([...])` never happens
  **Location**: PR 4 §1 Value rendering / §3 Unit tests
  `FilterClauseQueryApplier.value` calls `is_multi_valued` for every operator (`clause.py:52`), so a list becomes `@> (%s, %s)`. The planned "list value with CONTAINS" test, and the plan's claim that lists are handled, don't match what the code does.
- 🟡 **Architecture + Code Quality + Compatibility + Documentation**: Custom `query_converter` callers can't start from the projection default
  **Location**: PR 3 §2 Projection adapter wiring; §5 README
  Expose a named factory (e.g. `default_projection_query_converter(table_settings)`). Commit to exporting `FilterClauseConverter`, and give a complete README snippet that includes imports.
- 🟡 **Code Quality + Test Coverage + Correctness**: The literal branch splits value rendering and bypasses IS NULL handling
  **Location**: PR 3 §1 Filter converter option; §3 Unit tests
  Make the literal-vs-bound choice inside `value_for_path`, after its `has_value()` check. Test `name` with `None` (both EQUAL and NOT_EQUAL), and IN with `None`.
- 🟡 **Database + Test Coverage**: The non-partial-index plan test can pass before the change and is cost-fragile
  **Location**: PR 2 §4 Integration tests; PR 3 §4
  `Index Cond: (name = $1)` still names the `(name, k)` index. To make the test discriminate:
  - assert that the `Index Cond` contains `jsonb_extract_path`;
  - seed rows and run ANALYZE;
  - keep a permanent negative control.
- 🟡 **Database**: The `PREPARE` approach doesn't say how `%s` placeholders and parameter types are handled
  **Location**: PR 2 §4 Integration tests
  Use `EXPLAIN (GENERIC_PLAN)`, which the CI database (Postgres 16.3) supports, or `PREPARE` with explicit types and `$n` renumbering.
- 🟡 **Documentation**: PR 1's README guidance is incomplete if PRs 2 and 3 aren't merged
  **Location**: PR 1 README
  With today's bound keys and `name`, the recommended indexes stop matching under generic plans. State that caveat and a workaround (`prepare_threshold=None` / `force_custom_plan`). Later PRs should replace it.
- 🟡 **Database**: README index examples omit `CONCURRENTLY` for a live, write-heavy table
  **Location**: PR 1 README, Recommended strategies
- 🟡 **Database**: btree expression indexes on non-scalar JSON values can make upserts fail
  **Location**: PR 1 README, Index expressions
  The btree row limit is about 2704 bytes. Restrict the advice to bounded scalar keys.
- 🟡 **Database**: The partitioning advice omits a DEFAULT partition and the table rewrite needed to migrate
  **Location**: PR 1 README, PARTITION BY LIST

#### Minor

- 🔵 **Documentation + Database**: Index guidance should map operators to `jsonb_extract_path` vs `jsonb_extract_path_text`
  **Location**: PR 1 README
- 🔵 **Compatibility + Documentation**: Rendering changes should go under `### Breaking changes`, and note that `Jsonb` params lack value equality
  **Location**: PR 2 and PR 4 changelog fragments; Migration Notes
- 🔵 **Documentation**: Changelog fragments omit the newly exported `Literal` / `FilterClauseConverter`
  **Location**: PR 2 and PR 3 changelog fragments
- 🔵 **Architecture + Code Quality**: Validation hard-codes `Path("source")` in a second place, and it's unclear whether the check runs at construction or in the applier
  **Location**: PR 3 §1
- 🔵 **Architecture + Code Quality + Correctness + Compatibility**: `is_json_native` duplicates `types/json.py`, accepts `Mapping`/`Sequence` types that `json.dumps` rejects, and matches subclasses
  **Location**: PR 4 §1
  Match concrete `dict`/`list`/`tuple` types and build on `is_json_value`.
- 🔵 **Code Quality + Architecture + Compatibility**: `Literal` is easily confused with `typing.Literal` and `Constant`, and is typed `Any`
  **Location**: PR 2 §1
  Consider `InlineLiteral` with a scalar type, or keep it unexported.
- 🔵 **Architecture**: `literal_value_paths` is a narrow option; record that nested inlining is out of scope
  **Location**: PR 3 §1
- 🔵 **Code Quality**: `value_for_nested_path` keeps dead `path`/`operator` parameters
  **Location**: PR 4 §1
- 🔵 **Test Coverage**: The recursion boundaries of `is_json_native` (dict, nested, non-str keys, bytes, tuple, a list with a datetime) aren't tested directly
  **Location**: PR 4 §3
- 🔵 **Test Coverage + Correctness**: Shared cases can diverge between the in-memory and Postgres adapters (IndexError, `True == 1`, an object key "0")
  **Location**: PR 2 §4; PR 4 §2
- 🔵 **Test Coverage**: The new key-set paging test could lock in the known mixed-direction bug
  **Location**: PR 2 §3
- 🔵 **Performance**: Index-capable generic plans may be adopted more often under `plan_cache_mode = auto`
  **Location**: Migration Notes
- 🔵 **Performance**: Prepared-statement cache sizing is per pooled connection and isn't quantified
  **Location**: Performance Considerations, PR 3
- 🔵 **Database**: "Every filter is estimated at 0.5%" is only true for equality
  **Location**: PR 1 README, The trap
- 🔵 **Documentation + Database**: Claims about Postgres internals (`make_build_data`) need public citations and a mitigation
  **Location**: PR 1 README
- 🔵 **Documentation**: The `### Documentation` changelog category isn't a scriv default, and the fragment filename shouldn't be fixed in advance
  **Location**: PR 1 changelog fragment

#### Suggestions

- 🔵 **Database**: PR 4's extended-statistics benefit applies only under custom plans. Credit the gain to dropping the STABLE `to_jsonb` wrapper.
- 🔵 **Database**: Composite `(name, expr)` indexes give table-wide statistics for the expression, not per-type statistics.
- 🔵 **Performance**: Record `EXPLAIN (ANALYZE, BUFFERS)` latency before and after each PR, not just whether the index is used.
- 🔵 **Performance**: `is_json_native` walks containers twice (negligible).
- 🔵 **Test Coverage**: Pin string escaping (`"`, `\`, non-ASCII) and dict CONTAINS before PR 4.
- 🔵 **Correctness**: `bool` path segments render as `'True'`. Reject them or convert them with `int()`.
- 🔵 **Architecture**: PR 2's converter-level plan test belongs with the persistence/postgres integration tests.
- 🔵 **Documentation**: State the README fallback (when PR 1 hasn't merged) once, for all code PRs.
- 🔵 **Documentation**: Connect the README to the shipped `sql/create_projections_indices.sql`.

### Strengths

- ✅ The analysis of planner internals is accurate:
  - VARIADIC `ARRAY['k'::text]` folds to a `Const` when the keys are literals;
  - `examine_variable` skips statistics from partial indexes;
  - the STABLE `to_jsonb` blocks MCV matching.
- ✅ The changes go through existing seams. `Literal` fits the `Expression` hierarchy, and PR 2 changes one line in a shared helper, which every caller picks up. Filters are overridden by type in the registry, so no new extension point is needed.
- ✅ The `name` policy is scoped to the projection adapter, so the subscriber and subscription stores and custom converters are unaffected.
- ✅ The PRs are split cleanly, with explicit dependencies, and each carries its own README and changelog update.
- ✅ The tests are designed test-first, with shared cases that pin semantics against real Postgres before the change. The integer-segment bug fix gets regression tests at both levels.
- ✅ No schema changes, no new required arguments, and psycopg `>=3.2.3` already supports `sql.Literal`.
- ✅ Tradeoffs and non-goals are stated openly: PR 4's weak benefit, the cost to the prepared-statement cache, and no shipped indexes.
- ✅ `str()` on integer segments correctly targets `text[]`, and `sql.Literal` quotes `'` and `\` safely.

### Recommended Changes

1. **Escape `%` in the `Literal` node** (addresses: inline `%` literals). Render via `as_literal(...).replace(b"%", b"%%")` in a small `Composable`, or an equivalent. Add unit and integration cases for `'`, `\`, `%`, `%s` and `%%` in keys and in `name`.
2. **Fix PR 4's list handling** (addresses: CONTAINS list splitting). Either restrict `is_multi_valued` splitting to `IN`, and pin array-containment semantics with integration tests, or remove lists from PR 4's claims and tests.
3. **Route literal rendering through `value_for_path`** (addresses: literal branch / IS NULL, the source validation). Add a `supports_literal_value(path)` predicate in `helpers.py`, run validation in `__init__`, and add None/NOT_EQUAL tests.
4. **Add a projection default converter factory and export `FilterClauseConverter`** (addresses: custom converter theme, the README snippet).
5. **Rework the plan-shape tests** (addresses: tests that pass vacuously, PREPARE mechanics, empty table):
   - use `EXPLAIN (GENERIC_PLAN, FORMAT JSON)`;
   - seed rows and run ANALYZE;
   - assert that the `Index Cond` contains the expression;
   - keep a permanent negative-control case.
6. **Strengthen PR 1's README** (addresses: missing generic-plan caveat, CONCURRENTLY, btree size, partitioning, `_text` mapping, 0.5% wording, citations):
   - add the generic-plan caveat and workaround;
   - use `CREATE INDEX CONCURRENTLY`;
   - restrict the advice to scalar keys;
   - add a DEFAULT partition and describe the migration;
   - map operators to expressions;
   - give public sources for the Postgres internals.
7. **Changelog hygiene** (addresses: breaking changes, exports, category):
   - add `### Breaking changes` to PRs 2 and 4, including the `Jsonb` `.obj` comparison note;
   - add an Added entry for `Literal` / `FilterClauseConverter`;
   - use a scriv-default category for PR 1.
8. **Tighten `is_json_native`** (addresses: is_json_native design, dead params, boundary tests):
   - match concrete types and build on `is_json_value`;
   - drop the unused parameters;
   - add parameterised boundary tests.
9. **Smaller items:**
   - add notes on `plan_cache_mode = auto` and the prepared-statement cache;
   - note that PR 4's benefit applies only under custom plans;
   - make shared fixtures adapter-neutral;
   - limit the key-set test to a single sort direction;
   - reject `bool` path segments;
   - define the README fallback once.

## Per-Lens Results

### Architecture

**Summary**: The plan is structurally sound. Each change goes through an existing seam, which keeps the generic converter free of projection rules and leaves the subscriber and subscription stores untouched. The remaining concerns are about how it evolves: the narrow `literal_value_paths`, more special-casing of `source`, no reusable projection default, and a duplicated JSON type guard.

**Strengths**: The `name` policy lives in the projection adapter, not in `DelegatingQueryConverter`. The change reuses the registry's overwrite-by-type extension point. `Literal` fits the `Expression` hierarchy and is hashable, which key-set paging needs because it uses sort expressions as dict keys. Path rendering changes in one place. The PRs are split cleanly with explicit dependencies, and tradeoffs are stated openly.

**Findings**:
- 🟡 major / high: **Custom query_converter callers cannot start from the projection default** (PR 3 §2). The override is built inline and applies only when `query_converter is None`. Customising the converter silently brings the generic-plan problem back. Expose `default_projection_query_converter(table_settings)`.
- 🔵 minor / medium: **Validation hard-codes `Path("source")` into the generic filter converter** (PR 3 §1). This repeats the special case from `helpers.py:70-73`, and the plan is unclear whether the check belongs to the constructor or the applier. Validate once in `__init__` from a shared predicate.
- 🔵 minor / medium: **`literal_value_paths` is a narrow knob rather than a value-rendering strategy** (PR 3 §1). Inlining nested discriminators would mean reworking it. Either make it a policy object, or record that nested inlining is out of scope.
- 🔵 minor / high: **`is_json_native` duplicates the existing JSON type guard** (PR 4 §1). `types/json.py:34-73` already has `is_json_value`. Build a small `is_jsonb_bindable` on top of it.
- 🔵 suggestion / medium: **`Constant` and `Literal` names don't convey bind versus inline** (PR 2 §1). Consider `InlineLiteral`.
- 🔵 suggestion / low: **The generic-plan test for converter behaviour sits under the projection adapter tests** (PR 2 §4). Move PR 2's case to the persistence/postgres integration tests.

### Code Quality

**Summary**: The plan is well scoped and proportionate, and it follows existing patterns. The maintainability risks are concentrated in PR 3: the literal decision is split across two places, the validation is duplicated, and the opt-in has to be copied by hand. PR 4 leaves dead parameters behind and adds an overlapping predicate.

**Strengths**: `Literal` matches the shape of `Constant`/`Raw`. PR 2 changes one line in a shared helper. PR 3 uses override-by-type rather than a new extension point. PR 4 uses a single `match` type-dispatch rather than catching `TypeError`. The four PRs are each small.

**Findings**:
- 🟡 major / medium: **Literal-vs-bound decision split between applier and value_for_path** (PR 3 §1). The new branch duplicates the multi-valued loop and skips the `has_value()`/`empty` handling. Pass a node factory into `value_for_path`, or swap `Constant` for `Literal` after it returns.
- 🔵 minor / medium: **Validation rule duplicates value_for_path's special cases** (PR 3 §1). Add a `supports_literal_value(path)` predicate in `helpers.py`, and make the error message name the path and the reason.
- 🔵 minor / medium: **Custom-converter callers must copy the name opt-in by hand** (PR 3 §2, §5). Extract a named factory.
- 🔵 minor / high: **Conditional export of FilterClauseConverter should be a firm decision** (PR 3 §2). It is not currently exported, so add it to `__init__.py` and `__all__`.
- 🔵 minor / medium: **Literal is an overloaded name with an untyped value** (PR 2 §1). Narrow the type to `str | int | float | bool`, and consider renaming it.
- 🔵 minor / high: **value_for_nested_path keeps dead parameters after the rewrite** (PR 4 §1). Drop `path` and `operator`, and optionally rename the function `jsonb_value`.
- 🔵 suggestion / low: **is_json_native overlaps is_multi_valued's str/bytes/Sequence discrimination** (PR 4 §1). Make it private and reuse `is_multi_valued`.

### Test Coverage

**Summary**: The plan takes testing seriously. It is test-first, pins semantics against real Postgres, and adds plan-shape tests. The gaps are: the plan-shape tests are hard to make deterministic and their negative control is manual; quoting and literal-path edge cases are untested; and the `is_json_native` recursion and the parity between the adapters are weak.

**Strengths**: Shared cases are run before the rendering change. The integer-segment fix has regression tests at both levels. The plan-shape tests check the actual goal. PR 4 tests the fallback types. PR 3 tests both sides of the adapter wiring as well as validation.

**Findings**:
- 🟡 major / medium: **Plan-shape tests may be non-deterministic, and their negative control is manual and lost after merge** (PR 2 §4 / PR 3 §4). With an empty table, the primary key competes with the new index. Seed rows, run ANALYZE, and assert on `Index Cond`. Add a permanent negative control, and use `EXPLAIN (GENERIC_PLAN)`.
- 🟡 major / high: **No tests for inline literal quoting of keys or names containing quotes or special characters** (PR 2 §3 / PR 3 §3). Add a `Literal("it's")` → `'it''s'` unit test and shared adapter round-trip cases.
- 🟡 major / medium: **IS NULL / IS NOT NULL and non-value operators on a literal path are not tested** (PR 3 §1, §3). A naive implementation would render `"name" IS NULL NULL`. Add parameterised cases.
- 🔵 minor / high: **Recursive is_json_native boundaries are not directly tested** (PR 4 §1, §3). Cover dict, nested, non-str keys, tuple, bytes, and a list containing a datetime or NaN.
- 🔵 minor / medium: **Shared cases can diverge between in-memory and Postgres adapters on edge data** (PR 2 §4 / PR 4 §2). The in-memory adapter raises IndexError or ValueError where Postgres returns NULL, and Python treats `True == 1`.
- 🔵 minor / medium: **New key-set paging unit test could lock in the known mixed-direction bug** (PR 2 §3). Limit the test to a single sort direction.
- 🔵 suggestion / medium: **Add string-escaping and object-containment cases to the semantic pinning set** (PR 4 §2).

### Correctness

**Summary**: Most of the core reasoning holds up. `sql.Literal` quotes safely, and converting integer segments to text fixes the smallint bug. There are two correctness gaps: inline literals aren't `%`-escaped, which psycopg then re-parses (verified locally), and list values are always split before PR 4's `Jsonb` branch (verified at `clause.py:52`). Smaller gaps: IS NULL with literal paths, a mismatch between `is_json_native` and `json.dumps`, and integer-segment divergence between the adapters.

**Strengths**: `str()` on segments correctly targets `text[]`, including negative indices. `sql.Literal` doubles `'` and uses `E''` for backslashes without a connection, and renders `str` with no cast. `Jsonb` and `to_jsonb` produce equivalent scalars. Non-finite floats are correctly excluded. Parameter order is preserved. The PR 3 registry chaining is valid.

**Findings**:
- 🟡 major / high: **Inline literals containing '%' are re-parsed as placeholders by psycopg** (PR 2 §1). `'50%x'` raises `ProgrammingError`, `%s` inside a key adds a phantom placeholder, and `'a%%b'` silently becomes `'a%b'`. Escape `%` to `%%` in the node, and add tests.
- 🟡 major / high: **List values never reach Jsonb whole; CONTAINS with a list is split element by element** (PR 4 §1, §3). Either restrict splitting to IN, or remove lists from PR 4's claims.
- 🔵 minor / high: **Rendering literal-path values directly bypasses the has_value / IS NULL handling** (PR 3 §1). Make the literal choice inside `value_for_path`.
- 🔵 minor / high: **is_json_native accepts Mapping/Sequence types that json.dumps cannot serialise** (PR 4 §1). For example `MappingProxyType`, `deque`, `range` and `bytearray`. Match concrete types.
- 🔵 minor / medium: **Integer path segments behave differently in the in-memory and Postgres adapters** (PR 2 §4). Include a record with a short array in the fixtures, and decide whether to align the adapters.
- 🔵 suggestion / medium: **str() on bool segments yields 'True'/'False'** (PR 2 §2). Reject `bool` segments or convert them with `int()`.

### Database

**Summary**: The core Postgres planner behaviour is right. The risks are elsewhere. The PR 2 test can pass before the change, because the `(name, expr)` index is usable on `name` alone. And the README leaves out production hazards: CREATE INDEX blocking writes, the btree row-size limit, and a missing DEFAULT partition rejecting inserts.

**Strengths**: The VARIADIC Const folding analysis, the `examine_variable` partial-index citation and the STABLE `to_jsonb` diagnosis are all correct. Literal inlining is scoped to the low-cardinality `name`. `str(sub_level)` targets `text[]`. No schema changes are shipped.

**Findings**:
- 🟡 major / high: **Non-partial index plan test can pass before the change** (PR 2 §4). Assert that the `Index Cond` contains `jsonb_extract_path`, or use a single-column expression index.
- 🟡 major / medium: **PREPARE approach doesn't say how %s placeholders and parameter types are handled** (PR 2 §4). Use `EXPLAIN (GENERIC_PLAN)`, or explicitly typed `PREPARE` with `$n` renumbering.
- 🔵 minor / medium: **Plan tests on an empty, unanalysed table are cost-fragile** (PR 2/3 §4). Also, Postgres 18 changed how `enable_seqscan` works. Seed rows and run ANALYZE.
- 🟡 major / high: **README index examples omit CONCURRENTLY for a live, write-heavy table** (PR 1 README). Also cover recreating INVALID indexes and the rules for partitioned tables.
- 🟡 major / high: **Btree expression indexes on non-scalar JSON values can make upserts fail** (PR 1 README). Restrict the advice to bounded scalars, and point to GIN `jsonb_path_ops` for containment.
- 🟡 major / high: **Partitioning advice omits DEFAULT partition and table-rewrite migration** (PR 1 README). Also note that a literal `name` enables plan-time pruning.
- 🔵 minor / medium: **"Every filter is estimated at 0.5%" is only true for equality** (PR 1 README).
- 🔵 minor / high: **Guidance should cover the jsonb_extract_path_text rendering for IS NULL and regex** (PR 1 README).
- 🔵 minor / low: **Version-specific make_build_data leak claim needs a citation and a mitigation** (PR 1 README).
- 🔵 suggestion / medium: **PR 4's extended-statistics benefit disappears in the generic-plan scenario that motivates PRs 2–3** (PR 4 Overview / Key Discoveries).
- 🔵 suggestion / medium: **Composite (name, expr) indexes still give whole-table, not per-type, expression statistics** (PR 1 README).

### Performance

**Summary**: The change adds little client-side work. The gaps are second-order effects: statement-cache growth per pooled connection, `plan_cache_mode = auto` switching to generic plans more often, and verification that checks index use rather than latency.

**Strengths**: Only low-cardinality values are inlined. The cost to the prepared-statement cache is already acknowledged. The `to_jsonb` fallback avoids control flow driven by exceptions. PR 2 sends fewer params per execution.

**Findings**:
- 🔵 minor / medium: **Index-capable generic plans may be adopted more often under plan_cache_mode = auto** (Migration Notes / Performance Considerations). Document `force_custom_plan` as an escape hatch.
- 🔵 minor / medium: **Prepared-statement cache sizing is per pooled connection and not quantified** (Performance Considerations, PR 3). Give a sizing rule (shapes × names × 2), and mention `prepared_max`.
- 🔵 suggestion / medium: **Verification checks plan shape but not latency** (Manual Testing Steps). Record `EXPLAIN (ANALYZE, BUFFERS)` before and after each PR.
- 🔵 suggestion / low: **is_json_native walks container values twice before encoding** (PR 4 §1). This is negligible.

### Compatibility

**Summary**: The plan is careful about compatibility: no schema changes, purely additive API, a narrowly scoped wiring change, and unchanged semantics. The remaining gaps are in how the change to the rendered-SQL and params contract is communicated, the default and custom converters now rendering `name` differently, and PR 4 bypassing custom psycopg dumpers.

**Strengths**: Old and new versions are safe during a rolling deploy. `literal_value_paths` is optional with a default. The subscriber and subscription stores are untouched. PR 4 keeps `to_jsonb` for non-native types. The psycopg `>=3.2.3` bound is sufficient. Bundling into the 0.1.12 prerelease matches existing practice.

**Findings**:
- 🔵 minor / medium: **Rendered SQL contract changes filed under 'Changed' rather than the project's 'Breaking changes' category** (PR 2/4 changelog).
- 🔵 minor / high: **Param type change to Jsonb (no value equality) breaks consumer param assertions harder than text changes** (PR 4 changelog / Migration Notes).
- 🔵 minor / medium: **Default and caller-supplied converters now render the name filter differently, with no reusable default to opt into** (PR 3 §2).
- 🔵 suggestion / low: **Jsonb path bypasses consumer-registered psycopg dumpers and follows the global json dumps setting** (PR 4 §1).
- 🔵 suggestion / low: **Exporting `Literal` from the package widens public API and clashes with `typing.Literal`** (PR 2 §1).

### Documentation

**Summary**: The plan treats documentation as part of each PR, and the changelog entries are concrete. The main accuracy gap is that PR 1 omits the generic-plan limitation of today's rendering. The changelog fragments also miss new exports and the "Breaking changes" precedent, and the custom-converter guidance has no import path.

**Strengths**: The README section is placed where the project's how-to sections already live. Each PR updates the README only for what it changes. Changelog entries are searchable and quote the exact error. The Migration Notes address consumers directly, and the non-goals are explicit.

**Findings**:
- 🟡 major / high: **PR 1 README guidance is incomplete if PRs 2/3 are not merged** (PR 1 README).
- 🔵 minor / high: **Index-matching guidance doesn't say which operators render jsonb_extract_path_text** (PR 1 README).
- 🔵 minor / high: **Custom-converter opt-in instructions lack an import path and a working snippet** (PR 3 README / wiring).
- 🔵 minor / high: **Changelog fragments omit newly exported public API** (PR 2/3 changelog).
- 🔵 minor / medium: **Downstream-breaking rendering changes are not signposted per project precedent** (PR 2/4 changelog; Migration Notes).
- 🔵 minor / medium: **'Documentation' changelog category and fragment name diverge from project conventions** (PR 1 changelog).
- 🔵 minor / medium: **Claims about Postgres internals rest on a private external research doc** (PR 1 README; Overview).
- 🔵 suggestion / medium: **README fallback when PR 1 hasn't merged is only defined for PR 2** (Overview; PR 3/4 README).
- 🔵 suggestion / medium: **README doesn't relate its guidance to the shipped sql/create_projections_indices.sql** (Non-goals; PR 1 Manual Verification).

---
*Review generated by /accelerator:review-plan*
