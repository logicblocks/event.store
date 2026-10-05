---
type: plan
id: "2026-10-05-planner-friendly-postgres-query-rendering"
title: "Planner-Friendly Postgres Query Rendering Implementation Plan"
date: "2026-10-05T11:45:11+00:00"
author: "Paul Svalin"
producer: create-plan
status: draft
tags: ["postgres", "projection", "query", "converters", "indexes", "performance"]
revision: "1f6c4482722e008e233c755d02f3c8c40c64f7ce"
repository: "event.store"
last_updated: "2026-10-05T13:55:27+00:00"
last_updated_by: "Paul Svalin"
last_updated_note: "Addressed review 1: percent-safe InlineLiteral, literal rendering via value_for_path, default_projection_query_converter, discriminating generic-plan tests, PR 1 README hazards, breaking-change changelog entries, concrete-type jsonb predicate"
schema_version: 1
---

# Planner-Friendly Postgres Query Rendering Implementation Plan

## Overview

Change how the Postgres query converters render JSON paths, values and the projection `name` filter,
so that Postgres can use expression indexes and their statistics. Also document how to index the shared
`projections` table.

The work ships as **four PRs**. Each can be reviewed, accepted, or declined on its own merits:

| PR | Contents | Depends on | Risk |
|---|---|---|---|
| **1. Docs: indexing the projections table** | README section on the partial-index statistics trap and the recommended strategies | — | None (docs only) |
| **2. Literal JSON path keys** | Render path keys inline; fixes integer path segments | — | Low: SQL text changes only, plus a bug fix |
| **3. Literal projection `name`** | `FilterClauseConverter(literal_value_paths=…)`, wired into the projection adapter | PR 2 | Low; adds API surface |
| **4. jsonb value encoding** | Bind JSON-native values as `Jsonb` instead of `to_jsonb(CAST(… AS text))` | — | Medium: changes value encoding; weakest benefit |

- **Ordering.** PR 1 opens the discussion with the maintainers. PRs 2 and 4 are independent of each
  other. PR 3 is stacked on PR 2, because without literal path keys no expression matches an index
  under a generic plan, so PR 3's generic-plan test can't pass.
- **README.** Each code PR updates the README section from PR 1 for what it changes, replacing any
  caveat it removes rather than only appending. If the section doesn't exist yet (PR 1 unmerged or
  declined), the code PR creates a minimal one covering only its own behaviour.
- **Changelog.** Each PR adds its own changelog fragment, created with
  `mise run changelog:fragment:create` (the generated filename is used as is). Changes to rendered
  SQL or params go under `### Breaking changes`, following
  `changelog.d/20260626_135547_bivav.satyal_event_metadata.md`.
- **Base.** Every branch starts from the latest `main` (currently `1f6c448`, version `0.1.12a5`), except PR 3, which is stacked on PR 2.
- **Release.** Whichever PRs are merged can go out in the same next `0.1.12` prerelease.

**Motivating case.** compliance-journey-service had list queries with a ~700ms floor in prod (2–3.8s
locally). All its expression indexes were partial (`WHERE name = 'journey'`), and Postgres ignores the
statistics of partial indexes when estimating selectivity (`examine_variable` in `selfuncs.c`). Every
filter therefore got the 0.5% default, and the planner walked the wrong index. The service worked
around it with non-partial `(name, <expression>)` indexes. While investigating, it found three ways the
library's SQL shape makes the problem worse:

- **Bound path keys.** `jsonb_extract_path("state", $2)` matches no index under a generic plan.
  psycopg prepares statements after 5 executions, and `plan_cache_mode = auto` can then switch to
  generic. Locally this took 4.3s per query.
- **Bound `name`.** `"name" = $1` can't prove a partial index predicate under a generic plan.
- **`to_jsonb(CAST($v AS text))` values.** Multi-column statistics only match `expression op Const`
  clauses. For `name = 'journey' AND definition_type_key = 'onboarding'`, 8,611 actual rows were
  estimated at 2,707 with `to_jsonb(...)`, against 8,575 with a jsonb constant.

Full research: `co-compliance-journey-service`
`meta/research/codebase/2026-10-05-partial-expression-index-statistics-and-projection-search.md`.

## Current State Analysis

- **Rendering primitives** (`src/logicblocks/event/persistence/postgres/query.py`):
  - `Constant` renders `sql.SQL("%s")` with `[value]` (`:159-167`).
  - `FunctionApplication` renders the function name as an `sql.Identifier` (`:22-48`), and `Cast`
    renders its type name the same way (`:51-70`).
  - There is no node that renders an inline literal. `Raw` (`:170-175`) takes raw SQL text, so it is
    unsuitable for values.
  - `Constant` is exported from `logicblocks.event.persistence.postgres` (`__init__.py:6,45`).
    `FilterClauseConverter` is not; it is reachable only from `persistence.postgres.converters`.
- **psycopg re-parses composed SQL for placeholders.** Every adapter executes `cursor.execute(*query)`
  with a params list (never `None`), so psycopg scans the whole rendered text, including the contents of
  any inline literal, for `%` placeholders. Verified: `sql.Literal('50%x')` raises `ProgrammingError`, and
  `sql.Literal('a%%b')` silently becomes `'a%b'`. Any inline literal must therefore double `%`.
- **Path rendering** (`converters/helpers.py:9-30`). `expression_for_path` builds
  `jsonb_extract_path[_text]("state", Constant(k1), Constant(k2), …)`. `_text` is used when the
  operator's `comparison_type` is TEXT (regex, IS NULL, IS NOT NULL; `query.py:204-212`).
- **Value rendering** (`helpers.py:52-80`):
  - `value_for_path`:
    - returns `empty` for operators without a value;
    - returns `Constant(Jsonb(value.serialise()))` for `Path("source")`;
    - returns a bare `Constant` for TEXT comparisons and top-level paths;
    - otherwise calls `value_for_nested_path`.
  - `value_for_nested_path` renders `to_jsonb(CAST(%s AS text))` for `str` values and
    `to_jsonb(%s)` for everything else.
  - Multi-valued values are converted element by element (`clause.py:52-67`). `is_multi_valued` is
    true for any non-`str`/`bytes` `Sequence`, whatever the operator, so a `list` or `tuple` value never
    reaches `value_for_path` whole. CONTAINS with a list of more than one element therefore renders
    `@> (%s, %s)`, which is a row constructor and fails today.
  - JSON type guards already exist in `logicblocks.event.types.json` (`is_json_value` and friends,
    `:34-73`). They accept `None` and any `Sequence`/`Mapping` pattern match, so they are broader than
    what `json.dumps` can serialise.
- **Callers of the helpers:**
  - `FilterClauseQueryApplier` (`clause.py:20-75`);
  - `SortClauseQueryApplier.sortby` (`clause.py:121-128`), which renders the expression without an
    operator, so always `jsonb_extract_path`;
  - `SimilarityFunctionQueryApplier` (`function.py:19-34`);
  - `KeySetPagingClauseQueryApplier`, which reuses the sort expressions from `sortby_list`
    (`clause.py:159-675`).
- **Converter users:**
  - The projection store adapter: `projection/store/adapters/postgres.py:90-98`, using
    `postgres.QueryConverter(table_settings).with_default_converters()`.
  - The subscriber and subscription state stores. They use top-level filters only, so their SQL is
    unaffected.
  - The event store builds its own SQL and is unaffected.
- **Where the `name` filter comes from.** `ProjectionStore` adds
  `FilterClause(Operator.EQUAL, Path("name"), name)` in `locate`, `load` (`store.py:86-87,110-111`)
  and in callers' `search` filters. It renders as `"name" = %s`. The converter can't tell it apart from
  any other top-level filter.
- **Integer path segments are broken today.** `Path` allows `str | int` sub-levels
  (`query/utilities.py:6-12`). psycopg binds an `int` as `smallint`, and Postgres has no
  `jsonb_extract_path(jsonb, unknown, smallint)`. Verified against Postgres 16:
  `function jsonb_extract_path(jsonb, unknown, smallint) does not exist`. The text literal `'1'` works
  and indexes into arrays.
- **What psycopg's `Jsonb` handles.** It serialises `str`, `int`, `float`, `bool`, `list` and `dict`,
  and raises `TypeError` for `datetime`, `Decimal` and `UUID`. Those types currently reach Postgres
  through `to_jsonb(%s)`, which formats them server-side.
- **Tests:**
  - Unit tests assert on rendered SQL text and params:
    - `tests/unit/logicblocks/event/persistence/postgres/test_converters.py`, which covers filters,
      sorting, similarity, offset and key-set paging, CONTAINS and regex;
    - `test_query.py`, which hand-builds nodes and is unaffected unless new nodes are added.
  - Integration: `tests/integration/logicblocks/event/projection/store/adapters/test_postgres.py`
    runs the shared `ProjectionStorageAdapterCases`
    (`tests/shared/logicblocks/event/testcases/projection/store/adapters.py`) against Postgres. Not
    covered: bool values, `IN` with ints, integer path segments, regex, offset paging.
  - The integration database is `postgres:16.3` (`docker-compose.yml:3`), so `EXPLAIN (GENERIC_PLAN)`
    (added in Postgres 16) is available.
  - **In-memory and Postgres adapters differ on missing data.** The in-memory lookup
    (`memory/converters/types.py:33-66`) raises on a missing key or an out-of-range index, where Postgres
    returns NULL. Python also treats `True == 1`, while jsonb doesn't. Shared fixtures must contain every
    queried path and avoid bool/int distractors.
- **Reference SQL:** `sql/create_projections_table.sql`, plus `sql/create_projections_indices.sql`,
  which already ships `(name, source)` and `(name, id)` indexes.
- **Docs:** `README.md` has a Usage section, with "Finalising State" (`:101-136`) as the precedent for
  a focused how-to section. The docs site (`docs/index.md`, mkdocs) holds only an example plus API
  docs.
- **Changelog:** scriv fragments in `changelog.d/`, created with `mise run changelog:fragment:create`.

## Desired End State

After all four PRs:

- **Path keys.** Every `jsonb_extract_path` / `jsonb_extract_path_text` the converters render carries
  its path keys inline as text literals. This applies to filters, sorts, similarity and key-set
  paging. Integer segments work against Postgres.
- **The projection `name` filter.** The default `PostgresProjectionStorageAdapter` renders it as an
  inline literal. Other top-level filters stay bound.
- **Values.** Nested JSONB comparisons bind JSON-native values as `Jsonb(value)`. Other types keep
  `to_jsonb(%s)`. Matching semantics are unchanged.
- **Plan shape.** Under `force_generic_plan`, both partial (`WHERE name = '…'`) and non-partial
  expression indexes are used, which is proven by an integration test.
- **Docs.** The README explains the indexing trap, the recommended strategies, and how queries render.

Each PR's own end state is in its section.

### Key Discoveries:

- **Index-match form.** Literal keys render `jsonb_extract_path("state", 'k')`. Postgres parses
  this identically to an index defined as `jsonb_extract_path(state, 'k')`, stored as
  `VARIADIC ARRAY['k'::text]`, so the expressions match.
- **Dropping the `to_jsonb` wrapper is what lets extended statistics apply.** `to_jsonb` is STABLE, so
  `to_jsonb($v)` is never folded to a `Const`, even in a custom plan. A bare bind parameter is
  substituted as a `Const` in a custom plan, so MCV statistics match. Under generic plans it stays a
  `Param`: still a valid index condition, but the statistics don't apply. PR 4's estimate gains are
  therefore custom-plan only.
- **Semantics are unchanged for JSON-native values.**
  - `Jsonb("x")` is `"x"`, the same as `to_jsonb('x'::text)`.
  - `Jsonb(5)` is `5`, `Jsonb(True)` is `true`, and lists and dicts serialise to the same JSON.
  - `@>` against a scalar `Jsonb` behaves as today. Verified for `=`, `IN`, `@>` and `>` against
    Postgres 16.
- **Precedent for jsonb parameters.** The `source` filter already binds `Jsonb(...)`
  (`helpers.py:70-73`).
- **The `name` override needs no new extension point.** The registry overwrites by type
  (`persistence/converter.py:18-22`), so the adapter can register a configured
  `FilterClauseConverter` over the default.

## What We're NOT Doing

- **Changing the default table schema or shipping indexes in `sql/`.** Index choice depends on the
  application's queries. The README gives guidance instead.
- **Partitioning `projections` by `name`.** It's documented as an option in the README, without
  library support (no partition helpers).
- **Promoted or generated columns, or a path→column mapping in `TableSettings`.**
- **Inlining any other filter values.** Only `name` on the projection adapter is inlined, because it
  has few distinct values and partial indexes depend on it. `literal_value_paths` deliberately takes
  top-level paths only. Inlining nested discriminators (e.g. `state.type`) would need a jsonb literal,
  and the parameter is expected to change if that's ever wanted.
- **Fixing CONTAINS with a multi-element list value.** It is split element by element today
  (`clause.py:52`), and so fails. Fixing it means restricting the split to `IN`, which changes
  CONTAINS semantics for single-element lists. That is a separate change; raise it separately.
- **Changing `TableSettings`, or the generic `FilterClause` / `Path` types.**
- **Fixing the key-set paging issues seen while reading the code** (a nested sort expression selected
  from the `last` CTE in the mixed-direction path, and `ORDER BY <expression>` over a `UNION`). They
  are untested and unrelated; raise them separately.
- **Changing the in-memory converters.**
- **Moving the docs into the mkdocs site.** The README is where the existing how-to sections live.

## Implementation Approach

- **Test-first throughout.** Each code PR first adds or updates tests (red), then changes the code
  (green). Shared adapter cases that pin query semantics against real Postgres are written and run
  *before* the rendering change in the same PR, so a semantic change shows up as a failure rather than
  as edited expectations.
- **Small PRs.** Each leaves the build green, carries its own changelog fragment, and updates the
  README section from PR 1 only for what it changes.
- **PR 3's wiring.** It lives in the projection adapter, not in `DelegatingQueryConverter`, so the
  subscriber and subscription stores, and callers who build their own converter, are unaffected.

---

## PR 1: Document indexing the projections table

**Branch**: `docs-projection-indexing`
**Depends on**: nothing

### Overview

A docs-only PR that explains the partial-index statistics trap and the recommended indexing
strategies. It describes Postgres behaviour that holds today, whatever happens to PRs 2–4, and is the
starting point for the discussion with the maintainers.

### Changes Required:

#### 1. README

**File**: `README.md`
**Changes**: Add an "Indexing Projections in Postgres" subsection under Usage, after "Finalising
State". Keep it as concise as that section, and use examples rather than prose where possible. It
covers:

- **Index expressions must match the rendered SQL.** A short table maps each operator to the
  expression it renders, so readers know which form to index:

  | Operators | Rendered expression |
  |---|---|
  | `=`, `!=`, `<`, `<=`, `>`, `>=`, `IN`, CONTAINS (`@>`), sorting | `jsonb_extract_path("state", '<key>', …)` |
  | regex (`~`, `!~`), IS NULL, IS NOT NULL | `jsonb_extract_path_text("state", '<key>', …)` |

  Index expressions must use exactly that function. Never use `->` or `->>`, which parse to
  different expressions.
- **Index only bounded scalar keys with btree.** Target keys that hold ids, enums or timestamps. A
  btree entry over about 2.7kB fails with `index row size exceeds btree version 4 maximum`, so indexing
  a key that can hold a large object or array can make `save()` fail. For containment on larger
  structures, use GIN with `jsonb_path_ops`.
- **The trap:** Postgres ignores the statistics of *partial* expression indexes (`WHERE name = '…'`)
  when estimating selectivity. With only partial indexes, equality filters are estimated at the 0.5%
  default (and other operators at similar fixed defaults). Queries with several indexed filters can
  then pick a far less selective index. This matters most with large `state` documents, because every
  row the planner wrongly visits has to be detoasted.
- **Prepared statements (current rendering).** Today, path keys and `name` are bound parameters.
  psycopg prepares a statement after 5 executions, and `plan_cache_mode = auto` may then switch to a
  generic plan. That plan can't match any expression index, and can't prove a partial index's
  `name` predicate. The workarounds are to disable preparation on the pool's connections
  (`prepare_threshold=None`) or to set `plan_cache_mode = force_custom_plan` for the role or session.
  PRs 2 and 3 replace this paragraph as they land.
- **Recommended strategies.** These are in addition to the indexes shipped in
  `sql/create_projections_indices.sql`:
  - Non-partial composite indexes that lead with `name`, built concurrently on a live table:
    `CREATE INDEX CONCURRENTLY ON projections (name, jsonb_extract_path(state, 'account_id'), jsonb_extract_path(state, 'created_at') DESC);`.
    - Add a trailing sort expression when queries sort with a `LIMIT`.
    - Note that `CONCURRENTLY` can't run inside a transaction, and that a failed build leaves an
      `INVALID` index that must be dropped and recreated.
    - These indexes give table-wide statistics for each expression, not per-type statistics.
  - Or `PARTITION BY LIST (name)`, with plain expression indexes per partition, for per-type
    statistics. The primary key `(name, id)` already includes the partition key, so
    `ON CONFLICT (name, id)` upserts work unchanged. Before taking this route:
    - Add a `DEFAULT` partition. Without one, the first save of a new projection type fails with
      `no partition of relation "projections" found for row`.
    - An existing table can't be converted in place. It has to be recreated and its rows copied.
    - `CREATE INDEX CONCURRENTLY` isn't supported on the parent. Build each partition's index
      concurrently, then use `CREATE INDEX ON ONLY projections …` and `ALTER INDEX … ATTACH PARTITION`.
- **When partial indexes are fine:** a single candidate index with no competing filter, or pure
  ordering walks.
- **A caution:** `CREATE STATISTICS` on expressions over large TOASTed `state` columns can use
  excessive memory during `ANALYZE`. Memory grows with the number of sample rows times the detoasted
  `state` size. State the Postgres versions this was verified against, and link a public source (an
  upstream thread or reproduction) for it. As mitigations, lower the sample with
  `ALTER STATISTICS … SET STATISTICS <n>`, or avoid expression statistics on large documents.
- **A pointer:** run `ANALYZE projections` after creating indexes.

Every claim about Postgres internals in this section (`examine_variable`, the 0.5% default, the
`ANALYZE` memory growth) gets a public source in the PR description: Postgres source, docs, or an
upstream thread.

#### 2. Changelog fragment

**File**: the fragment generated by `mise run changelog:fragment:create`. `### Documentation` isn't a
scriv default category (none are configured in `[tool.scriv]`), so use `### Added`.

```markdown
### Added

- Added a README section on indexing the Postgres projections table,
  including why partial expression indexes give the planner no statistics,
  the recommended alternatives, and how prepared statements interact with
  the current rendering.
```

### Success Criteria:

#### Automated Verification:

- [ ] Full build passes: `mise run` (docs build, lint, format, types and unit tests pass; integration
      and component tests not run locally because port 5432 was taken by another service's database)
- [x] Docs build: `mise run docs:build`

#### Manual Verification:

- [x] The SQL examples in the section run as written against the reference schema in
      `sql/create_projections_table.sql` plus `sql/create_projections_indices.sql`.
- [ ] A maintainer reviews the section for accuracy and tone.

---

## PR 2: Render JSON path keys as literals

**Branch**: `literal-json-path-keys`
**Depends on**: nothing (PR 1 only for the README section it extends; if PR 1 hasn't merged, add
the README paragraph in whichever PR lands second)

### Overview

Add an `InlineLiteral` expression node, and use it for every path sub-level in `expression_for_path`.
Expression indexes then match under generic plans, and integer path segments stop failing.

### Changes Required:

#### 1. `InlineLiteral` node

**File**: `src/logicblocks/event/persistence/postgres/query.py`
**Changes**: Add a node next to `Constant` that renders the value inline through psycopg's quoting, with
no params.
- **Escape `%`.** It doubles `%`, because psycopg re-parses the composed query for placeholders (see
  Current State). Verified: `'50%x'`, `'a%%b'`, `'a%sb'`, `"it's"` and backslashes all round-trip to
  the intended literal.
- **Naming.** The name says inline, as opposed to `Constant`'s bound parameter, and avoids clashing
  with `typing.Literal` and `psycopg.sql.Literal`.
- **Typing.** The value is typed as a scalar, so a `Jsonb` or a list can't be passed in by mistake.
- **Visibility.** It is used only inside `persistence.postgres`, so it is not exported from the package.

```python
class _PlaceholderSafeLiteral(sql.Literal):
    def as_bytes(self, context: AdaptContext | None = None) -> bytes:
        return super().as_bytes(context).replace(b"%", b"%%")


@dataclass(frozen=True)
class InlineLiteral(Expression):
    value: str | int | float | bool

    def to_fragment(self) -> ParameterisedQueryFragment:
        return _PlaceholderSafeLiteral(self.value), []
```

#### 2. Path rendering

**File**: `src/logicblocks/event/persistence/postgres/converters/helpers.py`
**Changes**: Render sub-levels as text literals. `str()` makes integer segments array indices
(`'0'`), which `jsonb_extract_path`'s `text[]` signature accepts. `bool` is a subclass of `int`, so
`Path("state", "a", True)` type-checks; it would otherwise render `'True'` silently. Reject `bool`
sub-levels with `ValueError` instead.

```python
def path_key(sub_level: str | int) -> str:
    if isinstance(sub_level, bool):
        raise ValueError(f"Unsupported path sub-level: {sub_level!r}")
    return str(sub_level)


arguments = [
    postgresquery.ColumnReference(field=path.top_level),
    *[
        postgresquery.InlineLiteral(value=path_key(sub_level))
        for sub_level in path.sub_levels
    ],
]
```

#### 3. Unit tests

**File**: `tests/unit/logicblocks/event/persistence/postgres/test_converters.py`
**Changes**:
- Write the new expectations first (red). Every nested-path expectation moves its path keys from
  params into the SQL. For example, lines 140-145 become:

  ```python
  'WHERE "jsonb_extract_path"("state", \'value\') = '
  '"to_jsonb"(CAST(%s AS "text"))',
  ["test"],
  ```

- Cover filters, IS NULL / IS NOT NULL (`jsonb_extract_path_text`), IN, the multi-filter case with
  an int segment (lines 264-290, which become `'value_2', '0', 'value_3'`), nested sort,
  similarity, CONTAINS and regex.
- Add a key-set paging test that sorts on a nested path, so the CTE, row comparison and ORDER BY
  copies of the expression are all asserted with inline keys. Use only single-direction next-page
  cases, so the known mixed-direction bug (see What We're NOT Doing) isn't recorded as expected output.
- A `bool` path sub-level raises `ValueError`.

**File**: `tests/unit/logicblocks/event/persistence/postgres/test_query.py`
**Changes**: Parameterised cases asserting `InlineLiteral(v).to_fragment()` renders the expected text with
no params:
- `"a"` → `'a'`;
- `"it's"` → `'it''s'`;
- a backslash value → an `E''` literal;
- `"50%"` → `'50%%'`, and `"a%%b"` → `'a%%%%b'`.

Also add a case where a `FunctionApplication` with `InlineLiteral` arguments renders them inline.

#### 4. Integration tests

**File**: `tests/shared/logicblocks/event/testcases/projection/store/adapters.py`
**Changes**: Add `FindManyCases` cases that run for both the in-memory and Postgres adapters:
- A filter on a nested path with an integer segment (`Path("state", "values", 0)`). This fails on
  Postgres before the change and passes after. Every fixture projection has a `values` array long
  enough for the index, because the in-memory lookup raises on a short array (see Current State).
- A sort on a two-level nested path.
- A filter on a nested key containing `'`, `%` and `%s` (e.g. `Path("state", "it's 50%s")`). This
  proves that quoting and `%` escaping round-trip against Postgres.

**File**: `tests/integration/logicblocks/event/persistence/postgres/test_query_plans.py` (new; this
checks a property of the generic converter, so it lives with the persistence tests)
**Changes**: Against the integration database, with `projections` recreated:

- **Fixture.** Seed a few thousand rows across several `name` values with a skewed, distinct `k`,
  then run `ANALYZE projections`. Keep `SET enable_seqscan = off` only as a backstop.
- **Index.** Create a non-partial `(name, jsonb_extract_path(state, 'k'))` index.
- **Query.** Render a search with a `name` filter and a nested `k` filter through
  `postgres.QueryConverter(...).with_default_converters()`.
- **Plan.** Convert psycopg's `%s` placeholders to `$1…$n` with a small test helper over
  `as_string()`, collapsing `%%` back to `%`. Run `EXPLAIN (GENERIC_PLAN, FORMAT JSON) <sql>`, which
  always plans generically and needs no parameter values.
- **Assert.** Find the index scan node on the new index, and assert that its `Index Cond` contains
  `jsonb_extract_path`. Checking only for the index name isn't enough, because the index can be
  scanned on `name` alone with `k` as a Filter.
- **Negative control (permanent).** Render the same query with path keys bound (build the expression
  with `Constant` keys by hand), and assert that no `Index Cond` contains `jsonb_extract_path`. The
  test then stays able to fail.

#### 5. README

**File**: `README.md`
**Changes**: In the indexing section, update the prepared-statements paragraph:
- path keys now render inline, so expression indexes are usable under prepared (generic) plans;
- the bound `name` caveat still applies until PR 3;
- because generic plans can now use indexes, `plan_cache_mode = auto` may settle on them more often;
- workloads whose bound filter values are skewed and regress can set `force_custom_plan`.

#### 6. Changelog fragment

**File**: the fragment generated by `mise run changelog:fragment:create`

```markdown
### Changed

- Postgres query converters render JSON path keys as inline literals
  (`jsonb_extract_path("state", 'key')`) instead of bind parameters, so
  expression indexes stay usable under generic (prepared) plans.

### Breaking changes

- The SQL text and parameter list returned by `QueryConverter.convert_query`
  change for nested paths: path keys move from the parameters into the SQL.
  Tests or code that assert on, or post-process, rendered queries must be
  updated.

### Fixed

- Filters and sorts on paths with integer segments (array indices) no
  longer fail with `function jsonb_extract_path(jsonb, unknown, smallint)
  does not exist`.
```

### Success Criteria:

#### Automated Verification:

- [ ] Unit tests pass: `mise run test:unit`
- [ ] Integration tests pass, including the generic-plan test: `mise run test:integration`
- [ ] Types check: `mise run types:check`
- [ ] Lint and format pass: `mise run lint:check` and `mise run format:check`

#### Manual Verification:

- [ ] compliance-journey-service on a local build of this branch:
  - its tests pass after updating `tests/unit/shared/test_projection_index_expressions.py` to the new
    rendering;
  - `EXPLAIN` of its journeys-list query under `force_generic_plan` against its scratch database uses
    a journey expression index;
  - `EXPLAIN (ANALYZE, BUFFERS)` timings for that query under `auto` and `force_generic_plan`, before
    and after the change, are recorded in the PR description.

---

## PR 3: Render the projection `name` filter as a literal

**Branch**: `literal-projection-name`, stacked on `literal-json-path-keys`
**Depends on**: PR 2

### Overview

Add a `literal_value_paths` option to `FilterClauseConverter`, and configure it with `Path("name")` in
the default `PostgresProjectionStorageAdapter` converter. Partial `WHERE name = '…'` indexes then stay
usable under generic plans.

### Changes Required:

#### 1. Filter converter option

**File**: `src/logicblocks/event/persistence/postgres/converters/clause.py`
**Changes**:
- `FilterClauseConverter.__init__` takes `literal_value_paths: Collection[genericquery.Path] = ()`.
  - It validates each path once, at construction, with `supports_literal_value` from `helpers.py`, and
    raises a `ValueError` that names the path and the reason (nested, or `source`).
  - It stores the paths as a `frozenset` (`Path` is a frozen dataclass, so it is hashable) and passes
    them to `FilterClauseQueryApplier`.
- **One rendering path.** The applier doesn't render literals itself. It passes `literal=self._path in
  self._literal_value_paths` to `value_for_path`, which picks `InlineLiteral` over `Constant` only in its
  final top-level branch. This is after the `has_value()` check, so `name` equal to `None` still
  renders as `"name" IS NULL` with no value. The multi-valued loop stays in the applier, unchanged, so
  `IN` gets one literal per element.
- `supports_literal_value(path)` sits next to `value_for_path` in `helpers.py`, so the special cases
  for `source` and nested paths are written down once.

```python
def supports_literal_value(path: genericquery.Path) -> bool:
    return not path.is_nested() and path != genericquery.Path("source")


def value_for_path(
    value: Any,
    path: genericquery.Path,
    operator: postgresquery.Operator,
    literal: bool = False,
) -> postgresquery.Expression:
    ...
    else:
        return (
            postgresquery.InlineLiteral(value)
            if literal
            else postgresquery.Constant(value)
        )
```

#### 2. Projection adapter wiring

**File**: `src/logicblocks/event/projection/store/adapters/postgres.py`
**Changes**: Add a public module-level factory. The adapter uses it when the caller passes no
`query_converter`, and callers who need a custom converter can build on it with further `register_*`
calls instead of rebuilding the default by hand.

```python
def default_projection_query_converter(
    table_settings: postgres.TableSettings,
) -> postgres.QueryConverter:
    return (
        postgres.QueryConverter(table_settings=table_settings)
        .with_default_converters()
        .register_clause_converter(
            FilterClause,
            postgres.FilterClauseConverter(literal_value_paths=[Path("name")]),
        )
    )
```

**Exports**:
- Add `FilterClauseConverter` to `logicblocks.event.persistence.postgres` (`__init__.py` and
  `__all__`). It is not reachable there today.
- Export `default_projection_query_converter` alongside `PostgresProjectionStorageAdapter`.

#### 3. Unit tests

**File**: `tests/unit/logicblocks/event/persistence/postgres/test_converters.py`
**Changes**:
Parameterised cases with `literal_value_paths=[Path("name")]`, each asserting the full SQL and params:
- EQUAL `"journey"` renders `"name" = 'journey'` with no param;
- NOT_EQUAL with a value renders an inline literal;
- EQUAL `None` renders `"name" IS NULL`, and NOT_EQUAL `None` renders `"name" IS NOT NULL`, with no
  value or param;
- `IN` on `name` renders one inline literal per element;
- a `name` containing `'` and `%` renders escaped (`'it''s 50%%'`);
- other top-level filters stay bound;
- configuring a nested path or `Path("source")` raises `ValueError`, naming the path.

**File**: `tests/unit/logicblocks/event/projection/store/adapters/` (new or existing Postgres adapter
unit test; follow the existing layout)
**Changes**: `default_projection_query_converter` renders `name` inline. The adapter uses it by default,
and uses a caller-supplied `query_converter` unchanged.

**File**: `tests/shared/logicblocks/event/testcases/projection/store/adapters.py`
**Changes**: A `FindManyCases` case that saves and searches a projection whose `name` contains `'` and
`%`. With only a `name` filter, the query may have no other params, so this also proves that `%%`
collapses correctly against Postgres.

#### 4. Integration test

**File**: `tests/integration/logicblocks/event/projection/store/adapters/test_postgres_query_plans.py`
(new; this depends on the projection adapter's wiring, so it lives with the adapter tests)
**Changes**: Use the same seeded, analysed fixture and the `EXPLAIN (GENERIC_PLAN, FORMAT JSON)` helper
as PR 2. Add a partial index,
`CREATE INDEX … (jsonb_extract_path(state, 'k')) WHERE name = 'thing'`:

- Render a `ProjectionStore`-shaped search (`name` plus a nested `k` filter) through
  `default_projection_query_converter`.
- Assert the generic plan scans the partial index.
- Negative control (permanent): render the same search with
  `postgres.QueryConverter(...).with_default_converters()`, which binds `name`, and assert the partial
  index is not used.

#### 5. README

**File**: `README.md`
**Changes**: In the indexing section, replace the remaining bound-`name` caveat. Say that:
- the projection adapter inlines `name`, so partial indexes are usable under prepared plans;
- with a partitioned table, a literal `name` also lets Postgres prune partitions at plan time;
- a custom converter should start from `default_projection_query_converter`.

Show a complete snippet with imports:

```python
from logicblocks.event.projection.store import default_projection_query_converter

query_converter = default_projection_query_converter(table_settings).register_function_converter(...)
```

#### 6. Changelog fragment

**File**: the fragment generated by `mise run changelog:fragment:create`

```markdown
### Added

- `FilterClauseConverter` accepts `literal_value_paths`, rendering filter
  values on those top-level paths as inline literals. It is now exported
  from `logicblocks.event.persistence.postgres`.
- `default_projection_query_converter(table_settings)` returns the
  projection adapter's default query converter, for callers who supply a
  customised `query_converter`.

### Changed

- `PostgresProjectionStorageAdapter` renders the projection `name` filter
  as an inline literal, so partial `WHERE name = '...'` indexes stay usable
  under generic (prepared) plans. A caller-supplied `query_converter` keeps
  `name` bound unless it is built from `default_projection_query_converter`.
```

### Success Criteria:

#### Automated Verification:

- [ ] Unit tests pass: `mise run test:unit`
- [ ] Integration tests pass, including the partial-index plan test: `mise run test:integration`
- [ ] Component tests pass: `mise run test:component`
- [ ] Types check: `mise run types:check`
- [ ] Lint and format pass: `mise run lint:check` and `mise run format:check`

#### Manual Verification:

- [ ] compliance-journey-service on a local build of this branch still returns the same list results.

---

## PR 4: Render JSON-native values as jsonb parameters

**Branch**: `jsonb-filter-values`
**Depends on**: nothing (if PR 2 has merged, rebase onto it; the unit test expectations then
include literal path keys)

### Overview

For nested JSONB comparisons, bind `Jsonb(value)` directly when `Jsonb` can serialise the value, and
keep `to_jsonb(...)` otherwise. This lets multi-column extended statistics such as
`CREATE STATISTICS (mcv) ON name, (jsonb_extract_path(state, 'k'))` apply. The gain comes from dropping
the STABLE `to_jsonb` wrapper, and applies only under custom plans; under generic plans the value
stays a `Param` (see Key Discoveries).

It is the PR with the weakest immediate benefit: those statistics are unsafe on large `state` documents
until the Postgres `ANALYZE` memory issue is addressed. The maintainers may choose to defer it.

**Scope of "value".** Lists and tuples never reach this code whole: the applier splits every `Sequence`
value element by element (see Current State and What We're NOT Doing). So the values that reach
`Jsonb` are scalars, dicts, and lists nested inside a dict.

### Changes Required:

#### 1. Value rendering

**File**: `src/logicblocks/event/persistence/postgres/converters/helpers.py`
**Changes**: Replace `value_for_nested_path` with `jsonb_value(value)`. The old `path` and
`operator` parameters were unused, so they are dropped.

The predicate is private and matches exact built-in types only, i.e. the types `json.dumps`
serialises the same way `to_jsonb` would. That rules out:
- subclasses, which may have consumer-registered psycopg dumpers that `to_jsonb(%s)` respects today;
- `Mapping` or `Sequence` types `json.dumps` rejects (`MappingProxyType`, `deque`, `range`, `bytearray`);
- `None`.

It doesn't reuse `types/json.py`'s `is_json_value`, because that guard accepts `None` and any
`Sequence`/`Mapping` pattern match, which is broader than this needs.

```python
def _is_jsonb_bindable(value: Any) -> bool:
    value_type = type(value)
    if value_type in (bool, int, str):
        return True
    if value_type is float:
        return math.isfinite(value)
    if value_type is dict:
        return all(
            type(k) is str and _is_jsonb_bindable(v) for k, v in value.items()
        )
    if value_type in (list, tuple):
        return all(_is_jsonb_bindable(v) for v in value)
    return False


def jsonb_value(value: Any) -> postgresquery.Expression:
    if _is_jsonb_bindable(value):
        return postgresquery.Constant(Jsonb(value))

    return postgresquery.FunctionApplication(
        function_name="to_jsonb", arguments=[postgresquery.Constant(value)]
    )
```

- The `str` → `CAST(... AS text)` branch goes away: strings now take the `Jsonb` path, and the
  fallback never receives a plain `str`.
- `IN` keeps converting element by element (`clause.py:52-67`), so each element becomes its own
  `Jsonb` parameter.
- `None` never reaches here, because EQUALS/NOT_EQUALS with `None` become IS NULL / IS NOT NULL
  (`clause.py:39-45`).
- Non-finite floats keep `to_jsonb`, because `json.dumps` would emit `NaN` / `Infinity`, which
  Postgres rejects as JSON.

#### 2. Integration tests (written and run first)

**File**: `tests/shared/logicblocks/event/testcases/projection/store/adapters.py`
**Changes**: Add `FindManyCases` cases for:
- equality on a bool;
- equality on a float;
- `IN` with ints;
- `GREATER_THAN` on an ISO-8601 string;
- CONTAINS with a scalar int against a stored list;
- CONTAINS with a dict value against a stored object;
- equality on a string containing `"`, `\` and a non-ASCII character.

Every fixture projection contains the queried path, and no bool case has a `1`/`0` distractor, so the
results agree between the in-memory and Postgres adapters (see Current State). Run the cases against
Postgres before changing `helpers.py`, to pin today's matching semantics.

#### 3. Unit tests

**File**: `tests/unit/logicblocks/event/persistence/postgres/test_converters.py`
**Changes**:
- Nested JSONB comparisons render `= %s` with a `Jsonb(...)` param. `Jsonb` has no value equality,
  so compare params by `type(p) is Jsonb and p.obj == expected`; add a small helper in the test
  module.
- Add converter-level cases for:
  - `str`, `int`, `float` and `bool` values;
  - a dict value with CONTAINS (one `Jsonb` param);
  - `IN` with ints;
  - `datetime`, `Decimal` and `UUID` values (expect `"to_jsonb"(%s)` with the raw param);
  - a non-finite float (expects `to_jsonb`);
  - a dict holding a `datetime` (falls back to `to_jsonb`).
- Add a parameterised test of `_is_jsonb_bindable` over its boundaries:
  - true for a nested dict, and for a list and a tuple inside a dict;
  - false for a dict with non-`str` keys, `bytes`, `MappingProxyType`, a `str` subclass, `None`, NaN,
    and a list holding a `datetime` or NaN.
- Regex expectations are unchanged: the value stays a raw bound param.

#### 4. README

**File**: `README.md`
**Changes**: In the indexing section, note:
- JSON-native values are bound as `jsonb`, so multi-column extended statistics on `(name, <expression>)`
  can apply under custom plans only;
- this is subject to the `ANALYZE` memory caution from PR 1.

#### 5. Changelog fragment

**File**: the fragment generated by `mise run changelog:fragment:create`

```markdown
### Changed

- Nested JSONB filter values of built-in JSON types (str, int, float, bool,
  dict) are bound as `jsonb` parameters instead of
  `to_jsonb(CAST(... AS text))`, which lets extended statistics apply
  under custom plans. Other value types, including subclasses of these,
  keep the previous rendering.

### Breaking changes

- For nested JSONB filters, `QueryConverter.convert_query` now returns
  `psycopg.types.json.Jsonb` parameters, and SQL text without `to_jsonb`.
  `Jsonb` has no value equality, so tests that compare parameter lists with
  `==` must compare `param.obj` instead.
```

### Success Criteria:

#### Automated Verification:

- [ ] Unit tests pass: `mise run test:unit`
- [ ] Integration tests pass, including the cases added before the change:
      `mise run test:integration`
- [ ] Types check: `mise run types:check`
- [ ] Lint and format pass: `mise run lint:check` and `mise run format:check`

#### Manual Verification:

- [ ] On a table with `CREATE STATISTICS (mcv) ON name, (jsonb_extract_path(state, 'k'))` and a skewed
      `k`, `EXPLAIN` of a rendered `name = … AND k = …` search estimates within ~10% of actual rows.

---

## Testing Strategy

### Unit Tests:

- **PR 2:**
  - rendered SQL and params for every converter path that renders a nested expression: filter
    (JSONB and TEXT comparisons), sort, similarity, and single-direction key-set paging with a nested
    sort;
  - `InlineLiteral` quoting and `%` escaping;
  - rejection of `bool` sub-levels.
- **PR 3:** `literal_value_paths` with EQUAL, NOT_EQUAL, `None` (IS NULL / IS NOT NULL), `IN` and
  escaped values, plus validation; `default_projection_query_converter` and the caller-supplied
  converter passing through unchanged.
- **PR 4:** value encoding by type (`str`, `int`, `float`, `bool`, `dict`, `datetime`, `Decimal`, `UUID`,
  non-finite floats), and the boundaries of `_is_jsonb_bindable`.

### Integration Tests:

- **Shared `ProjectionStorageAdapterCases` additions.** They run against both in-memory and Postgres,
  with fixtures that contain every queried path:
  - PR 2: integer path segment, nested sort, a key containing `'`, `%` and `%s`;
  - PR 3: a projection `name` containing `'` and `%`;
  - PR 4: bool, float, int `IN`, ISO-string range, scalar-in-list and dict CONTAINS, an escaped
    string.
- **Plan-shape tests** use a seeded, analysed table and `EXPLAIN (GENERIC_PLAN, FORMAT JSON)`. They
  assert on `Index Cond`, and each has a permanent negative control:
  - PR 2 (`persistence/postgres/test_query_plans.py`): a non-partial expression index;
  - PR 3 (`projection/store/adapters/test_postgres_query_plans.py`): a partial index with a literal
    `name`.

### Manual Testing Steps:

1. Build each code PR's branch locally and install it into compliance-journey-service.
2. Run its test suite, updating `test_projection_index_expressions.py` to the new rendering.
3. Against its local scratch database, run `EXPLAIN` on the journeys-list query with
   `plan_cache_mode = force_generic_plan`. Check that a journey expression index is used (after PR 2,
   and after PR 3 for partial indexes).
4. Record `EXPLAIN (ANALYZE, BUFFERS)` timings under `auto` and `force_generic_plan`, before and after
   each PR, in the PR description.

## Performance Considerations

- **Literal path keys (PR 2):** these don't increase the number of distinct statements, since keys
  come from code and one query shape maps to one statement text. Fewer parameters travel.
- **Literal `name` (PR 3):** this multiplies each shape's statement texts by the number of projection
  names queried.
  - The rough count is shapes × names × 2, because `count` wraps each search in a further text.
  - psycopg keeps up to `prepared_max` (default 100) prepared statements per pooled connection, and
    prepares each text only after 5 executions on that connection. If the working set exceeds that,
    statements churn; raise `prepared_max` on the pool's connections.
  - The README states this sizing rule. Check it against compliance-journey-service's real set of
    shapes during manual testing.
- **Plan cache mode (PRs 2 and 3):** generic plans become index-capable, so `plan_cache_mode = auto`
  may adopt them more often. Generic plans estimate the remaining bound values with defaults, so skewed
  workloads can regress; `force_custom_plan` is the escape hatch, and it is documented in the README.
- **`Jsonb` encoding (PR 4):** values are encoded client-side with `json.dumps`. For scalar values
  that costs about the same as the current text parameter.

## Migration Notes

- **No schema changes** in any PR.
- **Existing expression indexes keep matching.** Under custom plans, the rendered expressions are
  identical once parameters are substituted.
- **Generic plans improve.** Under generic plans, indexes start being used where they weren't (PRs 2
  and 3). `auto` mode may now pick generic plans more often (see Performance Considerations).
- **Downstream tests break.** PRs 2–4 each change rendered SQL text or params, so tests that assert on
  them (e.g. compliance-journey-service's index-expression test) must be updated. Each changelog
  fragment says so under `### Breaking changes`. After PR 4, `Jsonb` params must be compared via
  `.obj`.
- **Custom converters.** Callers who pass their own `query_converter` keep `name` bound unless they
  build it from `default_projection_query_converter` (PR 3).
- **Check plans after upgrading.** Better estimates can change which index the planner picks.

## References

- Research: `co-compliance-journey-service/meta/research/codebase/2026-10-05-partial-expression-index-statistics-and-projection-search.md`
- Converter helpers: `src/logicblocks/event/persistence/postgres/converters/helpers.py:9-88`
- Filter converter: `src/logicblocks/event/persistence/postgres/converters/clause.py:20-100`
- Query nodes: `src/logicblocks/event/persistence/postgres/query.py:22-175`
- Projection adapter: `src/logicblocks/event/projection/store/adapters/postgres.py:66-98`
- Shared adapter cases: `tests/shared/logicblocks/event/testcases/projection/store/adapters.py`
- Converter unit tests: `tests/unit/logicblocks/event/persistence/postgres/test_converters.py`
- Precedent plan and README section: `meta/plans/2026-09-25-projector-finalise-state.md`, `README.md:101-136`
