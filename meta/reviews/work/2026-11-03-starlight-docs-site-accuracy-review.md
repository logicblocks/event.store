---
type: work-review
id: "2026-11-03-starlight-docs-site-accuracy-review"
title: "Accuracy Review: Starlight Docs Site"
date: "2026-11-03T15:00:00+00:00"
author: "Phil Helm"
status: in-progress
target: "work:2026-11-03-starlight-docs-site"
tags: [docs, starlight, reference, accuracy]
---

# Accuracy Review: Starlight Docs Site

The review checked the `docs-starlight-site` branch against the code in five
areas:

- Getting started and Home
- the Finalising state guide
- the build and housekeeping
- the generated API reference
- docstring content

The build passes, internal links are valid and griffe raises no warnings. The
Getting started example runs and prints the documented state. Every exported
symbol is documented somewhere.

The branch is rebased onto PR #142 (`fix-store-docstrings`), which fixes
the docstrings and the README copies of the guides.

Status values: `todo`, `in-progress`, `done`, `covered-by-#142`, `deferred`.

## Must fix

| # | Finding | Where | Status |
|---|---------|-------|--------|
| M1 | Deploy workflow still triggers on pushes to `docs-starlight-site`, a temporary preview. Remove that line before merging, then take the branch out of the `github-pages` environment's deployment policy. | `.github/workflows/docs.yaml:6` | todo (do last; it stops preview deploys) |
| M2 | The Utils reference page is empty. `utils/__init__.py` exports nothing and the utils modules have no `__all__`, so the walker skips them. That leaves `Clock`, `SystemClock`, `StaticClock`, `TimezoneRequiredStaticClock` and `class_fullname` undocumented, although `Clock` and `SystemClock` appear in public signatures. | `src/logicblocks/event/utils/` | done (generator skips packages with nothing to document; adding `__all__` to utils is a separate library PR) |
| M3 | Class-level attribute values are dropped. The value is hidden whenever griffe labels an attribute `instance-attribute`, which it also does for enum members and class constants. `Operator.EQUAL` renders as a bare `EQUAL`, and about 80 `<pre>` blocks hold only a name. | `tasks/reference/signatures.py:90` | done |
| M4 | Wrong docstrings: `EventCategory` claims `iter` support, and the `stream`/`category` accessors say they return "an event store". | `src/logicblocks/event/store/store.py` | covered-by-#142 |
| M5 | Finalising state guide: "always by `ProjectionEventProcessor`" is wrong, because the first event of a stream passes `state=None`. The "reserved name" claim is unenforced; such names fail with a `TypeError`, or `id-factory` replaces the state. | `docs/src/content/docs/guides/finalising-state.md` | done |

## Should fix

| # | Finding | Where | Status |
|---|---------|-------|--------|
| S1 | Renamed re-exports are inconsistent. The heading uses the public alias, but the signature shows the original name, for example a `QueryConverter` heading over `class DelegatingQueryConverter(`. Affected: `PostgresQuerySettings`/`QuerySettings`, `COORDINATOR_LOCK_NAME`/`LOCK_NAME`, `InMemoryTypeRegistryConstraintConverter`. | `tasks/reference/rendering.py`, `signatures.py` | done |
| S2 | Every name starting with `_` is skipped, which hides `event_store_transaction.__call__` (the decorator API) and `EventSource.__aiter__`. | `tasks/reference/surface.py` (`_public_object`) | done |
| S3 | The `publish` docstrings omit `condition`, the return value and `UnmetWriteConditionError`. | `src/logicblocks/event/store/store.py` | covered-by-#142 |
| S4 | The Getting started example never prints `profile`, and its expected-output comment sits outside `main()`. The page doesn't mention structlog output or explain the third `Projector` type argument. | `docs/src/content/docs/getting-started.md` | done |
| S5 | The Finalising state guide uses `ProfileProjector` without saying it comes from Getting started or linking there. | `docs/src/content/docs/guides/finalising-state.md` | done |

## Nice to have

| # | Finding | Where | Status |
|---|---------|-------|--------|
| N1 | Inherited members aren't listed, though bases are linked. | `tasks/reference/` | deferred |
| N2 | Long class headings with type parameters never wrap. | `tasks/reference/signatures.py` | deferred |
| N3 | Overload variants have no blank line between them. | `tasks/reference/rendering.py` | deferred |
| N4 | `ClassVar` and `@dataclass`/`frozen` aren't shown. | `tasks/reference/signatures.py` | deferred |
| N5 | `withoutLeadingTitle` strips whichever `#` heading comes first. It only works for CHANGELOG because scriv writes an anchor line first. | `docs/src/loaders.ts:61` | deferred |
| N6 | CONTRIBUTING doesn't mention `docs:serve` or `docs:build`. | `CONTRIBUTING.md` | deferred |
| N7 | The README still has a stale CircleCI badge; CI runs on GitHub Actions. | `README.md:7` | deferred |
| N8 | The `/event.store` base path is duplicated. | `tasks/docs.py`, `docs/astro.config.mjs` | deferred |
| N9 | Most of the public API has no docstrings: write conditions, retry helpers, `EventStorageAdapter`, `NewEvent`/`StoredEvent`, `Projector`, `ProjectionStore`, broker factories and `EventConsumerStateStore`. | `src/logicblocks/event/` | deferred (separate piece of work) |
| N10 | Private defaults such as `_default_logger` leak into signatures. They faithfully reflect the source. | reference pages | deferred |
| N11 | The `Unpack[...Params]` TypedDicts in the broker factory overloads don't link because they aren't exported. | `processing/broker/factories.py` | deferred |
