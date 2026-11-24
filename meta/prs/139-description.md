---
type: "pr-description"
id: "139"
title: "Research PostgreSQL push-based consumer delivery and add IDE and skill config"
date: "2026-10-13T13:50:23+00:00"
author: "Toby Clemson"
producer: "describe-pr"
status: "complete"
pr_url: "https://github.com/logicblocks/event.store/pull/139"
pr_number: 139
tags: []
revision: "1f640120d0ed7be1d86741ad5f6e4265c8de7b1d"
repository: "event.store"
last_updated: "2026-10-13T13:50:23+00:00"
last_updated_by: "Toby Clemson"
schema_version: 1
---

## Summary

> **Stacked on #138** (`initialise-accelerator`). Merge that first; this PR's
> diff is relative to it.

This PR adds a topic-research dossier on replacing the library's polling-based
consumer delivery with push delivery backed only by PostgreSQL. Alongside it,
it adds two repository-local Claude Code skills (`clean-comments` and
`raise-pr`) and the shared PyCharm project settings, and stops tracking the
IDE's own `.gitignore` through the root `.gitignore`. No library code changes.

## Motivation

Every subscriber is currently driven by a `PollingService` that scans the
`events` table on a 200 ms interval. Idle query load grows with
consumers × poll frequency, and each event waits on average half the interval
before any consumer sees it. The research asks whether PostgreSQL-native
notifications can remove that latency floor and most of the wasted load
without adding broker infrastructure. It is meant to inform a decision, not
to propose an implementation.

The skills and IDE settings support day-to-day work on the repository: one
enforces the strict comment policy, one makes raising (stacked) PRs
repeatable, and the PyCharm files give everyone the same interpreter and Ruff
setup.

## Changes

### Key Changes

- **Research dossier** in
  `meta/research/topics/postgres-push-based-consumers/`: the brief, outline,
  manifest, 10 findings (each with its level notes) from web and OpenAlex
  sources, and `synthesis.md`, which covers:
  - **Conclusion**: push is feasible on PostgreSQL alone, but only as a
    wake-up hint over the existing position-based scan. `LISTEN`/`NOTIFY` is
    at-most-once, does not survive disconnects, and cannot pass through
    transaction-mode poolers, so it cannot carry work or advance checkpoints.
  - **Costs to design around**: the cluster-wide commit lock taken by every
    notifying transaction, commit failures once the shared notification queue
    fills, fragile long-lived listener connections, and out-of-order
    visibility of `BIGSERIAL` positions under concurrent appends.
  - **Recommended mechanism**: keep the `LOG` serialisation guarantee as the
    safe read frontier for log-wide consumers; send a debounced
    `pg_notify` after commit on one versioned channel; run one dedicated
    listener connection per process that fans out to level-triggered
    per-subscriber flags; and give each consumer a loop with a long, jittered
    fallback poll, a poll-only mode, and canary-based health checks.
  - **Supporting material**: a comparison matrix, operational gotchas,
    expected effect, the conditions under which the recommendation holds, and
    open threads.
- **`clean-comments` skill**: audits comments and docstrings in a given scope
  against the repository's comment policy and removes those that restate code
  or reference stale artefacts.
- **`raise-pr` skill**: takes a branch to an open PR with a written
  description without assuming a VCS, handling stacked bases and safe re-runs.
- **PyCharm settings**: adds `.idea/pyProjectModel.xml` (use `pyproject.toml`
  for the project model) and `.idea/pyLspTools.xml` (enable Ruff).
- **Ignore rules**: the root `.gitignore` no longer ignores `.idea/.gitignore`
  or `.idea/dataSources.xml`, so the IDE's own ignore file is tracked and
  covers data sources. It now ignores `.idea/jujutsu.xml`, which holds
  per-user jj plugin settings.

### Implementation Details

The research was produced with Accelerator's `research-topic` skill. Most of
the size (about 32,000 lines across 143 files) is per-finding level notes kept
as the audit trail; `synthesis.md` is the document to read. The brief and
synthesis refer to code in this repository (`services/polling.py`,
`locks/postgres.py` and the adapter's `LOG` serialisation guarantee), so the
recommendation is tied to how the library behaves today.

## Breaking Changes

None. No library code, packaging or CI configuration changes.

### Migration Guide

Not applicable.

## How to Verify

### Automated Verification

- [x] All tests pass: `mise run test` (1747 unit, 134 integration, component
  suite passing), run at the tip of this stack.
- [x] Type checking passes: `mise run types:check`
- [x] Linting passes: `mise run lint:check`
- [x] Formatting is correct: `mise run format:check`

### Manual Verification

- [ ] Read `synthesis.md`, especially the Overview and "Recommendation for
  this library", and check the claims about the library's current behaviour
  (polling interval, serialisation guarantees, broker loops) against the code.
- [ ] Open the project in PyCharm and confirm it picks up `pyproject.toml` and
  enables Ruff without prompting.
- [ ] Run `/clean-comments` and `/raise-pr` in Claude Code to check that both
  skills load.

## Checklist

- [ ] I have read the [contributing guidelines](CONTRIBUTING.md)
- [x] I have added/updated tests for my changes — not applicable, no code
  changes
- [x] I have updated documentation as needed
- [x] I have added a changelog fragment (if user-facing changes) — not
  applicable, no user-facing changes
- [x] Breaking changes are documented with migration guidance — none

## Related Issues

Depends on #138.
