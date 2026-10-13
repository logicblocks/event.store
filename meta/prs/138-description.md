---
type: "pr-description"
id: "138"
title: "Initialise Accelerator with Linear integration and meta-directory migrations"
date: "2026-10-13T13:48:07+00:00"
author: "Toby Clemson"
producer: "describe-pr"
status: "complete"
pr_url: "https://github.com/logicblocks/event.store/pull/138"
pr_number: 138
tags: []
revision: "54b5ee036f540481d70a5e447890ac24ceb4055b"
repository: "event.store"
last_updated: "2026-10-13T13:48:07+00:00"
last_updated_by: "Toby Clemson"
schema_version: 1
---

## Summary

This PR initialises the Accelerator plugin in the repository. It adds the
shared `.accelerator/` configuration and state, applies Accelerator's
meta-directory migrations (0001, 0003–0009) to the existing `meta/` documents,
and configures Linear as the work-item tracker along with repository-specific
instructions for the planning skills. Only tooling and documentation change; no
library code changes.

## Motivation

Accelerator's skills (research, planning, implementation, PR description and
review) were already in use here, but their configuration lived in an ignored
`.accelerator/` directory and the `meta/` documents predated the plugin's
current layout and frontmatter schema. Committing the shared configuration and
migrating the corpus means everyone working on the repository gets the same
skill behaviour, the same work-item integration and a consistent, queryable
`meta/` directory.

## Changes

### Key Changes

- **Accelerator configuration**: adds `.accelerator/config.md`, which sets
  Linear as the work integration and defines the visualiser's kanban columns
  (`draft`, `ready`, `in-progress`, `done`).
- **Skill customisations**: adds `instructions.md` for `create-plan`,
  `implement-plan` and `review-plan`. These ask for test-driven, independently
  mergeable phases, a commit per completed phase, and very few code comments.
- **Linear integration state**: adds the team/workflow-state catalogue
  (Product Pod, `PP`) and last-sync record under
  `.accelerator/state/integrations/linear/`; per-user files (`viewer.json`,
  `.refresh-meta.json`, `.lock/`) are git-ignored.
- **Migrations applied**: records migrations 0001 and 0003–0009 in
  `.accelerator/state/migrations-applied`, plus the accepted decisions log for
  0007 (unify meta corpus frontmatter).
- **Directory layout**: moves the PR description template from
  `meta/templates/` to `.accelerator/templates/`, moves the poetry-to-uv
  research into `meta/research/codebase/`, and adds `.gitkeep` placeholders for
  the standard `meta/` and `.accelerator/` subdirectories.
- **Frontmatter normalisation**: updates existing plans, research, PR
  descriptions and PR reviews to the unified schema (`type`, `id`, `title`,
  `author`, `revision`, `schema_version`, quoted values, typed `relates_to`
  links). Superseded keys such as `researcher`/`git_commit` are renamed, and
  the event-metadata plan's `implemented` status becomes `done`.
- **`.gitignore`**: stops ignoring the whole `.accelerator/` directory; only
  the personal `.accelerator/config.local.md` is ignored now. Scratch files go
  under `.accelerator/tmp/`, which ignores its own contents.

### Implementation Details

The `meta/` changes are the mechanical output of Accelerator's migration
tooling, split into a separate commit from the hand-written configuration so
each can be reviewed on its own. The PR is split into three commits:
initialisation, migrations, then Linear and skill configuration.

## Breaking Changes

None. No library code, packaging or CI configuration changes.

### Migration Guide

Not applicable. Contributors using Accelerator should keep personal overrides
in `.accelerator/config.local.md`, which stays git-ignored.

## How to Verify

### Automated Verification

- [x] All tests pass: `mise run test` (1747 unit, 134 integration, 3
  component). The first run had 2 integration errors because the Postgres
  container was not yet accepting connections; a re-run passed cleanly.
- [x] Type checking passes: `mise run types:check`
- [x] Linting passes: `mise run lint:check`
- [x] Formatting is correct: `mise run format:check`

### Manual Verification

- [ ] Run `/accelerator:configure` (or any Accelerator skill) and confirm it
  picks up `.accelerator/config.md` and the per-skill instructions.
- [ ] Check that the migrated `meta/` documents still read correctly and that
  the moved research document is referenced from the poetry-to-uv plan at its
  new path.

## Checklist

- [ ] I have read the [contributing guidelines](CONTRIBUTING.md)
- [x] I have added/updated tests for my changes — not applicable, no code
  changes
- [x] I have updated documentation as needed
- [x] I have added a changelog fragment (if user-facing changes) — not
  applicable, no user-facing changes
- [x] Breaking changes are documented with migration guidance — none

## Related Issues

None.
