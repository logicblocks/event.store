---
type: tickets
id: "2026-11-03-starlight-docs-site-tickets"
title: "Starlight Documentation Site on GitHub Pages: Tickets"
date: "2026-11-03T13:10:18+00:00"
author: "Phil Helm"
status: draft
tags: ["docs", "starlight", "github-pages", "ci"]
spec: "2026-11-03-starlight-docs-site"
repository: "event.store"
---

# Starlight Documentation Site on GitHub Pages: Tickets

Breakdown of the
[Starlight Documentation Site spec](2026-11-03-starlight-docs-site.md) into
tracer-bullet tickets, in dependency order. All work happens on the
`docs-starlight-site` branch and is delivered as a single PR.

```
01 ──┬── 02 ───────────┐
     ├── 03 ── 04 ─────┼── 06 ── 07
     └── 05 ───────────┘
```

Tickets 02, 03 and 05 can be worked on at the same time once 01 is done.

---

## 01: Deploy a basic Starlight site from the branch

**What to build:** A Starlight site with the Home and Getting started pages,
live at `https://logicblocks.github.io/event.store/` and deployed
automatically from the `docs-starlight-site` branch, so it can be compared
with the existing Read the Docs site. Maintainers can preview and build it
locally with mise.

**Blocked by:** None (can start immediately)

**Status:** ready-for-agent

- [ ] Node 24 (LTS) is pinned in the mise tool configuration, and
      `mise install` provides everything the docs need.
- [ ] An Astro and Starlight project lives in the `docs` directory, uses npm
      and has its lockfile committed.
- [ ] Astro `site` and `base` are configured for
      `https://logicblocks.github.io/event.store/`.
- [ ] The site uses Starlight's default theme, the title
      "logicblocks.event.store", a GitHub social link and Pagefind search.
- [ ] The Home page (splash) has the introduction and installation
      instructions, and the Getting started page has the basic publish and
      project example. Both are carried over from the README and the existing
      docs index without being rewritten.
- [ ] `mise run docs:serve` starts the Astro dev server with live reload.
- [ ] `mise run docs:build` produces a production build.
- [ ] `starlight-links-validator` is enabled, and the build fails on a broken
      internal link.
- [ ] A dedicated docs workflow runs on pushes to `main` and to
      `docs-starlight-site`. It installs tools with `jdx/mise-action`, runs
      `docs:build`, and publishes with `actions/upload-pages-artifact` and
      `actions/deploy-pages`.
- [ ] GitHub Pages is enabled with the source set to "GitHub Actions", and the
      `github-pages` environment allows deployments from
      `docs-starlight-site`. This is done via the GitHub API, with the
      maintainer's approval before any command is run.
- [ ] The default `mise run` and `mise run check` are unchanged.
- [ ] The existing MkDocs files are left in place, and the Read the Docs site
      continues to serve (it only builds from `main`).
- [ ] A push to the branch results in the new site being live at the Pages
      URL.

---

## 02: Check the docs build on PRs

**What to build:** Reviewers see a failing check when a PR breaks the docs
build or introduces a broken internal link, without slowing down the existing
check, test and build jobs.

**Blocked by:** 01

**Status:** ready-for-agent

- [ ] The PR checks workflow has a separate, build-only docs job that installs
      tools through mise and runs `docs:build`.
- [ ] The job doesn't deploy.
- [ ] A PR that introduces a broken internal link fails the docs job.
- [ ] The existing check, test and build jobs are unchanged.

---

## 03: Generate a reference page for one package

**What to build:** A generated API reference page for the `store` package
appears on the site. It shows the package's public classes, functions and
attributes, with signatures, type annotations and Google-style docstrings, and
every object has a stable anchor based on its dotted import path. The page is
regenerated from the source on every build and dev run.

**Blocked by:** 01

**Status:** ready-for-agent

- [ ] A custom generator built on `griffe` lives with the existing `docs`
      invoke tasks.
- [ ] `griffe` replaces MkDocs Material and mkdocstrings in the `docs`
      dependency group.
- [ ] The generator writes plain `.md` (not `.mdx`) into the reference section
      of the Starlight content collection, and that output is gitignored.
- [ ] `docs:serve` and `docs:build` run the generator before Astro.
- [ ] The public surface is taken from `__all__`, and names with a leading
      underscore are excluded.
- [ ] Docstrings are parsed in Google style, with args, returns, raises and
      attributes rendered. Objects without docstrings show their signature
      only.
- [ ] Each object has an explicit anchor equal to its dotted import path (for
      example `logicblocks.event.store.EventStore`), independent of
      Starlight's heading slugs.
- [ ] Generic type syntax (`<`, `{`, `[`) in signatures renders correctly.
- [ ] The page frontmatter has a title, a sidebar order and `editUrl: false`.
- [ ] References within the page link to their anchors. References to other
      packages are shown as plain code for now, so the link validator passes.
- [ ] The generator is test-driven through its public entry point against a
      small fixture package, asserting on complete expected page contents. The
      tests live in a new task-tooling area of the unit suite, with the test
      Python path extended so task modules can be imported.
- [ ] The `store` reference page is visible on the branch deployment.

---

## 04: Cover all public packages and link between them

**What to build:** The reference covers every public package (`store`,
`types`, `projection`, `processing`, `persistence`, `query`, `sources`,
`testing` and `utils`), one page each, and type names link across pages to
their canonical reference entries. New public APIs appear automatically once
they are added to `__all__`.

**Blocked by:** 03

**Status:** ready-for-agent

- [ ] The generator produces one page per top-level public package under
      `logicblocks.event`, with no hand-maintained list of classes.
- [ ] Re-exported names are documented once, at their canonical public
      location, and references to them link there.
- [ ] Links between packages resolve to
      `<base>/reference/<package>/#<dotted.path>` and pass the link
      validator.
- [ ] Standard library and third-party types are shown as plain code without
      a link.
- [ ] Docstrings the parser can't read are reported as build warnings and
      don't fail the build.
- [ ] Fixture-based tests cover nested subpackages, re-exports, links between
      packages and unlinked external types.
- [ ] The Reference section in the sidebar lists every package page.

---

## 05: Move the existing content across and finish the sidebar

**What to build:** The site has the full sidebar from the spec, and all
existing user-facing documentation is on it: the "Finalising state" guide,
the changelog and the contributing guide. Hand-written pages have "Edit this
page" links.

**Blocked by:** 01

**Status:** ready-for-agent

- [ ] The sidebar is Home, Getting started, Guides (Finalising state),
      Reference, Changelog and Contributing, in that order.
- [ ] "Finalising state" is carried over from the README as its own guide page.
- [ ] The Changelog page is built from `CHANGELOG.md` at build time, not
      copied.
- [ ] The Contributing page is built from `CONTRIBUTING.md` at build time, and
      the Code of Conduct is linked on GitHub.
- [ ] Hand-written pages have a working "Edit this page" link to the
      repository.
- [ ] Internal notes, `meta/` and `skills/` content aren't published.
- [ ] The build and link validation pass.

---

## 06: Retire Read the Docs in the repo

**What to build:** Once the branch deployment has been compared against the
old site, the repository has only one docs toolchain, every documentation link
points at the new site, release notes say the docs have moved, and the PR is
ready to merge with only `main` deploying.

**Blocked by:** 02, 04, 05

**Status:** ready-for-agent

- [ ] The maintainer has compared the branch deployment with the Read the Docs
      site and is happy to proceed.
- [ ] The MkDocs configuration, Read the Docs build configuration and MkDocs
      theme overrides, scripts and styles are removed, and no MkDocs
      dependencies remain.
- [ ] The README documentation badge and link point to the new site.
- [ ] The package metadata's `Documentation` project URL points to the new
      site.
- [ ] A changelog fragment under "Changed", created with
      `mise run changelog:fragment:create`, says the docs have moved to
      `https://logicblocks.github.io/event.store/` and Read the Docs has been
      retired.
- [ ] The docs workflow's branch trigger is removed, so it runs only on pushes
      to `main`.
- [ ] `mise run` and the docs build both pass.

---

## 07: Finish the cut-over after merging (done by a person)

**What to build:** After the PR is merged, the production site is confirmed
live from `main`, the temporary branch deployment allowance is removed, and
anyone visiting the old Read the Docs URL is redirected to the new site.

**Blocked by:** 06, and the PR being merged

**Status:** needs-human

- [ ] The docs workflow on `main` succeeds and the site is live at
      `https://logicblocks.github.io/event.store/`.
- [ ] `docs-starlight-site` is removed from the `github-pages` environment's
      deployment branch policy.
- [ ] The Read the Docs project administrator (most likely Toby Clemson) has
      configured a redirect from eventstore.readthedocs.io to the new site.
- [ ] Builds are disabled on the Read the Docs project.
