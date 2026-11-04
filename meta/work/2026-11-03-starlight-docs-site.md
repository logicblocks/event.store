---
type: spec
id: "2026-11-03-starlight-docs-site"
title: "Starlight Documentation Site on GitHub Pages"
date: "2026-11-03T13:10:18+00:00"
author: "Phil Helm"
status: draft
tags: ["docs", "starlight", "github-pages", "ci"]
revision: "2ff57fca4a72f70cbe75f94d52befe727e0ba23f"
repository: "event.store"
---

# Starlight Documentation Site on GitHub Pages

## Problem Statement

The published documentation for `logicblocks.event.store` lives on Read the
Docs (eventstore.readthedocs.io) and is a single MkDocs Material page: one
usage example followed by an API reference for a hand-picked list of classes
from four packages (`store`, `types`, `projection`, `testing`). It has had no
content changes since it was first set up in December 2024, beyond keeping up
with renames and tooling changes.

As a result:

- Large parts of the public API (`processing`, `persistence`, `query`,
  `sources`, `utils`) do not appear in the published docs at all, and new
  public classes never appear unless someone remembers to list them.
- The docs toolchain (MkDocs, mkdocstrings, Read the Docs build config) is
  separate from the rest of the project's CI on GitHub Actions, and its hosting
  is administered outside the repository.
- Existing documentation (README sections, changelog, contributing guide) is
  scattered across the repository rather than presented as one site.

The team wants a modern docs site, built and hosted alongside the code, that
can grow into proper guides later.

## Solution

Replace the Read the Docs site with a Starlight (Astro) site, built in this
repository and published to GitHub Pages at
`https://logicblocks.github.io/event.store/`.

This first iteration is a platform migration, not a content rewrite:

- Existing documentation is carried over: introduction and installation, the
  basic example, "Finalising state", the changelog and the contributing guide.
- The API reference is generated at build time from the source code, covering
  every public package as declared by `__all__`, with one page per top-level
  package and working cross-links between pages.
- The site is built and deployed by GitHub Actions through the project's
  existing mise toolchain.
- While the work is in progress, the branch is deployed to GitHub Pages so the
  old and new sites can be compared side by side.
- Once the new site is live from `main`, Read the Docs is retired: MkDocs
  configuration is removed, links are updated, and the old site redirects to
  the new one.

## User Stories

1. As a library user, I want the documentation at a stable GitHub Pages URL,
   so that I can bookmark and share it.
2. As a library user, I want a home page with a short introduction and
   installation instructions, so that I can quickly decide whether the library
   suits me and get it installed.
3. As a library user, I want a getting started page with the basic publish and
   project example, so that I can see the core workflow end to end.
4. As a library user, I want the "Finalising state" documentation on its own
   page, so that I can find it without scrolling through the README.
5. As a library user, I want an API reference page for every public package,
   so that I can look up any public class or function, not only a hand-picked
   subset.
6. As a library user, I want each reference page to list the package's public
   classes, functions and attributes with their signatures and type
   annotations, so that I can use the API even where docstrings are missing.
7. As a library user, I want docstring descriptions, parameters, return values
   and raised exceptions rendered in the reference, so that I understand how to
   use each API.
8. As a library user, I want type names in the reference to link to their own
   reference entries, even when they live in another package, so that I can
   navigate the API by following types.
9. As a library user, I want every reference entry to have a stable anchor
   based on its dotted import path, so that I can link directly to a specific
   class or method.
10. As a library user, I want private names (leading underscore) and names not
    exported via `__all__` left out of the reference, so that I only see the
    supported public surface.
11. As a library user, I want full-text search across the site, so that I can
    find a concept or class by name.
12. As a library user, I want the changelog on the site, so that I can see what
    changed between releases without leaving the docs.
13. As a library user, I want light and dark modes, so that the docs are
    comfortable to read in either.
14. As a library user, I want a link to the GitHub repository in the site
    header, so that I can get to the source, issues and releases.
15. As a library user arriving via an old Read the Docs link or bookmark, I want
    to be redirected to the new site, so that I don't hit a dead page.
16. As a library user browsing PyPI, I want the project's documentation link to
    point to the new site, so that I land on current documentation.
17. As a library user reading the release notes, I want to be told the docs
    have moved, so that I update my bookmarks.
18. As a library user, I want the site to work well on a phone, so that I can
    look things up away from my desk.
19. As a contributor, I want a link to the contributing guide and the code of
    conduct from the docs, so that I know how to get involved.
20. As a contributor, I want an "Edit this page" link on hand-written pages, so
    that I can fix a typo quickly.
21. As a contributor, I want "Edit this page" hidden on generated reference
    pages, so that I'm not sent to a file that doesn't exist; those pages change
    via docstrings in the source.
22. As a maintainer, I want the reference generated from the code at build time
    and not committed, so that it can never drift from the source and doesn't
    add noise to diffs.
23. As a maintainer, I want new public APIs to appear in the reference
    automatically once they are added to `__all__`, so that the docs stay
    complete without manual curation.
24. As a maintainer, I want the changelog page built from `CHANGELOG.md`, so
    that there is only one copy to maintain.
25. As a maintainer, I want to preview the site locally with live reload using
    `mise run docs:serve`, so that I can check changes before pushing.
26. As a maintainer, I want to build the site locally with
    `mise run docs:build`, so that I can reproduce what CI does.
27. As a maintainer, I want Node pinned in `mise.toml` alongside Python and uv,
    so that a single `mise install` gives me everything the docs need.
28. As a maintainer, I want npm dependencies locked, so that docs builds are
    reproducible.
29. As a maintainer, I want the docs to live in the same repository as the code,
    so that a code change and its documentation change can land in one PR.
30. As a maintainer, I want the docs build to fail on broken internal links,
    including cross-links in the generated reference, so that broken
    navigation is caught before it is published.
31. As a reviewer, I want a docs build job in PR checks, so that a PR that
    breaks the docs fails visibly.
32. As a Python-only contributor, I want the default local `mise run` build to
    keep its current scope, so that my feedback loop isn't slowed by Node and
    Astro.
33. As a maintainer, I want the site deployed automatically on every push to
    `main`, so that the published docs always match the latest released code.
34. As a maintainer, I want the docs deployment in its own workflow, so that it
    can be re-run independently and doesn't hold up prerelease publishing.
35. As a maintainer, I want the docs hosted on GitHub Pages and deployed from
    GitHub Actions, so that hosting is managed with the repository rather than
    on an external service.
36. As a maintainer working on this migration, I want the work-in-progress
    branch deployed to GitHub Pages, so that I can compare the new site with
    the old Read the Docs site side by side before merging.
37. As a maintainer, I want the temporary branch deployment removed before
    merge, so that afterwards only `main` deploys to production.
38. As a maintainer, I want MkDocs, mkdocstrings and Read the Docs configuration
    removed once the new site is in place, so that there is only one docs
    toolchain to maintain.
39. As a maintainer, I want the README's documentation badge and link to point
    at the new site, so that the repository's front page is accurate.
40. As a maintainer, I want the generator's behaviour covered by fast unit
    tests, so that I can change it with confidence.
41. As a maintainer, I want docstrings that the parser cannot read surfaced
    during the build, so that they can be fixed in a later docs effort.
42. As a future docs author, I want a "Guides" section already in the
    navigation, so that new guides have an obvious home.
43. As a future maintainer, I want the site set up so that versioned docs (for
    example with `starlight-versions`) could be added later, so that adopting
    versioning around 1.0 doesn't need a rebuild.

## Implementation Decisions

### Platform and location

- The site is an Astro project using Starlight, living in the repository's
  `docs` directory and replacing the current MkDocs content there.
- Node 24 (LTS) is added to the mise tool configuration. npm is the package
  manager and the lockfile is committed.
- Starlight's default theme is used. The site title is
  "logicblocks.event.store", and the site has a GitHub social link,
  "Edit this page" links (disabled on generated pages) and the built-in
  Pagefind search. No custom branding in this iteration.
- Astro's `site` is `https://logicblocks.github.io` and `base` is
  `/event.store`. No custom domain.
- A single, unversioned site built from `main`. Versioning is deferred.

### Information architecture

```
Home (splash: intro and install)
Getting started (basic example)
Guides
  └─ Finalising state
Reference (generated, one page per top-level package)
  ├─ store · types · projection · processing · persistence
  ├─ query · sources · testing
  └─ utils
Changelog (from CHANGELOG.md)
Contributing (from CONTRIBUTING.md; Code of Conduct linked on GitHub)
```

- Hand-written content is carried over from the README and the existing docs
  index. Overlapping material is merged, not duplicated. Apart from the minimum
  needed to fit the new structure, the content is not rewritten.
- Internal notes (top-level and processing `NOTES.md`), `meta/` and the
  uncommitted `skills/` content are excluded.
- The changelog and contributing pages are generated from their root Markdown
  files at build time, not copied.

### API reference generator

- A custom generator built directly on `griffe`, living with the existing
  `docs` invoke tasks. `griffe2md` was evaluated and rejected: it produces one
  very large page per package with no frontmatter, its cross-links point to
  anchors that only exist on a single page (and that Starlight's heading slugs
  wouldn't match anyway), and object paths are derived from the folder it is
  given rather than the import path.
- Inputs: the `logicblocks.event` package from the source tree. Output: one
  Markdown page per top-level public package, written to the reference section
  of the Starlight content collection.
- Public surface: names exported through `__all__`, recursively, excluding
  names starting with an underscore.
- Docstrings are parsed in Google style (the same as the current mkdocstrings
  configuration).
- Pages are plain `.md`, not `.mdx`, so `<`, `{` and generic type syntax in
  signatures don't need MDX escaping. Raw HTML such as `<code>` and explicit
  anchors is used where Markdown alone isn't enough.
- Each page has Starlight frontmatter: a title, sidebar order, and
  `editUrl: false`.
- Each documented object gets a stable explicit anchor equal to its dotted
  import path (for example `logicblocks.event.store.EventStore`), independent
  of Starlight's heading slug generation.
- Cross-references resolve each object to its canonical public location and
  link to `<base>/reference/<package>/#<dotted.path>`. References to objects
  outside the library (standard library, third-party) are shown as plain code
  without a link.
- Generated output is gitignored and produced before every build and dev run.
- `griffe` replaces MkDocs Material and mkdocstrings in the `docs` dependency
  group.

### Build tooling

- `mise run docs:serve` runs the generator, then the Astro dev server.
- `mise run docs:build` runs the generator, then the Astro production build.
- The docs build is not added to the default `mise run` or `mise run check`
  pipeline.
- `starlight-links-validator` is enabled so that broken internal links,
  including generated cross-links, fail the build.

### CI and deployment

- A new dedicated docs workflow runs on push to `main`. It installs tools with
  `jdx/mise-action` (so Node, Python and uv come from mise), runs
  `docs:build`, then publishes with `actions/upload-pages-artifact` and
  `actions/deploy-pages`.
- `withastro/action` is not used, because it manages its own Node install and
  wouldn't provide the Python toolchain the generator needs.
- PR checks gain a separate, build-only docs job, so a broken docs build fails
  the PR without slowing the existing check, test and build jobs.
- GitHub Pages is enabled with its source set to "GitHub Actions", via the
  GitHub API, with the maintainer's approval.

### Rollout

1. Work happens on the `docs-starlight-site` branch.
2. Pages is enabled, and the `github-pages` environment's deployment branch
   policy temporarily allows `docs-starlight-site`.
3. The docs workflow temporarily also runs on pushes to that branch, so the new
   site is live for comparison while Read the Docs keeps serving the old one.
4. The final commits on the branch:
   - remove MkDocs configuration, Read the Docs configuration and the MkDocs
     theme overrides, scripts and styles;
   - update the README documentation badge and link;
   - set the `Documentation` project URL in the package metadata so PyPI links
     to the new site;
   - add a changelog fragment under "Changed" saying the docs have moved and
     Read the Docs has been retired;
   - remove the branch trigger from the docs workflow.
5. It is delivered as a single PR. After merging, the deploy from `main` is
   checked and the branch is removed from the `github-pages` environment
   policy.
6. A Read the Docs project administrator then configures a redirect from the
   old site to the new one and disables builds. This step is outside the
   repository.

## Testing Decisions

- Good tests exercise the generator through its public entry point and assert
  on externally observable output (the set of pages written and their complete
  contents), not on internal helpers or intermediate griffe objects. In line
  with the project's conventions, tests build the complete expected page and
  assert equality, with one clear assertion per test where practical.
- Seam: a single seam at the generator's entry point. Given a package to
  document and an output location, it writes the reference pages. Tests run it
  against a small, purpose-built fixture package that covers:
  - `__all__` filtering and exclusion of private names;
  - nested subpackages and re-exports, which resolve to the canonical public
    location;
  - Google-style docstring sections (args, returns, raises, attributes);
  - objects without docstrings (signature only);
  - cross-package type references (linked), and standard library or
    third-party types (not linked);
  - generic type syntax that must survive as valid Markdown;
  - frontmatter (title, sidebar order, `editUrl: false`);
  - anchor and link format, including the site base path.
- Tests are developed test-first, following the project's red-green-refactor
  practice.
- Tests live in the unit suite, in a new area for task tooling. The test
  configuration's Python path is extended so task modules can be imported.
- Prior art: the existing unit tests under the `logicblocks.event` test tree
  (for example the identifier and conversion tests) are plain pytest classes
  grouped by subject, using direct equality assertions on complete values.
- Not unit tested: the Astro and Starlight configuration, carried-over content
  and the workflows. These are verified by the docs build in PR checks
  (including link validation) and by reviewing the branch deployment against
  the old site.

## Out of Scope

- Writing new guides or concepts pages, or substantially rewriting existing
  content.
- Adding or improving docstrings in the source code.
- Versioned documentation (for example `starlight-versions`).
- A custom domain.
- Custom theming or branding beyond Starlight's defaults.
- Per-PR preview deployments.
- Documenting internal or private modules.
- Automated dependency updates for the docs' npm dependencies.
- Administering the Read the Docs project itself (the redirect and disabling
  builds), which is handed to its administrator.

## Further Notes

- Current docstring coverage is low (9 of 140 source files have any), so much
  of the generated reference will be signatures and types only. This is
  accepted for this iteration and is the motivation for a separate docstring
  effort.
- The `griffe2md` trial surfaced Google-style docstring sections that griffe
  can't parse (for example in the Postgres converters). The generator should
  report these as build warnings, not fail the build, so they can be fixed
  later.
- After merge, Read the Docs builds will fail because their configuration has
  been removed, but Read the Docs keeps serving the last successful build until
  the redirect is in place, so there is no gap in availability.
- The existing documentation was built and maintained solely by Toby Clemson.
  Toby, or whoever administers the `eventstore` Read the Docs project, is the
  likely owner of the final redirect step.
