---
date: 2025-11-26T20:43:26+00:00
researcher: Toby Clemson
git_commit: 5f1c67ba94beef907d1100ef221c1e0d31a75329
branch: switch-to-mise-and-uv
repository: event.store
topic: "Poetry to UV Migration Analysis"
tags: [ research, codebase, poetry, uv, migration, mise, invoke, ci-cd ]
status: complete
last_updated: 2025-11-26
last_updated_by: Toby Clemson
last_updated_note: "Updated with uv version management capabilities (uv 0.7.0+)"
---

# Research: Poetry to UV Migration Analysis

**Date**: 2025-11-26T20:43:26+00:00
**Researcher**: Toby Clemson
**Git Commit**: 5f1c67ba94beef907d1100ef221c1e0d31a75329
**Branch**: switch-to-mise-and-uv
**Repository**: event.store

## Research Question

We plan to migrate this codebase from poetry to uv. Perform research of the
entire codebase, including the mise task runner, the invoke tasks,
pyproject.toml and the GitHub Actions workflows so that we can come up with a
plan to do this in future.

## Summary

The codebase uses **Poetry 2.2.1** for dependency management, virtual
environment management, building, and publishing, orchestrated through **mise**
as the task runner which delegates to **invoke** for task execution. Poetry is
deeply integrated throughout the system:

- **24+ `poetry run` commands** in mise.toml
- **7 direct poetry commands** in invoke tasks
- **Poetry-specific sections** in pyproject.toml
- **ReadTheDocs** uses poetry for documentation builds
- **GitHub Actions** rely on mise which installs poetry

Migration to uv will require coordinated changes across all these layers, but as
of **uv 0.7.0**, uv has full feature parity with Poetry including version
management (`uv version --bump`), making the migration straightforward.

## Detailed Findings

### 1. pyproject.toml Configuration

**File:** `pyproject.toml`

#### Build System (Lines 128-130)

```toml
[build-system]
requires = ["poetry-core>=2.0"]
build-backend = "poetry.core.masonry.api"
```

#### Poetry Configuration (Lines 13-16)

```toml
[tool.poetry]
packages = [
    { include = "logicblocks", from = "src" },
]
```

#### Dependency Groups

- **Main dependencies** (Lines 18-25): psycopg, uvloop, pyheck, structlog, rich,
  aiologic
- **Dev dependencies** (Lines 27-38): coverage, pytest, ruff, pyright, etc.
- **Build dependencies** (Lines 40-41): invoke
- **Changelog dependencies** (Lines 43-44): scriv
- **Docs dependencies** (Lines 46-49): mkdocs-material, mkdocstrings, black

### 2. Mise Task Runner Configuration

**File:** `mise.toml`

#### Tool Version Management (Lines 11-14)

```toml
[tools]
python = "3.13.9"
poetry = "2.2.1"
watchexec = "2.3.2"
```

#### Post-install Hook (Line 9)

```toml
postinstall = "poetry install --only=build"
```

#### Task Definitions

All mise tasks use `poetry run invoke <command>` pattern:

| Mise Task                 | Command                              | Line |
|---------------------------|--------------------------------------|------|
| dependencies:install      | `poetry run invoke deps.install`     | 18   |
| build                     | `poetry run invoke build`            | 23   |
| lint:check                | `poetry run invoke lint.check`       | 29   |
| lint:fix                  | `poetry run invoke lint.fix`         | 34   |
| format:check              | `poetry run invoke format.check`     | 39   |
| format:fix                | `poetry run invoke format.fix`       | 44   |
| types:check               | `poetry run invoke types.check`      | 49   |
| test:unit                 | `poetry run invoke test.unit`        | 66   |
| test:integration          | `poetry run invoke test.integration` | 71   |
| test:component            | `poetry run invoke test.component`   | 77   |
| test:report               | `poetry run invoke test.report`      | 82   |
| docs:serve                | `poetry run mkdocs serve`            | 85   |
| docs:build                | `poetry run mkdocs build`            | 88   |
| changelog:fragment:create | `poetry run scrive create`           | 91   |
| changelog:assemble        | `poetry run scrive collect`          | 94   |
| prerelease                | `poetry run invoke prerelease`       | 97   |
| release                   | `poetry run invoke release`          | 100  |

### 3. Invoke Tasks

**Directory:** `tasks/`

#### Direct Poetry Commands

| File                      | Line | Command                  | Purpose              |
|---------------------------|------|--------------------------|----------------------|
| `tasks/deps.py`           | 7    | `poetry install`         | Install dependencies |
| `tasks/build.py`          | 7    | `poetry build`           | Build distribution   |
| `tasks/version.py`        | 9    | `poetry version {type}`  | Bump version         |
| `tasks/shared/project.py` | 5    | `poetry version --short` | Get current version  |
| `tasks/package.py`        | 6    | `poetry publish --build` | Publish to PyPI      |

#### Task Module Structure

```
tasks/
├── __init__.py      # Collection assembly
├── build.py         # Package building
├── deps.py          # Dependency installation
├── format.py        # Code formatting (ruff)
├── git.py           # Git operations
├── lint.py          # Linting (ruff)
├── package.py       # PyPI publishing
├── release.py       # Release workflow
├── test.py          # Test execution
├── types.py         # Type checking (pyright)
├── version.py       # Version bumping
└── shared/
    └── project.py   # Shared utilities
```

#### Known Issue in package.py

The file has bugs - missing `import os` and undefined `settings.url` reference.

### 4. GitHub Actions Workflows

**Directory:** `.github/workflows/`

#### Workflow Files

| File                    | Purpose                                      |
|-------------------------|----------------------------------------------|
| `pr-checks.yaml`        | PR validation (calls check-test-build.yaml)  |
| `check-test-build.yaml` | Reusable workflow for check, test, build     |
| `main.yaml`             | Main branch pipeline with prerelease/release |

#### Poetry Installation

All workflows use `jdx/mise-action@v3` which installs poetry via mise.toml
configuration.

#### Key Steps

**check-test-build.yaml:**

- Line 24: `mise run check`
- Line 51: `mise run test`
- Line 64: `mise build`

**main.yaml:**

- Line 35: `mise run prerelease` (with `PYPI_API_KEY` secret)
- Line 65: `mise run release` (with `PYPI_API_KEY` secret)

### 5. Other Poetry References

#### ReadTheDocs Configuration (`.readthedocs.yaml`)

```yaml
jobs:
  post_create_environment:
    - pip install poetry
  post_install:
    - VIRTUAL_ENV=$READTHEDOCS_VIRTUALENV_PATH poetry install --with docs
```

#### .gitignore (Lines 293-298)

Contains comments about poetry.lock version control.

#### CLAUDE.md (Lines 94-96)

Documents changelog commands using `poetry run poe`.

## Code References

### Poetry Commands

- `pyproject.toml:128-130` - Build system configuration
- `mise.toml:9` - Post-install hook
- `mise.toml:13` - Poetry version specification
- `mise.toml:17-100` - All task definitions
- `tasks/deps.py:7` - `poetry install`
- `tasks/build.py:7` - `poetry build`
- `tasks/version.py:9` - `poetry version {type}`
- `tasks/shared/project.py:5` - `poetry version --short`
- `tasks/package.py:6` - `poetry publish --build`
- `.readthedocs.yaml:9,11` - RTD poetry installation

### Environment Variables

- `POETRY_PYPI_TOKEN_PYPI` - Used in `tasks/package.py` for PyPI authentication
- `PYPI_API_KEY` - GitHub secret mapped to poetry token

## Architecture Insights

### Layered Architecture

```
┌─────────────────────────────────────┐
│         GitHub Actions              │
│    (calls mise tasks directly)      │
└────────────────┬────────────────────┘
                 │
┌────────────────▼────────────────────┐
│           Mise Tasks                │
│  (orchestrates with dependencies)   │
│  - Uses `poetry run invoke ...`     │
│  - Manages tool versions            │
└────────────────┬────────────────────┘
                 │
┌────────────────▼────────────────────┐
│         Invoke Tasks                │
│    (Python task definitions)        │
│  - Calls poetry directly            │
│  - Calls other CLI tools            │
└────────────────┬────────────────────┘
                 │
┌────────────────▼────────────────────┐
│            Poetry                   │
│  - Dependency management            │
│  - Virtual environment              │
│  - Building & Publishing            │
│  - Version management               │
└─────────────────────────────────────┘
```

### Poetry Features in Use

1. **Dependency Groups**: `dev`, `build`, `changelog`, `docs`
2. **Virtual Environment Management**: All commands run via `poetry run`
3. **Version Management**: `poetry version` for bumping
4. **Build System**: poetry-core backend
5. **Publishing**: `poetry publish` with token authentication
6. **Lock File**: `poetry.lock` for reproducibility

## Migration Strategy Considerations

### UV Equivalents for Poetry Commands

As of **uv 0.7.0** (April 2025), uv has full parity with Poetry for version
management via the `uv version` command.

| Poetry Command                  | UV Equivalent                       |
|---------------------------------|-------------------------------------|
| `poetry install`                | `uv sync`                           |
| `poetry install --only=build`   | `uv sync --only-group build`        |
| `poetry install --with docs`    | `uv sync --group docs`              |
| `poetry run <cmd>`              | `uv run <cmd>`                      |
| `poetry build`                  | `uv build`                          |
| `poetry publish`                | `uv publish`                        |
| `poetry version --short`        | `uv version --short`                |
| `poetry version patch`          | `uv version --bump patch`           |
| `poetry version minor`          | `uv version --bump minor`           |
| `poetry version major`          | `uv version --bump major`           |
| `poetry version prerelease`     | `uv version --bump alpha` (or beta/rc) |
| `poetry version prepatch`       | `uv version --bump patch --bump alpha` |
| `POETRY_PYPI_TOKEN_PYPI=<tok>`  | `UV_PUBLISH_TOKEN=<tok>`            |

### UV Version Command Details (uv 0.7.0+)

The `uv version` command provides comprehensive version management:

**Reading version:**

```bash
uv version              # Shows: project-name 1.2.3
uv version --short      # Shows: 1.2.3
```

**Setting exact version:**

```bash
uv version 2.0.0        # Sets version to 2.0.0
uv version 2.0.0 --dry-run  # Preview without changing
```

**Bumping versions with `--bump`:**

Supported components (largest to smallest): `major`, `minor`, `patch`, `stable`,
`alpha`, `beta`, `rc`, `post`, `dev`

```bash
uv version --bump patch           # 1.2.3 => 1.2.4
uv version --bump minor           # 1.2.4 => 1.3.0
uv version --bump major           # 1.3.0 => 2.0.0
uv version --bump beta            # 1.3.0b1 => 1.3.0b2
uv version --bump stable          # 1.3.1b2 => 1.3.1 (clears pre-release)
uv version --bump major --bump alpha  # 1.3.0 => 2.0.0a1
uv version --bump patch --bump beta   # 1.3.0 => 1.3.1b1
```

**Additional flags:**

- `--dry-run` - Preview changes without modifying pyproject.toml
- `--frozen` - Prevent lock and sync after version update
- `--no-sync` - Allow lock but prevent sync

### Files Requiring Changes

1. **pyproject.toml**
  - Convert `[tool.poetry.*]` sections to PEP 621 format
  - Change build-system to `hatchling` or keep `poetry-core`
  - Convert dependency groups to uv format

2. **mise.toml**
  - Change `poetry = "2.2.1"` to `uv = "<version>"`
  - Update postinstall hook
  - Change all `poetry run invoke` to `uv run invoke`
  - Change all `poetry run <tool>` to `uv run <tool>`
  - Update sources to reference `uv.lock` instead of `poetry.lock`

3. **tasks/*.py**
   - `tasks/deps.py` - Change `poetry install` to `uv sync`
   - `tasks/build.py` - Change `poetry build` to `uv build`
   - `tasks/version.py` - Change `poetry version {type}` to
     `uv version --bump {type}`
   - `tasks/package.py` - Change `poetry publish` to `uv publish`
   - `tasks/shared/project.py` - Change `poetry version --short` to
     `uv version --short`

4. **tasks/package.py** - Also fix existing bugs (missing imports)

5. **.readthedocs.yaml**
  - Replace poetry installation with uv
  - Update install command

6. **CLAUDE.md** - Update documentation

7. **poetry.lock** - Remove and generate `uv.lock`

8. **.gitignore** - Update comments to reference uv.lock

### Migration Complexity Assessment

| Area               | Complexity | Reason                                       |
|--------------------|------------|----------------------------------------------|
| pyproject.toml     | Medium     | UV supports Poetry-style dependency groups   |
| mise.toml          | Low        | Simple command replacements                  |
| Invoke tasks       | Low        | Direct uv equivalents for all poetry commands |
| GitHub Actions     | Low        | No direct poetry usage                       |
| ReadTheDocs        | Medium     | Different installation approach              |
| Version management | Low        | `uv version --bump` has full parity (0.7.0+) |

## Open Questions

1. **ReadTheDocs**: Does RTD have native uv support, or do we need `pip install
   uv`?
2. **Package Format**: Should we keep poetry-core build backend or switch to
   hatchling/setuptools?
3. **Lock File Migration**: Should we manually migrate lock file or regenerate
   from scratch?
4. **CI Caching**: Should we add explicit uv cache configuration to GitHub
   Actions?

## References

- [uv Documentation](https://docs.astral.sh/uv/)
- [uv Building and Publishing Guide](https://docs.astral.sh/uv/guides/package/)
- [uv GitHub Repository](https://github.com/astral-sh/uv)
- [astral-sh/setup-uv GitHub Action](https://github.com/astral-sh/setup-uv)
