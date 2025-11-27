# Poetry to UV Migration Implementation Plan

## Overview

Migrate from Poetry 2.2.1 to uv 0.9.12 for dependency management, building,
publishing, and version management. This migration leverages uv's full feature
parity with Poetry (as of uv 0.7.0+) while using hatchling as the build backend
for maximum compatibility.

## Current State Analysis

The codebase currently uses:

- **Poetry 2.2.1** for dependency management, virtual environments, building,
  publishing, and version management
- **mise** as the task runner, orchestrating Poetry commands
- **invoke** for Python task definitions that call Poetry directly
- **poetry-core** as the build backend

Poetry is referenced in:

- `pyproject.toml` - Build system and dependency configuration
- `mise.toml` - Tool installation and 17+ task definitions
- `tasks/*.py` - 5 invoke task files with direct Poetry commands
- `.readthedocs.yaml` - Documentation build configuration
- `CLAUDE.md` - Developer documentation

## Desired End State

After migration:

- **uv 0.9.12** handles all dependency management, building, publishing, and
  version management
- **hatchling** serves as the build backend
- All mise tasks use `uv run` instead of `poetry run`
- All invoke tasks use uv commands instead of poetry commands
- `uv.lock` replaces `poetry.lock` with equivalent dependency versions
- Documentation builds with uv instead of Poetry
- Full CI/CD pipeline works unchanged (mise abstracts the tool change)

### Verification

- `mise run default` completes successfully (build, fix, test)
- `mise run check` passes (lint, format, types)
- `mise run test` passes all unit, integration, and component tests
- `uv build` produces valid wheel and sdist
- Documentation builds on ReadTheDocs

## What We're NOT Doing

- Changing the project structure or package layout
- Modifying test configurations or test code
- Changing GitHub Actions workflows (they use mise, which abstracts the tool)
- Upgrading dependencies (preserving current versions)
- Changing the release workflow logic

## Implementation Approach

The migration follows a bottom-up approach:

1. Update `pyproject.toml` first (foundational configuration)
2. Update `mise.toml` (tool installation and task runner)
3. Update invoke tasks (Python task definitions)
4. Update auxiliary files (RTD, docs)
5. Migrate lock file (preserve versions)
6. Verify everything works

---

## Phase 1: Update pyproject.toml

### Overview

Convert Poetry-specific configuration to PEP 621 standard format with hatchling
build backend and uv dependency groups.

### Changes Required

#### 1. Convert Build System

**File**: `pyproject.toml`

**Current** (lines 128-130):

```toml
[build-system]
requires = ["poetry-core>=2.0"]
build-backend = "poetry.core.masonry.api"
```

**New**:

```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

#### 2. Add Hatchling Package Configuration

**File**: `pyproject.toml`

**Add after `[project]` section** (replaces `[tool.poetry]` packages config):

```toml
[tool.hatch.build.targets.wheel]
packages = ["src/logicblocks"]

[tool.hatch.build.targets.sdist]
include = ["src/logicblocks"]
```

#### 3. Convert Main Dependencies to PEP 621 Format

**File**: `pyproject.toml`

**Current** `[tool.poetry.dependencies]` (lines 18-25):

```toml
[tool.poetry.dependencies]
python = ">=3.13,<4.0"
psycopg = { extras = ["binary", "pool"], version = "^3.2.3" }
uvloop = "^0.21.0"
pyheck = "^0.1.5"
structlog = "^25.1.0"
rich = "^14.0.0"
aiologic = "^0.14.0"
```

**New** - Add to `[project]` section (after line 11):

```toml
dependencies = [
    "psycopg[binary,pool]>=3.2.3,<4",
    "uvloop>=0.21.0,<0.22",
    "pyheck>=0.1.5,<0.2",
    "structlog>=25.1.0,<26",
    "rich>=14.0.0,<15",
    "aiologic>=0.14.0,<0.15",
]
```

#### 4. Convert Dependency Groups

**File**: `pyproject.toml`

**Current** Poetry groups (lines 27-49):

```toml
[tool.poetry.group.dev.dependencies]
coverage = "^7.6.10"
junitparser = "^3.2.0"
pyright = { extras = ["nodejs"], version = "^1.1.391" }
pytest = "^8.3.4"
pytest-asyncio = "^0.26.0"
pytest-cov = "^6.0.0"
pytest-diff = "^0.1.14"
pytest-pretty = "^1.2.0"
pytest-repeat = "^0.9.3"
pytest-unordered = "^0.6.1"
ruff = "^0.11.0"

[tool.poetry.group.build.dependencies]
invoke = "^2.2.1"

[tool.poetry.group.changelog.dependencies]
scriv = "^1.5.1"

[tool.poetry.group.docs.dependencies]
mkdocs-material = "^9.5.47"
mkdocstrings = { extras = ["python"], version = "^0.29.0" }
black = "^25.1.0"
```

**New** - Replace with `[dependency-groups]`:

```toml
[dependency-groups]
dev = [
    "coverage>=7.6.10,<8",
    "junitparser>=3.2.0,<4",
    "pyright[nodejs]>=1.1.391,<2",
    "pytest>=8.3.4,<9",
    "pytest-asyncio>=0.26.0,<0.27",
    "pytest-cov>=6.0.0,<7",
    "pytest-diff>=0.1.14,<0.2",
    "pytest-pretty>=1.2.0,<2",
    "pytest-repeat>=0.9.3,<0.10",
    "pytest-unordered>=0.6.1,<0.7",
    "ruff>=0.11.0,<0.12",
]
build = [
    "invoke>=2.2.1,<3",
]
changelog = [
    "scriv>=1.5.1,<2",
]
docs = [
    "mkdocs-material>=9.5.47,<10",
    "mkdocstrings[python]>=0.29.0,<0.30",
    "black>=25.1.0,<26",
]
```

#### 5. Update scriv Configuration

**File**: `pyproject.toml`

**Current** (lines 124-126):

```toml
[tool.scriv]
version = "literal: pyproject.toml: tool.poetry.version"
format = "md"
```

**New**:

```toml
[tool.scriv]
version = "literal: pyproject.toml: project.version"
format = "md"
```

#### 6. Remove Poetry-Specific Sections

**File**: `pyproject.toml`

**Delete entirely**:

- `[tool.poetry]` section (lines 13-16)
- `[tool.poetry.dependencies]` section (lines 18-25)
- `[tool.poetry.group.dev.dependencies]` section (lines 27-38)
- `[tool.poetry.group.build.dependencies]` section (lines 40-41)
- `[tool.poetry.group.changelog.dependencies]` section (lines 43-44)
- `[tool.poetry.group.docs.dependencies]` section (lines 46-49)

### Success Criteria

#### Automated Verification

- [x] `uv sync` installs all dependencies without errors
- [x] `uv build` creates wheel and sdist in `dist/`
- [x] `uv run python -c "import logicblocks.event.store"` succeeds

#### Manual Verification

- [x] Review pyproject.toml for correctness
- [x] Verify all dependency versions match current poetry.lock

---

## Phase 2: Update mise.toml

### Overview

Replace Poetry tool with uv and update all task commands to use uv equivalents.

### Changes Required

#### 1. Update Tool Installation

**File**: `mise.toml`

**Current** (lines 11-14):

```toml
[tools]
python = "3.13.9"
poetry = "2.2.1"
watchexec = "2.3.2"
```

**New**:

```toml
[tools]
python = "3.13.9"
uv = "0.9.12"
watchexec = "2.3.2"
```

#### 2. Update Post-install Hook

**File**: `mise.toml`

**Current** (line 9):

```toml
postinstall = "poetry install --only=build"
```

**New**:

```toml
postinstall = "uv sync --only-group build"
```

#### 3. Update Dependencies Task

**File**: `mise.toml`

**Current** (lines 16-18):

```toml
[tasks."dependencies:install"]
sources = ["pyproject.toml", "poetry.lock"]
run = "poetry run invoke deps.install"
```

**New**:

```toml
[tasks."dependencies:install"]
sources = ["pyproject.toml", "uv.lock"]
run = "uv run invoke deps.install"
```

#### 4. Update All Task Commands

**File**: `mise.toml`

Replace `poetry run` with `uv run` in all tasks:

| Line | Current                              | New                              |
|------|--------------------------------------|----------------------------------|
| 23   | `poetry run invoke build`            | `uv run invoke build`            |
| 29   | `poetry run invoke lint.check`       | `uv run invoke lint.check`       |
| 34   | `poetry run invoke lint.fix`         | `uv run invoke lint.fix`         |
| 39   | `poetry run invoke format.check`     | `uv run invoke format.check`     |
| 44   | `poetry run invoke format.fix`       | `uv run invoke format.fix`       |
| 49   | `poetry run invoke types.check`      | `uv run invoke types.check`      |
| 66   | `poetry run invoke test.unit`        | `uv run invoke test.unit`        |
| 71   | `poetry run invoke test.integration` | `uv run invoke test.integration` |
| 77   | `poetry run invoke test.component`   | `uv run invoke test.component`   |
| 82   | `poetry run invoke test.report`      | `uv run invoke test.report`      |
| 85   | `poetry run mkdocs serve`            | `uv run mkdocs serve`            |
| 88   | `poetry run mkdocs build`            | `uv run mkdocs build`            |
| 91   | `poetry run scriv create`            | `uv run scriv create`            |
| 94   | `poetry run scriv collect`           | `uv run scriv collect`           |
| 97   | `poetry run invoke prerelease`       | `uv run invoke prerelease`       |
| 100  | `poetry run invoke release`          | `uv run invoke release`          |

### Success Criteria

#### Automated Verification

- [x] `mise install` completes without errors
- [x] `mise run dependencies:install` succeeds
- [x] `mise run check` passes (lint and format pass; types has pre-existing error)

#### Manual Verification

- [x] Verify mise recognizes uv tool: `mise ls`

---

## Phase 3: Update Invoke Tasks

### Overview

Update all invoke task files to use uv commands instead of Poetry commands.

### Changes Required

#### 1. Update tasks/deps.py

**File**: `tasks/deps.py`

**Current** (line 7):

```python
context.run("poetry install")
```

**New**:

```python
context.run("uv sync")
```

#### 2. Update tasks/build.py

**File**: `tasks/build.py`

**Current** (line 7):

```python
context.run("poetry build")
```

**New**:

```python
context.run("uv build")
```

#### 3. Update tasks/version.py

**File**: `tasks/version.py`

**Current** (line 9):

```python
context.run(f"poetry version {type}")
```

**New**:

```python
context.run(f"uv version --bump {type}")
```

#### 4. Update tasks/package.py

**File**: `tasks/package.py`

**Current** (lines 6-10):

```python
@task
def publish(context: Context) -> None:
    """Publish library to pypi."""
    context.run(f"poetry publish --build", env={
        "POETRY_PYPI_TOKEN_PYPI": os.getenv("PYPI_API_KEY"),
    })
```

**New**:

```python
@task
def publish(context: Context) -> None:
    """Publish library to pypi."""
    context.run("uv publish", env={
        "UV_PUBLISH_TOKEN": os.getenv("PYPI_API_KEY"),
    })
```

Note: `uv publish` does not have a `--build` flag; build separately if needed.
However, the release workflow calls `build` task before `publish`, so this is
fine.

#### 5. Update tasks/shared/project.py

**File**: `tasks/shared/project.py`

**Current** (line 5):

```python
return context.run("poetry version --short").stdout.strip()
```

**New**:

```python
return context.run("uv version --short").stdout.strip()
```

### Success Criteria

#### Automated Verification

- [x] `uv run invoke deps.install` succeeds
- [x] `uv run invoke build` creates artifacts in `dist/`
- [x] `uv run invoke lint.check` passes
- [x] `uv run invoke types.check` passes (pre-existing type error in test code)

#### Manual Verification

- [x] Review each task file for correctness

---

## Phase 4: Update Documentation and Config Files

### Overview

Update ReadTheDocs configuration, developer documentation, and gitignore.

### Changes Required

#### 1. Update .readthedocs.yaml

**File**: `.readthedocs.yaml`

**Current** (lines 7-11):

```yaml
  jobs:
    post_create_environment:
      - pip install poetry
    post_install:
      - VIRTUAL_ENV=$READTHEDOCS_VIRTUALENV_PATH poetry install --with docs
```

**New**:

```yaml
  jobs:
    post_create_environment:
      - pip install uv
    post_install:
      - uv sync --group docs
```

#### 2. Update CLAUDE.md

**File**: `CLAUDE.md`

**Current** (lines 93-96):

```markdown
- Changelog commands:
  - `poetry run poe changelog-fragment-create` to generate a changelog fragment
  - `poetry run poe changelog-assemble` to assemble changelog fragments into the
    CHANGELOG.md file
```

**New**:

```markdown
- Changelog commands:
  - `uv run scriv create` to generate a changelog fragment
  - `uv run scriv collect` to assemble changelog fragments into the
    CHANGELOG.md file
```

Note: The current documentation references `poe` (poethepoet) but the actual
mise tasks use `scriv` directly. Updating to match actual implementation.

#### 3. Update .gitignore (Optional)

**File**: `.gitignore`

**Current** (lines 293-298):

```gitignore
# poetry
#   Similar to Pipfile.lock, it is generally recommended to include poetry.lock in version control.
#   This is especially recommended for binary packages to ensure reproducibility, and is more
#   commonly ignored for libraries.
#   https://python-poetry.org/docs/basic-usage/#commit-your-poetrylock-file-to-version-control
#poetry.lock
```

**New**:

```gitignore
# uv
#   Similar to Pipfile.lock, it is generally recommended to include uv.lock in version control.
#   This is especially recommended for binary packages to ensure reproducibility, and is more
#   commonly ignored for libraries.
#   https://docs.astral.sh/uv/concepts/projects/layout/#the-lockfile
#uv.lock
```

### Success Criteria

#### Automated Verification

- [x] `.readthedocs.yaml` is valid YAML:
  `python -c "import yaml; yaml.safe_load(open('.readthedocs.yaml'))"`

#### Manual Verification

- [x] Review CLAUDE.md changes
- [ ] Trigger RTD build after merge to verify docs build

---

## Phase 5: Lock File Migration

### Overview

Remove poetry.lock and generate uv.lock while preserving dependency versions.

### Changes Required

#### 1. Export Current Dependency Versions

Before deleting poetry.lock, export the resolved versions for reference:

```bash
poetry export -f requirements.txt --without-hashes > requirements-reference.txt
```

#### 2. Generate uv.lock

```bash
uv lock
```

#### 3. Verify Versions Match

Compare the resolved versions in `uv.lock` with `requirements-reference.txt` to
ensure critical dependencies match.

#### 4. Clean Up

```bash
rm poetry.lock
rm requirements-reference.txt
```

### Success Criteria

#### Automated Verification

- [x] `uv lock` completes without errors
- [x] `uv sync` installs from uv.lock successfully
- [x] No poetry.lock file exists

#### Manual Verification

- [x] Compare key dependency versions between old and new lock files
- [x] Verify psycopg, pytest, ruff versions match

---

## Phase 6: Full Verification

### Overview

Run the complete build pipeline to verify the migration is successful.

### Changes Required

None - this phase is verification only.

### Success Criteria

#### Automated Verification

- [x] `mise run check` passes (lint, format pass; types has pre-existing error)
- [x] `mise run test:unit` passes (1546 tests)
- [ ]
  `DOCKER_HOST="unix://${HOME}/.colima/default/docker.sock" mise run test:integration`
  passes
- [ ]
  `DOCKER_HOST="unix://${HOME}/.colima/default/docker.sock" mise run test:component`
  passes
- [x] `mise run build` succeeds
- [ ] `mise run docs:build` succeeds (pre-existing module path issue)

#### Manual Verification

- [ ] `mise run default` completes full pipeline
- [x] Verify wheel can be installed:
  `uv pip install dist/*.whl --force-reinstall`
- [x] Import test: `uv run python -c "from logicblocks.event.store import *"`

---

## Testing Strategy

### Unit Tests

- Run via `mise run test:unit`
- All existing tests should pass unchanged

### Integration Tests

- Run via `mise run test:integration`
- Requires Docker for PostgreSQL
- All existing tests should pass unchanged

### Component Tests

- Run via `mise run test:component`
- Requires Docker for PostgreSQL
- All existing tests should pass unchanged

### Manual Testing Steps

1. Clean environment: `rm -rf .venv uv.lock`
2. Install tools: `mise install`
3. Sync dependencies: `uv sync`
4. Run full build: `mise run default`
5. Build package: `uv build`
6. Install and test wheel locally

---

## Rollback Plan

If the migration fails:

1. Revert all changes: `git checkout .`
2. Restore poetry.lock if deleted
3. Run `mise install` to reinstall Poetry
4. Run `poetry install` to restore environment

---

## References

- Research document: `meta/research/2025-11-26-poetry-to-uv-migration.md`
- [uv Documentation](https://docs.astral.sh/uv/)
- [uv GitHub Releases](https://github.com/astral-sh/uv/releases)
- [Hatchling Documentation](https://hatch.pypa.io/latest/)
- [PEP 621 - Project Metadata](https://peps.python.org/pep-0621/)
