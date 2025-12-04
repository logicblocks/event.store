# Contributing to logicblocks.event.store

Thank you for your interest in contributing to logicblocks.event.store! This
document provides guidelines and instructions for contributing.

## Code of Conduct

This project adheres to the [Contributor Covenant Code of Conduct](CODE_OF_CONDUCT.md).
By participating, you are expected to uphold this code. Please report
unacceptable behaviour to maintainers@infrablocks.io.

## How to Contribute

### Reporting Bugs

Before submitting a bug report:

1. Check the [existing issues](https://github.com/logicblocks/event.store/issues)
   to avoid duplicates
2. Collect information about the bug:
   - Stack trace and error messages
   - Python version (`python --version`)
   - Package version (`pip show logicblocks.event.store`)
   - Steps to reproduce the issue
   - Expected vs actual behaviour

Submit bug reports via [GitHub Issues](https://github.com/logicblocks/event.store/issues/new).

### Suggesting Features

Feature requests are welcome. Please:

1. Check existing issues and discussions first
2. Clearly describe the use case and motivation
3. Provide examples of how the feature would be used

### Submitting Changes

1. Fork the repository
2. Create a feature branch from `main`
3. Make your changes following the guidelines below
4. Submit a pull request

## Development Setup

### Prerequisites

- [mise](https://mise.jdx.dev/) - for tool version management
- [Docker](https://www.docker.com/) - for running integration tests (PostgreSQL)

### Getting Started

1. Clone your fork:

   ```shell
   git clone https://github.com/<your-username>/event.store.git
   cd event.store
   ```

2. Install tools and dependencies:

   ```shell
   mise install
   mise run dependencies:install
   ```

   This installs:
   - Python 3.13
   - uv (package manager)
   - All project dependencies

3. Verify your setup:

   ```shell
   mise run check
   ```

### Running Tests

The project has three test suites:

```shell
# Unit tests (no external dependencies)
mise run test:unit

# Integration tests (requires PostgreSQL via Docker)
mise run test:integration

# Component tests (requires PostgreSQL via Docker)
mise run test:component

# All tests
mise run test

# Run a specific test class
mise run test:unit[TestClassName]
```

Integration and component tests automatically provision a PostgreSQL container
via Docker Compose.

### Code Quality

```shell
# Run all checks
mise run check

# Individual checks
mise run lint:check      # Linting (ruff)
mise run format:check    # Formatting (ruff)
mise run types:check     # Type checking (pyrefly)

# Auto-fix issues
mise run fix             # Run all fixers
mise run lint:fix        # Fix linting issues
mise run format:fix      # Fix formatting issues
```

### Full Build

Run the complete build pipeline (lint, format, type check, build, all tests):

```shell
mise run
```

## Development Guidelines

### Test-Driven Development

This project follows TDD strictly. For every change:

1. **Red**: Write a failing test first
2. **Green**: Implement the minimum code to pass the test
3. **Refactor**: Improve the code while keeping tests green

### Code Style

#### Python Version

Target Python 3.13+. Use modern Python features:

```python
# Use built-in generics
def process(items: dict[str, list[int]]) -> None: ...

# Use new generic syntax
class Repository[T]:
    def get(self, id: str) -> T: ...

# Use TypedDict for complex parameters
class Options(TypedDict):
    timeout: NotRequired[int]
    retries: int
```

#### Type Hints

- Use type hints for public APIs
- Prefer immutable protocols from `collections.abc` (e.g., `Mapping` over `dict`)
- Use `@overload` for functions with multiple signatures
- Let Python infer types where obvious

#### Comments

- Avoid redundant comments; let code be self-documenting
- Use clear, descriptive names instead of comments
- Only comment non-obvious, risky behaviour

#### Design Principles

Follow SOLID principles and favour:

- Composition over inheritance
- Small, focused classes and functions
- Dependency injection
- Immutability where practical

### Test Structure

- One assertion per test where practical
- Clear, descriptive test names
- Follow existing patterns in the codebase
- Use parameterised tests for variations
- Build complete expected results and assert equality

## Pull Request Process

### Before Submitting

1. Ensure all tests pass: `mise run test`
2. Ensure code quality checks pass: `mise run check`
3. Add a changelog fragment (see below)
4. Update documentation if needed

### Changelog Fragments

This project uses [scriv](https://scriv.readthedocs.io/) for changelog
management. For user-facing changes:

1. Create a changelog fragment:

   ```shell
   mise run changelog:fragment:create
   ```

2. Edit the generated file in `changelog.d/` to describe your change
3. Uncomment the appropriate section (Added, Changed, Fixed, etc.)
4. Delete sections you don't use

Fragment categories:

- **Added**: New features
- **Changed**: Changes to existing functionality
- **Deprecated**: Features marked for removal
- **Removed**: Removed features
- **Fixed**: Bug fixes
- **Security**: Security-related changes

### PR Guidelines

- Keep PRs focused on a single concern
- Write a clear description of what and why
- Reference related issues
- Document breaking changes prominently with migration guidance
- Respond to review feedback promptly

### Review Process

All submissions require review. Maintainers will:

1. Review code quality and test coverage
2. Verify adherence to project guidelines
3. Test the changes locally if needed
4. Provide feedback or approve

## Questions?

- Open a [GitHub Discussion](https://github.com/logicblocks/event.store/discussions)
  for questions
- Check existing issues and discussions
- Review the [documentation](https://eventstore.readthedocs.io/)

## License

By contributing, you agree that your contributions will be licensed under the
[MIT License](LICENSE.md).
