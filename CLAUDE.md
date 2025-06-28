# Claude Development Guidelines

## Core Philosophy

TEST-DRIVEN DEVELOPMENT IS NON-NEGOTIABLE. Every single line of production code 
must be written in response to a failing test. No exceptions. This is not a 
suggestion or a preference - it is the fundamental practice that enables all 
other principles in this document.

I follow Test-Driven Development (TDD) with a strong emphasis on 
behaviour-driven testing and object-oriented principles. All work should be 
done in small, incremental changes that maintain a working state throughout 
development.

## Development Approach

### General Principles

- Follow Test-Driven Development (TDD) strictly, using the red-green-refactor 
  cycle, i.e.:
  - Write a failing test first, testing behaviour, not implementation
  - Implement the minimum code to make the test pass
  - Refactor the code to improve quality while ensuring all tests still pass
- Follow SOLID principles:
  1. Single Responsibility Principle (SRP): A class should have only one reason 
     to change—it should have only one job or responsibility.
  2. Open/Closed Principle (OCP): Software entities should be open for extension
     but closed for modification.
  3. Liskov Substitution Principle (LSP): Objects of a superclass should be 
     replaceable with objects of a subclass without breaking the application.
  4. Interface Segregation Principle (ISP): Clients should not be forced to 
     depend on interfaces they don't use.
  5. Dependency Inversion Principle (DIP): High-level modules should not depend
     on low-level modules. Both should depend on abstractions.
- Follow the following additional principles:
  1. Encapsulation: Hide internal implementation details and expose only what's
     necessary through public interfaces.
  2. Abstraction: Focus on essential features while hiding complex 
     implementation details.
  3. Inheritance: Create new classes based on existing ones to promote code 
     reuse.
  4. Polymorphism: Allow objects of different types to be treated as instances 
     of the same type through a common interface.
  5. Composition over Inheritance: Favour object composition over class 
     inheritance for flexibility.
  6. Don't Repeat Yourself (DRY): Avoid code duplication by extracting common 
     functionality.
  7. Law of Demeter: A unit should only talk to its immediate friends; don't 
     talk to strangers.

### Development Workflow

#### TDD Process - THE FUNDAMENTAL PRACTICE
CRITICAL: TDD is not optional. Every feature, every bug fix, every change MUST 
follow this process:

Follow Red-Green-Refactor strictly:

1. Red: Write a failing test for the desired behavior. NO PRODUCTION CODE until 
   you have a failing test.
2. Green: Write the MINIMUM code to make the test pass. Resist the urge to write
   more than needed.
3. Refactor: Assess the code for improvement opportunities. If refactoring would
   add value, clean up the code while keeping tests green. If the code is 
   already clean and expressive, move on.

Common TDD Violations to Avoid:

- Writing production code without a failing test first
- Writing multiple tests before making the first one pass
- Writing more production code than needed to pass the current test
- Skipping the refactor assessment step when code could be improved
- Adding functionality "while you're there" without a test driving it

- Remember: If you're typing production code and there isn't a failing test 
  demanding that code, you're not doing TDD.

#### Refactoring - The Critical Third Step

Evaluating refactoring opportunities is not optional - it's the third step in 
the TDD cycle. After achieving a green state and committing your work, you MUST 
assess whether the code can be improved. However, only refactor if there's 
clear value - if the code is already clean and expresses intent well, move on 
to the next test.

##### What is Refactoring?
Refactoring means changing the internal structure of code without changing its 
external behavior. The public API remains unchanged, all tests continue to pass,
but the code becomes cleaner, more maintainable, or more efficient. Remember: 
only refactor when it genuinely improves the code - not all code needs 
refactoring.

##### When to Refactor

- Always assess after green: Once tests pass, before moving to the next test, 
  evaluate if refactoring would add value
- When you see duplication: But understand what duplication really means 
  (see DRY below)
- When names could be clearer: Variable names, function names, or type names 
  that don't clearly express intent
- When structure could be simpler: Complex conditional logic, deeply nested 
  code, or long functions
- When patterns emerge: After implementing several similar features, useful 
  abstractions may become apparent

Remember: Not all code needs refactoring. If the code is already clean, 
expressive, and well-structured, commit and move on. Refactoring should improve 
the code - don't change things just for the sake of change.

##### Refactoring Guidelines

1. Commit Before Refactoring

Always commit your working code before starting any refactoring. This gives you 
a safe point to return to.

2. Look for Useful Abstractions Based on Semantic Meaning

Create abstractions only when code shares the same semantic meaning and purpose. 
Don't abstract based on structural similarity alone - duplicate code is far 
cheaper than the wrong abstraction.

Questions to ask before abstracting:

- Do these code blocks represent the same concept or different concepts that 
  happen to look similar?
- If the business rules for one change, should the others change too?
- Would a developer reading this abstraction understand why these things are 
  grouped together?
- Am I abstracting based on what the code IS (structure) or what it MEANS 
  (semantics)?

Remember: It's much easier to create an abstraction later when the semantic 
relationship becomes clear than to undo a bad abstraction that couples 
unrelated concepts.

3. Understanding DRY - It's About Knowledge, Not Code

DRY (Don't Repeat Yourself) is about not duplicating knowledge in the system, 
not about eliminating all code that looks similar.

4. Maintain External APIs During Refactoring

Refactoring must never break existing consumers of your code.

5. Verify and Commit After Refactoring

CRITICAL: After every refactoring:
- Run all tests - they must pass without modification
- Run static analysis (linting, type checking) - must pass
- Commit the refactoring separately from feature changes

## Clean Code Principles

### Comments and Documentation

- **No redundant comments**: Comments should only highlight extremely
  non-obvious, risky behaviour that readers need to understand
- Use small, well-named functions and methods to express intent instead of
  comments
- Follow a Clean Code approach and let the code speak for itself

### Code Structure

- No nested if/else statements - use early returns, guard clauses, or 
  composition
- Avoid deep nesting in general (max 2 levels)
- Keep functions and methods small and focused on a single responsibility
- Prefer flat, readable code over clever abstractions

### Naming Conventions

- Functions: snake_case, verb-based (e.g., calculate_total, validate_payment)
- Types: PascalCase (e.g., PaymentRequest, UserProfile)
- Constants: UPPER_SNAKE_CASE for true constants, camelCase for configuration
- Files: snake_case.py for all Python files, but prefer single word module and 
  package names
- Test files: test_*.py

### Python Version and Modern Features

- Target Python 3.13 or greater
- Use modern Python features and idioms:
  - Use `dict[str, Type]` instead of `Dict[str, Type]`
  - Use `class Klass[Name, Payload]` syntax for generic type variables
  - Use `TypedDict` for method/function parameter types instead of dataclasses 
    where appropriate
  - Leverage `@overload` for method signature variations

### Type System

- Use generic types with sensible defaults
- Make optional parameters truly optional with `NotRequired[Type]` in TypedDict
- Avoid explicit type hints where Python's inference is enough
- Use method overloading with `@overload` instead of separate method names
- Prefer immutable collection protocols from `collections.abc` (e.g., `Mapping` 
  instead of `dict`) for function signatures

## Testing

### Testing Principles

#### Behaviour-Driven Testing

- Tests should verify expected behaviour, treating implementation as a black box
- Test through the public API exclusively - internals should be invisible to 
  tests
- Tests that examine internal implementation details are wasteful and should be 
  avoided
- Coverage targets: 100% coverage should be expected at all times, but these 
  tests must ALWAYS be based on business behaviour, not implementation details
- Tests must document expected business behaviour

### Testing Strategy

- Follow Test-Driven Development (TDD) strictly
- Write failing tests before implementation
- Write a test at a time, focusing on one aspect of functionality and only move 
  on to the next when the current test passes
- Test commands:
  - `./go library:test:unit` for unit tests
  - `./go library:test:unit[TestClassName]` for specific unit test class
  - `DOCKER_HOST="unix://${HOME}/.colima/default/docker.sock" ./go library:test:integration` 
    for integration tests
  - `DOCKER_HOST="unix://${HOME}/.colima/default/docker.sock" ./go library:test:integration[TestClassName]` 
    for specific integration test class
  - `DOCKER_HOST="unix://${HOME}/.colima/default/docker.sock" ./go library:test:component`
    for component tests
- Code quality commands:
  - `./go library:lint:fix` for linting
  - `./go library:format:fix` for formatting
  - `./go library:type:check` for type checking
- Build commands:
  - `DOCKER_HOST="unix://${HOME}/.colima/default/docker.sock" ./go` for full 
    build (linting, type checking, formatting, building, and all tests)
  - `./go library:build` for library build only
- Changelog commands:
  - `poetry run poe changelog-fragment-create` to generate a changelog fragment
  - `poetry run poe changelog-assemble` to assemble changelog fragments into the
    CHANGELOG.md file

### Test Structure

- Follow existing test patterns in the codebase
- Prefer single assertion tests with one clear assertion per test where 
  practical
- Use parameterised test suites where applicable
- Extend shared test cases for adapter implementations
- Build complete expected results and assert equality rather than multiple
  smaller assertions
- Use clear, descriptive test names that indicate the operation type

## Working with git

- Keep commit messages to 8 lines or less
- Don't include any references to Claude
- Don't include Claude as a co-author

## Working with Claude

### Expectations

When working with my code:

1. ALWAYS FOLLOW TDD - No production code without a failing test. This is not 
   negotiable.
2. Think deeply before making any edits
3. Understand the full context of the code and requirements
4. Ask clarifying questions when requirements are ambiguous
5. Think from first principles - don't make assumptions
6. Assess refactoring after every green - Look for opportunities to improve 
   code structure, but only refactor if it adds value
7. Keep project docs current - update them whenever you introduce meaningful 
   changes

### Code Changes

When suggesting or making changes:

- Start with a failing test - always. No exceptions.
- After making tests pass, always assess refactoring opportunities (but only 
  refactor if it adds value)
- After refactoring, verify all tests and static analysis pass, then commit
- Respect the existing patterns and conventions
- Maintain test coverage for all behaviour changes
- Keep changes small and incremental
- Provide rationale for significant design decisions

If you find yourself writing production code without a failing test, STOP 
immediately and write the test first.

### Communication

- Be explicit about trade-offs in different approaches
- Explain the reasoning behind significant design decisions
- Flag any deviations from these guidelines with justification
- Suggest improvements that align with these principles
- When unsure, ask for clarification rather than assuming

## Using Gemini CLI for Large Codebase Analysis

When analysing large codebases or multiple files that might exceed context 
limits, use the Gemini CLI with its massive context window. Use `gemini -p` to 
leverage Google Gemini's large context capacity.

### File and Directory Inclusion Syntax

Use the `@` syntax to include files and directories in your Gemini prompts. The 
paths should be relative to WHERE you run the gemini command.

#### Research Examples:

**Single file analysis:**
```shell
gemini -p "@src/main.py Explain this file's purpose and structure"
```

Multiple files:
```shell
gemini -p "@package.json @src/index.js Analyse the dependencies used in the code"
```

Entire directory:
```shell
gemini -p "@src/ Summarise the architecture of this codebase"
```

Multiple directories:
```shell
gemini -p "@src/ @tests/ Analyse test coverage for the source code"
```

Current directory and subdirectories:
```shell
gemini -p "@./ Give me an overview of this entire project"

# Or use --all_files flag:
gemini --all_files -p "Analyse the project structure and dependencies"
```

#### Implementation Verification Examples

Check if a feature is implemented:
```shell
gemini -p "@src/ @lib/ Has dark mode been implemented in this codebase? Show me the relevant files and functions"
```

Verify authentication implementation:
```shell
gemini -p "@src/ @middleware/ Is JWT authentication implemented? List all auth-related endpoints and middleware"
```

Check for specific patterns:
```shell
gemini -p "@src/ Are there any React hooks that handle WebSocket connections? List them with file paths"
```

Verify error handling:
```shell
gemini -p "@src/ @api/ Is proper error handling implemented for all API endpoints? Show examples of try-catch blocks"
```

Check for rate limiting:
```shell
gemini -p "@backend/ @middleware/ Is rate limiting implemented for the API? Show the implementation details"
```

Verify caching strategy:
```shell
gemini -p "@src/ @lib/ @services/ Is Redis caching implemented? List all cache-related functions and their usage"
```

Check for specific security measures:
```shell
gemini -p "@src/ @api/ Are SQL injection protections implemented? Show how user inputs are sanitised"
```

Verify test coverage for features:
```shell
gemini -p "@src/payment/ @tests/ Is the payment processing module fully tested? List all test cases"
```

### When to Use Gemini CLI

Use `gemini -p` when:
- Analysing entire codebases or large directories
- Comparing multiple large files
- You need to understand project-wide patterns or architecture
- The current context window is insufficient for the task
- You are working with files totalling more than 100KB
- You are verifying if specific features, patterns, or security measures are 
  implemented
- Checking for the presence of certain coding patterns across the entire 
  codebase

### Important Notes

- Paths in @ syntax are relative to your current working directory when invoking
  gemini
- The CLI will include file contents directly in the context
- No need for --yolo flag for read-only analysis
- Gemini's context window can handle entire codebases that would overflow 
  Claude's context
- When checking implementations, be specific about what you're looking for to 
  get accurate results

## Summary
The key is to write clean, testable, functional code that evolves through small, 
safe increments. Every change should be driven by a test that describes the 
desired behaviour, and the implementation should be the simplest thing that 
makes that test pass. When in doubt, favor simplicity and readability over 
cleverness.
