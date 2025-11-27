# Claude Development Guidelines

## Development Approach

### General Principles

- Follow Test-Driven Development (TDD) strictly, using the red-green-refactor 
  cycle, i.e.:
  - Write a failing test first
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

## Clean Code Principles

### Comments and Documentation

- **No redundant comments**: Comments should only highlight extremely
  non-obvious, risky behaviour that readers need to understand
- Use small, well-named functions and methods to express intent instead of
  comments
- Follow a Clean Code approach and let the code speak for itself

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

### Testing Strategy

- Follow Test-Driven Development (TDD) strictly
- Write failing tests before implementation
- Write a test at a time, focusing on one aspect of functionality and only move 
  on to the next when the current test passes
- Test commands:
  - `mise run test:unit` for unit tests
  - `mise run test:unit[TestClassName]` for specific unit test class
  - `mise run test:integration` for integration tests
  - `mise run test:integration[TestClassName]` 
    for specific integration test class
  - `mise run test:component`
    for component tests
- Code quality commands:
  - `mise run lint:fix` for linting
  - `mise run format:fix` for formatting
  - `mise run type:check` for type checking
- Build commands:
  - `mise run` for full 
    build (linting, type checking, formatting, building, and all tests)
  - `mise run build` for library build only
- Changelog commands:
  - `mise run changelog:fragment:create` to generate a changelog fragment
  - `mise run changelog:assemble` to assemble changelog fragments into the
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
