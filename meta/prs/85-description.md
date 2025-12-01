## Summary

This PR switches the project's type checker from pyright to pyrefly. The stricter type checking revealed several type safety issues in the error handling APIs, which have been fixed with breaking changes that improve type correctness and API clarity.

## Motivation

Pyrefly provides stricter type checking than pyright, helping catch type errors that were previously undetected. During the migration, several type safety issues were discovered in the error handling system, particularly around default values that couldn't be correctly typed. Rather than work around these issues, this PR fixes them properly, resulting in a cleaner and more type-safe API.

## Changes

### Key Changes

- Replace pyright with pyrefly as the type checker
- Fix type errors in error handler implementations by removing incorrectly typed defaults
- Replace the generic `error_handler_type_mapping` function with four specialized functions
- Make `ContinueErrorHandler` and `ErrorHandlerDecision.continue_execution` require explicit values
- Rename `TypeMappingErrorHandler.default_decision_factory` to `default_error_handler`
- Make `make_subscriber` require explicit `subscriber_state_converter` parameter
- Add new `make_event_store_subscriber` convenience function
- Implement proper `__eq__` and `__hash__` on all `WriteCondition` subclasses

### Implementation Details

The error handling API changes were driven by type safety concerns:

1. **`ContinueErrorHandler`**: Previously accepted an optional `value_factory` with a default of `lambda _: None`, but this couldn't be correctly typed because the generic parameter `T` could be anything. Now requires an explicit factory.

2. **`ExitErrorHandler`**: Changed from `exit_code: int` to `exit_code_factory: Callable[[BaseException], int]` for consistency with other handlers and to allow dynamic exit codes.

3. **`TypeMappingErrorHandler`**: The `type_mappings` parameter now uses a proper `TypeMappings[T]` type instead of `TypeMappingsDict`, and uses `ErrorHandler` instances instead of decision factories.

4. **Specialized type mapping functions**: Instead of one generic `error_handler_type_mapping`, there are now four functions (`exit_fatally_type_mapping`, `raise_exception_type_mapping`, `continue_execution_type_mapping`, `retry_execution_type_mapping`) that provide better type safety for each error handling strategy.

## Breaking Changes

This PR contains multiple breaking changes. See the changelog fragment at `changelog.d/20251201_190227_ci_switch_to_pyrefly.md` for detailed migration guidance.

### Migration Guide

**1. `error_handler_type_mapping` → specialized functions:**
```python
# Before
error_handler_type_mappings(
    retry_execution=error_handler_type_mapping(types=[MyException], callback=cb)
)

# After
error_handler_type_mappings(
    retry_execution=retry_execution_type_mapping(types=[MyException], callback=cb)
)
```

**2. `ExitErrorHandler` constructor:**
```python
# Before
ExitErrorHandler(exit_code=1)

# After
ExitErrorHandler(exit_code_factory=lambda _: 1)
```

**3. `ContinueErrorHandler` constructor:**
```python
# Before
ContinueErrorHandler()

# After
ContinueErrorHandler(value_factory=lambda _: None)
```

**4. `ErrorHandlerDecision.continue_execution`:**
```python
# Before
ErrorHandlerDecision.continue_execution()

# After
ErrorHandlerDecision.continue_execution(value=None)
```

**5. `TypeMappingErrorHandler` constructor:**
```python
# Before
TypeMappingErrorHandler(default_decision_factory=lambda _: ErrorHandlerDecision.retry_execution())

# After
TypeMappingErrorHandler(default_error_handler=RetryErrorHandler())
```

**6. `continue_execution` in `error_handler_type_mappings`:**
```python
# Before
error_handler_type_mappings(continue_execution=[MyException])

# After
error_handler_type_mappings(
    continue_execution=continue_execution_type_mapping(
        types=[MyException],
        value_factory=lambda _: None,
    )
)
```

**7. `make_subscriber` function:**
```python
# Before
make_subscriber(subscriber_group="group", ...)

# After (option 1 - use new convenience function)
make_event_store_subscriber(subscriber_group="group", ...)

# After (option 2 - explicit converter)
make_subscriber(
    subscriber_group="group",
    subscriber_state_converter=StoredEventEventConsumerStateConverter(),
    ...
)
```

## How to Verify

### Automated Verification

- [x] All tests pass: `mise run test`
- [x] Type checking passes: `mise run types:check`
- [x] Linting passes: `mise run lint:check`
- [x] Formatting is correct: `mise run format:check`

### Manual Verification

No manual verification required - all changes are covered by automated tests.

## Checklist

- [x] I have read the [contributing guidelines](CONTRIBUTING.md)
- [x] I have added/updated tests for my changes
- [x] I have updated documentation as needed
- [x] I have added a changelog fragment (if user-facing changes)
- [x] Breaking changes are documented with migration guidance

## Related Issues

None
