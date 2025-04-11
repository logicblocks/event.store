### Added

- Added "ErrorHandlingService" to allow for injection of error handling 
  strategies in services.

### DevEx

- Allow for running `library:test:unit|integration|component` with filter
  options to run only tests matching a specific pattern. Example usage:
  `go "library:test:unit[TestAllTestsInFile]"` or
  `go "library:test:component[test_a_specific_test]"`.
