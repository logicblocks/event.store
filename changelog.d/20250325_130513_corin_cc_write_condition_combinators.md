### Changed

- WriteConditions can now be combined using the `&` and `|` operators
  - These can be chained together to create complex conditions, for example: `condition1 & condition2 | (condition3 & condition4)`
- Publish no longer takes a set of conditions, instead it accepts a single WriteCondition
- "No conditions" is now represented by the `NoCondition` singleton rather than an empty set
