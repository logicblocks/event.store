from abc import ABC
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol, Self

import logicblocks.event.query as query
from logicblocks.event.types import Converter


class Identifiable(Protocol):
    """A record with a string ID, as required for in-memory querying."""

    @property
    def id(self) -> str:
        raise NotImplementedError


class MissingNestedPathError(ValueError):
    """Raised when a sub-level of a path is missing from a record."""


@dataclass(frozen=True)
class Result[T: Identifiable]:
    """A record in a result set, with any values computed by functions.

    Attributes:
        record: The underlying record.
        extra: Computed values, keyed by function alias.
    """

    record: T
    extra: Mapping[str, Any]

    @classmethod
    def of(cls, record: T, extra: Mapping[str, Any] | None = None) -> Self:
        return cls(record=record, extra=extra if extra is not None else {})

    def _clone(self, record: T, extra: Mapping[str, Any]) -> Self:
        return self.__class__(record=record, extra=extra)

    def add_extra(self, key: str, value: Any) -> Self:
        """Return a copy of this result with an additional computed value."""
        return self._clone(
            record=self.record, extra={**self.extra, key: value}
        )

    def lookup_in_record(self, path: query.Path):
        """Resolve a path against the record's attributes.

        Args:
            path: The path to resolve.

        Returns:
            the value at the path.

        Raises:
            ValueError: If the record has no attribute named by the top
                level of the path.
            MissingNestedPathError: If a sub-level of the path is missing.
        """
        attribute_name = path.top_level
        remaining_path = path.sub_levels

        try:
            attribute = getattr(self.record, attribute_name)
        except AttributeError:
            raise ValueError(f"Invalid projection path: {path}.")

        value = attribute
        for path_segment in remaining_path:
            try:
                value = value[path_segment]
            except KeyError:
                raise MissingNestedPathError(
                    f"Invalid projection path: {path}."
                )

        return value

    def lookup_in_extra(self, path: query.Path) -> Any:
        """Resolve a path against the computed values.

        Args:
            path: The path to resolve.

        Returns:
            the value at the path.

        Raises:
            ValueError: If any level of the path is missing.
        """
        first_path_segment = path.top_level
        remaining_path_segments = path.sub_levels

        try:
            value = self.extra[first_path_segment]
        except KeyError:
            raise ValueError(f"Invalid projection path: {path}.")

        for path_segment in remaining_path_segments:
            try:
                value = value[path_segment]
            except KeyError:
                raise ValueError(f"Invalid projection path: {path}.")

        return value

    def lookup(self, path: query.Path) -> Any:
        """Resolve a path, preferring computed values over the record.

        Args:
            path: The path to resolve.

        Returns:
            the value at the path in the computed values if present,
                otherwise the value at the path in the record.

        Raises:
            ValueError: If the path cannot be resolved in either.
            MissingNestedPathError: If a sub-level of the path is missing
                from the record.
        """
        try:
            return self.lookup_in_extra(path)
        except ValueError:
            return self.lookup_in_record(path)


@dataclass(frozen=True)
class ResultSet[T: Identifiable]:
    """An immutable, ordered set of results that query transformers act on.

    Attributes:
        results: The results, in order.
        extra: Additional values associated with the set as a whole.
    """

    results: Sequence[Result[T]]
    extra: Mapping[str, Any]

    @classmethod
    def of(cls, *records: T) -> Self:
        """Create a result set containing the given records, in order."""
        return cls(
            results=[Result[T].of(record) for record in records], extra={}
        )

    def _clone(
        self, results: Sequence[Result[T]], extra: Mapping[str, Any]
    ) -> Self:
        return self.__class__(results=results, extra=extra)

    @property
    def records(self) -> Sequence[T]:
        """The underlying records, in order."""
        return [result.record for result in self.results]

    def with_results(self, results: Sequence[Result[T]]) -> Self:
        return self._clone(results=results, extra=self.extra)

    def with_extra(self, extra: Mapping[str, Any]) -> Self:
        return self._clone(results=self.results, extra=extra)


type ResultSetTransformer[T: Identifiable] = Callable[
    [ResultSet[T]], ResultSet[T]
]


class QueryConverter[R: Identifiable, Q: query.Query = query.Query](
    Converter[Q, ResultSetTransformer[R]], ABC
):
    """Converts a query into a transformer over in-memory result sets."""


class ClauseConverter[R: Identifiable, C: query.Clause = query.Clause](
    Converter[C, ResultSetTransformer[R]], ABC
):
    """Converts a clause into a transformer over in-memory result sets."""


class FunctionConverter[R: Identifiable, F: query.Function = query.Function](
    Converter[F, ResultSetTransformer[R]], ABC
):
    """Converts a function into a transformer that adds computed values."""
