from abc import ABC
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol, Self

import logicblocks.event.query as query
from logicblocks.event.types import Converter


class Identifiable(Protocol):
    @property
    def id(self) -> str:
        raise NotImplementedError


@dataclass(frozen=True)
class Result[T: Identifiable]:
    record: T
    extra: Mapping[str, Any]

    @classmethod
    def of(cls, record: T, extra: Mapping[str, Any] | None = None) -> Self:
        return cls(record=record, extra=extra if extra is not None else {})

    def _clone(self, record: T, extra: Mapping[str, Any]) -> Self:
        return self.__class__(record=record, extra=extra)

    def add_extra(self, key: str, value: Any) -> Self:
        return self._clone(
            record=self.record, extra={**self.extra, key: value}
        )

    def lookup_in_record(self, path: query.Path):
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
                raise ValueError(f"Invalid projection path: {path}.")

        return value

    def lookup_in_extra(self, path: query.Path) -> Any:
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
        try:
            return self.lookup_in_extra(path)
        except ValueError:
            return self.lookup_in_record(path)


@dataclass(frozen=True)
class ResultSet[T: Identifiable]:
    results: Sequence[Result[T]]
    extra: Mapping[str, Any]

    @classmethod
    def of(cls, *records: T) -> Self:
        return cls(
            results=[Result[T].of(record) for record in records], extra={}
        )

    def _clone(
        self, results: Sequence[Result[T]], extra: Mapping[str, Any]
    ) -> Self:
        return self.__class__(results=results, extra=extra)

    @property
    def records(self) -> Sequence[T]:
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
): ...


class ClauseConverter[R: Identifiable, C: query.Clause = query.Clause](
    Converter[C, ResultSetTransformer[R]], ABC
): ...


class FunctionConverter[R: Identifiable, F: query.Function = query.Function](
    Converter[F, ResultSetTransformer[R]], ABC
): ...
