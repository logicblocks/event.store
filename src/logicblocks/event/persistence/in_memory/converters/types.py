from abc import ABC
from collections.abc import Callable, Sequence
from typing import Protocol

import logicblocks.event.query as query
from logicblocks.event.types import Converter


class Identifiable(Protocol):
    @property
    def id(self) -> str:
        raise NotImplementedError


type ResultSet[T: Identifiable] = Sequence[T]
type ResultSetTransformer[T: Identifiable] = Callable[
    [ResultSet[T]], ResultSet[T]
]


class QueryConverter[R: Identifiable, Q: query.Query = query.Query](
    Converter[Q, ResultSetTransformer[R]], ABC
): ...


class ClauseConverter[R: Identifiable, C: query.Clause = query.Clause](
    Converter[C, ResultSetTransformer[R]], ABC
): ...
