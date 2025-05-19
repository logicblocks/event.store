from abc import ABC, abstractmethod

from logicblocks.event import query as query
from logicblocks.event.types import Converter

from ..query import QueryApplier
from ..types import ParameterisedQuery


class QueryConverter[Q: query.Query = query.Query](
    Converter[Q, ParameterisedQuery], ABC
):
    @abstractmethod
    def convert(self, item: Q) -> ParameterisedQuery:
        raise NotImplementedError


class ClauseConverter[C: query.Clause = query.Clause](
    Converter[C, QueryApplier], ABC
):
    @abstractmethod
    def convert(self, item: C) -> QueryApplier:
        raise NotImplementedError


class FunctionConverter[F: query.Function = query.Function](
    Converter[F, QueryApplier], ABC
):
    @abstractmethod
    def convert(self, item: F) -> QueryApplier:
        raise NotImplementedError
