from abc import ABC, abstractmethod

from logicblocks.event import query as query
from logicblocks.event.types import Applier, Converter

from .. import query as postgresquery
from ..types import ParameterisedQuery


class QueryApplier(Applier[postgresquery.Query], ABC):
    pass


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
