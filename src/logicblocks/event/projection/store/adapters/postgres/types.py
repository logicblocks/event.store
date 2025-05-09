from abc import ABC, abstractmethod

import logicblocks.event.db.postgres as postgres
import logicblocks.event.query as query
from logicblocks.event.types import Applier, Converter


class QueryApplier(Applier[postgres.Query], ABC):
    pass


class ClauseConverter[C: query.Clause = query.Clause](
    Converter[C, QueryApplier], ABC
):
    @abstractmethod
    def convert(self, item: C) -> QueryApplier:
        raise NotImplementedError


class QueryConverter[Q: query.Query = query.Query](
    Converter[Q, postgres.ParameterisedQuery], ABC
):
    @abstractmethod
    def convert(self, item: Q) -> postgres.ParameterisedQuery:
        raise NotImplementedError
