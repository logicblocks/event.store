from abc import ABC, abstractmethod

from logicblocks.event.db.postgres import Query


class QueryApplicator(ABC):
    @abstractmethod
    def apply(self, target: Query) -> Query:
        raise NotImplementedError
