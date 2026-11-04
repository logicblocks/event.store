from logicblocks.event.persistence.postgres import query as postgresquery

from .types import QueryApplier


class CombinedQueryApplier(QueryApplier):
    """Applies several appliers to a query in turn.

    Args:
        *appliers: The appliers to apply, in order.
    """

    def __init__(self, *appliers: QueryApplier):
        self.appliers = list(appliers)

    def apply(self, target: postgresquery.Query) -> postgresquery.Query:
        for applier in self.appliers:
            target = applier.apply(target)

        return target
