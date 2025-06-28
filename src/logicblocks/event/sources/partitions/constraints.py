from collections.abc import Callable
from typing import Any, MutableMapping

from logicblocks.event.store.constraints import (
    QueryConstraint,
    StreamNamePrefixConstraint,
)
from logicblocks.event.types import (
    PartitionIdentifier,
    StreamNamePrefixPartitionIdentifier,
)


class PartitionConstraintFactory:
    def __init__(self):
        self._constructors: MutableMapping[
            type[PartitionIdentifier], Callable[[Any], QueryConstraint]
        ] = {}
        self._register_default_constructors()

    def _register_default_constructors(self):
        self.register_constructor(
            StreamNamePrefixPartitionIdentifier,
            lambda p: StreamNamePrefixConstraint(prefix=p.value),
        )

    def register_constructor[P: PartitionIdentifier](
        self,
        partition_type: type[P],
        constructor: Callable[[P], QueryConstraint],
    ) -> None:
        self._constructors[partition_type] = constructor

    def construct(self, partition: PartitionIdentifier) -> QueryConstraint:
        constructor = self._constructors.get(type(partition))

        if constructor is None:
            raise ValueError(
                "No constraint constructor registered for partition "
                f"identifier type: {type(partition).__name__}"
            )
        return constructor(partition)
