from collections.abc import Sequence

from logicblocks.event.types import EventSourceIdentifier

from .base import EventSourcePartitioner


class NoOpEventSourcePartitioner(EventSourcePartitioner):
    def partition(
        self, identifiers: Sequence[EventSourceIdentifier]
    ) -> Sequence[EventSourceIdentifier]:
        return identifiers