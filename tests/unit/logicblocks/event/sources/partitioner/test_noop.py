from logicblocks.event.sources.partitioner.noop import (
    NoOpEventSourcePartitioner,
)
from logicblocks.event.types import CategoryIdentifier, LogIdentifier


class TestNoOpEventSourcePartitioner:
    def test_returns_same_identifiers_when_partitioning(self):
        partitioner = NoOpEventSourcePartitioner()
        identifiers = [
            LogIdentifier(),
            CategoryIdentifier(category="order"),
            CategoryIdentifier(category="payment"),
        ]

        result = partitioner.partition(identifiers)

        assert result == identifiers

    def test_returns_empty_sequence_when_no_identifiers(self):
        partitioner = NoOpEventSourcePartitioner()

        result = partitioner.partition([])

        assert result == []