import itertools
from collections.abc import Sequence

from logicblocks.event.types import (
    CategoryIdentifier,
    CategoryPartitionIdentifier,
    EventSourceIdentifier,
    LogIdentifier,
    LogPartitionIdentifier,
    StreamNamePrefixPartitionIdentifier,
)

from .base import EventSourcePartitioner


class StreamNamePrefixEventSourcePartitioner(EventSourcePartitioner):
    def __init__(self, prefix_length: int, character_set: str):
        if prefix_length <= 0:
            raise ValueError("prefix_length must be greater than 0")
        if not character_set:
            raise ValueError("character_set cannot be empty")

        self._prefix_length = prefix_length
        self._character_set = character_set

    def partition(
        self, identifiers: Sequence[EventSourceIdentifier]
    ) -> Sequence[EventSourceIdentifier]:
        return [
            partitioned_identifier
            for identifier in identifiers
            for partitioned_identifier in self._partition_identifier(
                identifier
            )
        ]

    def _partition_identifier(
        self, identifier: EventSourceIdentifier
    ) -> Sequence[EventSourceIdentifier]:
        match identifier:
            case CategoryIdentifier():
                return self._partition_category(identifier)
            case LogIdentifier():
                return self._partition_log()
            case _:
                return [identifier]

    def _partition_category(
        self, identifier: CategoryIdentifier
    ) -> Sequence[EventSourceIdentifier]:
        return [
            CategoryPartitionIdentifier(
                category=identifier.category,
                partition=StreamNamePrefixPartitionIdentifier(value=prefix),
            )
            for prefix in self._generate_prefixes()
        ]

    def _partition_log(self) -> Sequence[EventSourceIdentifier]:
        return [
            LogPartitionIdentifier(
                partition=StreamNamePrefixPartitionIdentifier(value=prefix)
            )
            for prefix in self._generate_prefixes()
        ]

    def _generate_prefixes(self) -> Sequence[str]:
        return [
            "".join(combination)
            for combination in itertools.product(
                self._character_set, repeat=self._prefix_length
            )
        ]
