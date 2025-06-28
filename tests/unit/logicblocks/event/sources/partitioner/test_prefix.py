import pytest

from logicblocks.event.sources import StreamNamePrefixEventSourcePartitioner
from logicblocks.event.types import (
    CategoryIdentifier,
    CategoryPartitionIdentifier,
    LogIdentifier,
    LogPartitionIdentifier,
    StreamIdentifier,
    StreamNamePrefixPartitionIdentifier,
)


class TestStreamNamePrefixEventSourcePartitioner:
    def test_partition_category_with_hex_charset_prefix_length_1_creates_16_partitions(
        self,
    ):
        partitioner = StreamNamePrefixEventSourcePartitioner(
            prefix_length=1, character_set="0123456789abcdef"
        )

        identifiers = [CategoryIdentifier(category="orders")]

        result = partitioner.partition(identifiers)

        expected = [
            CategoryPartitionIdentifier(
                category="orders",
                partition=StreamNamePrefixPartitionIdentifier(value=prefix),
            )
            for prefix in [
                "0",
                "1",
                "2",
                "3",
                "4",
                "5",
                "6",
                "7",
                "8",
                "9",
                "a",
                "b",
                "c",
                "d",
                "e",
                "f",
            ]
        ]

        assert result == expected

    def test_partition_category_with_hex_charset_prefix_length_2_creates_256_partitions(
        self,
    ):
        partitioner = StreamNamePrefixEventSourcePartitioner(
            prefix_length=2, character_set="0123456789abcdef"
        )

        identifiers = [CategoryIdentifier(category="orders")]

        result = partitioner.partition(identifiers)

        expected_prefixes = []
        for first in "0123456789abcdef":
            for second in "0123456789abcdef":
                expected_prefixes.append(first + second)

        expected = [
            CategoryPartitionIdentifier(
                category="orders",
                partition=StreamNamePrefixPartitionIdentifier(value=prefix),
            )
            for prefix in expected_prefixes
        ]

        assert result == expected

    def test_partition_category_with_alpha_charset_prefix_length_1_creates_26_partitions(
        self,
    ):
        partitioner = StreamNamePrefixEventSourcePartitioner(
            prefix_length=1, character_set="abcdefghijklmnopqrstuvwxyz"
        )

        identifiers = [CategoryIdentifier(category="orders")]

        result = partitioner.partition(identifiers)

        expected = [
            CategoryPartitionIdentifier(
                category="orders",
                partition=StreamNamePrefixPartitionIdentifier(value=prefix),
            )
            for prefix in "abcdefghijklmnopqrstuvwxyz"
        ]

        assert result == expected

    def test_partition_category_with_alpha_charset_prefix_length_2_creates_676_partitions(
        self,
    ):
        partitioner = StreamNamePrefixEventSourcePartitioner(
            prefix_length=2, character_set="abcdefghijklmnopqrstuvwxyz"
        )

        identifiers = [CategoryIdentifier(category="orders")]

        result = partitioner.partition(identifiers)

        expected_prefixes = []
        for first in "abcdefghijklmnopqrstuvwxyz":
            for second in "abcdefghijklmnopqrstuvwxyz":
                expected_prefixes.append(first + second)

        expected = [
            CategoryPartitionIdentifier(
                category="orders",
                partition=StreamNamePrefixPartitionIdentifier(value=prefix),
            )
            for prefix in expected_prefixes
        ]

        assert result == expected

    def test_partition_log_identifier_with_hex_charset(self):
        partitioner = StreamNamePrefixEventSourcePartitioner(
            prefix_length=1, character_set="0123456789abcdef"
        )

        identifiers = [LogIdentifier()]

        result = partitioner.partition(identifiers)

        expected = [
            LogPartitionIdentifier(
                partition=StreamNamePrefixPartitionIdentifier(value=prefix)
            )
            for prefix in [
                "0",
                "1",
                "2",
                "3",
                "4",
                "5",
                "6",
                "7",
                "8",
                "9",
                "a",
                "b",
                "c",
                "d",
                "e",
                "f",
            ]
        ]

        assert result == expected

    def test_partition_multiple_identifiers_preserves_order(self):
        partitioner = StreamNamePrefixEventSourcePartitioner(
            prefix_length=1, character_set="0123456789abcdef"
        )

        identifiers = [
            CategoryIdentifier(category="orders"),
            LogIdentifier(),
            CategoryIdentifier(category="users"),
        ]

        result = partitioner.partition(identifiers)

        hex_prefixes = [
            "0",
            "1",
            "2",
            "3",
            "4",
            "5",
            "6",
            "7",
            "8",
            "9",
            "a",
            "b",
            "c",
            "d",
            "e",
            "f",
        ]

        expected = (
            [
                CategoryPartitionIdentifier(
                    category="orders",
                    partition=StreamNamePrefixPartitionIdentifier(
                        value=prefix
                    ),
                )
                for prefix in hex_prefixes
            ]
            + [
                LogPartitionIdentifier(
                    partition=StreamNamePrefixPartitionIdentifier(value=prefix)
                )
                for prefix in hex_prefixes
            ]
            + [
                CategoryPartitionIdentifier(
                    category="users",
                    partition=StreamNamePrefixPartitionIdentifier(
                        value=prefix
                    ),
                )
                for prefix in hex_prefixes
            ]
        )

        assert result == expected

    def test_partition_stream_identifier_returns_unchanged(self):
        partitioner = StreamNamePrefixEventSourcePartitioner(
            prefix_length=1, character_set="0123456789abcdef"
        )

        stream_id = StreamIdentifier(category="orders", stream="12345")
        identifiers = [stream_id]

        result = partitioner.partition(identifiers)

        expected = [stream_id]

        assert result == expected

    def test_partition_with_numeric_charset_creates_correct_partitions(self):
        partitioner = StreamNamePrefixEventSourcePartitioner(
            prefix_length=2, character_set="0123456789"
        )

        identifiers = [CategoryIdentifier(category="orders")]

        result = partitioner.partition(identifiers)

        expected_prefixes = []
        for first in "0123456789":
            for second in "0123456789":
                expected_prefixes.append(first + second)

        expected = [
            CategoryPartitionIdentifier(
                category="orders",
                partition=StreamNamePrefixPartitionIdentifier(value=prefix),
            )
            for prefix in expected_prefixes
        ]

        assert result == expected

    def test_partition_empty_identifiers_returns_empty(self):
        partitioner = StreamNamePrefixEventSourcePartitioner(
            prefix_length=1, character_set="0123456789abcdef"
        )

        result = partitioner.partition([])

        expected = []

        assert result == expected

    def test_partition_mixed_identifiers_handles_each_appropriately(self):
        partitioner = StreamNamePrefixEventSourcePartitioner(
            prefix_length=1, character_set="0123456789abcdef"
        )

        identifiers = [
            CategoryIdentifier(category="orders"),
            StreamIdentifier(category="users", stream="12345"),
            LogIdentifier(),
        ]

        result = partitioner.partition(identifiers)

        hex_prefixes = [
            "0",
            "1",
            "2",
            "3",
            "4",
            "5",
            "6",
            "7",
            "8",
            "9",
            "a",
            "b",
            "c",
            "d",
            "e",
            "f",
        ]

        expected = (
            [
                CategoryPartitionIdentifier(
                    category="orders",
                    partition=StreamNamePrefixPartitionIdentifier(
                        value=prefix
                    ),
                )
                for prefix in hex_prefixes
            ]
            + [StreamIdentifier(category="users", stream="12345")]
            + [
                LogPartitionIdentifier(
                    partition=StreamNamePrefixPartitionIdentifier(value=prefix)
                )
                for prefix in hex_prefixes
            ]
        )

        assert result == expected

    def test_partition_with_prefix_length_0_raises_error(self):
        with pytest.raises(
            ValueError, match="prefix_length must be greater than 0"
        ):
            StreamNamePrefixEventSourcePartitioner(
                prefix_length=0, character_set="abc"
            )

    def test_partition_with_negative_prefix_length_raises_error(self):
        with pytest.raises(
            ValueError, match="prefix_length must be greater than 0"
        ):
            StreamNamePrefixEventSourcePartitioner(
                prefix_length=-1, character_set="abc"
            )

    def test_partition_with_empty_character_set_raises_error(self):
        with pytest.raises(ValueError, match="character_set cannot be empty"):
            StreamNamePrefixEventSourcePartitioner(
                prefix_length=1, character_set=""
            )

    def test_partition_with_single_character_set_creates_single_partition(
        self,
    ):
        partitioner = StreamNamePrefixEventSourcePartitioner(
            prefix_length=1, character_set="x"
        )

        identifiers = [CategoryIdentifier(category="orders")]

        result = partitioner.partition(identifiers)

        expected = [
            CategoryPartitionIdentifier(
                category="orders",
                partition=StreamNamePrefixPartitionIdentifier(value="x"),
            )
        ]

        assert result == expected
