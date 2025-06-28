import pytest

from logicblocks.event.sources.partitions.constraints import (
    PartitionConstraintFactory,
)
from logicblocks.event.store.constraints import (
    QueryConstraint,
    StreamNamePrefixConstraint,
)
from logicblocks.event.types import (
    PartitionIdentifier,
    StreamNamePrefixPartitionIdentifier,
)


class TestPartitionConstraintFactory:
    def test_constructs_stream_name_prefix_constraint_for_stream_name_prefix_partition(
        self,
    ):
        factory = PartitionConstraintFactory()

        partition = StreamNamePrefixPartitionIdentifier(value="abc")

        constraint = factory.construct(partition)

        expected = StreamNamePrefixConstraint(prefix="abc")

        assert constraint == expected

    def test_constructs_different_constraints_for_different_prefixes(self):
        factory = PartitionConstraintFactory()

        partition_1 = StreamNamePrefixPartitionIdentifier(value="123")
        partition_2 = StreamNamePrefixPartitionIdentifier(value="xyz")

        constraint_1 = factory.construct(partition_1)
        constraint_2 = factory.construct(partition_2)

        expected_1 = StreamNamePrefixConstraint(prefix="123")
        expected_2 = StreamNamePrefixConstraint(prefix="xyz")

        assert constraint_1 == expected_1
        assert constraint_2 == expected_2

    def test_raises_error_for_unsupported_partition_identifier(self):
        factory = PartitionConstraintFactory()

        class UnsupportedPartitionIdentifier(PartitionIdentifier):
            def serialise(self, fallback=None):
                return {"type": "unsupported"}

        partition = UnsupportedPartitionIdentifier()

        with pytest.raises(ValueError):
            factory.construct(partition)

    def test_can_register_custom_partition_identifier_constructor(self):
        factory = PartitionConstraintFactory()

        class CustomPartitionIdentifier(PartitionIdentifier):
            def __init__(self, custom_value: str):
                self.custom_value = custom_value

            def serialise(self, fallback=None):
                return {"type": "custom", "value": self.custom_value}

        class CustomConstraint(QueryConstraint):
            def __init__(self, value: str):
                self.value = value

            def __eq__(self, other):
                return (
                    isinstance(other, CustomConstraint)
                    and self.value == other.value
                )

        factory.register_constructor(
            CustomPartitionIdentifier,
            lambda p: CustomConstraint(value=p.custom_value),
        )

        partition = CustomPartitionIdentifier(custom_value="test")
        constraint = factory.construct(partition)

        expected = CustomConstraint(value="test")

        assert constraint == expected
