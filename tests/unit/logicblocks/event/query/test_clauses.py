import pytest

from logicblocks.event.query import OffsetPagingClause
from logicblocks.event.sources.constraints import OffsetPagingConstraint


class TestOffsetPagingClause:
    def test_calculates_offset_from_page_number_and_item_count(self):
        assert OffsetPagingClause(page_number=4, item_count=10).offset == 30

    def test_has_zero_offset_when_no_page_number_supplied(self):
        assert OffsetPagingClause(item_count=10).offset == 0


class TestOffsetPagingConstraint:
    def test_stores_page_number_and_item_count(self):
        constraint = OffsetPagingConstraint(page_number=3, item_count=20)

        assert constraint.page_number == 3
        assert constraint.item_count == 20

    def test_defaults_page_number_to_1(self):
        constraint = OffsetPagingConstraint(item_count=20)

        assert constraint.page_number == 1

    def test_defaults_item_count_to_10(self):
        constraint = OffsetPagingConstraint()

        assert constraint.item_count == 10

    def test_calculates_offset(self):
        constraint = OffsetPagingConstraint(page_number=4, item_count=10)

        assert constraint.offset == 30

    def test_raises_when_page_number_less_than_1(self):
        with pytest.raises(ValueError, match="page_number must be >= 1"):
            OffsetPagingConstraint(page_number=0)

    def test_raises_when_item_count_less_than_1(self):
        with pytest.raises(ValueError, match="item_count must be >= 1"):
            OffsetPagingConstraint(item_count=0)
