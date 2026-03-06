import pytest

from logicblocks.event.query import OffsetPagingClause


class TestOffsetPagingClause:
    def test_calculates_offset_from_page_number_and_item_count(self):
        assert OffsetPagingClause(page_number=4, item_count=10).offset == 30

    def test_has_zero_offset_when_no_page_number_supplied(self):
        assert OffsetPagingClause(item_count=10).offset == 0

    def test_raises_on_zero_page_number(self):
        with pytest.raises(ValueError, match="page_number"):
            OffsetPagingClause(page_number=0, item_count=10)

    def test_raises_on_negative_page_number(self):
        with pytest.raises(ValueError, match="page_number"):
            OffsetPagingClause(page_number=-1, item_count=10)

    def test_raises_on_zero_item_count(self):
        with pytest.raises(ValueError, match="item_count"):
            OffsetPagingClause(page_number=1, item_count=0)

    def test_raises_on_negative_item_count(self):
        with pytest.raises(ValueError, match="item_count"):
            OffsetPagingClause(page_number=1, item_count=-5)
