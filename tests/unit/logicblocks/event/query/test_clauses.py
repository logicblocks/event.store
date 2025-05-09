from logicblocks.event.query import OffsetPagingClause


class TestOffsetPagingClause:
    def test_calculates_offset_from_page_number_and_item_count(self):
        assert OffsetPagingClause(page_number=4, item_count=10).offset == 30

    def test_has_zero_offset_when_no_page_number_supplied(self):
        assert OffsetPagingClause(item_count=10).offset == 0
