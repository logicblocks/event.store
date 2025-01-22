from logicblocks.event.projection.store.query import OffsetPagingClause, Path


class TestPath:
    def test_is_not_nested_when_top_level_only(self):
        assert not Path("a").is_nested()

    def test_is_nested_when_has_sub_levels(self):
        assert Path("a", "b", "c").is_nested()


class TestOffsetPagingClause:
    def test_calculates_offset_from_page_number_and_item_count(self):
        assert OffsetPagingClause(page_number=4, item_count=10).offset == 30

    def test_has_zero_offset_when_no_page_number_supplied(self):
        assert OffsetPagingClause(item_count=10).offset == 0
