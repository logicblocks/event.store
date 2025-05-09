from logicblocks.event.query import Path


class TestPath:
    def test_is_not_nested_when_top_level_only(self):
        assert not Path("a").is_nested()

    def test_is_nested_when_has_sub_levels(self):
        assert Path("a", "b", "c").is_nested()
