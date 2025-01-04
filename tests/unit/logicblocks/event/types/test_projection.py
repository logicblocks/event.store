import sys
from typing import Hashable

import pytest

from logicblocks.event.types import Projection


class TestProjection:
    def test_projection_returns_json_representation(self):
        assert (
            Projection(state={"a": 1}, version=1).json()
            == '{"state": {"a": 1}, "version": 1}'
        )

    def test_projection_returns_debug_representation(self):
        assert (
            repr(Projection(state={"a": 1}, version=1))
            == "Projection(state={'a': 1},version=1)"
        )

    def test_hashes_projection_using_json_representation(self):
        assert isinstance(Projection(state={"a": 1}, version=1), Hashable)
        assert hash(Projection(state={"a": 1}, version=1)) == hash(
            Projection(state={"a": 1}, version=1).json()
        )

    def test_exposes_state(self):
        assert Projection(state={"a": 1}, version=1).state == {"a": 1}

    def test_exposes_version(self):
        assert Projection(state={"a": 1}, version=1).version == 1


if __name__ == "__main__":
    sys.exit(pytest.main([__file__]))
