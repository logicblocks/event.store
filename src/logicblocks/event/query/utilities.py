from collections.abc import Sequence
from dataclasses import dataclass


@dataclass(frozen=True)
class Path:
    top_level: str
    sub_levels: Sequence[str | int]

    def __init__(self, top_level: str, *sub_levels: str | int):
        object.__setattr__(self, "top_level", top_level)
        object.__setattr__(self, "sub_levels", sub_levels)

    def __repr__(self):
        return repr([self.top_level, *self.sub_levels])

    def is_nested(self):
        return len(self.sub_levels) > 0
