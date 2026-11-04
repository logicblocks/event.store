from collections.abc import Sequence
from dataclasses import dataclass


@dataclass(frozen=True)
class Path:
    """A path to a value within a record.

    The top level names an attribute of the record (or a column in
    Postgres) and each sub-level indexes further into its nested value, for
    example `Path("state", "address", "postcode")`.

    Args:
        top_level: The name of the top-level attribute.
        *sub_levels: The keys or indices into the nested value.
    """

    top_level: str
    sub_levels: Sequence[str | int]

    def __init__(self, top_level: str, *sub_levels: str | int):
        object.__setattr__(self, "top_level", top_level)
        object.__setattr__(self, "sub_levels", sub_levels)

    def __repr__(self):
        return repr([self.top_level, *self.sub_levels])

    def is_nested(self):
        """Return whether the path has any sub-levels."""
        return len(self.sub_levels) > 0
