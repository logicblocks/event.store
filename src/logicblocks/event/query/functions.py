from abc import ABC
from dataclasses import dataclass

from .utilities import Path


@dataclass(frozen=True)
class Function(ABC):
    """Base class for values computed per record during a query.

    A function's result is exposed under its alias, so that it can be used
    as a `SortField`.

    Attributes:
        alias: The name under which the computed value is exposed.
    """

    alias: str


@dataclass(frozen=True, kw_only=True)
class Similarity(Function):
    """Score the textual similarity between a field and a string.

    Scores are based on trigrams and range from 0 (no similarity) to 1
    (identical). The Postgres implementation uses the `similarity` function
    from the `pg_trgm` extension, which must be installed; the in-memory
    implementation approximates it.

    Attributes:
        alias: The name under which the score is exposed.
        left: The path to the text to compare.
        right: The text to compare against.
    """

    left: Path
    right: str
