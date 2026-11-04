import re
from collections.abc import Set
from typing import Self

from logicblocks.event.query import (
    Function,
    Similarity,
)
from logicblocks.event.types import Converter

from ...converter import TypeRegistryConverter
from .types import (
    FunctionConverter,
    Identifiable,
    ResultSet,
    ResultSetTransformer,
)


def find_ngrams(text: str, number: int = 3) -> Set[str]:
    """Return the n-grams of each word in some text.

    Words are lower-cased and padded with two leading spaces and one
    trailing space, approximating the trigrams used by Postgres `pg_trgm`.
    """
    if not text:
        return set()

    words = [f"  {x} " for x in re.split(r"\W+", text.lower()) if x.strip()]
    ngrams: set[str] = set()

    for word in words:
        for x in range(0, len(word) - number + 1):
            ngrams.add(word[x : x + number])

    return ngrams


def score_similarity(text1: str, text2: str, number: int = 3) -> float:
    """Return the proportion of n-grams shared by two texts, from 0 to 1."""
    ngrams1 = find_ngrams(text1, number)
    ngrams2 = find_ngrams(text2, number)

    num_unique = len(ngrams1 | ngrams2)
    num_equal = len(ngrams1 & ngrams2)

    return float(num_equal) / float(num_unique)


class SimilarityFunctionConverter[R: Identifiable](
    FunctionConverter[R, Similarity]
):
    """Converts `Similarity` into a transformer adding a score per result."""

    def convert(self, item: Similarity) -> ResultSetTransformer[R]:
        def handler(result_set: ResultSet[R]) -> ResultSet[R]:
            return result_set.with_results(
                [
                    result.add_extra(
                        item.alias,
                        score_similarity(result.lookup(item.left), item.right),
                    )
                    for result in result_set.results
                ]
            )

        return handler


class TypeRegistryFunctionConverter[R: Identifiable](
    TypeRegistryConverter[Function, ResultSetTransformer[R]]
):
    """Converts functions using the converter registered for their type."""

    def register[F: Function](
        self,
        item_type: type[F],
        converter: Converter[F, ResultSetTransformer[R]],
    ) -> Self:
        return super()._register(item_type, converter)
