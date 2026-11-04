from functools import reduce
from typing import Sequence

from .types import Identifiable, ResultSet, ResultSetTransformer


def compose_transformers[R: Identifiable](
    functions: Sequence[ResultSetTransformer[R]],
) -> ResultSetTransformer[R]:
    """Compose transformers into one that applies each in sequence order.

    Args:
        functions: The transformers to compose, first applied first.

    Returns:
        the composed transformer, or the identity if there are none.
    """

    def accumulator(
        f: ResultSetTransformer[R],
        g: ResultSetTransformer[R],
    ) -> ResultSetTransformer[R]:
        def handler(results: ResultSet[R]) -> ResultSet[R]:
            return g(f(results))

        return handler

    def initial(results: ResultSet[R]) -> ResultSet[R]:
        return results

    return reduce(accumulator, functions, initial)
