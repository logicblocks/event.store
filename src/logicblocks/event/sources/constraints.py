"""Query constraints for filtering the events read from an event source.

Constraints are declarative: each event source or storage adapter converts
them into its own representation (for example, a predicate or a SQL clause)
before applying them.
"""

from abc import ABC
from collections.abc import Callable
from dataclasses import dataclass

from logicblocks.event.types import Event

type QueryConstraintCheck[E: Event] = Callable[[E], bool]


class QueryConstraint(ABC):
    """Base class for constraints restricting the events read from a source."""


@dataclass(frozen=True)
class SequenceNumberAfterConstraint(QueryConstraint):
    """Constraint matching events with a sequence number greater than a value.

    Attributes:
        sequence_number: The exclusive lower bound on sequence number.
    """

    sequence_number: int


def sequence_number_after(sequence_number: int) -> QueryConstraint:
    """Create a constraint matching events after a given sequence number.

    Args:
        sequence_number: The exclusive lower bound on sequence number.

    Returns:
        a `SequenceNumberAfterConstraint` for the sequence number.
    """
    return SequenceNumberAfterConstraint(sequence_number=sequence_number)
