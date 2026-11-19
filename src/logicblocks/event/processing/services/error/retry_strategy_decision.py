from dataclasses import dataclass
from datetime import timedelta

from .error_handler_decision import ErrorHandlerDecision


class RetryStrategyDecision:
    """Base class for decisions made by a `RetryStrategy`.

    Provides factory methods for each kind of decision.
    """

    @staticmethod
    def retry_immediately() -> "RetryImmediatelyDecision":
        """Return a decision to retry without waiting."""
        return RetryImmediatelyDecision()

    @staticmethod
    def wait(duration: timedelta) -> "WaitRetryStrategyDecision":
        """Return a decision to wait before retrying.

        Args:
            duration: The time to wait before retrying.
        """
        return WaitRetryStrategyDecision(duration=duration)

    @staticmethod
    def override[R](
        error_handler_decision: ErrorHandlerDecision[R],
    ) -> "OverrideRetryStrategyDecision[R]":
        """Return a decision to use an error handler decision instead.

        Allows a retry strategy to stop retrying, for example by raising or
        exiting.

        Args:
            error_handler_decision: The error handler decision to use.
        """
        return OverrideRetryStrategyDecision(
            error_handler_decision=error_handler_decision
        )


@dataclass(frozen=True)
class RetryImmediatelyDecision(RetryStrategyDecision):
    """Decision to retry without waiting."""

    pass


@dataclass(frozen=True)
class WaitRetryStrategyDecision(RetryStrategyDecision):
    """Decision to wait before retrying.

    Attributes:
        duration: The time to wait before retrying.
    """

    duration: timedelta


@dataclass(frozen=True)
class OverrideRetryStrategyDecision[T](RetryStrategyDecision):
    """Decision to use an error handler decision in place of retrying.

    Attributes:
        error_handler_decision: The error handler decision to use.
    """

    error_handler_decision: ErrorHandlerDecision[T]
