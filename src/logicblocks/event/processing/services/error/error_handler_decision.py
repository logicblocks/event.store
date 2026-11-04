from dataclasses import dataclass
from datetime import timedelta
from typing import Any


class ErrorHandlerDecision[T]:
    """Base class for decisions made by an `ErrorHandler`.

    Provides factory methods for each kind of decision.
    """

    @staticmethod
    def exit_fatally(*, exit_code: int = 1) -> "ExitErrorHandlerDecision":
        """Return a decision to exit the process.

        Args:
            exit_code: The exit code to exit with.
        """
        return ExitErrorHandlerDecision(exit_code=exit_code)

    @staticmethod
    def raise_exception(
        exception: BaseException,
    ) -> "RaiseErrorHandlerDecision":
        """Return a decision to raise an exception.

        Args:
            exception: The exception to raise.
        """
        return RaiseErrorHandlerDecision(exception=exception)

    @staticmethod
    def continue_execution[R](
        *, value: R
    ) -> "ContinueErrorHandlerDecision[R]":
        """Return a decision to continue, returning a value as the result.

        Args:
            value: The value to return.
        """
        return ContinueErrorHandlerDecision[R](value=value)

    @staticmethod
    def retry_execution(
        *,
        wait_before_retry: timedelta | None = None,
    ) -> "RetryErrorHandlerDecision":
        """Return a decision to retry the failed execution.

        Args:
            wait_before_retry: The time to wait before retrying. If `None`,
                retries immediately.
        """
        return RetryErrorHandlerDecision(wait_before_retry=wait_before_retry)


@dataclass(frozen=True)
class ExitErrorHandlerDecision(ErrorHandlerDecision[Any]):
    """Decision to exit the process by raising `SystemExit`.

    Attributes:
        exit_code: The exit code to exit with.
    """

    exit_code: int


@dataclass(frozen=True)
class RaiseErrorHandlerDecision(ErrorHandlerDecision[Any]):
    """Decision to raise an exception.

    Attributes:
        exception: The exception to raise.
    """

    exception: BaseException


@dataclass(frozen=True)
class ContinueErrorHandlerDecision[T](ErrorHandlerDecision[T]):
    """Decision to continue, returning a value in place of a result.

    Attributes:
        value: The value to return.
    """

    value: T


@dataclass(frozen=True)
class RetryErrorHandlerDecision(ErrorHandlerDecision[Any]):
    """Decision to retry the failed execution.

    Attributes:
        wait_before_retry: The time to wait before retrying. If `None`,
            retries immediately.
    """

    wait_before_retry: timedelta | None = None
