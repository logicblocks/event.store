from abc import ABC, abstractmethod
from typing import Any, Callable

from .error_handler_decision import ErrorHandlerDecision
from .retry_strategy import RetryStrategy
from .retry_strategy_decision import (
    OverrideRetryStrategyDecision,
    WaitRetryStrategyDecision,
)


def default_exit_code_factory(_: BaseException) -> int:
    return 1


def default_exception_factory(exception: BaseException) -> BaseException:
    return exception


class ErrorHandler[T](ABC):
    """Base class for handlers deciding how to respond to an exception."""

    @abstractmethod
    def handle(self, exception: BaseException) -> ErrorHandlerDecision[T]:
        """Decide how to respond to the given exception.

        Args:
            exception: The exception to handle.

        Returns:
            the decision on how to respond.
        """
        raise NotImplementedError


class ExitErrorHandler(ErrorHandler[Any]):
    """Error handler that decides to exit the process.

    Args:
        exit_code_factory: Returns the exit code for an exception. Defaults
            to always returning 1.
    """

    def __init__(
        self,
        exit_code_factory: Callable[[BaseException], int] = (
            default_exit_code_factory
        ),
    ):
        self.exit_code_factory = exit_code_factory

    def handle(self, exception: BaseException) -> ErrorHandlerDecision[Any]:
        return ErrorHandlerDecision.exit_fatally(
            exit_code=self.exit_code_factory(exception)
        )


class RaiseErrorHandler(ErrorHandler[Any]):
    """Error handler that decides to raise an exception.

    Args:
        exception_factory: Returns the exception to raise for a handled
            exception. Defaults to returning the handled exception.
    """

    def __init__(
        self,
        exception_factory: Callable[[BaseException], BaseException] = (
            default_exception_factory
        ),
    ):
        self.exception_factory = exception_factory

    def handle(self, exception: BaseException) -> ErrorHandlerDecision[Any]:
        resolved_exception = exception
        if self.exception_factory is not None:
            resolved_exception = self.exception_factory(exception)

        return ErrorHandlerDecision.raise_exception(resolved_exception)


class ContinueErrorHandler[T](ErrorHandler[T]):
    """Error handler that decides to continue, returning a value.

    Exceptions that are not `Exception` subclasses, such as
    `asyncio.CancelledError`, are raised instead.

    Args:
        value_factory: Returns the value to continue with for an exception.
    """

    def __init__(
        self,
        value_factory: Callable[[BaseException], T],
    ):
        self.value_factory: Callable[[BaseException], T] = value_factory

    def handle(self, exception: BaseException) -> ErrorHandlerDecision[T]:
        if isinstance(exception, Exception):
            return ErrorHandlerDecision.continue_execution(
                value=self.value_factory(exception)
            )
        else:
            return ErrorHandlerDecision.raise_exception(exception)


class RetryErrorHandler(ErrorHandler[Any]):
    """Error handler that decides to retry the failed execution.

    Exceptions that are not `Exception` subclasses, such as
    `asyncio.CancelledError`, are raised instead.

    Args:
        retry_strategy: Decides how to retry for each exception. If `None`,
            retries immediately.
    """

    def __init__(self, retry_strategy: RetryStrategy | None = None):
        self._retry_strategy = retry_strategy

    def _apply_retry_strategy(
        self, retry_strategy: RetryStrategy, exception: Exception
    ):
        match retry_strategy.calculate(exception):
            case OverrideRetryStrategyDecision(error_handler_decision):
                return error_handler_decision
            case WaitRetryStrategyDecision(duration):
                return ErrorHandlerDecision.retry_execution(
                    wait_before_retry=duration
                )
            case _:
                return ErrorHandlerDecision.retry_execution()

    def handle(self, exception: BaseException) -> ErrorHandlerDecision[Any]:
        if isinstance(exception, Exception):
            if self._retry_strategy is None:
                return ErrorHandlerDecision.retry_execution()

            return self._apply_retry_strategy(self._retry_strategy, exception)
        else:
            return ErrorHandlerDecision.raise_exception(exception)
