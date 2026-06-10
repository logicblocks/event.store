from abc import ABC, abstractmethod
from typing import Any, Callable

from .error_handler_decision import ErrorHandlerDecision
from .retry_strategy import RetryStrategy


def default_exit_code_factory(_: BaseException) -> int:
    return 1


def default_exception_factory(exception: BaseException) -> BaseException:
    return exception


class ErrorHandler[T](ABC):
    @abstractmethod
    def handle(self, exception: BaseException) -> ErrorHandlerDecision[T]:
        raise NotImplementedError


class ExitErrorHandler(ErrorHandler[Any]):
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
    def __init__(self, retry_strategy: RetryStrategy | None = None):
        self._retry_strategy = retry_strategy

    def handle(self, exception: BaseException) -> ErrorHandlerDecision[Any]:
        if isinstance(exception, Exception):
            result = (
                self._retry_strategy.calculate(exception)
                if self._retry_strategy
                else None
            )
            if isinstance(result, ErrorHandlerDecision):
                return result

            return ErrorHandlerDecision.retry_execution(
                wait_before_retry=result
            )
        else:
            return ErrorHandlerDecision.raise_exception(exception)
