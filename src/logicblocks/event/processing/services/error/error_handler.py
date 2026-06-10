from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Callable

from .retry_strategy import RetryStrategy


def default_exit_code_factory(_: BaseException) -> int:
    return 1


def default_exception_factory(exception: BaseException) -> BaseException:
    return exception


class ErrorHandlerDecision[T]:
    @staticmethod
    def exit_fatally(*, exit_code: int = 1) -> "ExitErrorHandlerDecision":
        return ExitErrorHandlerDecision(exit_code=exit_code)

    @staticmethod
    def raise_exception(
        exception: BaseException,
    ) -> "RaiseErrorHandlerDecision":
        return RaiseErrorHandlerDecision(exception=exception)

    @staticmethod
    def continue_execution[R](
        *, value: R
    ) -> "ContinueErrorHandlerDecision[R]":
        return ContinueErrorHandlerDecision[R](value=value)

    @staticmethod
    def retry_execution(
        *,
        wait_before_retry: timedelta | None = None,
    ) -> "RetryErrorHandlerDecision":
        return RetryErrorHandlerDecision(wait_before_retry=wait_before_retry)


@dataclass(frozen=True)
class ExitErrorHandlerDecision(ErrorHandlerDecision[Any]):
    exit_code: int


@dataclass(frozen=True)
class RaiseErrorHandlerDecision(ErrorHandlerDecision[Any]):
    exception: BaseException


@dataclass(frozen=True)
class ContinueErrorHandlerDecision[T](ErrorHandlerDecision[T]):
    value: T


@dataclass(frozen=True)
class RetryErrorHandlerDecision(ErrorHandlerDecision[Any]):
    wait_before_retry: timedelta | None = None


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
    def __init__(self, wait_strategy: RetryStrategy | None = None):
        self._wait_strategy = wait_strategy

    def handle(self, exception: BaseException) -> ErrorHandlerDecision[Any]:
        if isinstance(exception, Exception):
            wait_before_retry = (
                self._wait_strategy.wait_time(exception)
                if self._wait_strategy
                else None
            )
            return ErrorHandlerDecision.retry_execution(
                wait_before_retry=wait_before_retry
            )
        else:
            return ErrorHandlerDecision.raise_exception(exception)
