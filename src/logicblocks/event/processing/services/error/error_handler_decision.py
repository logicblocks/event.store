from dataclasses import dataclass
from datetime import timedelta
from typing import Any


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
