from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import timedelta

from .error_handler_decision import ErrorHandlerDecision

type RetryStrategyResult[T] = timedelta | ErrorHandlerDecision[T] | None


class RetryStrategy[T](ABC):
    @abstractmethod
    def calculate(self, exception: Exception) -> RetryStrategyResult[T]: ...


@dataclass(frozen=True)
class ConstantRetryStrategy[T](RetryStrategy[T]):
    time: timedelta

    def calculate(self, exception: Exception) -> RetryStrategyResult[T]:
        return self.time


class IncludeExceptionsRetryStrategy[T](RetryStrategy[T]):
    def __init__(
        self, delegate: RetryStrategy, include_list: Sequence[type[Exception]]
    ):
        self._delegate = delegate
        self._include_list = tuple(include_list)

    def calculate(self, exception: Exception) -> RetryStrategyResult[T]:
        if not isinstance(exception, self._include_list):
            return timedelta()

        return self._delegate.calculate(exception)

    def __repr__(self):
        return f"{type(self).__name__}(delegate={self._delegate!r}, include_list={self._include_list})"


class ExcludeExceptionsRetryStrategy[T](RetryStrategy[T]):
    def __init__(
        self, delegate: RetryStrategy, exclude_list: Sequence[type[Exception]]
    ):
        self._delegate = delegate
        self._exclude_list = tuple(exclude_list)

    def calculate(self, exception: Exception) -> RetryStrategyResult[T]:
        if isinstance(exception, self._exclude_list):
            return timedelta()

        return self._delegate.calculate(exception)

    def __repr__(self):
        return f"{type(self).__name__}(delegate={self._delegate!r}, exclude_list={self._exclude_list})"
