from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import timedelta

from .retry_strategy_decision import RetryStrategyDecision


class RetryStrategy[T](ABC):
    @abstractmethod
    def calculate(self, exception: Exception) -> RetryStrategyDecision: ...


@dataclass(frozen=True)
class ConstantRetryStrategy[T](RetryStrategy[T]):
    time: timedelta

    def calculate(self, exception: Exception) -> RetryStrategyDecision:
        return RetryStrategyDecision.wait(self.time)


class IncludeExceptionsRetryStrategy[T](RetryStrategy[T]):
    def __init__(
        self, delegate: RetryStrategy, include_list: Sequence[type[Exception]]
    ):
        self._delegate = delegate
        self._include_list = tuple(include_list)

    def calculate(self, exception: Exception) -> RetryStrategyDecision:
        if not isinstance(exception, self._include_list):
            return RetryStrategyDecision.retry_immediately()

        return self._delegate.calculate(exception)

    def __repr__(self):
        return f"{type(self).__name__}(delegate={self._delegate!r}, include_list={self._include_list})"


class ExcludeExceptionsRetryStrategy[T](RetryStrategy[T]):
    def __init__(
        self, delegate: RetryStrategy, exclude_list: Sequence[type[Exception]]
    ):
        self._delegate = delegate
        self._exclude_list = tuple(exclude_list)

    def calculate(self, exception: Exception) -> RetryStrategyDecision:
        if isinstance(exception, self._exclude_list):
            return RetryStrategyDecision.retry_immediately()

        return self._delegate.calculate(exception)

    def __repr__(self):
        return f"{type(self).__name__}(delegate={self._delegate!r}, exclude_list={self._exclude_list})"
