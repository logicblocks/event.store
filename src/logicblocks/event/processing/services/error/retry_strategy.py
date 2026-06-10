from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import timedelta


class RetryStrategy(ABC):
    @abstractmethod
    def wait_time(self, exception: Exception) -> timedelta: ...


@dataclass(frozen=True)
class ConstantRetryStrategy(RetryStrategy):
    time: timedelta

    def wait_time(self, exception: Exception) -> timedelta:
        return self.time


class IncludeExceptionsRetryStrategy(RetryStrategy):
    def __init__(
        self, delegate: RetryStrategy, include_list: Sequence[type[Exception]]
    ):
        self._delegate = delegate
        self._include_list = tuple(include_list)

    def wait_time(self, exception: Exception) -> timedelta:
        if not isinstance(exception, self._include_list):
            return timedelta()

        return self._delegate.wait_time(exception)

    def __repr__(self):
        return f"{type(self).__name__}(delegate={self._delegate!r}, include_list={self._include_list})"


class ExcludeExceptionsRetryStrategy(RetryStrategy):
    def __init__(
        self, delegate: RetryStrategy, exclude_list: Sequence[type[Exception]]
    ):
        self._delegate = delegate
        self._exclude_list = tuple(exclude_list)

    def wait_time(self, exception: Exception) -> timedelta:
        if isinstance(exception, self._exclude_list):
            return timedelta()

        return self._delegate.wait_time(exception)

    def __repr__(self):
        return f"{type(self).__name__}(delegate={self._delegate!r}, exclude_list={self._exclude_list})"
