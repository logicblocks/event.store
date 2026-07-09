from dataclasses import dataclass
from datetime import timedelta

from .error_handler_decision import ErrorHandlerDecision


class RetryStrategyDecision:
    @staticmethod
    def retry_immediately() -> "RetryImmediatelyDecision":
        return RetryImmediatelyDecision()

    @staticmethod
    def wait(duration: timedelta) -> "WaitRetryStrategyDecision":
        return WaitRetryStrategyDecision(duration=duration)

    @staticmethod
    def override[R](
        error_handler_decision: ErrorHandlerDecision[R],
    ) -> "OverrideRetryStrategyDecision[R]":
        return OverrideRetryStrategyDecision(
            error_handler_decision=error_handler_decision
        )


@dataclass(frozen=True)
class RetryImmediatelyDecision(RetryStrategyDecision):
    pass


@dataclass(frozen=True)
class WaitRetryStrategyDecision(RetryStrategyDecision):
    duration: timedelta


@dataclass(frozen=True)
class OverrideRetryStrategyDecision[T](RetryStrategyDecision):
    error_handler_decision: ErrorHandlerDecision[T]
