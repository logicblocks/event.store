from datetime import timedelta

from logicblocks.event.processing import (
    ErrorHandlerDecision,
)
from logicblocks.event.processing.services.error import (
    RetryStrategyDecision,
)


class TestRetryStrategyDecisionRetryImmediately:
    def test_creates_retry_immediately_decision(self):
        decision = RetryStrategyDecision.retry_immediately()

        assert isinstance(decision, RetryStrategyDecision)

    def test_retry_immediately_decisions_are_equal(self):
        assert (
            RetryStrategyDecision.retry_immediately()
            == RetryStrategyDecision.retry_immediately()
        )


class TestRetryStrategyDecisionWait:
    def test_creates_wait_decision_with_duration(self):
        decision = RetryStrategyDecision.wait(timedelta(seconds=5))

        assert isinstance(decision, RetryStrategyDecision)

    def test_wait_decisions_with_same_duration_are_equal(self):
        assert RetryStrategyDecision.wait(
            timedelta(seconds=5)
        ) == RetryStrategyDecision.wait(timedelta(seconds=5))

    def test_wait_decisions_with_different_durations_are_not_equal(self):
        assert RetryStrategyDecision.wait(
            timedelta(seconds=5)
        ) != RetryStrategyDecision.wait(timedelta(seconds=10))

    def test_wait_decision_exposes_duration(self):
        decision = RetryStrategyDecision.wait(timedelta(seconds=3))

        assert decision.duration == timedelta(seconds=3)


class TestRetryStrategyDecisionOverride:
    def test_creates_override_decision_with_error_handler_decision(self):
        error_decision = ErrorHandlerDecision.exit_fatally(exit_code=1)

        decision = RetryStrategyDecision.override(error_decision)

        assert isinstance(decision, RetryStrategyDecision)

    def test_override_decisions_with_same_error_decision_are_equal(self):
        assert RetryStrategyDecision.override(
            ErrorHandlerDecision.exit_fatally(exit_code=1)
        ) == RetryStrategyDecision.override(
            ErrorHandlerDecision.exit_fatally(exit_code=1)
        )

    def test_override_decisions_with_different_error_decisions_are_not_equal(
        self,
    ):
        assert RetryStrategyDecision.override(
            ErrorHandlerDecision.exit_fatally(exit_code=1)
        ) != RetryStrategyDecision.override(
            ErrorHandlerDecision.exit_fatally(exit_code=2)
        )

    def test_override_decision_exposes_error_handler_decision(self):
        error_decision = ErrorHandlerDecision.continue_execution(value=42)

        decision = RetryStrategyDecision.override(error_decision)

        assert decision.error_handler_decision == error_decision


class TestRetryStrategyDecisionInequality:
    def test_retry_immediately_is_not_equal_to_wait(self):
        assert RetryStrategyDecision.retry_immediately() != (
            RetryStrategyDecision.wait(timedelta())
        )

    def test_retry_immediately_is_not_equal_to_override(self):
        assert RetryStrategyDecision.retry_immediately() != (
            RetryStrategyDecision.override(
                ErrorHandlerDecision.retry_execution()
            )
        )

    def test_wait_is_not_equal_to_override(self):
        assert RetryStrategyDecision.wait(
            timedelta(seconds=1)
        ) != RetryStrategyDecision.override(
            ErrorHandlerDecision.retry_execution(
                wait_before_retry=timedelta(seconds=1)
            )
        )
