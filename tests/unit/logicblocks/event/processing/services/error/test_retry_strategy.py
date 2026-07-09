from datetime import timedelta

from logicblocks.event.processing import RaiseErrorHandlerDecision
from logicblocks.event.processing.services import (
    ConstantRetryStrategy,
    ErrorHandlerDecision,
    ExcludeExceptionsRetryStrategy,
    IncludeExceptionsRetryStrategy,
    RetryStrategy,
)
from logicblocks.event.processing.services.error import (
    OverrideRetryStrategyDecision,
    RetryStrategyDecision,
)


class TestConstantRetryStrategy:
    def test_returns_wait_decision_with_configured_time(self):
        strategy = ConstantRetryStrategy(time=timedelta(seconds=5))

        assert strategy.calculate(
            RuntimeError("oops")
        ) == RetryStrategyDecision.wait(timedelta(seconds=5))

    def test_returns_wait_decision_regardless_of_exception_type(self):
        strategy = ConstantRetryStrategy(time=timedelta(milliseconds=500))

        assert strategy.calculate(
            ValueError("bad")
        ) == RetryStrategyDecision.wait(timedelta(milliseconds=500))
        assert strategy.calculate(IOError("io")) == RetryStrategyDecision.wait(
            timedelta(milliseconds=500)
        )


class TestIncludeExceptionsRetryStrategy:
    def test_delegates_to_inner_strategy_for_included_exception_type(self):
        class IncludedException(Exception):
            pass

        delegate = ConstantRetryStrategy(time=timedelta(seconds=3))
        strategy = IncludeExceptionsRetryStrategy(
            delegate=delegate, include_list=[IncludedException]
        )

        assert strategy.calculate(
            IncludedException()
        ) == RetryStrategyDecision.wait(timedelta(seconds=3))

    def test_retries_immediately_for_non_included_exception_type(self):
        class IncludedException(Exception):
            pass

        class OtherException(Exception):
            pass

        delegate = ConstantRetryStrategy(time=timedelta(seconds=3))
        strategy = IncludeExceptionsRetryStrategy(
            delegate=delegate, include_list=[IncludedException]
        )

        assert (
            strategy.calculate(OtherException())
            == RetryStrategyDecision.retry_immediately()
        )

    def test_delegates_for_subclass_of_included_exception_type(self):
        class IncludedException(Exception):
            pass

        class SubException(IncludedException):
            pass

        delegate = ConstantRetryStrategy(time=timedelta(seconds=3))
        strategy = IncludeExceptionsRetryStrategy(
            delegate=delegate, include_list=[IncludedException]
        )

        assert strategy.calculate(
            SubException()
        ) == RetryStrategyDecision.wait(timedelta(seconds=3))

    def test_supports_multiple_included_exception_types(self):
        class ExceptionA(Exception):
            pass

        class ExceptionB(Exception):
            pass

        class ExceptionC(Exception):
            pass

        delegate = ConstantRetryStrategy(time=timedelta(seconds=2))
        strategy = IncludeExceptionsRetryStrategy(
            delegate=delegate, include_list=[ExceptionA, ExceptionB]
        )

        assert strategy.calculate(ExceptionA()) == RetryStrategyDecision.wait(
            timedelta(seconds=2)
        )
        assert strategy.calculate(ExceptionB()) == RetryStrategyDecision.wait(
            timedelta(seconds=2)
        )
        assert (
            strategy.calculate(ExceptionC())
            == RetryStrategyDecision.retry_immediately()
        )


class TestExcludeExceptionsRetryStrategy:
    def test_delegates_to_inner_strategy_for_non_excluded_exception_type(self):
        class ExcludedException(Exception):
            pass

        class OtherException(Exception):
            pass

        delegate = ConstantRetryStrategy(time=timedelta(seconds=4))
        strategy = ExcludeExceptionsRetryStrategy(
            delegate=delegate, exclude_list=[ExcludedException]
        )

        assert strategy.calculate(
            OtherException()
        ) == RetryStrategyDecision.wait(timedelta(seconds=4))

    def test_retries_immediately_for_excluded_exception_type(self):
        class ExcludedException(Exception):
            pass

        delegate = ConstantRetryStrategy(time=timedelta(seconds=4))
        strategy = ExcludeExceptionsRetryStrategy(
            delegate=delegate, exclude_list=[ExcludedException]
        )

        assert (
            strategy.calculate(ExcludedException())
            == RetryStrategyDecision.retry_immediately()
        )

    def test_retries_immediately_for_subclass_of_excluded_exception_type(
        self,
    ):
        class ExcludedException(Exception):
            pass

        class SubException(ExcludedException):
            pass

        delegate = ConstantRetryStrategy(time=timedelta(seconds=4))
        strategy = ExcludeExceptionsRetryStrategy(
            delegate=delegate, exclude_list=[ExcludedException]
        )

        assert (
            strategy.calculate(SubException())
            == RetryStrategyDecision.retry_immediately()
        )

    def test_supports_multiple_excluded_exception_types(self):
        class ExceptionA(Exception):
            pass

        class ExceptionB(Exception):
            pass

        class ExceptionC(Exception):
            pass

        delegate = ConstantRetryStrategy(time=timedelta(seconds=2))
        strategy = ExcludeExceptionsRetryStrategy(
            delegate=delegate, exclude_list=[ExceptionA, ExceptionB]
        )

        assert (
            strategy.calculate(ExceptionA())
            == RetryStrategyDecision.retry_immediately()
        )
        assert (
            strategy.calculate(ExceptionB())
            == RetryStrategyDecision.retry_immediately()
        )
        assert strategy.calculate(ExceptionC()) == RetryStrategyDecision.wait(
            timedelta(seconds=2)
        )


class TestRetryStrategyReturningOverrideDecision:
    def test_retry_strategy_can_return_override_with_continue(self):
        class ContinueAfterErrorStrategy(RetryStrategy[str]):
            def calculate(self, exception):
                return RetryStrategyDecision.override(
                    ErrorHandlerDecision.continue_execution(value="fallback")
                )

        strategy = ContinueAfterErrorStrategy()

        assert strategy.calculate(
            RuntimeError("oops")
        ) == RetryStrategyDecision.override(
            ErrorHandlerDecision.continue_execution(value="fallback")
        )

    def test_retry_strategy_can_return_override_with_raise(self):
        class RaiseWrappedStrategy(RetryStrategy):
            def calculate(self, exception):
                wrapped = ValueError(f"Wrapped: {exception}")
                return RetryStrategyDecision.override(
                    ErrorHandlerDecision.raise_exception(wrapped)
                )

        strategy = RaiseWrappedStrategy()
        result = strategy.calculate(RuntimeError("oops"))

        assert isinstance(result, OverrideRetryStrategyDecision)
        assert isinstance(
            result.error_handler_decision, RaiseErrorHandlerDecision
        )
        assert str(result.error_handler_decision.exception) == "Wrapped: oops"

    def test_retry_strategy_can_return_override_with_exit(self):
        class FatalStrategy(RetryStrategy):
            def calculate(self, exception):
                return RetryStrategyDecision.override(
                    ErrorHandlerDecision.exit_fatally(exit_code=99)
                )

        strategy = FatalStrategy()

        assert strategy.calculate(
            RuntimeError("fatal")
        ) == RetryStrategyDecision.override(
            ErrorHandlerDecision.exit_fatally(exit_code=99)
        )


class TestIncludeExceptionsRetryStrategyWithOverrideDecision:
    def test_passes_through_override_decision_from_delegate(self):
        class IncludedException(Exception):
            pass

        class ContinueStrategy(RetryStrategy[str]):
            def calculate(self, exception):
                return RetryStrategyDecision.override(
                    ErrorHandlerDecision.continue_execution(value="recovered")
                )

        strategy = IncludeExceptionsRetryStrategy(
            delegate=ContinueStrategy(),
            include_list=[IncludedException],
        )

        assert strategy.calculate(
            IncludedException()
        ) == RetryStrategyDecision.override(
            ErrorHandlerDecision.continue_execution(value="recovered")
        )

    def test_retries_immediately_for_non_included_even_when_delegate_returns_override(
        self,
    ):
        class IncludedException(Exception):
            pass

        class OtherException(Exception):
            pass

        class ContinueStrategy(RetryStrategy[str]):
            def calculate(self, exception):
                return RetryStrategyDecision.override(
                    ErrorHandlerDecision.continue_execution(value="recovered")
                )

        strategy = IncludeExceptionsRetryStrategy(
            delegate=ContinueStrategy(),
            include_list=[IncludedException],
        )

        assert (
            strategy.calculate(OtherException())
            == RetryStrategyDecision.retry_immediately()
        )


class TestExcludeExceptionsRetryStrategyWithOverrideDecision:
    def test_passes_through_override_decision_from_delegate(self):
        class ExcludedException(Exception):
            pass

        class OtherException(Exception):
            pass

        class ExitStrategy(RetryStrategy):
            def calculate(self, exception):
                return RetryStrategyDecision.override(
                    ErrorHandlerDecision.exit_fatally(exit_code=2)
                )

        strategy = ExcludeExceptionsRetryStrategy(
            delegate=ExitStrategy(),
            exclude_list=[ExcludedException],
        )

        assert strategy.calculate(
            OtherException()
        ) == RetryStrategyDecision.override(
            ErrorHandlerDecision.exit_fatally(exit_code=2)
        )

    def test_retries_immediately_for_excluded_even_when_delegate_returns_override(
        self,
    ):
        class ExcludedException(Exception):
            pass

        class ExitStrategy(RetryStrategy):
            def calculate(self, exception):
                return RetryStrategyDecision.override(
                    ErrorHandlerDecision.exit_fatally(exit_code=2)
                )

        strategy = ExcludeExceptionsRetryStrategy(
            delegate=ExitStrategy(),
            exclude_list=[ExcludedException],
        )

        assert (
            strategy.calculate(ExcludedException())
            == RetryStrategyDecision.retry_immediately()
        )
