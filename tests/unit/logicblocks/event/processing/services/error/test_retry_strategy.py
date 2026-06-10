from datetime import timedelta

from logicblocks.event.processing.services import (
    ConstantRetryStrategy,
    ErrorHandlerDecision,
    ExcludeExceptionsRetryStrategy,
    IncludeExceptionsRetryStrategy,
    RetryStrategy,
)


class TestConstantRetryStrategy:
    def test_returns_configured_time_for_any_exception(self):
        strategy = ConstantRetryStrategy(time=timedelta(seconds=5))

        assert strategy.calculate(RuntimeError("oops")) == timedelta(seconds=5)

    def test_returns_configured_time_regardless_of_exception_type(self):
        strategy = ConstantRetryStrategy(time=timedelta(milliseconds=500))

        assert strategy.calculate(ValueError("bad")) == timedelta(
            milliseconds=500
        )
        assert strategy.calculate(IOError("io")) == timedelta(milliseconds=500)


class TestIncludeExceptionsRetryStrategy:
    def test_delegates_to_inner_strategy_for_included_exception_type(self):
        class IncludedException(Exception):
            pass

        delegate = ConstantRetryStrategy(time=timedelta(seconds=3))
        strategy = IncludeExceptionsRetryStrategy(
            delegate=delegate, include_list=[IncludedException]
        )

        assert strategy.calculate(IncludedException()) == timedelta(seconds=3)

    def test_returns_zero_timedelta_for_non_included_exception_type(self):
        class IncludedException(Exception):
            pass

        class OtherException(Exception):
            pass

        delegate = ConstantRetryStrategy(time=timedelta(seconds=3))
        strategy = IncludeExceptionsRetryStrategy(
            delegate=delegate, include_list=[IncludedException]
        )

        assert strategy.calculate(OtherException()) == timedelta()

    def test_delegates_for_subclass_of_included_exception_type(self):
        class IncludedException(Exception):
            pass

        class SubException(IncludedException):
            pass

        delegate = ConstantRetryStrategy(time=timedelta(seconds=3))
        strategy = IncludeExceptionsRetryStrategy(
            delegate=delegate, include_list=[IncludedException]
        )

        assert strategy.calculate(SubException()) == timedelta(seconds=3)

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

        assert strategy.calculate(ExceptionA()) == timedelta(seconds=2)
        assert strategy.calculate(ExceptionB()) == timedelta(seconds=2)
        assert strategy.calculate(ExceptionC()) == timedelta()


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

        assert strategy.calculate(OtherException()) == timedelta(seconds=4)

    def test_returns_zero_timedelta_for_excluded_exception_type(self):
        class ExcludedException(Exception):
            pass

        delegate = ConstantRetryStrategy(time=timedelta(seconds=4))
        strategy = ExcludeExceptionsRetryStrategy(
            delegate=delegate, exclude_list=[ExcludedException]
        )

        assert strategy.calculate(ExcludedException()) == timedelta()

    def test_returns_zero_timedelta_for_subclass_of_excluded_exception_type(
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

        assert strategy.calculate(SubException()) == timedelta()

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

        assert strategy.calculate(ExceptionA()) == timedelta()
        assert strategy.calculate(ExceptionB()) == timedelta()
        assert strategy.calculate(ExceptionC()) == timedelta(seconds=2)


class TestRetryStrategyReturningAlternativeDecision:
    def test_retry_strategy_can_return_continue_decision(self):
        class ContinueAfterErrorStrategy(RetryStrategy[str]):
            def calculate(self, exception):
                return ErrorHandlerDecision.continue_execution(
                    value="fallback"
                )

        strategy = ContinueAfterErrorStrategy()

        assert strategy.calculate(
            RuntimeError("oops")
        ) == ErrorHandlerDecision.continue_execution(value="fallback")

    def test_retry_strategy_can_return_raise_decision(self):
        class RaiseWrappedStrategy(RetryStrategy):
            def calculate(self, exception):
                wrapped = ValueError(f"Wrapped: {exception}")
                return ErrorHandlerDecision.raise_exception(wrapped)

        strategy = RaiseWrappedStrategy()
        result = strategy.calculate(RuntimeError("oops"))

        assert isinstance(
            result, type(ErrorHandlerDecision.raise_exception(RuntimeError()))
        )
        assert str(result.exception) == "Wrapped: oops"

    def test_retry_strategy_can_return_exit_decision(self):
        class FatalStrategy(RetryStrategy):
            def calculate(self, exception):
                return ErrorHandlerDecision.exit_fatally(exit_code=99)

        strategy = FatalStrategy()

        assert strategy.calculate(
            RuntimeError("fatal")
        ) == ErrorHandlerDecision.exit_fatally(exit_code=99)


class TestIncludeExceptionsRetryStrategyWithAlternativeDecision:
    def test_passes_through_alternative_decision_from_delegate(self):
        class IncludedException(Exception):
            pass

        class ContinueStrategy(RetryStrategy[str]):
            def calculate(self, exception):
                return ErrorHandlerDecision.continue_execution(
                    value="recovered"
                )

        strategy = IncludeExceptionsRetryStrategy(
            delegate=ContinueStrategy(),
            include_list=[IncludedException],
        )

        assert strategy.calculate(
            IncludedException()
        ) == ErrorHandlerDecision.continue_execution(value="recovered")

    def test_returns_zero_timedelta_for_non_included_even_when_delegate_returns_decision(
        self,
    ):
        class IncludedException(Exception):
            pass

        class OtherException(Exception):
            pass

        class ContinueStrategy(RetryStrategy[str]):
            def calculate(self, exception):
                return ErrorHandlerDecision.continue_execution(
                    value="recovered"
                )

        strategy = IncludeExceptionsRetryStrategy(
            delegate=ContinueStrategy(),
            include_list=[IncludedException],
        )

        assert strategy.calculate(OtherException()) == timedelta()


class TestExcludeExceptionsRetryStrategyWithAlternativeDecision:
    def test_passes_through_alternative_decision_from_delegate(self):
        class ExcludedException(Exception):
            pass

        class OtherException(Exception):
            pass

        class ExitStrategy(RetryStrategy):
            def calculate(self, exception):
                return ErrorHandlerDecision.exit_fatally(exit_code=2)

        strategy = ExcludeExceptionsRetryStrategy(
            delegate=ExitStrategy(),
            exclude_list=[ExcludedException],
        )

        assert strategy.calculate(
            OtherException()
        ) == ErrorHandlerDecision.exit_fatally(exit_code=2)

    def test_returns_zero_timedelta_for_excluded_even_when_delegate_returns_decision(
        self,
    ):
        class ExcludedException(Exception):
            pass

        class ExitStrategy(RetryStrategy):
            def calculate(self, exception):
                return ErrorHandlerDecision.exit_fatally(exit_code=2)

        strategy = ExcludeExceptionsRetryStrategy(
            delegate=ExitStrategy(),
            exclude_list=[ExcludedException],
        )

        assert strategy.calculate(ExcludedException()) == timedelta()
