from datetime import timedelta

from logicblocks.event.processing.services import (
    ConstantWaitStrategy,
    ExcludeExceptionsWaitStrategy,
    IncludeExceptionsWaitStrategy,
)


class TestConstantWaitStrategy:
    def test_returns_configured_time_for_any_exception(self):
        strategy = ConstantWaitStrategy(time=timedelta(seconds=5))

        assert strategy.wait_time(RuntimeError("oops")) == timedelta(seconds=5)

    def test_returns_configured_time_regardless_of_exception_type(self):
        strategy = ConstantWaitStrategy(time=timedelta(milliseconds=500))

        assert strategy.wait_time(ValueError("bad")) == timedelta(
            milliseconds=500
        )
        assert strategy.wait_time(IOError("io")) == timedelta(milliseconds=500)


class TestIncludeExceptionsWaitStrategy:
    def test_delegates_to_inner_strategy_for_included_exception_type(self):
        class IncludedException(Exception):
            pass

        delegate = ConstantWaitStrategy(time=timedelta(seconds=3))
        strategy = IncludeExceptionsWaitStrategy(
            delegate=delegate, include_list=[IncludedException]
        )

        assert strategy.wait_time(IncludedException()) == timedelta(seconds=3)

    def test_returns_zero_timedelta_for_non_included_exception_type(self):
        class IncludedException(Exception):
            pass

        class OtherException(Exception):
            pass

        delegate = ConstantWaitStrategy(time=timedelta(seconds=3))
        strategy = IncludeExceptionsWaitStrategy(
            delegate=delegate, include_list=[IncludedException]
        )

        assert strategy.wait_time(OtherException()) == timedelta()

    def test_delegates_for_subclass_of_included_exception_type(self):
        class IncludedException(Exception):
            pass

        class SubException(IncludedException):
            pass

        delegate = ConstantWaitStrategy(time=timedelta(seconds=3))
        strategy = IncludeExceptionsWaitStrategy(
            delegate=delegate, include_list=[IncludedException]
        )

        assert strategy.wait_time(SubException()) == timedelta(seconds=3)

    def test_supports_multiple_included_exception_types(self):
        class ExceptionA(Exception):
            pass

        class ExceptionB(Exception):
            pass

        class ExceptionC(Exception):
            pass

        delegate = ConstantWaitStrategy(time=timedelta(seconds=2))
        strategy = IncludeExceptionsWaitStrategy(
            delegate=delegate, include_list=[ExceptionA, ExceptionB]
        )

        assert strategy.wait_time(ExceptionA()) == timedelta(seconds=2)
        assert strategy.wait_time(ExceptionB()) == timedelta(seconds=2)
        assert strategy.wait_time(ExceptionC()) == timedelta()


class TestExcludeExceptionsWaitStrategy:
    def test_delegates_to_inner_strategy_for_non_excluded_exception_type(self):
        class ExcludedException(Exception):
            pass

        class OtherException(Exception):
            pass

        delegate = ConstantWaitStrategy(time=timedelta(seconds=4))
        strategy = ExcludeExceptionsWaitStrategy(
            delegate=delegate, exclude_list=[ExcludedException]
        )

        assert strategy.wait_time(OtherException()) == timedelta(seconds=4)

    def test_returns_zero_timedelta_for_excluded_exception_type(self):
        class ExcludedException(Exception):
            pass

        delegate = ConstantWaitStrategy(time=timedelta(seconds=4))
        strategy = ExcludeExceptionsWaitStrategy(
            delegate=delegate, exclude_list=[ExcludedException]
        )

        assert strategy.wait_time(ExcludedException()) == timedelta()

    def test_returns_zero_timedelta_for_subclass_of_excluded_exception_type(
        self,
    ):
        class ExcludedException(Exception):
            pass

        class SubException(ExcludedException):
            pass

        delegate = ConstantWaitStrategy(time=timedelta(seconds=4))
        strategy = ExcludeExceptionsWaitStrategy(
            delegate=delegate, exclude_list=[ExcludedException]
        )

        assert strategy.wait_time(SubException()) == timedelta()

    def test_supports_multiple_excluded_exception_types(self):
        class ExceptionA(Exception):
            pass

        class ExceptionB(Exception):
            pass

        class ExceptionC(Exception):
            pass

        delegate = ConstantWaitStrategy(time=timedelta(seconds=2))
        strategy = ExcludeExceptionsWaitStrategy(
            delegate=delegate, exclude_list=[ExceptionA, ExceptionB]
        )

        assert strategy.wait_time(ExceptionA()) == timedelta()
        assert strategy.wait_time(ExceptionB()) == timedelta()
        assert strategy.wait_time(ExceptionC()) == timedelta(seconds=2)
