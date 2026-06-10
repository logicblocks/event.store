import warnings
from collections.abc import Callable
from datetime import timedelta

import pytest

from logicblocks.event.processing import (
    CallableService,
    ConstantRetryStrategy,
    ContinueErrorHandler,
    ErrorHandler,
    ErrorHandlerDecision,
    ErrorHandlingService,
    ExitErrorHandler,
    RaiseErrorHandler,
    RaiseErrorHandlerDecision,
    RetryErrorHandler,
    RetryStrategy,
    Service,
    TypeMappingErrorHandler,
    continue_execution_type_mapping,
    error_handler_type_mappings,
    exit_fatally_type_mapping,
    raise_exception_type_mapping,
    retry_execution_type_mapping,
)
from logicblocks.event.processing.services.error import (
    RetryStrategyDecision,
)


class TestExitErrorHandler:
    def test_requests_exit_with_default_exit_code_on_handle(self):
        class TestException(Exception):
            pass

        error_handler = ExitErrorHandler()

        assert error_handler.handle(
            TestException()
        ) == ErrorHandlerDecision.exit_fatally(exit_code=1)

    def test_requests_exit_with_configured_exit_code_on_handle(self):
        class TestException(Exception):
            pass

        error_handler = ExitErrorHandler(exit_code_factory=lambda _: 12)

        assert error_handler.handle(
            TestException()
        ) == ErrorHandlerDecision.exit_fatally(exit_code=12)


class TestRaiseErrorHandler:
    def test_requests_raise_with_passed_exception_by_default_on_handle(self):
        class TestException(Exception):
            pass

        exception = TestException()

        error_handler = RaiseErrorHandler()

        assert error_handler.handle(
            exception
        ) == ErrorHandlerDecision.raise_exception(exception)

    def test_requests_raise_with_exception_from_factory_when_provided_on_handle(
        self,
    ):
        class TestException(Exception):
            pass

        class OtherException(Exception):
            pass

        def exception_factory(ex: BaseException) -> BaseException:
            exception = OtherException()
            exception.__cause__ = ex
            return exception

        test_exception = TestException()

        error_handler = RaiseErrorHandler(exception_factory=exception_factory)

        decision = error_handler.handle(test_exception)
        assert isinstance(decision, RaiseErrorHandlerDecision)
        assert isinstance(decision.exception, OtherException)
        assert decision.exception.__cause__ == test_exception


class TestContinueErrorHandler:
    def test_requests_continue_with_value_on_handle_of_exception_subclass(
        self,
    ):
        class TestException(Exception):
            pass

        def value_factory(_ex: BaseException) -> int:
            return 10

        exception = TestException()

        error_handler = ContinueErrorHandler[int](value_factory)

        assert error_handler.handle(
            exception
        ) == ErrorHandlerDecision.continue_execution(value=10)

    def test_requests_raise_on_handle_of_non_exception_subclass(self):
        class FatalTestException(BaseException):
            pass

        def value_factory(_ex: BaseException) -> int:
            return 10

        exception = FatalTestException()

        error_handler = ContinueErrorHandler(value_factory)

        assert error_handler.handle(
            exception
        ) == ErrorHandlerDecision.raise_exception(exception)


class TestRetryErrorHandler:
    def test_requests_retry_on_handle_of_exception_subclass(self):
        class HandleableTestException(Exception):
            pass

        error_handler = RetryErrorHandler()

        assert (
            error_handler.handle(HandleableTestException())
            == ErrorHandlerDecision.retry_execution()
        )

    def test_requests_raise_on_handle_of_non_exception_subclass(self):
        class FatalTestException(BaseException):
            pass

        exception = FatalTestException()

        error_handler = RetryErrorHandler()

        assert error_handler.handle(
            exception
        ) == ErrorHandlerDecision.raise_exception(exception)

    def test_requests_retry_with_wait_time_from_retry_strategy(self):
        class HandleableTestException(Exception):
            pass

        retry_strategy = ConstantRetryStrategy(time=timedelta(seconds=5))
        error_handler = RetryErrorHandler(retry_strategy=retry_strategy)

        assert error_handler.handle(
            HandleableTestException()
        ) == ErrorHandlerDecision.retry_execution(
            wait_before_retry=timedelta(seconds=5)
        )

    def test_requests_retry_with_no_wait_time_when_no_retry_strategy(self):
        class HandleableTestException(Exception):
            pass

        error_handler = RetryErrorHandler()

        assert error_handler.handle(
            HandleableTestException()
        ) == ErrorHandlerDecision.retry_execution(wait_before_retry=None)

    def test_returns_override_decision_when_strategy_returns_override_with_continue(
        self,
    ):
        class HandleableTestException(Exception):
            pass

        class ContinueStrategy(RetryStrategy[str]):
            def calculate(self, exception):
                return RetryStrategyDecision.override(
                    ErrorHandlerDecision.continue_execution(value="fallback")
                )

        error_handler = RetryErrorHandler(retry_strategy=ContinueStrategy())

        assert error_handler.handle(
            HandleableTestException()
        ) == ErrorHandlerDecision.continue_execution(value="fallback")

    def test_returns_override_decision_when_strategy_returns_override_with_exit(
        self,
    ):
        class HandleableTestException(Exception):
            pass

        class ExitStrategy(RetryStrategy):
            def calculate(self, exception):
                return RetryStrategyDecision.override(
                    ErrorHandlerDecision.exit_fatally(exit_code=5)
                )

        error_handler = RetryErrorHandler(retry_strategy=ExitStrategy())

        assert error_handler.handle(
            HandleableTestException()
        ) == ErrorHandlerDecision.exit_fatally(exit_code=5)

    def test_returns_override_decision_when_strategy_returns_override_with_raise(
        self,
    ):
        class HandleableTestException(Exception):
            pass

        class RaiseStrategy(RetryStrategy):
            def calculate(self, exception):
                return RetryStrategyDecision.override(
                    ErrorHandlerDecision.raise_exception(ValueError("wrapped"))
                )

        error_handler = RetryErrorHandler(retry_strategy=RaiseStrategy())

        decision = error_handler.handle(HandleableTestException())

        assert isinstance(decision, RaiseErrorHandlerDecision)
        assert isinstance(decision.exception, ValueError)
        assert str(decision.exception) == "wrapped"

    def test_retries_immediately_when_strategy_returns_retry_immediately(self):
        class HandleableTestException(Exception):
            pass

        class RetryImmediatelyStrategy(RetryStrategy):
            def calculate(self, exception):
                return RetryStrategyDecision.retry_immediately()

        error_handler = RetryErrorHandler(
            retry_strategy=RetryImmediatelyStrategy()
        )

        assert error_handler.handle(
            HandleableTestException()
        ) == ErrorHandlerDecision.retry_execution(wait_before_retry=None)

    def test_still_raises_for_base_exception_even_when_strategy_provided(self):
        class FatalTestException(BaseException):
            pass

        class ContinueStrategy(RetryStrategy[str]):
            def calculate(self, exception):
                return RetryStrategyDecision.override(
                    ErrorHandlerDecision.continue_execution(value="fallback")
                )

        exception = FatalTestException()
        error_handler = RetryErrorHandler(retry_strategy=ContinueStrategy())

        assert error_handler.handle(
            exception
        ) == ErrorHandlerDecision.raise_exception(exception)


class TestTypeMappingErrorHandler:
    def test_requests_raise_exception_by_default(self):
        class TestException(Exception):
            pass

        exception = TestException()

        handler = TypeMappingErrorHandler()

        assert handler.handle(
            exception
        ) == ErrorHandlerDecision.raise_exception(exception)

    def test_requests_decision_from_error_handler_when_provided(self):
        class TestException(Exception):
            pass

        exception = TestException()

        handler = TypeMappingErrorHandler(
            default_error_handler=RetryErrorHandler()
        )

        assert (
            handler.handle(exception) == ErrorHandlerDecision.retry_execution()
        )

    def test_requests_exit_for_specified_exception_types(self):
        class TestException1(Exception):
            pass

        class TestException2(Exception):
            pass

        class TestException3(Exception):
            pass

        exception1 = TestException1()
        exception2 = TestException2()
        exception3 = TestException3()

        handler = TypeMappingErrorHandler(
            type_mappings=error_handler_type_mappings(
                exit_fatally=[TestException1, TestException2]
            )
        )

        assert handler.handle(exception1) == ErrorHandlerDecision.exit_fatally(
            exit_code=1
        )
        assert handler.handle(exception2) == ErrorHandlerDecision.exit_fatally(
            exit_code=1
        )
        assert handler.handle(
            exception3
        ) == ErrorHandlerDecision.raise_exception(exception3)

    def test_requests_exit_with_exit_code_for_specified_exception_types(self):
        class TestException1(Exception):
            pass

        class TestException2(Exception):
            pass

        class TestException3(Exception):
            pass

        exception1 = TestException1()
        exception2 = TestException2()
        exception3 = TestException3()

        handler = TypeMappingErrorHandler(
            type_mappings=error_handler_type_mappings(
                exit_fatally=exit_fatally_type_mapping(
                    types=[TestException1, TestException2],
                    exit_code_factory=lambda _: 10,
                )
            )
        )

        assert handler.handle(exception1) == ErrorHandlerDecision.exit_fatally(
            exit_code=10
        )
        assert handler.handle(exception2) == ErrorHandlerDecision.exit_fatally(
            exit_code=10
        )
        assert handler.handle(
            exception3
        ) == ErrorHandlerDecision.raise_exception(exception3)

    def test_requests_raise_exception_for_specified_exception_types(self):
        class TestException1(Exception):
            pass

        class TestException2(Exception):
            pass

        class TestException3(Exception):
            pass

        exception1 = TestException1()
        exception2 = TestException2()
        exception3 = TestException3()

        handler = TypeMappingErrorHandler(
            type_mappings=error_handler_type_mappings(
                raise_exception=[TestException1, TestException2]
            ),
            default_error_handler=RetryErrorHandler(),
        )

        assert handler.handle(
            exception1
        ) == ErrorHandlerDecision.raise_exception(exception1)
        assert handler.handle(
            exception2
        ) == ErrorHandlerDecision.raise_exception(exception2)
        assert (
            handler.handle(exception3)
            == ErrorHandlerDecision.retry_execution()
        )

    def test_requests_raise_with_specific_exception_for_specified_exception_types(
        self,
    ):
        class TestException1(Exception):
            pass

        class TestException2(Exception):
            pass

        class TestException3(Exception):
            pass

        exception1 = TestException1()
        exception2 = TestException2()
        exception3 = TestException3()

        raised_exception = ValueError("Oops!")

        handler = TypeMappingErrorHandler(
            type_mappings=error_handler_type_mappings(
                raise_exception=raise_exception_type_mapping(
                    types=[TestException1, TestException2],
                    exception_factory=lambda _: raised_exception,
                )
            ),
            default_error_handler=RetryErrorHandler(),
        )

        assert handler.handle(
            exception1
        ) == ErrorHandlerDecision.raise_exception(raised_exception)
        assert handler.handle(
            exception2
        ) == ErrorHandlerDecision.raise_exception(raised_exception)
        assert (
            handler.handle(exception3)
            == ErrorHandlerDecision.retry_execution()
        )

    def test_requests_retry_for_specified_exception_types(self):
        class TestException1(Exception):
            pass

        class TestException2(Exception):
            pass

        class TestException3(Exception):
            pass

        exception1 = TestException1()
        exception2 = TestException2()
        exception3 = TestException3()

        handler = TypeMappingErrorHandler(
            type_mappings=error_handler_type_mappings(
                retry_execution=[TestException1, TestException2]
            )
        )

        assert (
            handler.handle(exception1)
            == ErrorHandlerDecision.retry_execution()
        )
        assert (
            handler.handle(exception2)
            == ErrorHandlerDecision.retry_execution()
        )
        assert handler.handle(
            exception3
        ) == ErrorHandlerDecision.raise_exception(exception3)

    def test_requests_continue_with_value_for_specified_exception_types(self):
        class TestException1(Exception):
            pass

        class TestException2(Exception):
            pass

        class TestException3(Exception):
            pass

        exception1 = TestException1()
        exception2 = TestException2()
        exception3 = TestException3()

        handler = TypeMappingErrorHandler[str](
            type_mappings=error_handler_type_mappings(
                continue_execution=continue_execution_type_mapping(
                    types=[TestException1, TestException2],
                    value_factory=lambda _: "failed",
                )
            )
        )

        assert handler.handle(
            exception1
        ) == ErrorHandlerDecision.continue_execution(value="failed")
        assert handler.handle(
            exception2
        ) == ErrorHandlerDecision.continue_execution(value="failed")
        assert handler.handle(
            exception3
        ) == ErrorHandlerDecision.raise_exception(exception3)

    def test_subclass_of_mapped_exception_type_is_handled_the_same(self):
        class BaseExceptionType(Exception):
            pass

        class SubExceptionType(BaseExceptionType):
            pass

        base_exception = BaseExceptionType()
        sub_exception = SubExceptionType()

        handler = TypeMappingErrorHandler(
            type_mappings=error_handler_type_mappings(
                retry_execution=[BaseExceptionType]
            )
        )

        assert (
            handler.handle(base_exception)
            == ErrorHandlerDecision.retry_execution()
        )
        assert (
            handler.handle(sub_exception)
            == ErrorHandlerDecision.retry_execution()
        )

    def test_most_specific_type_mapping_is_used_when_both_base_and_subtype_are_mapped(
        self,
    ):
        class BaseExceptionType(Exception):
            pass

        class SubExceptionType(BaseExceptionType):
            pass

        base_exception = BaseExceptionType()
        sub_exception = SubExceptionType()

        handler = TypeMappingErrorHandler(
            type_mappings=error_handler_type_mappings(
                retry_execution=[BaseExceptionType],
                continue_execution=continue_execution_type_mapping(
                    types=[SubExceptionType], value_factory=lambda _: None
                ),
            )
        )

        assert (
            handler.handle(base_exception)
            == ErrorHandlerDecision.retry_execution()
        )
        assert handler.handle(
            sub_exception
        ) == ErrorHandlerDecision.continue_execution(value=None)

    def test_requests_retry_with_wait_time_for_specified_exception_types(self):
        class TestException1(Exception):
            pass

        class TestException2(Exception):
            pass

        exception1 = TestException1()
        exception2 = TestException2()

        retry_strategy = ConstantRetryStrategy(time=timedelta(seconds=2))

        handler = TypeMappingErrorHandler(
            type_mappings=error_handler_type_mappings(
                retry_execution=retry_execution_type_mapping(
                    types=[TestException1, TestException2],
                    retry_strategy=retry_strategy,
                )
            )
        )

        assert handler.handle(
            exception1
        ) == ErrorHandlerDecision.retry_execution(
            wait_before_retry=timedelta(seconds=2)
        )
        assert handler.handle(
            exception2
        ) == ErrorHandlerDecision.retry_execution(
            wait_before_retry=timedelta(seconds=2)
        )

    def test_calls_callback_when_specified(self):
        class TestException(Exception):
            pass

        callback_invoked = {}

        def callback(exception):
            callback_invoked["called"] = True
            callback_invoked["exception"] = exception

        exception = TestException()

        handler = TypeMappingErrorHandler(
            type_mappings=error_handler_type_mappings(
                retry_execution=retry_execution_type_mapping(
                    types=[TestException],
                    callback=callback,
                )
            )
        )

        handler.handle(exception)

        assert callback_invoked == {"called": True, "exception": exception}


class ExceptionThrowingService(Service[int]):
    __test__ = False

    def __init__(self, call_callback: Callable[[], None]):
        self._call_callback = call_callback

    async def execute(self) -> int:
        self._call_callback()
        raise RuntimeError("Something went wrong.")


class TestErrorHandlingServiceWithCustomService:
    async def test_raises_exception_when_raise_error_handler_decision(self):
        call_count = 0

        def call_callback():
            nonlocal call_count
            call_count += 1

        class TestException(Exception):
            pass

        service = ErrorHandlingService(
            service=ExceptionThrowingService(call_callback=call_callback),
            error_handler=RaiseErrorHandler(
                exception_factory=lambda ex: TestException(
                    "Unhandleable exception occurred."
                )
            ),
        )

        with pytest.raises(TestException):
            await service.execute()

        assert call_count == 1

    async def test_returns_value_when_continue_error_handler_decision(self):
        call_count = 0

        def call_callback():
            nonlocal call_count
            call_count += 1

        service = ErrorHandlingService(
            service=ExceptionThrowingService(call_callback=call_callback),
            error_handler=ContinueErrorHandler(value_factory=lambda ex: 10),
        )

        result = await service.execute()

        assert call_count == 1
        assert result == 10

    async def test_raises_system_exit_when_exit_error_handler_decision(self):
        call_count = 0

        def call_callback():
            nonlocal call_count
            call_count += 1

        service = ErrorHandlingService(
            service=ExceptionThrowingService(call_callback=call_callback),
            error_handler=ExitErrorHandler(exit_code_factory=lambda _: 42),
        )

        with pytest.raises(SystemExit) as exc_info:
            await service.execute()

        assert exc_info.value.code == 42
        assert call_count == 1

    async def test_retries_when_retry_error_handler_decision(self):
        call_count = 0

        class GiveUpTestException(Exception):
            pass

        class RetryTestException(Exception):
            pass

        class ConditionallyFailingService(Service[int]):
            async def execute(self) -> int:
                nonlocal call_count
                call_count += 1
                if call_count < 3:
                    raise RetryTestException("Let's try again.")
                else:
                    raise GiveUpTestException("Giving up.")

        class RetryOrContinueErrorHandler(ErrorHandler):
            def handle(
                self, exception: BaseException
            ) -> ErrorHandlerDecision[int]:
                if isinstance(exception, RetryTestException):
                    return ErrorHandlerDecision.retry_execution()
                elif isinstance(exception, GiveUpTestException):
                    return ErrorHandlerDecision.continue_execution(value=5)
                else:
                    return ErrorHandlerDecision.raise_exception(exception)

        service = ErrorHandlingService(
            service=ConditionallyFailingService(),
            error_handler=RetryOrContinueErrorHandler(),
        )

        await service.execute()

        assert call_count == 3


class TestErrorHandlingServiceWaitBeforeRetry:
    async def test_sleeps_for_wait_time_before_retrying(self):
        call_count = 0
        sleep_calls: list[float] = []

        async def fake_sleep(seconds: float):
            sleep_calls.append(seconds)

        class RetryThenSucceedService(Service[int]):
            async def execute(self) -> int:
                nonlocal call_count
                call_count += 1
                if call_count < 3:
                    raise RuntimeError("Not yet.")
                return 42

        retry_strategy = ConstantRetryStrategy(time=timedelta(seconds=5))

        service = ErrorHandlingService(
            service=RetryThenSucceedService(),
            error_handler=RetryErrorHandler(retry_strategy=retry_strategy),
            sleep=fake_sleep,
        )

        result = await service.execute()

        assert result == 42
        assert call_count == 3
        assert sleep_calls == [5.0, 5.0]

    async def test_does_not_sleep_when_wait_before_retry_is_none(self):
        call_count = 0
        sleep_calls: list[float] = []

        async def fake_sleep(seconds: float):
            sleep_calls.append(seconds)

        class RetryThenSucceedService(Service[int]):
            async def execute(self) -> int:
                nonlocal call_count
                call_count += 1
                if call_count < 2:
                    raise RuntimeError("Not yet.")
                return 42

        service = ErrorHandlingService(
            service=RetryThenSucceedService(),
            error_handler=RetryErrorHandler(),
            sleep=fake_sleep,
        )

        result = await service.execute()

        assert result == 42
        assert call_count == 2
        assert sleep_calls == []

    async def test_does_not_sleep_when_wait_before_retry_is_zero(self):
        call_count = 0
        sleep_calls: list[float] = []

        async def fake_sleep(seconds: float):
            sleep_calls.append(seconds)

        class RetryThenSucceedService(Service[int]):
            async def execute(self) -> int:
                nonlocal call_count
                call_count += 1
                if call_count < 2:
                    raise RuntimeError("Not yet.")
                return 42

        retry_strategy = ConstantRetryStrategy(time=timedelta())

        service = ErrorHandlingService(
            service=RetryThenSucceedService(),
            error_handler=RetryErrorHandler(retry_strategy=retry_strategy),
            sleep=fake_sleep,
        )

        result = await service.execute()

        assert result == 42
        assert call_count == 2
        assert sleep_calls == []


class TestErrorHandlingServiceWithAlternativeRetryDecision:
    async def test_returns_value_when_strategy_returns_override_with_continue(
        self,
    ):
        call_count = 0

        class AlwaysFailingService(Service[str]):
            async def execute(self) -> str:
                nonlocal call_count
                call_count += 1
                raise RuntimeError("failing")

        class ContinueAfterRetryStrategy(RetryStrategy[str]):
            def __init__(self):
                self._attempts = 0

            def calculate(self, exception):
                self._attempts += 1
                if self._attempts >= 2:
                    return RetryStrategyDecision.override(
                        ErrorHandlerDecision.continue_execution(
                            value="gave up"
                        )
                    )
                return RetryStrategyDecision.retry_immediately()

        service = ErrorHandlingService(
            service=AlwaysFailingService(),
            error_handler=RetryErrorHandler(
                retry_strategy=ContinueAfterRetryStrategy()
            ),
        )

        result = await service.execute()

        assert result == "gave up"
        assert call_count == 2

    async def test_raises_system_exit_when_strategy_returns_override_with_exit(
        self,
    ):
        call_count = 0

        class AlwaysFailingService(Service[int]):
            async def execute(self) -> int:
                nonlocal call_count
                call_count += 1
                raise RuntimeError("failing")

        class ExitAfterRetryStrategy(RetryStrategy):
            def __init__(self):
                self._attempts = 0

            def calculate(self, exception):
                self._attempts += 1
                if self._attempts >= 3:
                    return RetryStrategyDecision.override(
                        ErrorHandlerDecision.exit_fatally(exit_code=7)
                    )
                return RetryStrategyDecision.retry_immediately()

        service = ErrorHandlingService(
            service=AlwaysFailingService(),
            error_handler=RetryErrorHandler(
                retry_strategy=ExitAfterRetryStrategy()
            ),
        )

        with pytest.raises(SystemExit) as exc_info:
            await service.execute()

        assert exc_info.value.code == 7
        assert call_count == 3

    async def test_raises_exception_when_strategy_returns_override_with_raise(
        self,
    ):
        call_count = 0

        class AlwaysFailingService(Service[int]):
            async def execute(self) -> int:
                nonlocal call_count
                call_count += 1
                raise RuntimeError("failing")

        class RaiseAfterRetryStrategy(RetryStrategy):
            def __init__(self):
                self._attempts = 0

            def calculate(self, exception):
                self._attempts += 1
                if self._attempts >= 2:
                    return RetryStrategyDecision.override(
                        ErrorHandlerDecision.raise_exception(
                            ValueError("too many retries")
                        )
                    )
                return RetryStrategyDecision.retry_immediately()

        service = ErrorHandlingService(
            service=AlwaysFailingService(),
            error_handler=RetryErrorHandler(
                retry_strategy=RaiseAfterRetryStrategy()
            ),
        )

        with pytest.raises(ValueError, match="too many retries"):
            await service.execute()

        assert call_count == 2

    async def test_retries_with_wait_before_strategy_returns_override(self):
        call_count = 0
        sleep_calls: list[float] = []

        async def fake_sleep(seconds: float):
            sleep_calls.append(seconds)

        class AlwaysFailingService(Service[str]):
            async def execute(self) -> str:
                nonlocal call_count
                call_count += 1
                raise RuntimeError("failing")

        class RetryThenContinueStrategy(RetryStrategy[str]):
            def __init__(self):
                self._attempts = 0

            def calculate(self, exception):
                self._attempts += 1
                if self._attempts >= 3:
                    return RetryStrategyDecision.override(
                        ErrorHandlerDecision.continue_execution(value="done")
                    )
                return RetryStrategyDecision.wait(timedelta(seconds=1))

        service = ErrorHandlingService(
            service=AlwaysFailingService(),
            error_handler=RetryErrorHandler(
                retry_strategy=RetryThenContinueStrategy()
            ),
            sleep=fake_sleep,
        )

        result = await service.execute()

        assert result == "done"
        assert call_count == 3
        assert sleep_calls == [1.0, 1.0]


class TestErrorHandlingServiceRepr:
    async def test_includes_class_name_and_callable_repr(self):
        async def my_handler():
            pass

        service = ErrorHandlingService(
            callable=my_handler,
            error_handler=ContinueErrorHandler(value_factory=lambda ex: None),
        )

        assert (
            repr(service)
            == f"ErrorHandlingService(CallableService({my_handler.__qualname__}))"
        )


class TestErrorHandlingServiceWithCallable:
    async def test_raises_exception_when_raise_error_handler_decision(self):
        call_count = 0

        async def callable():
            nonlocal call_count
            call_count += 1
            raise RuntimeError("Something went wrong.")

        class TestException(Exception):
            pass

        service = ErrorHandlingService(
            service=CallableService(callable),
            error_handler=RaiseErrorHandler(
                exception_factory=lambda ex: TestException(
                    "Unhandleable exception occurred."
                )
            ),
        )

        with pytest.raises(TestException):
            await service.execute()

        assert call_count == 1

    async def test_returns_value_when_continue_error_handler_decision(self):
        call_count = 0

        async def callable() -> int:
            nonlocal call_count
            call_count += 1
            raise RuntimeError("Something went wrong.")

        service = ErrorHandlingService(
            service=CallableService(callable),
            error_handler=ContinueErrorHandler(value_factory=lambda ex: 10),
        )

        result = await service.execute()

        assert call_count == 1
        assert result == 10

    async def test_raises_system_exit_when_exit_error_handler_decision(self):
        call_count = 0

        async def callable():
            nonlocal call_count
            call_count += 1
            raise RuntimeError("Something went wrong.")

        service = ErrorHandlingService(
            service=CallableService(callable),
            error_handler=ExitErrorHandler(exit_code_factory=lambda _: 42),
        )

        with pytest.raises(SystemExit) as exc_info:
            await service.execute()

        assert exc_info.value.code == 42
        assert call_count == 1

    async def test_retries_when_retry_error_handler_decision(self):
        call_count = 0

        class GiveUpTestException(Exception):
            pass

        class RetryTestException(Exception):
            pass

        async def callable():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise RetryTestException("Let's try again.")
            else:
                raise GiveUpTestException("Giving up.")

        class RetryOrContinueErrorHandler(ErrorHandler):
            def handle(
                self, exception: BaseException
            ) -> ErrorHandlerDecision[int]:
                if isinstance(exception, RetryTestException):
                    return ErrorHandlerDecision.retry_execution()
                elif isinstance(exception, GiveUpTestException):
                    return ErrorHandlerDecision.continue_execution(value=5)
                else:
                    return ErrorHandlerDecision.raise_exception(exception)

        service = ErrorHandlingService(
            service=CallableService(callable),
            error_handler=RetryOrContinueErrorHandler(),
        )

        await service.execute()

        assert call_count == 3


class TestErrorHandlingServiceWithCallableBackwardsCompatibility:
    async def test_accepts_callable_and_auto_wraps(self):
        call_count = 0

        async def callable():
            nonlocal call_count
            call_count += 1
            raise RuntimeError("Something went wrong.")

        service = ErrorHandlingService(
            service=callable,
            error_handler=ContinueErrorHandler(value_factory=lambda ex: 10),
        )

        result = await service.execute()

        assert call_count == 1
        assert result == 10

    async def test_writes_deprecation_warning_when_callable_is_used(self):
        async def callable():
            raise RuntimeError("Something went wrong.")

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            ErrorHandlingService(
                service=callable,
                error_handler=ContinueErrorHandler(
                    value_factory=lambda ex: 10
                ),
            )

            assert w is not None
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert str(w[0].message) == "Wrap callable in CallableService"

    async def test_accepts_callable_kwarg_for_backwards_compatibility(self):
        call_count = 0

        async def callable():
            nonlocal call_count
            call_count += 1
            raise RuntimeError("Something went wrong.")

        service = ErrorHandlingService(
            callable=callable,
            error_handler=ContinueErrorHandler(value_factory=lambda ex: 10),
        )

        result = await service.execute()

        assert call_count == 1
        assert result == 10

    async def test_writes_deprecation_warning_when_callable_kwarg_is_used(
        self,
    ):
        async def callable():
            raise RuntimeError("Something went wrong.")

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            ErrorHandlingService(
                callable=callable,
                error_handler=ContinueErrorHandler(
                    value_factory=lambda ex: 10
                ),
            )

            assert w is not None
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert str(w[0].message) == "Use 'service' instead of 'callable'"

    async def test_accepts_callable_positional_arg_for_backwards_compatibility(
        self,
    ):
        call_count = 0

        async def callable():
            nonlocal call_count
            call_count += 1
            raise RuntimeError("Something went wrong.")

        service = ErrorHandlingService(
            callable,
            error_handler=ContinueErrorHandler(value_factory=lambda ex: 10),
        )

        result = await service.execute()

        assert call_count == 1
        assert result == 10

    async def test_writes_deprecation_warning_when_callable_positional_arg_is_used(
        self,
    ):
        async def callable():
            raise RuntimeError("Something went wrong.")

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            ErrorHandlingService(
                callable,
                error_handler=ContinueErrorHandler(
                    value_factory=lambda ex: 10
                ),
            )

            assert w is not None
            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert str(w[0].message) == "Wrap callable in CallableService"

    async def test_writes_no_warnings_when_service_is_used(self):
        async def callable():
            raise RuntimeError("Something went wrong.")

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            ErrorHandlingService[int](
                service=CallableService(callable),
                error_handler=ContinueErrorHandler(
                    value_factory=lambda ex: 10
                ),
            )

            assert w is not None
            assert len(w) == 0

    async def test_writes_no_warnings_when_service_is_used_positionally(self):
        async def callable():
            raise RuntimeError("Something went wrong.")

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            ErrorHandlingService[int](
                CallableService(callable),
                error_handler=ContinueErrorHandler(
                    value_factory=lambda ex: 10
                ),
            )

            assert w is not None
            assert len(w) == 0
