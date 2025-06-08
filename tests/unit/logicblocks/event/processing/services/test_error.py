from logicblocks.event.processing import (
    ContinueErrorHandler,
    ErrorHandlerDecision,
    ErrorHandlingService,
    ExitErrorHandler,
    RaiseErrorHandler,
    RaiseErrorHandlerDecision,
    RetryErrorHandler,
    TypeMappingErrorHandler,
    error_handler_type_mapping,
    error_handler_type_mappings,
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

        error_handler = ExitErrorHandler(exit_code=12)

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


class TestReturnErrorHandler:
    def test_requests_continue_with_none_by_default_on_handle(self):
        class TestException(Exception):
            pass

        error_handler = ContinueErrorHandler()

        assert (
            error_handler.handle(TestException())
            == ErrorHandlerDecision.continue_execution()
        )

    def test_requests_continue_with_value_from_provided_factory_on_handle(
        self,
    ):
        class TestException(Exception):
            pass

        def value_factory(ex: BaseException) -> int:
            return 10

        exception = TestException()

        error_handler = ContinueErrorHandler[int](value_factory=value_factory)

        assert error_handler.handle(
            exception
        ) == ErrorHandlerDecision.continue_execution(value=10)


class TestRetryErrorHandler:
    def test_requests_retry_on_handle(self):
        class TestException(Exception):
            pass

        error_handler = RetryErrorHandler()

        assert (
            error_handler.handle(TestException())
            == ErrorHandlerDecision.retry_execution()
        )


class TestTypeMappingErrorHandler:
    def test_requests_raise_exception_by_default(self):
        class TestException(Exception):
            pass

        exception = TestException()

        handler = TypeMappingErrorHandler()

        assert handler.handle(
            exception
        ) == ErrorHandlerDecision.raise_exception(exception)

    def test_requests_decision_from_factory_when_provided(self):
        class TestException(Exception):
            pass

        exception = TestException()

        handler = TypeMappingErrorHandler(
            default_decision_factory=lambda _: ErrorHandlerDecision.retry_execution()
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
            default_decision_factory=lambda _: ErrorHandlerDecision.retry_execution(),
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

        handler = TypeMappingErrorHandler(
            type_mappings=error_handler_type_mappings(
                continue_execution=[TestException1, TestException2]
            )
        )

        assert (
            handler.handle(exception1)
            == ErrorHandlerDecision.continue_execution()
        )
        assert (
            handler.handle(exception2)
            == ErrorHandlerDecision.continue_execution()
        )
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
                continue_execution=[SubExceptionType],
            )
        )

        assert (
            handler.handle(base_exception)
            == ErrorHandlerDecision.retry_execution()
        )
        assert (
            handler.handle(sub_exception)
            == ErrorHandlerDecision.continue_execution()
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
                retry_execution=error_handler_type_mapping(
                    types=[TestException],
                    callback=callback,
                )
            )
        )

        handler.handle(exception)

        assert callback_invoked == {"called": True, "exception": exception}





class TestErrorHandlingService:
    async def test_captures_handleable_exceptions_and_continues(self):
        class TestException(Exception):
            pass

        invocations = 0
        handled_errors = 0

        async def invocation_counter() -> None:
            nonlocal invocations
            invocations += 1
            raise TestException()

        def error_handler(ex: BaseException) -> None:
            if type(ex) is TestException:
                nonlocal handled_errors
                handled_errors += 1
            else:
                raise ex

        service = ErrorHandlingService(
            callable=invocation_counter, error_handler=error_handler
        )

        await service.execute()

        assert invocations == 1
        assert handled_errors == 1

    async def test_does_not_invoke_error_handler_when_no_errors_occur(self):
        invocations = 0
        handled_errors = 0

        async def invocation_counter() -> None:
            nonlocal invocations
            invocations += 1

        def error_handler(ex: BaseException) -> None:
            nonlocal handled_errors
            handled_errors += 1

        service = ErrorHandlingService(
            callable=invocation_counter, error_handler=error_handler
        )

        await service.execute()

        assert invocations == 1
        assert handled_errors == 0
