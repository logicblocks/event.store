from logicblocks.event.processing import CallableService
from logicblocks.event.processing.services.callable import as_callable_service


class TestAsService:
    async def test_returns_service_unchanged_when_given_service(self):
        async def noop():
            pass

        service = CallableService(noop)

        assert as_callable_service(service) is service

    async def test_wraps_callable_in_callable_service(self):
        async def noop():
            pass

        result = as_callable_service(noop)

        assert isinstance(result, CallableService)

    async def test_wrapped_callable_executes_correctly(self):
        async def return_value():
            return 42

        service = as_callable_service(return_value)

        assert await service.execute() == 42

    async def test_wrapped_callable_has_function_name(self):
        async def my_function():
            pass

        service = as_callable_service(my_function)

        assert service.name == "my_function"
