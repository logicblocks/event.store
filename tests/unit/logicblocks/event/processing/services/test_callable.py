from logicblocks.event.processing import CallableService, Service
from logicblocks.event.processing.services import as_callable_service


class TestServiceName:
    async def test_returns_class_name_with_service_suffix_removed(self):
        class MyCustomService(Service):
            async def execute(self):
                pass

        service = MyCustomService()

        assert service.name == "MyCustom"

    async def test_returns_full_class_name_when_no_service_suffix(self):
        class MyWorker(Service):
            async def execute(self):
                pass

        service = MyWorker()

        assert service.name == "MyWorker"


class TestCallableServiceExecute:
    async def test_executes_wrapped_callable(self):
        async def return_value():
            return 42

        service = CallableService(return_value)

        assert await service.execute() == 42


class TestCallableServiceName:
    async def test_returns_function_name(self):
        async def my_function():
            pass

        service = CallableService(my_function)

        assert service.name == "my_function"

    async def test_falls_back_to_class_name_for_callable_objects(self):
        class MyCallable:
            async def __call__(self):
                pass

        service = CallableService(MyCallable())

        assert service.name == "MyCallable"


class TestAsCallableService:
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
