import asyncio
from typing import assert_type

from logicblocks.event.processing import CallableService


class TestCallableServiceRepr:
    async def test_uses_qualname_of_named_function(self):
        async def my_function():
            return 42

        service = CallableService(my_function)

        assert repr(service) == (
            f"CallableService(callable={my_function.__qualname__})"
        )

    async def test_uses_qualname_of_lambda(self):
        fn = lambda: asyncio.sleep(0)  # noqa: E731

        service = CallableService(fn)

        assert repr(service) == (
            f"CallableService(callable={fn.__qualname__})"
        )

    async def test_uses_name_when_qualname_not_available(self):
        class MyCallable:
            __name__ = "my_callable"

            async def __call__(self):
                return 42

        service = CallableService(MyCallable())

        assert repr(service) == "CallableService(callable=my_callable)"

    async def test_uses_repr_for_callable_object_without_name(self):
        class MyCallable:
            async def __call__(self):
                return 42

            def __repr__(self):
                return "MyCallable()"

        service = CallableService(MyCallable())

        assert repr(service) == "CallableService(callable=MyCallable())"

    async def test_uses_default_repr_for_callable_object_without_name(self):
        class MyCallable:
            async def __call__(self):
                return 42

        my_callable = MyCallable()

        service = CallableService(my_callable)

        assert repr(service) == f"CallableService(callable={my_callable!r})"


class TestCallableServiceExecute:
    async def test_executes_wrapped_callable(self):
        async def return_value():
            return 42

        service = CallableService(return_value)

        assert await service.execute() == 42


class TestCallableServiceFromMaybeCallable:
    async def test_returns_service_unchanged_when_given_service(self):
        async def noop():
            pass

        service = CallableService(noop)

        assert CallableService.from_maybe_callable(service) is service

    async def test_wraps_callable_in_callable_service(self):
        async def noop():
            pass

        result = CallableService.from_maybe_callable(noop)

        assert isinstance(result, CallableService)

    async def test_wrapped_callable_executes_correctly(self):
        async def return_value():
            return 42

        service = CallableService.from_maybe_callable(return_value)

        assert await service.execute() == 42


class TestCallableServiceFromMaybeCallableTypes:
    async def test_callable_returns_callable_service(self):
        async def noop():
            return 1

        result = CallableService.from_maybe_callable(noop)

        assert_type(result, CallableService[int])
        assert isinstance(result, CallableService)

    async def test_service_returns_same_service_type(self):
        async def noop():
            pass

        service = CallableService(noop)

        result = CallableService.from_maybe_callable(service)

        assert_type(result, CallableService[None])
        assert result is service

    async def test_parameterised_callable_service_preserves_type(self):
        async def noop():
            return 1

        result = CallableService[int].from_maybe_callable(noop)

        assert_type(result, CallableService[int])
        assert isinstance(result, CallableService)

    async def test_parameterised_callable_service_uses_service_type(self):
        async def noop() -> str:
            return "hello"

        service = CallableService(noop)

        result = CallableService[int].from_maybe_callable(service)

        assert_type(service, CallableService[str])
        assert_type(result, CallableService[str])
        assert isinstance(result, CallableService)
