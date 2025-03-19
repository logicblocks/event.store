from typing import assert_type
from unittest.mock import AsyncMock

import pytest

from logicblocks.event.store.transactions import (
    event_store_transaction,
    ignore_on_error,
    retry_on_error,
)


class TestEventStoreTransaction:
    async def test_event_store_transaction_calls_handler(self):
        class TestTransaction(event_store_transaction):
            async def wrap_handler(self, handler):
                return await handler()

        transaction = TestTransaction()
        handler = AsyncMock(return_value="result")

        wrapped_handler = transaction(handler)
        result = await wrapped_handler("arg1", arg2=1)

        handler.assert_called_once_with("arg1", arg2=1)
        handler.assert_awaited_once()

        assert result == "result"

    async def test_event_store_transaction_uses_same_return_type_as_handler(
        self,
    ):
        class TestTransaction(event_store_transaction):
            async def wrap_handler(self, handler):
                return await handler()

        transaction = TestTransaction()

        async def handler() -> int:
            return 1

        wrapped_handler = transaction(handler)
        result = await wrapped_handler()

        assert_type(result, int)

    async def test_event_store_transaction_adds_additional_return_type(self):
        class TestTransaction(event_store_transaction[str]):
            async def wrap_handler(self, handler):
                return await handler()

        transaction = TestTransaction()

        async def handler() -> int:
            return 1

        wrapped_handler = transaction(handler)
        result = await wrapped_handler()

        assert_type(result, int | str)

    async def test_event_store_transaction_adds_additional_return_type_union(
        self,
    ):
        class TestTransaction(event_store_transaction[str | float]):
            async def wrap_handler(self, handler):
                return await handler()

        transaction = TestTransaction()

        async def handler() -> int:
            return 1

        wrapped_handler = transaction(handler)
        result = await wrapped_handler()

        assert_type(result, int | str | float)


class TestRetryOnError:
    async def test_retry_on_error_retries_on_exception(self):
        class TestException(Exception):
            pass

        class RetryOnTestException(retry_on_error):
            exception = TestException

        handler = AsyncMock(side_effect=[TestException, "success"])
        transaction = RetryOnTestException(tries=2)

        wrapped_handler = transaction(handler)
        result = await wrapped_handler()

        assert handler.call_count == 2
        assert result == "success"

    async def test_retry_on_error_stops_after_max_retries(self):
        class TestException(Exception):
            pass

        class TestRetryOnError(retry_on_error):
            exception = TestException

        handler = AsyncMock(side_effect=TestException)
        transaction = TestRetryOnError(tries=3)

        wrapped_handler = transaction(handler)
        with pytest.raises(TestException):
            await wrapped_handler()

        assert handler.call_count == 3

    async def test_retry_on_error_ignores_other_exceptions(self):
        class TestException(Exception):
            pass

        class TestExceptionOther(Exception):
            pass

        class RetryOnTestException(retry_on_error):
            exception = TestException

        handler = AsyncMock(side_effect=TestExceptionOther)
        transaction = RetryOnTestException(tries=2)

        wrapped_handler = transaction(handler)
        with pytest.raises(TestExceptionOther):
            await wrapped_handler()

    async def test_retry_on_error_has_correct_types(self):
        class TestException(Exception):
            pass

        class RetryOnTestException(retry_on_error):
            exception = TestException

        async def handler() -> int:
            return 1

        transaction = RetryOnTestException(tries=2)

        wrapped_handler = transaction(handler)
        result = await wrapped_handler()

        assert_type(result, int)


class TestIgnoreOnError:
    async def test_ignore_on_error_ignores_exception(self):
        class TestException(Exception):
            pass

        class IgnoreOnTestException(ignore_on_error):
            exception = TestException

        handler = AsyncMock(side_effect=TestException)

        transaction = IgnoreOnTestException()

        wrapped_handler = transaction(handler)
        result = await wrapped_handler()

        handler.assert_called_once()
        handler.assert_awaited_once()

        assert result is None

    async def test_ignore_on_error_returns_result(self):
        class TestException(Exception):
            pass

        class IgnoreOnTestException(ignore_on_error):
            exception = TestException

        handler = AsyncMock(return_value="success")
        transaction = IgnoreOnTestException()

        wrapped_handler = transaction(handler)
        result = await wrapped_handler()

        handler.assert_called_once()
        assert result == "success"

    async def test_ignore_on_error_raises_other_exception(self):
        class TestException(Exception):
            pass

        class TestExceptionOther(Exception):
            pass

        class IgnoreOnTestException(ignore_on_error):
            exception = TestException

        handler = AsyncMock(side_effect=TestExceptionOther)

        transaction = IgnoreOnTestException()

        wrapped_handler = transaction(handler)

        with pytest.raises(TestExceptionOther):
            await wrapped_handler()

    async def test_ignore_on_error_has_correct_types(self):
        class TestException(Exception):
            pass

        class IgnoreOnTestException(ignore_on_error):
            exception = TestException

        async def handler() -> int:
            return 1

        transaction = IgnoreOnTestException()

        wrapped_handler = transaction(handler)
        result = await wrapped_handler()

        assert_type(result, int | None)
