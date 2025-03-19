from unittest.mock import AsyncMock

import pytest

from logicblocks.event.store.transactions import (
    event_store_transaction,
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
