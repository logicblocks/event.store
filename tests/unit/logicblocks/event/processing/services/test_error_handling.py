import asyncio
from datetime import timedelta

from logicblocks.event.processing.services import ErrorHandlingService


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

        task = asyncio.create_task(service.execute())

        await asyncio.sleep(timedelta(milliseconds=50).total_seconds())

        task.cancel()

        await asyncio.gather(task, return_exceptions=True)

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

        task = asyncio.create_task(service.execute())

        await asyncio.sleep(timedelta(milliseconds=50).total_seconds())

        task.cancel()

        await asyncio.gather(task, return_exceptions=True)

        assert invocations == 1
        assert handled_errors == 0
