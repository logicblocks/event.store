import asyncio
from contextlib import asynccontextmanager
from datetime import timedelta

import pytest


@asynccontextmanager
async def task_shutdown(task: asyncio.Task):
    try:
        yield
    finally:
        if not task.cancelled():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def assert_task_eventually_done(task: asyncio.Task):
    timeout = timedelta(milliseconds=500)
    try:
        await asyncio.wait_for(task, timeout=timeout.total_seconds())
    except asyncio.TimeoutError:
        pytest.fail(
            "Expected task to eventually be done but timed out waiting."
        )
    except asyncio.CancelledError:
        pytest.fail("Expected task to eventually be done but was cancelled.")
