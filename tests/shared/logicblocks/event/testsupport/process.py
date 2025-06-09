import asyncio
from datetime import timedelta

import pytest

from logicblocks.event.processing import (
    Process,
    ProcessStatus,
)


async def status_not_equal_to(process: Process, status: ProcessStatus):
    while True:
        if process.status != status:
            return
        await asyncio.sleep(0)


async def status_equal_to(process: Process, status: ProcessStatus):
    while True:
        if process.status == status:
            return
        await asyncio.sleep(0)


async def status_eventually_equal_to(process: Process, status: ProcessStatus):
    timeout = timedelta(milliseconds=500)
    await asyncio.wait_for(
        status_equal_to(process, status),
        timeout=timeout.total_seconds(),
    )


async def assert_status_eventually(process: Process, status: ProcessStatus):
    try:
        await status_eventually_equal_to(process, status)
    except asyncio.TimeoutError:
        pytest.fail(
            f"Expected status to eventually equal '{status}' "
            f"but timed out waiting."
        )
