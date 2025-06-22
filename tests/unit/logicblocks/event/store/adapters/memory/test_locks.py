import asyncio
from unittest.mock import AsyncMock

import aiologic
import pytest

from logicblocks.event.store.adapters.memory import MultiLock


class TestMultiLock:
    async def test_acquires_all_locks_on_enter(self):
        lock1 = AsyncMock(spec=aiologic.Lock)
        lock2 = AsyncMock(spec=aiologic.Lock)
        lock3 = AsyncMock(spec=aiologic.Lock)

        multi_lock = MultiLock([lock1, lock2, lock3])

        async with multi_lock:
            pass

        lock1.async_acquire.assert_awaited_once()
        lock2.async_acquire.assert_awaited_once()
        lock3.async_acquire.assert_awaited_once()

    async def test_releases_all_locks_on_exit(self):
        lock1 = AsyncMock(spec=aiologic.Lock)
        lock2 = AsyncMock(spec=aiologic.Lock)
        lock3 = AsyncMock(spec=aiologic.Lock)

        multi_lock = MultiLock([lock1, lock2, lock3])

        async with multi_lock:
            pass

        lock1.async_release.assert_called_once()
        lock2.async_release.assert_called_once()
        lock3.async_release.assert_called_once()

    async def test_releases_all_locks_on_exception(self):
        lock1 = AsyncMock(spec=aiologic.Lock)
        lock2 = AsyncMock(spec=aiologic.Lock)
        lock3 = AsyncMock(spec=aiologic.Lock)

        multi_lock = MultiLock([lock1, lock2, lock3])

        with pytest.raises(ValueError):
            async with multi_lock:
                raise ValueError("Test exception")

        lock1.async_release.assert_called_once()
        lock2.async_release.assert_called_once()
        lock3.async_release.assert_called_once()

    async def test_releases_locks_in_reverse_order(self):
        lock1 = AsyncMock(spec=aiologic.Lock)
        lock2 = AsyncMock(spec=aiologic.Lock)
        lock3 = AsyncMock(spec=aiologic.Lock)

        multi_lock = MultiLock([lock1, lock2, lock3])

        all_calls = []

        def track_call(lock_name):
            def mock_release():
                all_calls.append(lock_name)
                return asyncio.sleep(0)

            return mock_release

        lock1.async_release.side_effect = track_call("lock1")
        lock2.async_release.side_effect = track_call("lock2")
        lock3.async_release.side_effect = track_call("lock3")

        async with multi_lock:
            pass

        assert all_calls == ["lock3", "lock2", "lock1"]
