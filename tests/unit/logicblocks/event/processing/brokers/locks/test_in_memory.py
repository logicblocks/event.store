import asyncio

from logicblocks.event.processing.broker import InMemoryLockManager


class TestInMemoryLockManager:
    async def test_obtain_lock_success(self):
        lock_manager = InMemoryLockManager()
        lock_id = "test_lock"

        async with lock_manager.with_lock(lock_id) as locked:
            assert locked is True
        async with lock_manager.with_lock(lock_id) as locked:
            assert locked is True

    async def test_obtain_lock_already_locked(self):
        lock_manager = InMemoryLockManager()
        lock_id = "test_lock"

        async with lock_manager.with_lock(lock_id) as locked:
            assert locked is True
            async with lock_manager.with_lock(lock_id) as locked_again:
                assert locked_again is False

    async def test_obtain_two_locks(self):
        lock_manager = InMemoryLockManager()
        lock_id_1 = "test_lock"
        lock_id_2 = "test_lock_2"

        async with lock_manager.with_lock(lock_id_1) as locked:
            assert locked is True
            async with lock_manager.with_lock(lock_id_2) as locked_again:
                assert locked_again is True

    async def test_obtain_two_locks_concurrently(self):
        lock_manager = InMemoryLockManager()
        lock_id = "test_lock"

        b = asyncio.Barrier(2)

        async def get_lock(id):
            async with lock_manager.with_lock(id) as locked:
                await b.wait()
                return locked

        results = await asyncio.gather(
            get_lock(lock_id),
            get_lock(lock_id),
        )

        assert len([r for r in results if r is True]) == 1
