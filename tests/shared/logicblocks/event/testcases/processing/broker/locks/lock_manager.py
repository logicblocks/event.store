import asyncio
from abc import abstractmethod
from datetime import timedelta

from logicblocks.event.processing.broker import (
    Lock,
    LockManager,
)


class LockManagerCases:
    @abstractmethod
    def construct_lock_manager(self) -> LockManager:
        raise NotImplementedError()

    async def test_try_to_obtain_lock_and_succeed(self):
        lock_manager = self.construct_lock_manager()
        lock_name = "test-lock"

        async with lock_manager.try_lock(lock_name) as lock:
            assert lock == Lock(name=lock_name, locked=True)

        async with lock_manager.try_lock(lock_name) as lock:
            assert lock == Lock(name=lock_name, locked=True)

    async def test_try_to_obtain_lock_but_already_locked(self):
        lock_manager = self.construct_lock_manager()
        lock_name = "test-lock"

        async with lock_manager.try_lock(lock_name) as outer_lock:
            assert outer_lock == Lock(name=lock_name, locked=True)
            async with lock_manager.try_lock(lock_name) as inner_lock:
                assert inner_lock == Lock(name=lock_name, locked=False)

    async def test_try_to_obtain_two_locks(self):
        lock_manager = self.construct_lock_manager()
        lock_name_1 = "test-lock-1"
        lock_name_2 = "test-lock-2"

        async with lock_manager.try_lock(lock_name_1) as lock_1:
            assert lock_1 == Lock(name=lock_name_1, locked=True)
            async with lock_manager.try_lock(lock_name_2) as lock_2:
                assert lock_2 == Lock(name=lock_name_2, locked=True)

    async def test_try_to_obtain_lock_concurrently(self):
        lock_manager = self.construct_lock_manager()
        lock_name = "test-lock"

        barrier = asyncio.Barrier(2)

        async def get_lock(id) -> Lock:
            async with lock_manager.try_lock(id) as lock:
                await barrier.wait()
                return lock

        locks = await asyncio.gather(
            get_lock(lock_name),
            get_lock(lock_name),
        )

        assert set(locks) == {
            Lock(name=lock_name, locked=True),
            Lock(name=lock_name, locked=False),
        }

    async def test_wait_to_obtain_lock_and_succeed(self):
        lock_manager = self.construct_lock_manager()
        lock_name = "test-lock"

        async with lock_manager.wait_for_lock(lock_name) as lock:
            assert lock.name == lock_name
            assert lock.locked is True
            assert lock.timed_out is False
        async with lock_manager.wait_for_lock(lock_name) as lock:
            assert lock.name == lock_name
            assert lock.locked is True
            assert lock.timed_out is False

    async def test_wait_to_obtain_two_locks(self):
        lock_manager = self.construct_lock_manager()
        lock_name_1 = "test-lock-1"
        lock_name_2 = "test-lock-2"

        async with lock_manager.wait_for_lock(lock_name_1) as lock_1:
            assert lock_1.name == lock_name_1
            assert lock_1.locked is True
            assert lock_1.timed_out is False

            async with lock_manager.wait_for_lock(lock_name_2) as lock_2:
                assert lock_2.name == lock_name_2
                assert lock_2.locked is True
                assert lock_2.timed_out is False

    async def test_wait_to_obtain_lock_and_timeout(self):
        lock_manager = self.construct_lock_manager()
        lock_name = "test-lock"
        timeout = timedelta(milliseconds=20)

        async with lock_manager.wait_for_lock(lock_name) as outer_lock:
            assert outer_lock.name == lock_name
            assert outer_lock.locked is True
            assert outer_lock.timed_out is False

            async with lock_manager.wait_for_lock(
                lock_name, timeout=timeout
            ) as inner_lock:
                assert inner_lock.name == lock_name
                assert inner_lock.locked is False
                assert inner_lock.timed_out is True
                assert inner_lock.wait_time >= timeout

    async def test_wait_to_obtain_lock_and_wait_for_release(self):
        lock_manager = self.construct_lock_manager()
        lock_name = "test-lock"

        hold_1 = asyncio.Event()
        hold_2 = asyncio.Event()

        async def get_lock(id, event) -> Lock:
            async with lock_manager.wait_for_lock(id) as lock:
                while not event.is_set():
                    await asyncio.sleep(0)
                return lock

        lock_1_task = asyncio.create_task(get_lock(lock_name, hold_1))
        lock_2_task = asyncio.create_task(get_lock(lock_name, hold_2))

        await asyncio.sleep(timedelta(milliseconds=100).total_seconds())

        hold_1.set()
        hold_2.set()

        locks = await asyncio.gather(lock_1_task, lock_2_task)

        for lock in locks:
            assert lock.name == lock_name
            assert lock.locked is True
            assert lock.timed_out is False

        wait_times = [lock.wait_time for lock in locks]

        assert max(wait_times) >= timedelta(milliseconds=100)
        assert min(wait_times) <= timedelta(milliseconds=100)
