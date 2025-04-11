from logicblocks.event.processing.broker import (
    InMemoryLockManager,
)
from logicblocks.event.testcases import (
    LockManagerCases,
)


class TestInMemoryLockManager(LockManagerCases):
    def construct_lock_manager(self) -> InMemoryLockManager:
        return InMemoryLockManager()

    async def test_does_not_leak_when_try_lock_on_many_different_locks(self):
        lock_manager = self.construct_lock_manager()

        for name in [str(index) for index in range(100)]:
            async with lock_manager.try_lock(name):
                pass

        assert len(lock_manager._locks) == 0

    async def test_does_not_leak_when_wait_for_lock_with_many_different_locks(
        self,
    ):
        lock_manager = self.construct_lock_manager()

        for name in [str(index) for index in range(100)]:
            async with lock_manager.wait_for_lock(name):
                pass

        assert len(lock_manager._locks) == 0
