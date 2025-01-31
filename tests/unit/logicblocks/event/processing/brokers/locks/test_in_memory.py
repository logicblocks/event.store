from logicblocks.event.processing.broker import (
    InMemoryLockManager,
    LockManager,
)
from logicblocks.event.testcases.processing.broker.locks.lock_manager import (
    BaseTestLockManager,
)


class TestInMemoryLockManager(BaseTestLockManager):
    def construct_lock_manager(self) -> LockManager:
        return InMemoryLockManager()
