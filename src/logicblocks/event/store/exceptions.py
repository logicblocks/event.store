from typing import Any

from logicblocks.event.store.conditions import WriteCondition


class UnmetWriteConditionError(Exception):
    def __init__(self, condition: WriteCondition[Any]):
        message = "Unmet write condition: {}".format(condition)
        super().__init__(message)
        self.message = message
