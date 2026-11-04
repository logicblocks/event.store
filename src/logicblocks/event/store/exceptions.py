class UnmetWriteConditionError(Exception):
    """Raised when a write condition is not met at the time of a write.

    When raised, none of the events in the write have been stored.

    Args:
        message: A description of the unmet condition.
    """

    def __init__(self, message: str):
        super().__init__(message)
        self.message = message

    def __repr__(self):
        return f"UnmetWriteConditionError({self.message})"
