class UnmetWriteConditionError(Exception):
    def __init__(self, message: str):
        super().__init__(message)
        self.message = message

    def __repr__(self):
        return f"UnmetWriteConditionError({self.message})"
