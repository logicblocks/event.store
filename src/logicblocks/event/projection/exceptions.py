from logicblocks.event.types import StoredEvent


class MissingHandlerError(Exception):
    def __init__(self, event: StoredEvent):
        message = "Missing handler for event: " + event.name
        super().__init__(message)
        self.message = message
