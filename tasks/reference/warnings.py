import logging
from collections.abc import Iterator
from contextlib import contextmanager


class _CollectingHandler(logging.Handler):
    def __init__(self, messages: list[str]):
        super().__init__(level=logging.WARNING)
        self._messages = messages

    def emit(self, record: logging.LogRecord) -> None:
        self._messages.append(record.getMessage())


@contextmanager
def collected_griffe_warnings() -> Iterator[list[str]]:
    messages: list[str] = []
    logger = logging.getLogger("griffe")
    handler = _CollectingHandler(messages)
    propagate = logger.propagate
    logger.addHandler(handler)
    logger.propagate = False
    try:
        yield messages
    finally:
        logger.removeHandler(handler)
        logger.propagate = propagate
