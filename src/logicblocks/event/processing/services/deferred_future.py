from asyncio import Future


class DeferredFuture[T]:
    """A handle to the future result of a service not yet scheduled.

    Supports a subset of the `asyncio.Future` interface, delegating to the
    underlying future once the service has been scheduled. It can be
    awaited.

    Args:
        name: The name of the service, used in error messages.
    """

    def __init__(self, name: str):
        self._name = name
        self._inner: Future[T] | None = None

    def _resolve(self, future: Future[T]) -> None:
        self._inner = future

    def result(self) -> T:
        """Return the result of the service.

        Raises:
            BaseException: Any exception raised by the service.
            asyncio.CancelledError: If the service was cancelled.
            RuntimeError: If the service has not been scheduled yet.
            asyncio.InvalidStateError: If the service has not finished.
        """
        if self._inner is None:
            raise RuntimeError(
                f"Service '{self._name}' has not been scheduled yet."
            )
        return self._inner.result()

    def exception(self) -> BaseException | None:
        """Return the exception raised by the service, if any.

        Raises:
            RuntimeError: If the service has not been scheduled yet.
            asyncio.InvalidStateError: If the service has not finished.
        """
        if self._inner is None:
            raise RuntimeError(
                f"Service '{self._name}' has not been scheduled yet."
            )
        return self._inner.exception()

    def done(self) -> bool:
        """Return whether the service has been scheduled and finished."""
        if self._inner is None:
            return False
        return self._inner.done()

    def __await__(self):
        if self._inner is None:
            raise RuntimeError(
                f"Service '{self._name}' has not been scheduled yet."
            )
        return self._inner.__await__()
