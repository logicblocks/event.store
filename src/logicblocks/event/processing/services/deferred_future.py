from asyncio import Future


class DeferredFuture[T]:
    def __init__(self, name: str):
        self._name = name
        self._inner: Future[T] | None = None

    def _resolve(self, future: Future[T]) -> None:
        self._inner = future

    def result(self) -> T:
        if self._inner is None:
            raise RuntimeError(
                f"Service '{self._name}' has not been scheduled yet."
            )
        return self._inner.result()

    def exception(self) -> BaseException | None:
        if self._inner is None:
            raise RuntimeError(
                f"Service '{self._name}' has not been scheduled yet."
            )
        return self._inner.exception()

    def done(self) -> bool:
        if self._inner is None:
            return False
        return self._inner.done()

    def __await__(self):
        if self._inner is None:
            raise RuntimeError(
                f"Service '{self._name}' has not been scheduled yet."
            )
        return self._inner.__await__()
