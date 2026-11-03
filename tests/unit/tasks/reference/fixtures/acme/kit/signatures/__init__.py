from collections.abc import Callable, Mapping, Sequence
from typing import overload

type Pair[K, V] = tuple[K, V]

DEFAULTS: Mapping[str, int] = {"retries": 3}


def first[T](
    items: Sequence[T],
    /,
    default: T | None = None,
    *,
    strict: bool = False,
) -> T | None:
    return items[0] if items else default


async def fetch(*names: str, **options: int) -> dict[str, list[int]]:
    return {}


class Box[T: (int, str) = int]:
    size: int

    def __init__(self, item: T) -> None:
        self.item = item

    @property
    def contents(self) -> T:
        return self.item

    @classmethod
    def empty(cls) -> object:
        return object()

    @staticmethod
    def kind() -> str:
        return "box"

    @overload
    def get(self, index: int) -> T: ...

    @overload
    def get(self, index: slice) -> list[T]: ...

    def get(self, index: int | slice) -> T | list[T]:
        return self.item

    def _private(self) -> None:
        pass


class Label(str):
    pass


def register(callback: Callable[[str], None]) -> None:
    pass


__all__ = [
    "Box",
    "DEFAULTS",
    "Label",
    "Pair",
    "fetch",
    "first",
    "register",
]
