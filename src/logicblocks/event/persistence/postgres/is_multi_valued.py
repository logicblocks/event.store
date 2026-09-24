from collections.abc import Iterable, Mapping
from typing import Any, TypeGuard


def is_multi_valued(value: Any) -> TypeGuard[Iterable[Any]]:
    return isinstance(value, Iterable) and not isinstance(value, (str, bytes, Mapping))
