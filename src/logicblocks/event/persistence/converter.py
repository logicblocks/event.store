from abc import ABC, abstractmethod
from collections.abc import Mapping
from typing import Any, Self

from logicblocks.event.types import Converter
from logicblocks.event.utils.klass import class_fullname


class TypeRegistryConverter[I, R](Converter[I, R], ABC):
    def __init__(
        self,
        registry: Mapping[type[I], Converter[Any, R]] | None = None,
    ):
        self._registry = dict(registry) if registry is not None else {}

    def _register(
        self, item_type: type[I], converter: Converter[Any, R]
    ) -> Self:
        self._registry[item_type] = converter
        return self

    @abstractmethod
    def register(self, item_type: type[I], converter: Converter[I, R]) -> Self:
        raise NotImplementedError

    def convert(self, item: I) -> R:
        if item.__class__ not in self._registry:
            raise ValueError(
                "No converter registered for type: "
                f"{class_fullname(item.__class__)}."
            )
        return self._registry[item.__class__].convert(item)
