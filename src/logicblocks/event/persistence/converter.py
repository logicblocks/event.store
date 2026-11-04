from abc import ABC, abstractmethod
from collections.abc import Mapping
from typing import Any, Self

from logicblocks.event.types import Converter
from logicblocks.event.utils.klass import class_fullname


class TypeRegistryConverter[I, R](Converter[I, R], ABC):
    """Convert items by delegating to a converter registered for their type.

    Each item is converted by the converter registered for its exact class.
    Lookup does not consider base classes, so a converter registered for a
    class is not used for its subclasses. Registering a converter for a type
    that already has one replaces it.

    Subclasses implement `register`, typically by calling `_register`, to
    narrow the accepted item and converter types.

    Args:
        registry: An initial mapping from item type to the converter for
            that type. The mapping is copied, so later changes to it have no
            effect. Defaults to an empty registry.
    """

    def __init__(
        self,
        registry: Mapping[type[I], Converter[Any, R]] | None = None,
    ):
        self._registry: dict[type[I], Converter[Any, R]] = (
            dict(registry) if registry is not None else {}
        )

    def _register(
        self, item_type: type[I], converter: Converter[Any, R]
    ) -> Self:
        self._registry[item_type] = converter
        return self

    @abstractmethod
    def register(self, item_type: type[I], converter: Converter[I, R]) -> Self:
        """Register the converter to use for items of a type.

        Args:
            item_type: The exact class of the items to convert.
            converter: The converter to use for items of that class.

        Returns:
            this converter, to allow chaining.
        """
        raise NotImplementedError

    def convert(self, item: I) -> R:
        """Convert an item using the converter registered for its class.

        Args:
            item: The item to convert.

        Returns:
            the result of the registered converter.

        Raises:
            ValueError: If no converter is registered for the item's class.
        """
        if item.__class__ not in self._registry:
            raise ValueError(
                "No converter registered for type: "
                f"{class_fullname(item.__class__)}."
            )
        return self._registry[item.__class__].convert(item)
