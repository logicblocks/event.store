from abc import ABC, abstractmethod


class Converter[F, T](ABC):
    """Abstract base class for converting values of one type to another.

    Type Parameters:
        F: The type converted from.
        T: The type converted to.
    """

    @abstractmethod
    def convert(self, item: F) -> T:
        """Convert an item.

        Args:
            item: The item to convert.

        Returns:
            the converted item.
        """
        raise NotImplementedError


class Applier[V](ABC):
    """Abstract base class for transforming a value into a new value.

    Type Parameters:
        V: The type of value transformed.
    """

    @abstractmethod
    def apply(self, target: V) -> V:
        """Apply the transformation to a target.

        Args:
            target: The value to transform.

        Returns:
            the transformed value.
        """
        raise NotImplementedError
