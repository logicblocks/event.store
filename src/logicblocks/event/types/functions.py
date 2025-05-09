from abc import ABC, abstractmethod


class Converter[F, T](ABC):
    @abstractmethod
    def convert(self, item: F) -> T:
        raise NotImplementedError


class Applier[V](ABC):
    @abstractmethod
    def apply(self, target: V) -> V:
        raise NotImplementedError
