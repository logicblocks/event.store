from abc import ABC
from dataclasses import dataclass

from .utilities import Path


class Function(ABC):
    pass


@dataclass(frozen=True)
class Similarity(Function):
    path: Path
    value: str
