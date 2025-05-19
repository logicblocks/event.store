from abc import ABC
from dataclasses import dataclass

from .utilities import Path


@dataclass(frozen=True)
class Function(ABC):
    alias: str


@dataclass(frozen=True, kw_only=True)
class Similarity(Function):
    left: Path
    right: str
