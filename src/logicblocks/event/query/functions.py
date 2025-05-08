from dataclasses import dataclass

from .utilities import Path


class Function:
    pass


@dataclass(frozen=True)
class Similarity(Function):
    path: Path
    value: str
