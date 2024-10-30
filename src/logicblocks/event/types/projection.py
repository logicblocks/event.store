import json

from collections.abc import Mapping
from typing import Any
from dataclasses import dataclass


@dataclass(frozen=True)
class Projection(object):
    state: Mapping[str, Any]
    position: int

    def __init__(
        self,
        *,
        state: Mapping[str, Any],
        position: int,
    ):
        object.__setattr__(self, "state", state)
        object.__setattr__(self, "position", position)

    def json(self):
        return json.dumps(
            {
                "state": self.state,
                "position": self.position,
            },
            sort_keys=True,
        )

    def __repr__(self):
        return (
            f"Projection("
            f"state={dict(self.state)}, "
            f"position={self.position})"
        )

    def __hash__(self):
        return hash(self.json())
