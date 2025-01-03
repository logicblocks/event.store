import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Projection(object):
    state: Mapping[str, Any]
    position: int

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
