import json

from collections.abc import Mapping
from typing import Any
from dataclasses import dataclass


@dataclass(frozen=True)
class Projection(object):
    state: Mapping[str, Any]

    def __init__(
        self,
        *,
        state: Mapping[str, Any],
    ):
        object.__setattr__(self, "state", state)

    def json(self):
        return json.dumps(
            {
                "state": self.state,
            },
            sort_keys=True,
        )

    def __repr__(self):
        return f"Projection(" f"state={dict(self.state)})"

    def __hash__(self):
        return hash(self.json())
