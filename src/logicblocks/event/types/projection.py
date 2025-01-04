import json
from dataclasses import dataclass


@dataclass(frozen=True)
class Projection[T]:
    state: T
    version: int

    def __init__(self, *, state: T, version: int):
        object.__setattr__(self, "state", state)
        object.__setattr__(self, "version", version)

    def json(self):
        return json.dumps(
            {
                "state": self.state,
                "version": self.version,
            },
            sort_keys=True,
        )

    def __repr__(self):
        return (
            f"Projection(" f"state={self.state}, " f"version={self.version})"
        )

    def __hash__(self):
        return hash(self.json())
