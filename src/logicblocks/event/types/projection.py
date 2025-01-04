import json
from dataclasses import dataclass


@dataclass(frozen=True)
class Projection[T]:
    state: T
    version: int

    def json(self):
        return json.dumps(
            {
                "state": self.state,
                "version": self.version,
            },
            sort_keys=True,
        )

    def __repr__(self):
        return f"Projection(state={self.state},version={self.version})"

    def __hash__(self):
        return hash(self.json())
