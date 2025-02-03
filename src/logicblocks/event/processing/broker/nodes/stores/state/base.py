from abc import abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class NodeState:
    node_id: str
    last_seen: datetime


class NodeStateStore:
    @abstractmethod
    async def list(self) -> Sequence[NodeState]:
        raise NotImplementedError()

    @abstractmethod
    async def heartbeat(self) -> None:
        raise NotImplementedError()

    @abstractmethod
    async def stop(self) -> None:
        raise NotImplementedError()
