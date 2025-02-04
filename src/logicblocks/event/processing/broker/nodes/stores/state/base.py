from abc import abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass(frozen=True)
class NodeState:
    node_id: str
    last_seen: datetime


class NodeStateStore:
    @abstractmethod
    async def add(self, node_id: str) -> None:
        raise NotImplementedError()

    @abstractmethod
    async def remove(self, node_id: str) -> None:
        raise NotImplementedError()

    @abstractmethod
    async def list(
        self, max_time_since_last_seen: timedelta | None = None
    ) -> Sequence[NodeState]:
        raise NotImplementedError()

    @abstractmethod
    async def heartbeat(self, node_id: str) -> None:
        raise NotImplementedError()

    @abstractmethod
    async def purge(
        self, max_time_since_last_seen: timedelta = timedelta(minutes=5)
    ) -> None:
        raise NotImplementedError()
