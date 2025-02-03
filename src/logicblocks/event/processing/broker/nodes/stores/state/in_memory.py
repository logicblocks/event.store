from collections.abc import Sequence

from logicblocks.event.utils.clock import Clock, SystemClock

from .base import NodeState, NodeStateStore


class InMemoryNodeStateStore(NodeStateStore):
    def __init__(
        self,
        node_id: str,
        clock: Clock = SystemClock(),
    ):
        self.node_id = node_id
        self.clock = clock

    async def list(self) -> Sequence[NodeState]:
        raise NotImplementedError()

    async def heartbeat(self) -> None:
        raise NotImplementedError()

    async def stop(self) -> None:
        raise NotImplementedError()
