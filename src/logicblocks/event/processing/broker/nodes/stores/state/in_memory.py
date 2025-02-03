from collections.abc import Sequence
from datetime import UTC, timedelta

from logicblocks.event.utils.clock import Clock, SystemClock

from .base import NodeState, NodeStateStore


class InMemoryNodeStateStore(NodeStateStore):
    def __init__(
        self,
        clock: Clock = SystemClock(),
    ):
        self.clock = clock
        self.nodes: dict[str, NodeState] = {}

    async def add(self, node_id: str) -> None:
        existing = self.nodes.get(node_id, None)
        if existing is not None:
            await self.heartbeat(node_id)

        self.nodes[node_id] = NodeState(
            node_id=node_id, last_seen=self.clock.now(UTC)
        )

    async def remove(self, node_id: str) -> None:
        existing = self.nodes.get(node_id, None)
        if existing is None:
            raise ValueError("Can't remove missing node.")

        self.nodes.pop(node_id)

    async def list(
        self, max_time_since_last_seen: timedelta | None = None
    ) -> Sequence[NodeState]:
        states = [state for state in self.nodes.values()]
        if max_time_since_last_seen is not None:
            states = [
                state
                for state in states
                if (
                    state.last_seen
                    > self.clock.now(UTC) - max_time_since_last_seen
                )
            ]
        return states

    async def heartbeat(self, node_id: str) -> None:
        existing = self.nodes.get(node_id, None)
        if existing is None:
            raise ValueError("Can't heartbeat missing node.")

        self.nodes[node_id] = NodeState(node_id, self.clock.now(UTC))

    async def purge(
        self, max_time_since_last_seen: timedelta = timedelta(seconds=300)
    ) -> None:
        now = self.clock.now(UTC)
        cutoff = now - max_time_since_last_seen
        for _, state in self.nodes.copy().items():
            if state.last_seen <= cutoff:
                self.nodes.pop(state.node_id)
