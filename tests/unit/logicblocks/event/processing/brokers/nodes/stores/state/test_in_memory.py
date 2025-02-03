from typing import Sequence

from logicblocks.event.processing.broker import NodeState, NodeStateStore
from logicblocks.event.processing.broker.nodes.stores.state.in_memory import (
    InMemoryNodeStateStore,
)
from logicblocks.event.testcases import NodeStateStoreCases
from logicblocks.event.utils.clock import Clock


class TestInMemoryNodeStateStore(NodeStateStoreCases):
    def construct_store(self, clock: Clock) -> NodeStateStore:
        return InMemoryNodeStateStore(clock=clock)

    async def read_nodes(self, store: NodeStateStore) -> Sequence[NodeState]:
        return await store.list()
