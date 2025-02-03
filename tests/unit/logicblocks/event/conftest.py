import pytest

for package in [
    "logicblocks.event.testcases.processing.broker.locks.lock_manager",
    "logicblocks.event.testcases.processing.broker.nodes.stores.state",
    "logicblocks.event.testcases.processing.broker.subscriber.stores.state",
    "logicblocks.event.testcases.processing.broker.subscription.stores.state"
    "logicblocks.event.testcases.projection.store.adapters",
    "logicblocks.event.testcases.store.adapters",
]:
    pytest.register_assert_rewrite(package)
