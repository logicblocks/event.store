import logging

import pytest
import structlog

structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(logging.CRITICAL)
)

for package in [
    "logicblocks.event.testcases.processing.locks.lock_manager",
    "logicblocks.event.testcases.processing.broker.strategies.distributed.subscriber.stores.state",
    "logicblocks.event.testcases.processing.broker.strategies.distributed.subscription.stores.state"
    "logicblocks.event.testcases.projection.store.adapters",
    "logicblocks.event.testcases.store.adapters",
]:
    pytest.register_assert_rewrite(package)
