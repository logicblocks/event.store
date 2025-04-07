import logging

import pytest
import structlog

cr = structlog.dev.ConsoleRenderer(
    exception_formatter=structlog.dev.RichTracebackFormatter(
        width=180,
        word_wrap=True,
    )
)

structlog.configure(
    processors=structlog.get_config()["processors"][:-1] + [cr],
    wrapper_class=structlog.make_filtering_bound_logger(logging.DEBUG),
)

for package in [
    "logicblocks.event.testcases.processing.broker.locks.lock_manager",
    "logicblocks.event.testcases.processing.broker.nodes.stores.state",
    "logicblocks.event.testcases.processing.broker.subscriber.stores.state",
    "logicblocks.event.testcases.processing.broker.subscription.stores.state"
    "logicblocks.event.testcases.projection.store.adapters",
    "logicblocks.event.testcases.store.adapters",
]:
    pytest.register_assert_rewrite(package)
