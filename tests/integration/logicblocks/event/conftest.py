import logging
import sys
from collections.abc import Mapping
from typing import Any, cast

import pytest
import structlog
from structlog.stdlib import BoundLogger


class CompatibleBoundLogger(BoundLogger):
    def is_enabled_for(self, level: int) -> bool:
        return self.isEnabledFor(level)

    def get_effective_level(self) -> int:
        return self.getEffectiveLevel()

    def bind(self, **new_values: Any) -> "CompatibleBoundLogger":
        return cast(CompatibleBoundLogger, super().bind(**new_values))

    def unbind(self, *keys: str) -> "CompatibleBoundLogger":
        return cast(CompatibleBoundLogger, super().unbind(*keys))

    def try_unbind(self, *keys: str) -> "CompatibleBoundLogger":
        return cast(CompatibleBoundLogger, super().try_unbind(*keys))

    def new(self, **new_values: Any) -> "CompatibleBoundLogger":
        return cast(CompatibleBoundLogger, super().new(**new_values))

    async def awarn(self, event: str, *args: Any, **kw: Any) -> None:
        await self.awarning(event, *args, **kw)


timestamper = structlog.processors.TimeStamper(fmt="iso")

shared_processors = [
    timestamper,
    structlog.stdlib.add_log_level,
    structlog.stdlib.add_logger_name,
    structlog.contextvars.merge_contextvars,
    structlog.stdlib.ExtraAdder(),
]

structlog.configure(
    processors=shared_processors
    + [
        structlog.processors.StackInfoRenderer(),
        structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
    ],
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=CompatibleBoundLogger,
    cache_logger_on_first_use=True,
)

render_processors = [
    structlog.dev.ConsoleRenderer(
        exception_formatter=structlog.dev.RichTracebackFormatter(
            width=180,
            word_wrap=True,
        ),
        colors=True,
    )
]

standard_output_processor = structlog.stdlib.ProcessorFormatter(
    foreign_pre_chain=shared_processors,
    processors=[structlog.stdlib.ProcessorFormatter.remove_processors_meta]
    + render_processors,
)

root_logger = logging.getLogger()
for handler in root_logger.handlers:
    root_logger.removeHandler(handler)


def noise_filter(event: logging.LogRecord) -> bool:
    if event.name in ["asyncio", "faker.factory"]:
        return False
    if cast(Mapping[str, Any], event.msg)["event"] in [
        "event.processing.broker.subscriber-manager.subscriber-healthy",
        "event.processing.broker.subscriber-manager.sending-heartbeats",
        "event.processing.broker.subscriber-manager.purging-subscribers",
        "event.processing.broker.observer.synchronisation.starting",
        "event.processing.broker.observer.synchronisation.complete",
        "event.processing.broker.coordinator.distribution.starting",
        "event.processing.broker.coordinator.distribution.complete",
        "event.consumer.source.starting-consume",
        "event.consumer.source.consuming-event",
        "event.consumer.source.completed-consume",
        "event.consumer.subscription.starting-consume",
        "event.consumer.subscription.consuming-source",
        "event.consumer.subscription.completed-consume",
        "event.category.iterating",
        "event.stream.publishing",
        "event.stream.reading-latest",
    ]:
        return False
    return True


standard_output_handler = logging.StreamHandler(stream=sys.stdout)
standard_output_handler.setFormatter(standard_output_processor)
standard_output_handler.addFilter(noise_filter)

root_logger.addHandler(standard_output_handler)
root_logger.setLevel("CRITICAL")

faker_logger = logging.getLogger("faker.factory")
faker_logger.setLevel("CRITICAL")

faker_logger = logging.getLogger("asyncio")
faker_logger.setLevel("CRITICAL")

for logger_name in logging.root.manager.loggerDict:
    logger = logging.getLogger(logger_name)
    for handler in logger.handlers:
        logger.removeHandler(handler)

for package in [
    "logicblocks.event.testcases.processing.locks.lock_manager",
    "logicblocks.event.testcases.processing.broker.strategies.distributed.subscriber.stores.state",
    "logicblocks.event.testcases.processing.broker.strategies.distributed.subscription.stores.state"
    "logicblocks.event.testcases.projection.store.adapters",
    "logicblocks.event.testcases.store.adapters",
]:
    pytest.register_assert_rewrite(package)
