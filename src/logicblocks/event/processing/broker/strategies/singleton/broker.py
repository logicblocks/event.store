import asyncio
from datetime import timedelta
from types import NoneType

from structlog.types import FilteringBoundLogger

from logicblocks.event.sources.factory.base import EventSourceFactory
from logicblocks.event.types import Event

from ....process import ProcessStatus
from ....services import (
    ErrorHandler,
    ErrorHandlingServiceMixin,
    RetryErrorHandler,
)
from ...base import EventBroker
from ...logger import default_logger
from ...subscribers import EventSubscriberStore
from ...types import EventSubscriber


def log_event_name(event: str) -> str:
    return f"event.processing.broker.{event}"


class SingletonEventBroker[E: Event](
    EventBroker[E], ErrorHandlingServiceMixin[NoneType]
):
    def __init__(
        self,
        node_id: str,
        event_subscriber_store: EventSubscriberStore[E],
        event_source_factory: EventSourceFactory[E],
        error_handler: ErrorHandler[NoneType] = RetryErrorHandler(),
        logger: FilteringBoundLogger = default_logger,
        distribution_interval: timedelta = timedelta(seconds=30),
    ):
        super().__init__(error_handler)
        self._node_id = node_id
        self._event_subscriber_store = event_subscriber_store
        self._event_source_factory = event_source_factory
        self._logger = logger.bind(node=node_id)
        self._distribution_interval = distribution_interval
        self._status = ProcessStatus.INITIALISED

    @property
    def status(self) -> ProcessStatus:
        return self._status

    async def register(self, subscriber: EventSubscriber[E]) -> None:
        await self._event_subscriber_store.add(subscriber)

    async def _do_execute(self) -> None:
        distribution_interval_seconds = (
            self._distribution_interval.total_seconds()
        )

        await self._logger.ainfo(
            log_event_name("starting"),
            distribution_interval_seconds=distribution_interval_seconds,
        )
        self._status = ProcessStatus.STARTING

        try:
            await self._logger.ainfo(log_event_name("running"))
            self._status = ProcessStatus.RUNNING
            while True:
                subscribers = await self._event_subscriber_store.list()
                for subscriber in subscribers:
                    for source in subscriber.subscription_requests:
                        await subscriber.accept(
                            self._event_source_factory.construct(source)
                        )

                await asyncio.sleep(distribution_interval_seconds)

        except asyncio.CancelledError:
            self._status = ProcessStatus.STOPPED
            await self._logger.ainfo(log_event_name("stopped"))
            raise
        except BaseException:
            self._status = ProcessStatus.ERRORED
            await self._logger.aexception(log_event_name("failed"))
            raise
