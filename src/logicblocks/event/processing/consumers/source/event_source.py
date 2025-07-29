from structlog.typing import FilteringBoundLogger

from logicblocks.event.processing.consumers.logger import default_logger
from logicblocks.event.processing.consumers.source.event_source_iterator import (
    EventSourceIteratorConsumer,
)
from logicblocks.event.processing.consumers.state import (
    EventConsumerStateStore,
)
from logicblocks.event.processing.consumers.types import (
    EventConsumer,
    EventIterator,
    EventIteratorProcessor,
    EventProcessor,
)
from logicblocks.event.sources import EventSource
from logicblocks.event.types import (
    Event,
    EventSourceIdentifier,
)


def log_event_name(event: str) -> str:
    return f"event.consumer.source.{event}"


class SimpleEventSourceIteratorProcessor[E: Event](EventIteratorProcessor[E]):
    def __init__(self, processor: EventProcessor[E]):
        self._processor = processor

    async def process(self, events: EventIterator[E]) -> None:
        async for event in events:
            await self._processor.process_event(event)
            events.acknowledge(event)
            await events.commit()


class EventSourceConsumer[I: EventSourceIdentifier, E: Event](EventConsumer):
    def __init__(
        self,
        *,
        source: EventSource[I, E],
        processor: EventProcessor[E],
        state_store: EventConsumerStateStore,
        logger: FilteringBoundLogger = default_logger,
    ):
        self._source = source
        self._state_store = state_store
        self._logger = logger
        self._processor = SimpleEventSourceIteratorProcessor(processor)
        self._iterator_consumer = EventSourceIteratorConsumer(
            source=source,
            processor=self._processor,
            state_store=state_store,
            logger=logger,
        )

    async def consume_all(self) -> None:
        await self._iterator_consumer.consume_all()
        await self._state_store.save()
