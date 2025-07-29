import asyncio
from collections.abc import AsyncIterator, Sequence

from structlog.typing import FilteringBoundLogger

from logicblocks.event.processing.consumers.logger import default_logger
from logicblocks.event.processing.consumers.state import (
    EventConsumerStateStore,
)
from logicblocks.event.processing.consumers.types import (
    EventConsumer,
    EventIterator,
    EventIteratorProcessor,
)
from logicblocks.event.sources import EventSource, constraints
from logicblocks.event.types import (
    Event,
    EventSourceIdentifier,
    str_serialisation_fallback,
)


def log_event_name(event: str) -> str:
    return f"event.consumer.source.{event}"


class EventSourceIterator[E: Event](EventIterator[E]):
    def __init__(
        self,
        *,
        source_iterator: AsyncIterator[E],
        state_store: EventConsumerStateStore,
        logger: FilteringBoundLogger = default_logger,
    ):
        self._state_store = state_store
        self._logger = logger
        self._processed_events = 0
        self._iterator = self._get_iterator(source_iterator)

    async def _get_iterator(
        self, source_iterator: AsyncIterator[E]
    ) -> AsyncIterator[E]:
        async for event in source_iterator:
            await self._logger.adebug(
                log_event_name("consuming-event"),
                envelope=event.summarise(),
            )
            try:
                yield event
            except (asyncio.CancelledError, GeneratorExit):
                raise
            except BaseException:
                await self._logger.aexception(
                    log_event_name("processor-failed"),
                    envelope=event.summarise(),
                )
                raise

    def __anext__(self):
        return anext(self._iterator)

    @property
    def processed_events(self):
        return self._processed_events

    def acknowledge(self, events: E | Sequence[E]) -> None:
        for event in events if isinstance(events, Sequence) else [events]:
            self._state_store.record_processed(event)
            self._processed_events = self._processed_events + 1

    async def commit(self) -> None:
        await self._state_store.save()


class AutoCommitEventSourceIterator[E: Event](EventSourceIterator[E]):
    async def _get_iterator(
        self, source_iterator: AsyncIterator[E]
    ) -> AsyncIterator[E]:
        async for event in super()._get_iterator(source_iterator):
            yield event
            self.acknowledge(event)
            await self.commit()


class EventSourceIteratorConsumer[I: EventSourceIdentifier, E: Event](
    EventConsumer
):
    def __init__(
        self,
        *,
        source: EventSource[I, E],
        processor: EventIteratorProcessor[E],
        state_store: EventConsumerStateStore,
        logger: FilteringBoundLogger = default_logger,
    ):
        self._source = source
        self._processor = processor
        self._state_store = state_store
        self._logger = logger.bind(
            source=self._source.identifier.serialise(
                fallback=str_serialisation_fallback
            )
        )

    async def _run_consume(self, source_iterator: AsyncIterator[E]):
        event_iterator = EventSourceIterator(
            source_iterator=source_iterator,
            state_store=self._state_store,
            logger=self._logger,
        )
        await self._processor.process(event_iterator)
        return event_iterator

    async def consume_all(self) -> None:
        state = await self._state_store.load()
        last_sequence_number = (
            None if state is None else state.last_sequence_number
        )

        await self._logger.adebug(
            log_event_name("starting-consume"),
            last_sequence_number=last_sequence_number,
        )

        if last_sequence_number is None:
            source = self._source.iterate()
        else:
            source = self._source.iterate(
                constraints={
                    constraints.sequence_number_after(last_sequence_number)
                }
            )

        event_iterator = await self._run_consume(source)

        await self._logger.adebug(
            log_event_name("completed-consume"),
            consumed_count=event_iterator.processed_events,
        )


class SampleEventIteratorProcessor[E: Event](EventIteratorProcessor[E]):
    async def process(self, events: EventIterator[E]) -> None:
        async for event in events:
            await asyncio.sleep(0)
            events.acknowledge(event)
            await events.commit()
