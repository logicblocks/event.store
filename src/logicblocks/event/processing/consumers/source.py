import asyncio
from collections.abc import AsyncIterator, Sequence

from structlog.typing import FilteringBoundLogger

from logicblocks.event.sources import EventSource, constraints
from logicblocks.event.types import (
    Event,
    EventSourceIdentifier,
    str_serialisation_fallback,
)

from ...sources.iterator import EventIteratorManagerI
from .logger import default_logger
from .state import EventConsumerStateStore
from .types import EventConsumer, EventIteratorProcessor, EventProcessor


def log_event_name(event: str) -> str:
    return f"event.consumer.source.{event}"


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
        self._processor = processor
        self._state_store = state_store
        self._logger = logger

    async def consume_all(self) -> None:
        state = await self._state_store.load()
        last_sequence_number = (
            None if state is None else state.last_sequence_number
        )

        await self._logger.adebug(
            log_event_name("starting-consume"),
            source=self._source.identifier.serialise(
                fallback=str_serialisation_fallback
            ),
            last_sequence_number=last_sequence_number,
        )

        source = self._source
        if last_sequence_number is not None:
            source = self._source.iterate(
                constraints={
                    constraints.sequence_number_after(last_sequence_number)
                }
            )

        consumed_count = 0
        async for event in source:
            await self._logger.adebug(
                log_event_name("consuming-event"),
                source=self._source.identifier.serialise(
                    fallback=str_serialisation_fallback
                ),
                envelope=event.summarise(),
            )
            try:
                await self._processor.process_event(event)
                await self._state_store.record_processed(event)
                consumed_count += 1
            except (asyncio.CancelledError, GeneratorExit):
                raise
            except BaseException:
                await self._logger.aexception(
                    log_event_name("processor-failed"),
                    source=self._source.identifier.serialise(
                        fallback=str_serialisation_fallback
                    ),
                    envelope=event.summarise(),
                )
                raise

        await self._state_store.save()
        await self._logger.adebug(
            log_event_name("completed-consume"),
            source=self._source.identifier.serialise(
                fallback=str_serialisation_fallback
            ),
            consumed_count=consumed_count,
        )


class EventIteratorManager[E: Event](EventIteratorManagerI[E]):
    def __init__(
        self,
        *,
        source_iterator: AsyncIterator[E],
        state_store: EventConsumerStateStore,
        logger: FilteringBoundLogger = default_logger,
    ):
        self._source_iterator = source_iterator
        self._state_store = state_store
        self._logger = logger
        self._event_count = 0

    async def __aiter__(self):
        async for event in self._source_iterator:
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

    @property
    def processed_events(self):
        return self._event_count

    async def acknowledge(self, events: E | Sequence[E]) -> None:
        pass

    async def commit(self) -> None:
        pass


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
        self._logger = logger

    async def _run_consume(self, source_iterator: AsyncIterator[E]):
        logger = self._logger.bind(
            source=self._source.identifier.serialise(
                fallback=str_serialisation_fallback
            )
        )
        event_iterator = EventIteratorManager(
            source_iterator=source_iterator,
            state_store=self._state_store,
            logger=logger,
        )
        await self._processor.process(event_iterator)
        return event_iterator.processed_events

    async def consume_all(self) -> None:
        state = await self._state_store.load()
        last_sequence_number = (
            None if state is None else state.last_sequence_number
        )

        await self._logger.adebug(
            log_event_name("starting-consume"),
            source=self._source.identifier.serialise(
                fallback=str_serialisation_fallback
            ),
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

        consumed_count = await self._run_consume(source)

        await self._logger.adebug(
            log_event_name("completed-consume"),
            source=self._source.identifier.serialise(
                fallback=str_serialisation_fallback
            ),
            consumed_count=consumed_count,
        )


class SampleEventIteratorProcessor[E: Event](EventIteratorProcessor[E]):
    async def process(self, events: EventIteratorManagerI[E]) -> None:
        async for event in events:
            await asyncio.sleep(0)
            await events.acknowledge(event)
            await events.commit()
