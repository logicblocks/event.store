import asyncio
from collections.abc import AsyncIterator
from random import random

from structlog.typing import FilteringBoundLogger

from logicblocks.event.sources import EventSource, constraints
from logicblocks.event.types import (
    Event,
    EventSourceIdentifier,
    str_serialisation_fallback,
)

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


class Counter:
    count: int = 0

    def increment(self) -> None:
        self.count += 1


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

    async def _mark_event_for_save(
        self, events_to_save_in_state: AsyncIterator[E], events_to_save: set[E]
    ):
        async for event in events_to_save_in_state:
            events_to_save.add(event)

    async def _generate_events_from_source(
        self,
        source_iterator: AsyncIterator[E],
        events_to_save: set[E],
        event_count: Counter,
    ) -> AsyncIterator[E]:
        async for event in source_iterator:
            await self._logger.adebug(
                log_event_name("consuming-event"),
                source=self._source.identifier.serialise(
                    fallback=str_serialisation_fallback
                ),
                envelope=event.summarise(),
            )
            try:
                yield event
                await self._state_store.record_processed(event)
                if event in events_to_save:
                    await self._state_store.save()
                    events_to_save.remove(event)

                event_count.increment()
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

    async def _run_consume(self, source_iterator: AsyncIterator[E]):
        event_count = Counter()
        events_to_save: set[E] = set()
        event_generator = self._generate_events_from_source(
            source_iterator, events_to_save, event_count
        )
        save_state_on_events = self._processor.process(event_generator)
        await self._mark_event_for_save(save_state_on_events, events_to_save)

        return event_count

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
            consumed_count=consumed_count.count,
        )


class SampleEventIteratorProcessor[E: Event](EventIteratorProcessor[E]):
    async def process(self, events: AsyncIterator[E]) -> AsyncIterator[E]:
        async for event in events:
            await asyncio.sleep(0)
            if random() < 0.1:
                # yelding means that we want to save the event state
                yield event
