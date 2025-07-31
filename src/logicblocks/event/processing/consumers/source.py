import asyncio
from collections.abc import AsyncIterator, Sequence

from structlog.typing import FilteringBoundLogger

from logicblocks.event.processing.consumers.logger import default_logger
from logicblocks.event.processing.consumers.state import (
    EventConsumerStateStore,
)
from logicblocks.event.processing.consumers.types import (
    AutoCommitEventIteratorProcessor,
    EventConsumer,
    EventIterator,
    EventProcessor,
    EventProcessorManager,
    ManagedEventIteratorProcessor,
    SupportedProcessors,
)
from logicblocks.event.sources import EventSource, constraints
from logicblocks.event.types import (
    Event,
    EventSourceIdentifier,
    str_serialisation_fallback,
)


def log_event_name(event: str) -> str:
    return f"event.consumer.source.{event}"


class StateStoreEventProcessorManager[E: Event](EventProcessorManager[E]):
    def __init__(self, state_store: EventConsumerStateStore):
        self._state_store = state_store
        self._consumed_events = 0
        self._processed_events = 0

    @property
    def processed_events(self):
        return self._processed_events

    @property
    def consumed_events(self):
        return self._consumed_events

    def increment_consumed(self):
        self._consumed_events = self._consumed_events + 1

    def acknowledge(self, events: E | Sequence[E]) -> None:
        for event in events if isinstance(events, Sequence) else [events]:
            self._state_store.record_processed(event)
            self._processed_events = self._processed_events + 1

    async def commit(self, *, force: bool = False) -> None:
        if force:
            await self._state_store.save()
        else:
            await self._state_store.save_if_needed()


async def base_event_iterator[E: Event](
    source_iterator: AsyncIterator[E],
    processor_manager: StateStoreEventProcessorManager[E],
    logger: FilteringBoundLogger = default_logger,
) -> EventIterator[E]:
    async for event in source_iterator:
        await logger.adebug(
            log_event_name("consuming-event"),
            envelope=event.summarise(),
        )
        yield event
        processor_manager.increment_consumed()


async def auto_commit_event_iterator[E: Event](
    source_iterator: AsyncIterator[E],
    processor_manager: StateStoreEventProcessorManager[E],
    logger: FilteringBoundLogger = default_logger,
) -> EventIterator[E]:
    async for event in base_event_iterator(
        source_iterator, processor_manager, logger
    ):
        yield event
        processor_manager.acknowledge(event)
        await processor_manager.commit()


async def process_managed_event_iterator[E: Event](
    source_iterator: AsyncIterator[E],
    processor: ManagedEventIteratorProcessor[E],
    processor_manager: StateStoreEventProcessorManager[E],
    logger: FilteringBoundLogger = default_logger,
) -> None:
    event_iterator = base_event_iterator(
        source_iterator, processor_manager, logger
    )
    await processor.process(event_iterator, processor_manager)


async def process_auto_commit_event_iterator[E: Event](
    source_iterator: AsyncIterator[E],
    processor: AutoCommitEventIteratorProcessor[E],
    processor_manager: StateStoreEventProcessorManager[E],
    logger: FilteringBoundLogger = default_logger,
) -> None:
    event_iterator = auto_commit_event_iterator(
        source_iterator, processor_manager, logger
    )
    await processor.process(event_iterator)


async def process_callback_event_iterator[E: Event](
    source_iterator: AsyncIterator[E],
    processor: EventProcessor[E],
    processor_manager: StateStoreEventProcessorManager[E],
    logger: FilteringBoundLogger = default_logger,
) -> None:
    event_iterator = auto_commit_event_iterator(
        source_iterator, processor_manager, logger
    )
    async for event in event_iterator:
        try:
            await processor.process_event(event)
        except (asyncio.CancelledError, GeneratorExit):
            raise
        except BaseException:
            await logger.aexception(
                log_event_name("processor-failed"),
                envelope=event.summarise(),
            )
            raise

    await processor_manager.commit(force=True)


async def process_event_iterator[E: Event](
    source_iterator: AsyncIterator[E],
    processor: SupportedProcessors[E],
    processor_manager: StateStoreEventProcessorManager[E],
    logger: FilteringBoundLogger = default_logger,
) -> None:
    match processor:
        case ManagedEventIteratorProcessor():
            await process_managed_event_iterator(
                source_iterator, processor, processor_manager, logger
            )
        case AutoCommitEventIteratorProcessor():
            await process_auto_commit_event_iterator(
                source_iterator, processor, processor_manager, logger
            )
        case EventProcessor():
            await process_callback_event_iterator(
                source_iterator, processor, processor_manager, logger
            )
        case _:
            raise TypeError(
                f"Unsupported processor type: {type(processor).__name__}"
            )


class EventSourceConsumer[I: EventSourceIdentifier, E: Event](EventConsumer):
    def __init__(
        self,
        *,
        source: EventSource[I, E],
        processor: SupportedProcessors[E],
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

        processor_manager = StateStoreEventProcessorManager[E](
            state_store=self._state_store
        )
        await process_event_iterator(
            source,
            self._processor,
            processor_manager,
            self._logger,
        )

        await self._logger.adebug(
            log_event_name("completed-consume"),
            consumed_count=processor_manager.consumed_events,
            processed_count=processor_manager.processed_events,
        )
