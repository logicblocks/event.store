import asyncio
from collections.abc import AsyncIterator, Sequence
from typing import overload

from structlog.typing import FilteringBoundLogger

from logicblocks.event.sources import EventSource
from logicblocks.event.types import (
    Event,
    EventSourceIdentifier,
    str_serialisation_fallback,
)

from .logger import default_logger
from .state import (
    EventConsumerStateStore,
)
from .types import (
    AutoCommitEventIteratorProcessor,
    EventConsumer,
    EventIterator,
    EventProcessor,
    EventProcessorManager,
    ManagedEventIteratorProcessor,
    SupportedProcessors,
)


def log_event_name(event: str) -> str:
    return f"event.consumer.source.{event}"


class StateStoreEventProcessorManager[E: Event](EventProcessorManager[E]):
    """Processor manager backed by an `EventConsumerStateStore`.

    Also tracks counts of consumed and processed events for logging.

    Args:
        state_store: The store used to record and save consumer state.
    """

    def __init__(self, state_store: EventConsumerStateStore[E]):
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
    logger: FilteringBoundLogger,
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
    logger: FilteringBoundLogger,
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
    logger: FilteringBoundLogger,
) -> None:
    event_iterator = base_event_iterator(
        source_iterator, processor_manager, logger
    )
    await processor.process(event_iterator, processor_manager)


async def process_auto_commit_event_iterator[E: Event](
    source_iterator: AsyncIterator[E],
    processor: AutoCommitEventIteratorProcessor[E],
    processor_manager: StateStoreEventProcessorManager[E],
    logger: FilteringBoundLogger,
) -> None:
    event_iterator = auto_commit_event_iterator(
        source_iterator, processor_manager, logger
    )
    await processor.process(event_iterator)


async def process_callback_event_iterator[E: Event](
    source_iterator: AsyncIterator[E],
    processor: EventProcessor[E],
    processor_manager: StateStoreEventProcessorManager[E],
    logger: FilteringBoundLogger,
    save_state_after_consumption: bool,
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

    if save_state_after_consumption:
        await processor_manager.commit(force=True)


class EventSourceConsumer[I: EventSourceIdentifier, E: Event](EventConsumer):
    """Consumes events from a single event source, tracking progress.

    On each call to `consume_all`, the consumer loads its last recorded
    state, iterates the source from that point onwards and passes events to
    the processor, recording progress in the state store.

    How events are acknowledged depends on the processor type:

    - `EventProcessor`: each event is acknowledged after `process_event`
      returns, with state saved whenever the persistence interval is
      reached. If `save_state_after_consumption` is `True`, state is also
      saved once all events have been consumed.
    - `AutoCommitEventIteratorProcessor`: each event is acknowledged when
      the processor requests the next event.
    - `ManagedEventIteratorProcessor`: the processor acknowledges events
      and commits state itself via the provided manager.

    Args:
        source: The event source to consume from.
        processor: The processor to pass events to.
        state_store: The store used to load and save consumer state.
        logger: The logger to use.
        save_state_after_consumption: Whether to force a save of consumer
            state after all events have been consumed. Only applies to
            `EventProcessor` processors.
    """

    @overload
    def __init__(
        self,
        *,
        source: EventSource[I, E],
        processor: EventProcessor[E],
        state_store: EventConsumerStateStore[E],
        logger: FilteringBoundLogger = default_logger,
        save_state_after_consumption: bool = True,
    ) -> None: ...

    @overload
    def __init__(
        self,
        *,
        source: EventSource[I, E],
        processor: AutoCommitEventIteratorProcessor[E]
        | ManagedEventIteratorProcessor[E],
        state_store: EventConsumerStateStore[E],
        logger: FilteringBoundLogger = default_logger,
    ) -> None: ...

    def __init__(
        self,
        *,
        source: EventSource[I, E],
        processor: SupportedProcessors[E],
        state_store: EventConsumerStateStore[E],
        logger: FilteringBoundLogger = default_logger,
        save_state_after_consumption: bool = True,
    ):
        self._source = source
        self._processor = processor
        self._state_store = state_store
        self._save_state_after_consumption = save_state_after_consumption
        self._logger = logger.bind(
            source=self._source.identifier.serialise(
                fallback=str_serialisation_fallback
            )
        )

    async def _process_event_iterator(
        self,
        source_iterator: AsyncIterator[E],
        processor_manager: StateStoreEventProcessorManager[E],
    ) -> None:
        match self._processor:
            case ManagedEventIteratorProcessor():
                await process_managed_event_iterator(
                    source_iterator,
                    self._processor,
                    processor_manager,
                    self._logger,
                )
            case AutoCommitEventIteratorProcessor():
                await process_auto_commit_event_iterator(
                    source_iterator,
                    self._processor,
                    processor_manager,
                    self._logger,
                )
            case EventProcessor():
                await process_callback_event_iterator(
                    source_iterator,
                    self._processor,
                    processor_manager,
                    self._logger,
                    self._save_state_after_consumption,
                )
            case _:
                raise TypeError(
                    f"Unsupported processor type: {type(self._processor).__name__}"
                )

    async def consume_all(self) -> None:
        """Consume all events in the source after the last recorded position.

        Raises:
            TypeError: If the processor is not of a supported type.
        """
        constraint = await self._state_store.load_to_query_constraint()

        await self._logger.adebug(
            log_event_name("starting-consume"),
            constraint=constraint,
        )

        if constraint is not None:
            source = self._source.iterate(constraints={constraint})
        else:
            source = self._source.iterate()

        processor_manager = StateStoreEventProcessorManager[E](
            state_store=self._state_store
        )
        await self._process_event_iterator(
            source,
            processor_manager,
        )

        await self._logger.adebug(
            log_event_name("completed-consume"),
            consumed_count=processor_manager.consumed_events,
            processed_count=processor_manager.processed_events,
        )
