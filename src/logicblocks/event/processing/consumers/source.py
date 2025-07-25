import asyncio

from structlog.typing import FilteringBoundLogger

from logicblocks.event.sources import EventSource, constraints
from logicblocks.event.types import (
    Event,
    EventSourceIdentifier,
    str_serialisation_fallback,
)

from .logger import default_logger
from .state import EventConsumerStateStore
from .types import EventConsumer, EventProcessor


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
        save_state_after_consumption: bool = False,
    ):
        self._source = source
        self._processor = processor
        self._state_store = state_store
        self._logger = logger
        self._save_state_after_consumption = save_state_after_consumption

    async def consume_all(self) -> None:
        state = await self._state_store.load()
        last_ordering_id = None if state is None else state.last_ordering_id

        await self._logger.adebug(
            log_event_name("starting-consume"),
            source=self._source.identifier.serialise(
                fallback=str_serialisation_fallback
            ),
            last_ordering_id=last_ordering_id,
        )

        source = self._source
        if last_ordering_id is not None:
            source = self._source.iterate(
                constraints={constraints.ordering_id_after(last_ordering_id)}
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

        if self._save_state_after_consumption and consumed_count > 0:
            await self._state_store.save()

        await self._logger.adebug(
            log_event_name("completed-consume"),
            source=self._source.identifier.serialise(
                fallback=str_serialisation_fallback
            ),
            consumed_count=consumed_count,
        )
