from structlog.typing import FilteringBoundLogger

from logicblocks.event.store import EventSource, constraints

from .logger import default_logger
from .state import EventConsumerStateStore
from .types import EventConsumer, EventProcessor


class EventSourceConsumer(EventConsumer):
    def __init__(
        self,
        *,
        source: EventSource,
        processor: EventProcessor,
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

        await self._logger.ainfo(
            "event.consumer.source.starting-consume",
            source=self._source.identifier.dict(),
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
                "event.consumer.source.consuming-event",
                source=self._source.identifier.dict(),
                envelope=event.envelope(),
            )
            await self._processor.process_event(event)
            await self._state_store.record_processed(event)
            consumed_count += 1

        await self._state_store.save()
        await self._logger.ainfo(
            "event.consumer.source.completed-consume",
            source=self._source.identifier.dict(),
            consumed_count=consumed_count,
        )
