from logicblocks.event.store import EventSource, constraints

from .state import EventConsumerStateStore
from .types import EventConsumer, EventProcessor


class EventSourceConsumer(EventConsumer):
    def __init__(
        self,
        *,
        source: EventSource,
        processor: EventProcessor,
        state_store: EventConsumerStateStore,
    ):
        self._source = source
        self._processor = processor
        self._state_store = state_store

    async def consume_all(self) -> None:
        state = await self._state_store.load()
        source = self._source
        if state is not None:
            source = self._source.iterate(
                constraints={
                    constraints.sequence_number_after(
                        state.last_sequence_number
                    )
                }
            )

        async for event in source:
            await self._processor.process_event(event)
            await self._state_store.record_processed(event)

        await self._state_store.save()
