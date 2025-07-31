import asyncio
from collections.abc import Sequence

from logicblocks.event.processing import (
    EventConsumerState,
    EventConsumerStateStore,
    EventCount,
)
from logicblocks.event.store import (
    EventStore,
    InMemoryEventStorageAdapter,
    UnmetWriteConditionError,
)
from logicblocks.event.store.state import (
    StoredEventEventConsumerStateConverter,
)
from logicblocks.event.testing import data
from logicblocks.event.testing.builders import (
    NewEventBuilder,
    StoredEventBuilder,
)
from logicblocks.event.types import NewEvent


class TestEventConsumerStateStoreLoad:
    async def test_loads_initial_state_from_category(self):
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_category = event_store.category(
            category=data.random_event_category_name()
        )
        state_store = EventConsumerStateStore(
            category=state_category,
            converter=StoredEventEventConsumerStateConverter(),
            persistence_interval=EventCount(10),
        )
        await state_category.stream(stream="default").publish(
            events=[
                NewEventBuilder()
                .with_name("state-changed")
                .with_payload(
                    {"state": {"last_sequence_number": 42, "extra": "state"}}
                )
                .build()
            ]
        )

        state = await state_store.load()

        assert state == EventConsumerState(
            state={"last_sequence_number": 42, "extra": "state"}
        )

    async def test_loads_initial_state_from_category_when_stored_on_top_level(
        self,
    ):
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_category = event_store.category(
            category=data.random_event_category_name()
        )
        state_store = EventConsumerStateStore(
            category=state_category,
            converter=StoredEventEventConsumerStateConverter(),
            persistence_interval=EventCount(10),
        )
        await state_category.stream(stream="default").publish(
            events=[
                NewEventBuilder()
                .with_name("state-changed")
                .with_payload(
                    {"last_sequence_number": 42, "state": {"extra": "state"}}
                )
                .build()
            ]
        )

        state = await state_store.load()

        assert state == EventConsumerState(
            state={"last_sequence_number": 42, "extra": "state"}
        )

    async def test_returns_none_when_no_state_in_category(self):
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_category = event_store.category(
            category=data.random_event_category_name()
        )
        state_store = EventConsumerStateStore(
            category=state_category,
            converter=StoredEventEventConsumerStateConverter(),
            persistence_interval=EventCount(10),
        )

        state = await state_store.load()

        assert state is None

    async def test_loads_for_specified_partition(self):
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_category = event_store.category(
            category=data.random_event_category_name()
        )
        state_store = EventConsumerStateStore(
            category=state_category,
            converter=StoredEventEventConsumerStateConverter(),
            persistence_interval=EventCount(10),
        )

        partition_1 = "a"
        partition_2 = "b"

        await state_category.stream(stream=partition_2).publish(
            events=[
                NewEventBuilder()
                .with_name("state-changed")
                .with_payload(
                    {"last_sequence_number": 35, "state": {"other": "state"}}
                )
                .build()
            ]
        )
        await state_category.stream(stream=partition_1).publish(
            events=[
                NewEventBuilder()
                .with_name("state-changed")
                .with_payload(
                    {"last_sequence_number": 42, "state": {"extra": "state"}}
                )
                .build()
            ]
        )

        state = await state_store.load(partition=partition_2)

        assert state == EventConsumerState(
            state={"last_sequence_number": 35, "other": "state"}
        )

    async def test_returns_none_when_no_state_for_specified_partition(self):
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_category = event_store.category(
            category=data.random_event_category_name()
        )
        state_store = EventConsumerStateStore(
            category=state_category,
            converter=StoredEventEventConsumerStateConverter(),
            persistence_interval=EventCount(10),
        )

        partition_1 = "a"
        partition_2 = "b"

        await state_category.stream(stream=partition_1).publish(
            events=[
                NewEventBuilder()
                .with_name("state-changed")
                .with_payload(
                    {"last_sequence_number": 42, "state": {"extra": "state"}}
                )
                .build()
            ]
        )

        state = await state_store.load(partition=partition_2)

        assert state is None


class TestEventConsumerStateStoreRecordProcessed:
    async def test_records_event_as_processed(self):
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_store = EventConsumerStateStore(
            category=event_store.category(
                category=data.random_event_category_name(),
            ),
            converter=StoredEventEventConsumerStateConverter(),
            persistence_interval=EventCount(10),
        )

        processed_event = StoredEventBuilder().build()

        await state_store.record_processed(event=processed_event)

        state = await state_store.load()

        assert state == EventConsumerState(
            state={"last_sequence_number": processed_event.sequence_number}
        )

    async def test_captures_extra_state(self):
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_store = EventConsumerStateStore(
            category=event_store.category(
                category=data.random_event_category_name(),
            ),
            converter=StoredEventEventConsumerStateConverter(),
            persistence_interval=EventCount(10),
        )

        processed_event = StoredEventBuilder().build()

        await state_store.record_processed(
            event=processed_event, extra_state={"extra": "state"}
        )

        state = await state_store.load()

        assert state == EventConsumerState(
            state={
                "last_sequence_number": processed_event.sequence_number,
                "extra": "state",
            },
        )

    async def test_records_event_per_partition(self):
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_store = EventConsumerStateStore(
            category=event_store.category(
                category=data.random_event_category_name(),
            ),
            converter=StoredEventEventConsumerStateConverter(),
            persistence_interval=EventCount(10),
        )

        partition_1 = "a"
        partition_2 = "b"

        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(5).build(),
            extra_state={"some": "state-a"},
            partition=partition_1,
        )
        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(8).build(),
            extra_state={"some": "state-b"},
            partition=partition_2,
        )

        state = await state_store.load(partition=partition_1)

        assert state == EventConsumerState(
            state={"last_sequence_number": 5, "some": "state-a"}
        )

    async def test_stores_state_every_hundred_events_by_default(self):
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_category = event_store.category(
            category=data.random_event_category_name()
        )
        state_store = EventConsumerStateStore(
            category=state_category,
            converter=StoredEventEventConsumerStateConverter(),
        )

        for i in range(1, 101):
            await state_store.record_processed(
                event=StoredEventBuilder().with_sequence_number(i).build()
            )

        events = await state_category.stream(stream="default").read()

        assert len(events) == 1
        assert events[0].payload == {
            "state": {
                "last_sequence_number": 100,
            },
        }

    async def test_does_not_store_default_state_before_threshold_reached(self):
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_category = event_store.category(
            category=data.random_event_category_name()
        )
        state_store = EventConsumerStateStore(
            category=state_category,
            converter=StoredEventEventConsumerStateConverter(),
            persistence_interval=EventCount(2),
        )

        await state_store.record_processed(
            event=StoredEventBuilder().build(), extra_state={"extra": "state"}
        )

        state = await state_category.stream(stream="default").latest()

        assert state is None

    async def test_does_not_store_partitioned_state_before_threshold_reached(
        self,
    ):
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_category = event_store.category(
            category=data.random_event_category_name()
        )
        state_store = EventConsumerStateStore(
            category=state_category,
            converter=StoredEventEventConsumerStateConverter(),
            persistence_interval=EventCount(2),
        )

        partition_1 = "a"
        partition_2 = "b"

        await state_store.record_processed(
            event=StoredEventBuilder().build(),
            extra_state={"extra": "state"},
            partition=partition_1,
        )
        await state_store.record_processed(
            event=StoredEventBuilder().build(),
            extra_state={"extra": "state"},
            partition=partition_2,
        )

        partition_1_events = await state_category.stream(
            stream=partition_1
        ).read()
        partition_2_events = await state_category.stream(
            stream=partition_2
        ).read()

        assert len(partition_1_events) == 0
        assert len(partition_2_events) == 0

    async def test_stores_default_state_on_threshold_reached(self):
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_category = event_store.category(
            category=data.random_event_category_name()
        )
        state_store = EventConsumerStateStore(
            category=state_category,
            converter=StoredEventEventConsumerStateConverter(),
            persistence_interval=EventCount(3),
        )

        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(4).build(),
            extra_state={"extra": "state1"},
        )
        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(12).build(),
            extra_state={"extra": "state2"},
        )
        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(13).build(),
            extra_state={"extra": "state3"},
        )

        state_event = await state_category.stream(stream="default").latest()

        assert state_event is not None
        assert state_event.payload == {
            "state": {"last_sequence_number": 13, "extra": "state3"},
        }

    async def test_stores_default_state_when_existing_state_events(self):
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_category = event_store.category(
            category=data.random_event_category_name()
        )

        await state_category.stream(stream="default").publish(
            events=[
                (
                    NewEventBuilder()
                    .with_payload({"state": {"last_sequence_number": 10}})
                    .build()
                )
            ]
        )

        state_store = EventConsumerStateStore(
            category=state_category,
            converter=StoredEventEventConsumerStateConverter(),
            persistence_interval=EventCount(1),
        )

        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(14).build(),
            extra_state={"extra": "state"},
        )

        state_events = await state_category.stream(stream="default").read()

        assert len(state_events) == 2
        assert state_events[0].payload == {
            "state": {
                "last_sequence_number": 10,
            },
        }
        assert state_events[1].payload == {
            "state": {"last_sequence_number": 14, "extra": "state"},
        }

    async def test_stores_partitioned_state_on_threshold_reached(self):
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_category = event_store.category(
            category=data.random_event_category_name()
        )
        state_store = EventConsumerStateStore(
            category=state_category,
            converter=StoredEventEventConsumerStateConverter(),
            persistence_interval=EventCount(2),
        )

        partition_1 = "a"
        partition_2 = "b"

        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(4).build(),
            extra_state={"extra": "state1"},
            partition=partition_1,
        )
        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(12).build(),
            extra_state={"extra": "state2"},
            partition=partition_2,
        )
        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(13).build(),
            extra_state={"extra": "state3"},
            partition=partition_1,
        )

        partition_1_events = await state_category.stream(
            stream=partition_1
        ).read()
        partition_2_events = await state_category.stream(
            stream=partition_1
        ).read()

        assert len(partition_1_events) == 1
        assert partition_1_events[0].payload == {
            "state": {"last_sequence_number": 13, "extra": "state3"},
        }
        assert len(partition_2_events) == 1

    async def test_stores_partitioned_state_when_existing_state_events(self):
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_category = event_store.category(
            category=data.random_event_category_name()
        )

        partition_1 = "a"
        partition_2 = "b"

        await state_category.stream(stream=partition_1).publish(
            events=[
                (
                    NewEventBuilder()
                    .with_payload(
                        {
                            "state": {
                                "last_sequence_number": 10,
                            }
                        }
                    )
                    .build()
                )
            ]
        )
        await state_category.stream(stream=partition_2).publish(
            events=[
                (
                    NewEventBuilder()
                    .with_payload(
                        {
                            "state": {
                                "last_sequence_number": 11,
                            }
                        }
                    )
                    .build()
                )
            ]
        )

        state_store = EventConsumerStateStore(
            category=state_category,
            converter=StoredEventEventConsumerStateConverter(),
            persistence_interval=EventCount(1),
        )

        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(14).build(),
            extra_state={"extra": "state1"},
            partition=partition_1,
        )
        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(15).build(),
            extra_state={"extra": "state2"},
            partition=partition_2,
        )

        partition_1_events = await state_category.stream(
            stream=partition_1
        ).read()
        partition_2_events = await state_category.stream(
            stream=partition_2
        ).read()

        assert len(partition_1_events) == 2
        assert partition_1_events[0].payload == {
            "state": {
                "last_sequence_number": 10,
            },
        }
        assert partition_1_events[1].payload == {
            "state": {"last_sequence_number": 14, "extra": "state1"},
        }

        assert len(partition_2_events) == 2
        assert partition_2_events[0].payload == {
            "state": {
                "last_sequence_number": 11,
            },
        }
        assert partition_2_events[1].payload == {
            "state": {"last_sequence_number": 15, "extra": "state2"},
        }

    async def test_stores_default_state_every_time_threshold_reached(self):
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_category = event_store.category(
            category=data.random_event_category_name()
        )
        state_store = EventConsumerStateStore(
            category=state_category,
            converter=StoredEventEventConsumerStateConverter(),
            persistence_interval=EventCount(2),
        )

        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(4).build(),
            extra_state={"extra": "state1"},
        )
        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(12).build(),
            extra_state={"extra": "state2"},
        )
        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(13).build(),
            extra_state={"extra": "state3"},
        )
        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(17).build(),
            extra_state={"extra": "state4"},
        )
        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(20).build(),
            extra_state={"extra": "state5"},
        )

        state_events = await state_category.stream(stream="default").read()

        assert len(state_events) == 2
        assert state_events[0].payload == {
            "state": {"last_sequence_number": 12, "extra": "state2"},
        }
        assert state_events[1].payload == {
            "state": {"last_sequence_number": 17, "extra": "state4"},
        }

    async def test_stores_partitioned_state_every_time_threshold_reached(self):
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_category = event_store.category(
            category=data.random_event_category_name()
        )
        state_store = EventConsumerStateStore(
            category=state_category,
            converter=StoredEventEventConsumerStateConverter(),
            persistence_interval=EventCount(2),
        )

        partition_1 = "a"
        partition_2 = "b"

        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(4).build(),
            extra_state={"extra": "state_a_1"},
            partition=partition_1,
        )
        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(12).build(),
            extra_state={"extra": "state_b_1"},
            partition=partition_2,
        )
        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(13).build(),
            extra_state={"extra": "state_a_2"},
            partition=partition_1,
        )
        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(17).build(),
            extra_state={"extra": "state_b_2"},
            partition=partition_2,
        )
        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(20).build(),
            extra_state={"extra": "state_b_3"},
            partition=partition_1,
        )

        partition_1_events = await state_category.stream(
            stream=partition_1
        ).read()
        partition_2_events = await state_category.stream(
            stream=partition_2
        ).read()

        assert len(partition_1_events) == 1
        assert partition_1_events[0].payload == {
            "state": {"last_sequence_number": 13, "extra": "state_a_2"},
        }
        assert len(partition_2_events) == 1
        assert partition_2_events[0].payload == {
            "state": {"last_sequence_number": 17, "extra": "state_b_2"},
        }

    async def test_consistent_concurrent_execution_when_no_stored_default_state(
        self,
    ):
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_category = event_store.category(
            category=data.random_event_category_name()
        )
        state_store = EventConsumerStateStore(
            category=state_category,
            converter=StoredEventEventConsumerStateConverter(),
            persistence_interval=EventCount(1),
        )

        async def record_event_as_processed(sequence_number: int):
            return await state_store.record_processed(
                event=(
                    StoredEventBuilder()
                    .with_sequence_number(sequence_number)
                    .build()
                ),
                extra_state={"extra": f"state{sequence_number}"},
            )

        results = await asyncio.gather(
            record_event_as_processed(sequence_number=1),
            record_event_as_processed(sequence_number=2),
            return_exceptions=True,
        )

        state_events = await state_category.stream(stream="default").read()

        successful_writes: Sequence[EventConsumerState] = [
            result
            for result in results
            if not isinstance(result, BaseException)
        ]
        failed_writes: Sequence[BaseException] = [
            result for result in results if isinstance(result, Exception)
        ]

        assert len(successful_writes) == 1
        assert len(failed_writes) == 1

        assert isinstance(failed_writes[0], UnmetWriteConditionError)

        assert len(state_events) == 1
        assert state_events[0].payload == {
            "state": successful_writes[0].state,
        }

    async def test_consistent_concurrent_execution_when_stored_default_state(
        self,
    ):
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_category = event_store.category(
            category=data.random_event_category_name()
        )
        state_store = EventConsumerStateStore(
            category=state_category,
            converter=StoredEventEventConsumerStateConverter(),
            persistence_interval=EventCount(1),
        )

        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(1).build(),
            extra_state={"initial": "state"},
        )

        async def record_event_as_processed(sequence_number: int):
            return await state_store.record_processed(
                event=(
                    StoredEventBuilder()
                    .with_sequence_number(sequence_number)
                    .build()
                ),
                extra_state={"extra": f"state{sequence_number}"},
            )

        results = await asyncio.gather(
            record_event_as_processed(sequence_number=2),
            record_event_as_processed(sequence_number=3),
            return_exceptions=True,
        )

        state_events = await state_category.stream(stream="default").read()

        successful_writes: Sequence[EventConsumerState] = [
            result
            for result in results
            if not isinstance(result, BaseException)
        ]
        failed_writes: Sequence[BaseException] = [
            result for result in results if isinstance(result, Exception)
        ]

        assert len(successful_writes) == 1
        assert len(failed_writes) == 1

        assert isinstance(failed_writes[0], UnmetWriteConditionError)

        assert len(state_events) == 2
        assert state_events[0].payload == {
            "state": {"last_sequence_number": 1, "initial": "state"},
        }
        assert state_events[1].payload == {
            "state": successful_writes[0].state,
        }

    async def test_consistent_concurrent_execution_across_partitions(self):
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_category = event_store.category(
            category=data.random_event_category_name()
        )
        state_store = EventConsumerStateStore(
            category=state_category,
            converter=StoredEventEventConsumerStateConverter(),
            persistence_interval=EventCount(1),
        )

        partition_1 = "a"
        partition_2 = "b"

        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(1).build(),
            extra_state={"initial": "state"},
            partition=partition_1,
        )

        async def record_event_as_processed(
            sequence_number: int, partition: str
        ):
            return await state_store.record_processed(
                event=(
                    StoredEventBuilder()
                    .with_sequence_number(sequence_number)
                    .build()
                ),
                extra_state={"extra": f"state_{partition}_{sequence_number}"},
                partition=partition,
            )

        results = await asyncio.gather(
            record_event_as_processed(
                sequence_number=2, partition=partition_1
            ),
            record_event_as_processed(
                sequence_number=3, partition=partition_2
            ),
            record_event_as_processed(
                sequence_number=4, partition=partition_1
            ),
            record_event_as_processed(
                sequence_number=5, partition=partition_2
            ),
            return_exceptions=True,
        )

        partition_1_events = await state_category.stream(
            stream=partition_1
        ).read()
        partition_2_events = await state_category.stream(
            stream=partition_2
        ).read()

        successful_writes: Sequence[EventConsumerState] = [
            result
            for result in results
            if not isinstance(result, BaseException)
        ]
        failed_writes: Sequence[BaseException] = [
            result for result in results if isinstance(result, Exception)
        ]

        assert len(successful_writes) == 2
        assert len(failed_writes) == 2

        partition_1_write = next(
            write
            for write in successful_writes
            if write.state["last_sequence_number"] in {2, 4}
        )
        partition_2_write = next(
            write
            for write in successful_writes
            if write.state["last_sequence_number"] in {3, 5}
        )

        assert isinstance(failed_writes[0], UnmetWriteConditionError)
        assert isinstance(failed_writes[1], UnmetWriteConditionError)

        assert len(partition_1_events) == 2
        assert partition_1_events[0].payload == {
            "state": {"last_sequence_number": 1, "initial": "state"},
        }
        assert partition_1_events[1].payload == {
            "state": partition_1_write.state,
        }

        assert len(partition_2_events) == 1
        assert partition_2_events[0].payload == {
            "state": partition_2_write.state,
        }


class TestEventConsumerStateStoreSave:
    async def test_stores_state_immediately_for_default_partition(self):
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_category = event_store.category(
            category=data.random_event_category_name()
        )
        state_store = EventConsumerStateStore(
            category=state_category,
            converter=StoredEventEventConsumerStateConverter(),
            persistence_interval=EventCount(2),
        )

        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(1).build()
        )

        category_events = await state_category.read()

        assert len(category_events) == 0

        await state_store.save()

        state_events = await state_category.stream(stream="default").read()

        assert len(state_events) == 1
        assert state_events[0].payload == {
            "state": {
                "last_sequence_number": 1,
            },
        }

    async def test_skips_store_when_default_partition_already_saved(self):
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_category = event_store.category(
            category=data.random_event_category_name()
        )
        state_store = EventConsumerStateStore(
            category=state_category,
            converter=StoredEventEventConsumerStateConverter(),
            persistence_interval=EventCount(2),
        )

        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(1).build()
        )
        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(2).build()
        )

        await state_store.save()

        state_events = await state_category.stream(stream="default").read()

        assert len(state_events) == 1
        assert state_events[0].payload == {
            "state": {
                "last_sequence_number": 2,
            },
        }

    async def test_stores_default_partition_when_existing_state_events(self):
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_category = event_store.category(
            category=data.random_event_category_name()
        )

        await state_category.stream(stream="default").publish(
            events=[
                NewEvent(
                    name="state-changed",
                    payload={
                        "state": {
                            "last_sequence_number": 1,
                        }
                    },
                )
            ]
        )

        state_store = EventConsumerStateStore(
            category=state_category,
            converter=StoredEventEventConsumerStateConverter(),
        )

        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(2).build()
        )

        await state_store.save()

        state_events = await state_category.stream(stream="default").read()

        assert len(state_events) == 2
        assert state_events[0].payload == {
            "state": {
                "last_sequence_number": 1,
            },
        }
        assert state_events[1].payload == {
            "state": {
                "last_sequence_number": 2,
            },
        }

    async def test_stores_state_immediately_for_all_tracked_partitions(self):
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_category = event_store.category(
            category=data.random_event_category_name()
        )
        state_store = EventConsumerStateStore(
            category=state_category,
            converter=StoredEventEventConsumerStateConverter(),
            persistence_interval=EventCount(2),
        )

        partition_1 = "a"
        partition_2 = "b"

        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(1).build(),
            partition=partition_1,
        )
        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(2).build(),
            partition=partition_2,
        )

        category_events = await state_category.read()

        assert len(category_events) == 0

        await state_store.save()

        partition_1_events = await state_category.stream(
            stream=partition_1
        ).read()
        partition_2_events = await state_category.stream(
            stream=partition_2
        ).read()

        assert len(partition_1_events) == 1
        assert partition_1_events[0].payload == {
            "state": {
                "last_sequence_number": 1,
            },
        }
        assert len(partition_2_events) == 1
        assert partition_2_events[0].payload == {
            "state": {
                "last_sequence_number": 2,
            },
        }

    async def test_skips_store_if_tracked_partition_already_saved(self):
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_category = event_store.category(
            category=data.random_event_category_name()
        )
        state_store = EventConsumerStateStore(
            category=state_category,
            converter=StoredEventEventConsumerStateConverter(),
            persistence_interval=EventCount(2),
        )

        partition_1 = "a"
        partition_2 = "b"

        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(1).build(),
            partition=partition_1,
        )
        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(2).build(),
            partition=partition_2,
        )
        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(3).build(),
            partition=partition_2,
        )

        category_events = await state_category.read()

        assert len(category_events) == 1

        await state_store.save()

        partition_1_events = await state_category.stream(
            stream=partition_1
        ).read()
        partition_2_events = await state_category.stream(
            stream=partition_2
        ).read()

        category_events = await state_category.read()

        assert len(category_events) == 2

        assert len(partition_1_events) == 1
        assert partition_1_events[0].payload == {
            "state": {
                "last_sequence_number": 1,
            },
        }
        assert len(partition_2_events) == 1
        assert partition_2_events[0].payload == {
            "state": {
                "last_sequence_number": 3,
            },
        }

    async def test_stores_tracked_partitions_when_existing_state_events(self):
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_category = event_store.category(
            category=data.random_event_category_name()
        )

        partition_1 = "a"
        partition_2 = "b"

        await state_category.stream(stream=partition_1).publish(
            events=[
                NewEvent(
                    name="state-changed",
                    payload={
                        "state": {
                            "last_sequence_number": 1,
                        }
                    },
                )
            ]
        )
        await state_category.stream(stream=partition_2).publish(
            events=[
                NewEvent(
                    name="state-changed",
                    payload={
                        "state": {
                            "last_sequence_number": 2,
                        }
                    },
                )
            ]
        )

        state_store = EventConsumerStateStore(
            category=state_category,
            converter=StoredEventEventConsumerStateConverter(),
        )

        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(3).build(),
            partition=partition_1,
        )
        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(4).build(),
            partition=partition_2,
        )

        await state_store.save()

        partition_1_events = await state_category.stream(
            stream=partition_1
        ).read()
        partition_2_events = await state_category.stream(
            stream=partition_2
        ).read()

        assert len(partition_1_events) == 2
        assert partition_1_events[0].payload == {
            "state": {
                "last_sequence_number": 1,
            },
        }
        assert partition_1_events[1].payload == {
            "state": {
                "last_sequence_number": 3,
            },
        }

        assert len(partition_2_events) == 2
        assert partition_2_events[0].payload == {
            "state": {
                "last_sequence_number": 2,
            },
        }
        assert partition_2_events[1].payload == {
            "state": {
                "last_sequence_number": 4,
            },
        }

    async def test_does_nothing_when_no_state_for_tracked_partitions(self):
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_category = event_store.category(
            category=data.random_event_category_name()
        )
        state_store = EventConsumerStateStore(
            category=state_category,
            converter=StoredEventEventConsumerStateConverter(),
            persistence_interval=EventCount(2),
        )

        await state_store.save()

        category_events = await state_category.read()

        assert len(category_events) == 0

    async def test_stores_state_immediately_for_specified_partition(self):
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_category = event_store.category(
            category=data.random_event_category_name()
        )
        state_store = EventConsumerStateStore(
            category=state_category,
            converter=StoredEventEventConsumerStateConverter(),
            persistence_interval=EventCount(2),
        )

        partition_1 = "a"
        partition_2 = "b"

        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(1).build(),
            partition=partition_1,
        )
        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(2).build(),
            partition=partition_2,
        )

        category_events = await state_category.read()

        assert len(category_events) == 0

        await state_store.save(partition=partition_1)

        partition_1_events = await state_category.stream(
            stream=partition_1
        ).read()
        partition_2_events = await state_category.stream(
            stream=partition_2
        ).read()

        assert len(partition_1_events) == 1
        assert partition_1_events[0].payload == {
            "state": {
                "last_sequence_number": 1,
            },
        }
        assert len(partition_2_events) == 0

    async def test_skips_store_for_specified_partition_if_already_saved(self):
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_category = event_store.category(
            category=data.random_event_category_name()
        )
        state_store = EventConsumerStateStore(
            category=state_category,
            converter=StoredEventEventConsumerStateConverter(),
            persistence_interval=EventCount(2),
        )

        partition_1 = "a"
        partition_2 = "b"

        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(1).build(),
            partition=partition_1,
        )
        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(2).build(),
            partition=partition_2,
        )
        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(3).build(),
            partition=partition_2,
        )

        category_events = await state_category.read()

        assert len(category_events) == 1

        await state_store.save(partition=partition_2)

        partition_1_events = await state_category.stream(
            stream=partition_1
        ).read()
        partition_2_events = await state_category.stream(
            stream=partition_2
        ).read()

        category_events = await state_category.read()

        assert len(category_events) == 1

        assert len(partition_1_events) == 0
        assert len(partition_2_events) == 1
        assert partition_2_events[0].payload == {
            "state": {
                "last_sequence_number": 3,
            },
        }

    async def test_stores_specified_partition_when_existing_state_events(self):
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_category = event_store.category(
            category=data.random_event_category_name()
        )

        partition_1 = "a"
        partition_2 = "b"

        await state_category.stream(stream=partition_1).publish(
            events=[
                NewEvent(
                    name="state-changed",
                    payload={
                        "state": {
                            "last_sequence_number": 1,
                        }
                    },
                )
            ]
        )

        state_store = EventConsumerStateStore(
            category=state_category,
            converter=StoredEventEventConsumerStateConverter(),
        )

        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(2).build(),
            partition=partition_1,
        )
        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(3).build(),
            partition=partition_2,
        )

        await state_store.save(partition=partition_1)

        partition_1_events = await state_category.stream(
            stream=partition_1
        ).read()
        partition_2_events = await state_category.stream(
            stream=partition_2
        ).read()

        assert len(partition_1_events) == 2
        assert partition_1_events[0].payload == {
            "state": {
                "last_sequence_number": 1,
            },
        }
        assert partition_1_events[1].payload == {
            "state": {
                "last_sequence_number": 2,
            },
        }

        assert len(partition_2_events) == 0

    async def test_does_nothing_when_no_state_for_specified_partition(self):
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_category = event_store.category(
            category=data.random_event_category_name()
        )
        state_store = EventConsumerStateStore(
            category=state_category,
            converter=StoredEventEventConsumerStateConverter(),
            persistence_interval=EventCount(2),
        )

        partition_1 = "a"
        partition_2 = "b"

        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(1).build(),
            partition=partition_1,
        )

        await state_store.save(partition=partition_2)

        category_events = await state_category.read()

        assert len(category_events) == 0

    async def test_consistent_concurrent_execution_when_no_stored_default_state(
        self,
    ):
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_category = event_store.category(
            category=data.random_event_category_name()
        )
        state_store = EventConsumerStateStore(
            category=state_category,
            converter=StoredEventEventConsumerStateConverter(),
            persistence_interval=EventCount(2),
        )

        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(1).build(),
        )

        results = await asyncio.gather(
            state_store.save(), state_store.save(), return_exceptions=True
        )

        state_events = await state_category.stream(stream="default").read()

        successful_writes: Sequence[None] = [
            result
            for result in results
            if not isinstance(result, BaseException)
        ]
        failed_writes: Sequence[BaseException] = [
            result for result in results if isinstance(result, Exception)
        ]

        assert len(successful_writes) == 1
        assert len(failed_writes) == 1

        assert isinstance(failed_writes[0], UnmetWriteConditionError)

        assert len(state_events) == 1
        assert state_events[0].payload == {
            "state": {
                "last_sequence_number": 1,
            },
        }

    async def test_consistent_concurrent_execution_when_stored_default_state(
        self,
    ):
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_category = event_store.category(
            category=data.random_event_category_name()
        )
        state_store = EventConsumerStateStore(
            category=state_category,
            converter=StoredEventEventConsumerStateConverter(),
            persistence_interval=EventCount(2),
        )

        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(1).build(),
            extra_state={"extra": "state_1"},
        )
        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(2).build(),
            extra_state={"extra": "state_2"},
        )
        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(3).build(),
            extra_state={"extra": "state_3"},
        )

        results = await asyncio.gather(
            state_store.save(), state_store.save(), return_exceptions=True
        )

        state_events = await state_category.stream(stream="default").read()

        successful_writes: Sequence[None] = [
            result
            for result in results
            if not isinstance(result, BaseException)
        ]
        failed_writes: Sequence[BaseException] = [
            result for result in results if isinstance(result, Exception)
        ]

        assert len(successful_writes) == 1
        assert len(failed_writes) == 1

        assert isinstance(failed_writes[0], UnmetWriteConditionError)

        assert len(state_events) == 2
        assert state_events[0].payload == {
            "state": {"last_sequence_number": 2, "extra": "state_2"},
        }
        assert state_events[1].payload == {
            "state": {"last_sequence_number": 3, "extra": "state_3"},
        }

    async def test_consistent_concurrent_execution_when_existing_default_state_event(
        self,
    ):
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_category = event_store.category(
            category=data.random_event_category_name()
        )

        await state_category.stream(stream="default").publish(
            events=[
                (
                    NewEventBuilder()
                    .with_payload(
                        {
                            "state": {
                                "last_sequence_number": 1,
                            }
                        }
                    )
                    .build()
                ),
            ]
        )

        state_store = EventConsumerStateStore(
            category=state_category,
            converter=StoredEventEventConsumerStateConverter(),
            persistence_interval=EventCount(2),
        )

        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(2).build(),
        )

        results = await asyncio.gather(
            state_store.save(), state_store.save(), return_exceptions=True
        )

        state_events = await state_category.stream(stream="default").read()

        successful_writes: Sequence[None] = [
            result
            for result in results
            if not isinstance(result, BaseException)
        ]
        failed_writes: Sequence[BaseException] = [
            result for result in results if isinstance(result, Exception)
        ]

        assert len(successful_writes) == 1
        assert len(failed_writes) == 1

        assert isinstance(failed_writes[0], UnmetWriteConditionError)

        assert len(state_events) == 2
        assert state_events[0].payload == {
            "state": {
                "last_sequence_number": 1,
            },
        }
        assert state_events[1].payload == {
            "state": {
                "last_sequence_number": 2,
            },
        }

    async def test_consistent_concurrent_execution_when_no_stored_partitioned_state_all_partitions(
        self,
    ):
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_category = event_store.category(
            category=data.random_event_category_name()
        )
        state_store = EventConsumerStateStore(
            category=state_category,
            converter=StoredEventEventConsumerStateConverter(),
            persistence_interval=EventCount(2),
        )

        partition_1 = "a"
        partition_2 = "b"

        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(1).build(),
            partition=partition_1,
        )
        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(2).build(),
            partition=partition_2,
        )

        results = await asyncio.gather(
            state_store.save(), state_store.save(), return_exceptions=True
        )

        partition_1_events = await state_category.stream(
            stream=partition_1
        ).read()
        partition_2_events = await state_category.stream(
            stream=partition_2
        ).read()

        successful_writes: Sequence[None] = [
            result
            for result in results
            if not isinstance(result, BaseException)
        ]
        failed_writes: Sequence[BaseException] = [
            result for result in results if isinstance(result, Exception)
        ]

        assert len(successful_writes) == 1
        assert len(failed_writes) == 1

        assert isinstance(failed_writes[0], UnmetWriteConditionError)

        assert len(partition_1_events) == 1
        assert partition_1_events[0].payload == {
            "state": {
                "last_sequence_number": 1,
            },
        }
        assert len(partition_2_events) == 1
        assert partition_2_events[0].payload == {
            "state": {
                "last_sequence_number": 2,
            },
        }

    async def test_consistent_concurrent_execution_when_stored_partitioned_state_all_partitions(
        self,
    ):
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_category = event_store.category(
            category=data.random_event_category_name()
        )
        state_store = EventConsumerStateStore(
            category=state_category,
            converter=StoredEventEventConsumerStateConverter(),
            persistence_interval=EventCount(2),
        )

        partition_1 = "a"
        partition_2 = "b"

        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(1).build(),
            partition=partition_1,
        )
        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(2).build(),
            partition=partition_2,
        )
        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(3).build(),
            partition=partition_2,
        )

        results = await asyncio.gather(
            state_store.save(), state_store.save(), return_exceptions=True
        )

        partition_1_events = await state_category.stream(
            stream=partition_1
        ).read()
        partition_2_events = await state_category.stream(
            stream=partition_2
        ).read()

        successful_writes: Sequence[None] = [
            result
            for result in results
            if not isinstance(result, BaseException)
        ]
        failed_writes: Sequence[BaseException] = [
            result for result in results if isinstance(result, Exception)
        ]

        assert len(successful_writes) == 1
        assert len(failed_writes) == 1

        assert isinstance(failed_writes[0], UnmetWriteConditionError)

        assert len(partition_1_events) == 1
        assert partition_1_events[0].payload == {
            "state": {
                "last_sequence_number": 1,
            },
        }
        assert len(partition_2_events) == 1
        assert partition_2_events[0].payload == {
            "state": {
                "last_sequence_number": 3,
            },
        }

    async def test_consistent_concurrent_execution_when_existing_partitioned_state_events_all_partitions(
        self,
    ):
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_category = event_store.category(
            category=data.random_event_category_name()
        )

        partition_1 = "a"
        partition_2 = "b"
        partition_3 = "c"

        await state_category.stream(stream=partition_1).publish(
            events=[
                (
                    NewEventBuilder()
                    .with_payload(
                        {
                            "state": {
                                "last_sequence_number": 1,
                            }
                        }
                    )
                    .build()
                ),
            ]
        )
        await state_category.stream(stream=partition_2).publish(
            events=[
                (
                    NewEventBuilder()
                    .with_payload(
                        {
                            "state": {
                                "last_sequence_number": 2,
                            }
                        }
                    )
                    .build()
                ),
            ]
        )

        state_store = EventConsumerStateStore(
            category=state_category,
            converter=StoredEventEventConsumerStateConverter(),
            persistence_interval=EventCount(2),
        )

        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(3).build(),
            partition=partition_2,
        )
        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(4).build(),
            partition=partition_3,
        )

        results = await asyncio.gather(
            state_store.save(), state_store.save(), return_exceptions=True
        )

        partition_1_events = await state_category.stream(
            stream=partition_1
        ).read()
        partition_2_events = await state_category.stream(
            stream=partition_2
        ).read()
        partition_3_events = await state_category.stream(
            stream=partition_3
        ).read()

        successful_writes: Sequence[None] = [
            result
            for result in results
            if not isinstance(result, BaseException)
        ]
        failed_writes: Sequence[BaseException] = [
            result for result in results if isinstance(result, Exception)
        ]

        assert len(successful_writes) == 1
        assert len(failed_writes) == 1

        assert isinstance(failed_writes[0], UnmetWriteConditionError)

        assert len(partition_1_events) == 1
        assert partition_1_events[0].payload == {
            "state": {
                "last_sequence_number": 1,
            },
        }
        assert len(partition_2_events) == 2
        assert partition_2_events[0].payload == {
            "state": {
                "last_sequence_number": 2,
            },
        }
        assert partition_2_events[1].payload == {
            "state": {
                "last_sequence_number": 3,
            },
        }

        assert len(partition_3_events) == 1
        assert partition_3_events[0].payload == {
            "state": {
                "last_sequence_number": 4,
            },
        }

    async def test_consistent_concurrent_execution_when_stored_partitioned_state_specific_partitions(
        self,
    ):
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_category = event_store.category(
            category=data.random_event_category_name()
        )
        state_store = EventConsumerStateStore(
            category=state_category,
            converter=StoredEventEventConsumerStateConverter(),
            persistence_interval=EventCount(2),
        )

        partition_1 = "a"
        partition_2 = "b"

        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(1).build(),
            partition=partition_1,
        )
        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(2).build(),
            partition=partition_2,
        )

        results = await asyncio.gather(
            state_store.save(partition=partition_1),
            state_store.save(partition=partition_1),
            return_exceptions=True,
        )

        partition_1_events = await state_category.stream(
            stream=partition_1
        ).read()

        successful_writes: Sequence[None] = [
            result
            for result in results
            if not isinstance(result, BaseException)
        ]
        failed_writes: Sequence[BaseException] = [
            result for result in results if isinstance(result, Exception)
        ]

        assert len(successful_writes) == 1
        assert len(failed_writes) == 1

        assert isinstance(failed_writes[0], UnmetWriteConditionError)

        assert len(partition_1_events) == 1
        assert partition_1_events[0].payload == {
            "state": {
                "last_sequence_number": 1,
            },
        }

    async def test_consistent_concurrent_execution_when_no_stored_partitioned_state_specific_partitions(
        self,
    ):
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_category = event_store.category(
            category=data.random_event_category_name()
        )
        state_store = EventConsumerStateStore(
            category=state_category,
            converter=StoredEventEventConsumerStateConverter(),
            persistence_interval=EventCount(2),
        )

        partition_1 = "a"
        partition_2 = "b"

        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(1).build(),
            partition=partition_1,
        )
        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(2).build(),
            partition=partition_2,
        )
        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(3).build(),
            partition=partition_1,
        )

        results = await asyncio.gather(
            state_store.save(partition=partition_1),
            state_store.save(partition=partition_1),
            return_exceptions=True,
        )

        partition_1_events = await state_category.stream(
            stream=partition_1
        ).read()

        successful_writes: Sequence[None] = [
            result
            for result in results
            if not isinstance(result, BaseException)
        ]
        failed_writes: Sequence[BaseException] = [
            result for result in results if isinstance(result, Exception)
        ]

        assert len(successful_writes) == 2
        assert len(failed_writes) == 0

        assert len(partition_1_events) == 1
        assert partition_1_events[0].payload == {
            "state": {
                "last_sequence_number": 3,
            },
        }

    async def test_consistent_concurrent_execution_when_existing_partitioned_state_events_specific_partitions(
        self,
    ):
        event_store = EventStore(adapter=InMemoryEventStorageAdapter())
        state_category = event_store.category(
            category=data.random_event_category_name()
        )

        partition_1 = "a"
        partition_2 = "b"
        partition_3 = "c"

        await state_category.stream(stream=partition_1).publish(
            events=[
                (
                    NewEventBuilder()
                    .with_payload(
                        {
                            "state": {
                                "last_sequence_number": 1,
                            }
                        }
                    )
                    .build()
                )
            ]
        )
        await state_category.stream(stream=partition_2).publish(
            events=[
                (
                    NewEventBuilder()
                    .with_payload(
                        {
                            "state": {
                                "last_sequence_number": 2,
                            }
                        }
                    )
                    .build()
                )
            ]
        )

        state_store = EventConsumerStateStore(
            category=state_category,
            converter=StoredEventEventConsumerStateConverter(),
            persistence_interval=EventCount(2),
        )

        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(3).build(),
            partition=partition_2,
        )
        await state_store.record_processed(
            event=StoredEventBuilder().with_sequence_number(4).build(),
            partition=partition_3,
        )

        results = await asyncio.gather(
            state_store.save(partition=partition_2),
            state_store.save(partition=partition_2),
            return_exceptions=True,
        )

        partition_1_events = await state_category.stream(
            stream=partition_1
        ).read()
        partition_2_events = await state_category.stream(
            stream=partition_2
        ).read()
        partition_3_events = await state_category.stream(
            stream=partition_3
        ).read()

        successful_writes: Sequence[None] = [
            result
            for result in results
            if not isinstance(result, BaseException)
        ]
        failed_writes: Sequence[BaseException] = [
            result for result in results if isinstance(result, Exception)
        ]

        assert len(successful_writes) == 1
        assert len(failed_writes) == 1

        assert len(partition_1_events) == 1
        assert partition_1_events[0].payload == {
            "state": {
                "last_sequence_number": 1,
            },
        }

        assert len(partition_2_events) == 2
        assert partition_2_events[0].payload == {
            "state": {
                "last_sequence_number": 2,
            },
        }
        assert partition_2_events[1].payload == {
            "state": {
                "last_sequence_number": 3,
            },
        }

        assert len(partition_3_events) == 0
