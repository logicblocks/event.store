from logicblocks.event.store import EventStore
from logicblocks.event.store.adapters import InMemoryEventStorageAdapter
from logicblocks.event.store.tracing import start_tracing
from logicblocks.event.testing import NewEventBuilder, data


class TestStreamPublishTracingMetadata:
    async def test_includes_tracing_metadata_on_stored_events(self):
        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()

        store = EventStore(adapter=InMemoryEventStorageAdapter())
        stream = store.stream(category=category_name, stream=stream_name)

        stored_events = await stream.publish(
            events=[NewEventBuilder().build()]
        )

        assert "tracing" in stored_events[0].metadata

    async def test_includes_trace_id_in_tracing_metadata(self):
        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()

        store = EventStore(adapter=InMemoryEventStorageAdapter())
        stream = store.stream(category=category_name, stream=stream_name)

        stored_events = await stream.publish(
            events=[NewEventBuilder().build()]
        )

        trace_id = stored_events[0].metadata["tracing"]["trace_id"]
        assert isinstance(trace_id, str)
        assert len(trace_id) > 0

    async def test_includes_no_causation_event_id_when_no_tracing_context(
        self,
    ):
        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()

        store = EventStore(adapter=InMemoryEventStorageAdapter())
        stream = store.stream(category=category_name, stream=stream_name)

        stored_events = await stream.publish(
            events=[NewEventBuilder().build()]
        )

        causation_event_id = stored_events[0].metadata["tracing"][
            "causation_event_id"
        ]
        assert causation_event_id is None

    async def test_includes_causation_event_id_from_active_tracing_context(
        self,
    ):
        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()

        store = EventStore(adapter=InMemoryEventStorageAdapter())
        stream = store.stream(category=category_name, stream=stream_name)

        with start_tracing(event_id="causing-event-id"):
            stored_events = await stream.publish(
                events=[NewEventBuilder().build()]
            )

        causation_event_id = stored_events[0].metadata["tracing"][
            "causation_event_id"
        ]
        assert causation_event_id == "causing-event-id"

    async def test_shares_trace_id_from_active_tracing_context(self):
        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()

        store = EventStore(adapter=InMemoryEventStorageAdapter())
        stream = store.stream(category=category_name, stream=stream_name)

        with start_tracing(event_id="causing-event-id") as tracing:
            stored_events = await stream.publish(
                events=[NewEventBuilder().build()]
            )

        trace_id = stored_events[0].metadata["tracing"]["trace_id"]
        assert trace_id == tracing.trace_id

    async def test_applies_same_metadata_to_all_events_in_batch(self):
        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()

        store = EventStore(adapter=InMemoryEventStorageAdapter())
        stream = store.stream(category=category_name, stream=stream_name)

        with start_tracing(event_id="causing-event-id") as tracing:
            stored_events = await stream.publish(
                events=[
                    NewEventBuilder().build(),
                    NewEventBuilder().build(),
                    NewEventBuilder().build(),
                ]
            )

        for event in stored_events:
            assert (
                event.metadata["tracing"]["causation_event_id"]
                == "causing-event-id"
            )
            assert event.metadata["tracing"]["trace_id"] == tracing.trace_id

    async def test_tracing_metadata_persists_through_read(self):
        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()

        store = EventStore(adapter=InMemoryEventStorageAdapter())
        stream = store.stream(category=category_name, stream=stream_name)

        with start_tracing(event_id="causing-event-id") as tracing:
            await stream.publish(events=[NewEventBuilder().build()])

        read_events = await stream.read()

        assert read_events[0].metadata["tracing"]["trace_id"] == (
            tracing.trace_id
        )
        assert read_events[0].metadata["tracing"]["causation_event_id"] == (
            "causing-event-id"
        )

    async def test_generates_unique_trace_ids_for_independent_publishes(self):
        category_name = data.random_event_category_name()
        stream_name = data.random_event_stream_name()

        store = EventStore(adapter=InMemoryEventStorageAdapter())
        stream = store.stream(category=category_name, stream=stream_name)

        stored_events_1 = await stream.publish(
            events=[NewEventBuilder().build()]
        )
        stored_events_2 = await stream.publish(
            events=[NewEventBuilder().build()]
        )

        trace_id_1 = stored_events_1[0].metadata["tracing"]["trace_id"]
        trace_id_2 = stored_events_2[0].metadata["tracing"]["trace_id"]
        assert trace_id_1 != trace_id_2
