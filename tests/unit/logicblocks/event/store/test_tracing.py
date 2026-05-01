import pytest

from logicblocks.event.store.tracing import (
    get_tracing_metadata,
    maybe_get_tracing_metadata,
    start_tracing,
)


class TestGetTracingMetadata:
    def test_returns_metadata_with_no_causation_event_id_when_no_context(self):
        metadata = get_tracing_metadata()

        assert metadata.causation_event_id is None

    def test_returns_metadata_with_string_trace_id_when_no_context(self):
        metadata = get_tracing_metadata()

        assert isinstance(metadata.trace_id, str)
        assert len(metadata.trace_id) > 0

    def test_returns_different_trace_ids_on_separate_calls_when_no_context(
        self,
    ):
        metadata_1 = get_tracing_metadata()
        metadata_2 = get_tracing_metadata()

        assert metadata_1.trace_id != metadata_2.trace_id

    def test_returns_active_context_metadata_when_tracing_is_started(self):
        with start_tracing(event_id="event-123") as active:
            metadata = get_tracing_metadata()

            assert metadata == active


class TestStartTracing:
    def test_sets_causation_event_id_from_provided_event_id(self):
        with start_tracing(event_id="source-event-id") as metadata:
            assert metadata.causation_event_id == "source-event-id"

    def test_sets_causation_event_id_to_none_when_event_id_is_none(self):
        with start_tracing(event_id=None) as metadata:
            assert metadata.causation_event_id is None

    def test_generates_string_trace_id_when_no_existing_context(self):
        with start_tracing(event_id="event-1") as metadata:
            assert isinstance(metadata.trace_id, str)
            assert len(metadata.trace_id) > 0

    def test_preserves_trace_id_from_existing_context(self):
        with start_tracing(event_id="outer-event") as outer:
            with start_tracing(event_id="inner-event") as inner:
                assert inner.trace_id == outer.trace_id

    def test_makes_metadata_available_via_get_tracing_metadata(self):
        with start_tracing(event_id="source-event-id"):
            metadata = get_tracing_metadata()

            assert metadata.causation_event_id == "source-event-id"

    def test_resets_context_after_block_exits(self):
        with start_tracing(event_id="event-1"):
            pass

        assert maybe_get_tracing_metadata() is None

    def test_restores_previous_context_after_block_exits(self):
        with start_tracing(event_id="outer-event") as outer:
            with start_tracing(event_id="inner-event"):
                pass

            metadata = get_tracing_metadata()
            assert metadata == outer

    def test_resets_context_even_if_exception_is_raised(self):
        with start_tracing(event_id="outer") as outer:
            with pytest.raises(RuntimeError):
                with start_tracing(event_id="failing"):
                    raise RuntimeError("something went wrong")

            metadata = get_tracing_metadata()
            assert metadata == outer

    def test_supports_nested_tracing_with_shared_trace_id(self):
        with start_tracing(event_id="outer-event") as outer:
            with start_tracing(event_id="inner-event") as inner:
                assert inner.trace_id == outer.trace_id
                assert inner.causation_event_id == "inner-event"

            restored = get_tracing_metadata()
            assert restored.causation_event_id == "outer-event"
