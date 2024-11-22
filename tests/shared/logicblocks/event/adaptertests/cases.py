from abc import ABC, abstractmethod
from collections.abc import Sequence

import pytest

from logicblocks.event.store import conditions
from logicblocks.event.store.adapters import StorageAdapter
from logicblocks.event.store.exceptions import UnmetWriteConditionError
from logicblocks.event.testing import NewEventBuilder
from logicblocks.event.testing.data import (
    random_event_category_name,
    random_event_stream_name,
)
from logicblocks.event.types import StoredEvent, identifier


class Base(ABC):
    @property
    @abstractmethod
    def adapter(self) -> StorageAdapter:
        raise NotImplementedError()

    @abstractmethod
    def retrieve_events(
        self, category: str | None = None, stream: str | None = None
    ) -> Sequence[StoredEvent]:
        raise NotImplementedError()


class SaveCases(Base, ABC):
    def test_stores_single_event_for_later_retrieval(self):
        event_category = random_event_category_name()
        event_stream = random_event_stream_name()

        new_event = NewEventBuilder().build()

        stored_events = self.adapter.save(
            target=identifier.Stream(
                category=event_category, stream=event_stream
            ),
            events=[new_event],
        )
        stored_event = stored_events[0]

        actual_events = self.retrieve_events()
        expected_events = [
            StoredEvent(
                id=stored_event.id,
                name=new_event.name,
                category=event_category,
                stream=event_stream,
                position=0,
                sequence_number=stored_event.sequence_number,
                payload=new_event.payload,
                observed_at=new_event.observed_at,
                occurred_at=new_event.occurred_at,
            )
        ]

        assert actual_events == expected_events

    def test_stores_multiple_events_in_same_stream(self):
        event_category = random_event_category_name()
        event_stream = random_event_stream_name()

        new_event_1 = NewEventBuilder().build()
        new_event_2 = NewEventBuilder().build()

        stored_events = self.adapter.save(
            target=identifier.Stream(
                category=event_category, stream=event_stream
            ),
            events=[new_event_1, new_event_2],
        )
        stored_event_1 = stored_events[0]
        stored_event_2 = stored_events[1]

        actual_events = self.retrieve_events()
        expected_events = [
            StoredEvent(
                id=stored_event_1.id,
                name=new_event_1.name,
                category=event_category,
                stream=event_stream,
                position=0,
                sequence_number=stored_event_1.sequence_number,
                payload=new_event_1.payload,
                observed_at=new_event_1.observed_at,
                occurred_at=new_event_1.occurred_at,
            ),
            StoredEvent(
                id=stored_event_2.id,
                name=new_event_2.name,
                category=event_category,
                stream=event_stream,
                position=1,
                sequence_number=stored_event_2.sequence_number,
                payload=new_event_2.payload,
                observed_at=new_event_2.observed_at,
                occurred_at=new_event_2.occurred_at,
            ),
        ]

        assert actual_events == expected_events

    def test_stores_multiple_events_in_sequential_saves(self):
        event_category = random_event_category_name()
        event_stream = random_event_stream_name()

        new_event_1 = NewEventBuilder().build()
        new_event_2 = NewEventBuilder().build()

        stored_events_1 = self.adapter.save(
            target=identifier.Stream(
                category=event_category, stream=event_stream
            ),
            events=[new_event_1],
        )
        stored_event_1 = stored_events_1[0]

        stored_events_2 = self.adapter.save(
            target=identifier.Stream(
                category=event_category, stream=event_stream
            ),
            events=[new_event_2],
        )
        stored_event_2 = stored_events_2[0]

        actual_events = self.retrieve_events()
        expected_events = [
            StoredEvent(
                id=stored_event_1.id,
                name=new_event_1.name,
                category=event_category,
                stream=event_stream,
                position=0,
                sequence_number=stored_event_1.sequence_number,
                payload=new_event_1.payload,
                observed_at=new_event_1.observed_at,
                occurred_at=new_event_1.occurred_at,
            ),
            StoredEvent(
                id=stored_event_2.id,
                name=new_event_2.name,
                category=event_category,
                stream=event_stream,
                position=1,
                sequence_number=stored_event_2.sequence_number,
                payload=new_event_2.payload,
                observed_at=new_event_2.observed_at,
                occurred_at=new_event_2.occurred_at,
            ),
        ]

        assert actual_events == expected_events


class WriteConditionCases(Base, ABC):
    def test_writes_if_empty_stream_condition_and_stream_empty(self):
        event_category = random_event_category_name()
        event_stream = random_event_stream_name()

        new_event = NewEventBuilder().build()

        stored_events = self.adapter.save(
            target=identifier.Stream(
                category=event_category, stream=event_stream
            ),
            events=[new_event],
            conditions={conditions.stream_is_empty()},
        )
        stored_event = stored_events[0]

        actual_events = self.retrieve_events(
            category=event_category, stream=event_stream
        )
        expected_events = [
            StoredEvent(
                id=stored_event.id,
                name=new_event.name,
                category=event_category,
                stream=event_stream,
                position=0,
                sequence_number=stored_event.sequence_number,
                payload=new_event.payload,
                observed_at=new_event.observed_at,
                occurred_at=new_event.occurred_at,
            )
        ]

        assert actual_events == expected_events

    def test_writes_if_empty_stream_condition_and_category_not_empty(self):
        event_category = random_event_category_name()
        event_stream_1 = random_event_stream_name()
        event_stream_2 = random_event_stream_name()

        new_event_1 = NewEventBuilder().build()
        new_event_2 = NewEventBuilder().build()

        self.adapter.save(
            target=identifier.Stream(
                category=event_category, stream=event_stream_1
            ),
            events=[new_event_1],
        )

        stored_events = self.adapter.save(
            target=identifier.Stream(
                category=event_category, stream=event_stream_2
            ),
            events=[new_event_2],
            conditions={conditions.stream_is_empty()},
        )
        stored_event = stored_events[0]

        actual_records = self.retrieve_events(
            category=event_category, stream=event_stream_2
        )
        expected_records = [
            StoredEvent(
                id=stored_event.id,
                name=new_event_2.name,
                category=event_category,
                stream=event_stream_2,
                position=0,
                sequence_number=stored_event.sequence_number,
                payload=new_event_2.payload,
                observed_at=new_event_2.observed_at,
                occurred_at=new_event_2.occurred_at,
            )
        ]

        assert actual_records == expected_records

    def test_writes_if_empty_stream_condition_and_log_not_empty(self):
        event_category_1 = random_event_category_name()
        event_category_2 = random_event_category_name()
        event_stream_1 = random_event_stream_name()
        event_stream_2 = random_event_stream_name()

        new_event_1 = NewEventBuilder().build()
        new_event_2 = NewEventBuilder().build()

        self.adapter.save(
            target=identifier.Stream(
                category=event_category_1, stream=event_stream_1
            ),
            events=[new_event_1],
        )

        stored_events = self.adapter.save(
            target=identifier.Stream(
                category=event_category_2, stream=event_stream_2
            ),
            events=[new_event_2],
            conditions={conditions.stream_is_empty()},
        )
        stored_event = stored_events[0]

        actual_records = self.retrieve_events(
            category=event_category_2, stream=event_stream_2
        )
        expected_records = [
            StoredEvent(
                id=stored_event.id,
                name=new_event_2.name,
                category=event_category_2,
                stream=event_stream_2,
                position=0,
                sequence_number=stored_event.sequence_number,
                payload=new_event_2.payload,
                observed_at=new_event_2.observed_at,
                occurred_at=new_event_2.occurred_at,
            )
        ]

        assert actual_records == expected_records

    def test_raises_if_empty_stream_condition_and_stream_not_empty(self):
        event_category = random_event_category_name()
        event_stream = random_event_stream_name()

        self.adapter.save(
            target=identifier.Stream(
                category=event_category, stream=event_stream
            ),
            events=[NewEventBuilder().build()],
        )

        with pytest.raises(UnmetWriteConditionError):
            self.adapter.save(
                target=identifier.Stream(
                    category=event_category, stream=event_stream
                ),
                events=[NewEventBuilder().build()],
                conditions={conditions.stream_is_empty()},
            )

    def test_writes_if_position_condition_and_correct_position(self):
        event_category = random_event_category_name()
        event_stream = random_event_stream_name()

        new_event_1 = NewEventBuilder().build()
        new_event_2 = NewEventBuilder().build()

        stored_events_1 = self.adapter.save(
            target=identifier.Stream(
                category=event_category, stream=event_stream
            ),
            events=[new_event_1],
        )
        stored_event_1 = stored_events_1[0]

        stored_events_2 = self.adapter.save(
            target=identifier.Stream(
                category=event_category, stream=event_stream
            ),
            events=[new_event_2],
            conditions={conditions.position_is(0)},
        )

        stored_event_2 = stored_events_2[0]

        actual_records = self.retrieve_events()
        expected_records = [
            StoredEvent(
                id=stored_event_1.id,
                name=new_event_1.name,
                category=event_category,
                stream=event_stream,
                position=0,
                sequence_number=stored_event_1.sequence_number,
                payload=new_event_1.payload,
                observed_at=new_event_1.observed_at,
                occurred_at=new_event_1.occurred_at,
            ),
            StoredEvent(
                id=stored_event_2.id,
                name=new_event_2.name,
                category=event_category,
                stream=event_stream,
                position=1,
                sequence_number=stored_event_2.sequence_number,
                payload=new_event_2.payload,
                observed_at=new_event_2.observed_at,
                occurred_at=new_event_2.occurred_at,
            ),
        ]

        assert actual_records == expected_records

    def test_raises_if_position_condition_and_less_than_expected(self):
        self.adapter.save(
            target=identifier.Stream(
                category=random_event_category_name(),
                stream=random_event_stream_name(),
            ),
            events=[NewEventBuilder().build()],
        )

        with pytest.raises(UnmetWriteConditionError):
            self.adapter.save(
                target=identifier.Stream(
                    category=random_event_category_name(),
                    stream=random_event_stream_name(),
                ),
                events=[NewEventBuilder().build()],
                conditions={conditions.position_is(1)},
            )

    def test_raises_if_position_condition_and_greater_than_expected(self):
        self.adapter.save(
            target=identifier.Stream(
                category=random_event_category_name(),
                stream=random_event_stream_name(),
            ),
            events=[
                NewEventBuilder().build(),
                NewEventBuilder().build(),
                NewEventBuilder().build(),
            ],
        )

        with pytest.raises(UnmetWriteConditionError):
            self.adapter.save(
                target=identifier.Stream(
                    category=random_event_category_name(),
                    stream=random_event_stream_name(),
                ),
                events=[NewEventBuilder().build()],
                conditions={conditions.position_is(1)},
            )

    def test_raises_if_position_condition_and_stream_empty(self):
        with pytest.raises(UnmetWriteConditionError):
            self.adapter.save(
                target=identifier.Stream(
                    category=random_event_category_name(),
                    stream=random_event_stream_name(),
                ),
                events=[NewEventBuilder().build()],
                conditions={conditions.position_is(0)},
            )


class ScanCases(Base, ABC):
    def test_log_scan_scans_no_events_when_store_empty(self):
        scanned_events = list(self.adapter.scan(target=identifier.Log()))

        assert scanned_events == []

    def test_log_scan_scans_single_event_in_single_stream(self):
        stored_events = self.adapter.save(
            target=identifier.Stream(
                category=random_event_category_name(),
                stream=random_event_stream_name(),
            ),
            events=[NewEventBuilder().build()],
        )

        scanned_events = list(self.adapter.scan(target=identifier.Log()))

        assert scanned_events == stored_events

    def test_log_scan_scans_multiple_events_in_single_stream(self):
        stored_events = self.adapter.save(
            target=identifier.Stream(
                category=random_event_category_name(),
                stream=random_event_stream_name(),
            ),
            events=[
                NewEventBuilder().build(),
                NewEventBuilder().build(),
                NewEventBuilder().build(),
            ],
        )

        scanned_events = list(self.adapter.scan(target=identifier.Log()))

        assert scanned_events == stored_events

    def test_log_scan_scans_events_across_streams_in_sequence_order(self):
        event_category = random_event_category_name()
        event_stream_1 = random_event_stream_name()
        event_stream_2 = random_event_stream_name()

        stored_events_1 = self.adapter.save(
            target=identifier.Stream(
                category=event_category, stream=event_stream_1
            ),
            events=[
                NewEventBuilder().build(),
                NewEventBuilder().build(),
            ],
        )
        stored_events_2 = self.adapter.save(
            target=identifier.Stream(
                category=event_category, stream=event_stream_1
            ),
            events=[NewEventBuilder().build()],
        )
        stored_events_3 = self.adapter.save(
            target=identifier.Stream(
                category=event_category, stream=event_stream_2
            ),
            events=[
                NewEventBuilder().build(),
                NewEventBuilder().build(),
            ],
        )
        stored_events_4 = self.adapter.save(
            target=identifier.Stream(
                category=event_category, stream=event_stream_2
            ),
            events=[NewEventBuilder().build()],
        )

        stored_events = (
            list(stored_events_1)
            + list(stored_events_2)
            + list(stored_events_3)
            + list(stored_events_4)
        )
        scanned_events = list(self.adapter.scan(target=identifier.Log()))

        assert scanned_events == stored_events

    def test_log_scan_scans_events_across_categories_in_sequence_order(self):
        event_category_1 = random_event_category_name()
        event_category_2 = random_event_category_name()
        event_stream_1 = random_event_stream_name()
        event_stream_2 = random_event_stream_name()

        stored_events_1 = self.adapter.save(
            target=identifier.Stream(
                category=event_category_1, stream=event_stream_1
            ),
            events=[
                NewEventBuilder().build(),
                NewEventBuilder().build(),
            ],
        )
        stored_events_2 = self.adapter.save(
            target=identifier.Stream(
                category=event_category_1, stream=event_stream_1
            ),
            events=[NewEventBuilder().build()],
        )
        stored_events_3 = self.adapter.save(
            target=identifier.Stream(
                category=event_category_2, stream=event_stream_2
            ),
            events=[
                NewEventBuilder().build(),
                NewEventBuilder().build(),
            ],
        )
        stored_events_4 = self.adapter.save(
            target=identifier.Stream(
                category=event_category_2, stream=event_stream_2
            ),
            events=[NewEventBuilder().build()],
        )

        stored_events = (
            list(stored_events_1)
            + list(stored_events_2)
            + list(stored_events_3)
            + list(stored_events_4)
        )
        scanned_events = list(self.adapter.scan(target=identifier.Log()))

        assert scanned_events == stored_events

    def test_category_scan_scans_no_events_when_store_empty(self):
        scanned_events = list(
            self.adapter.scan(
                target=identifier.Category(
                    category=random_event_category_name()
                )
            )
        )

        assert scanned_events == []

    def test_category_scan_scans_no_events_when_category_empty(self):
        scan_event_category = random_event_category_name()
        other_event_category = random_event_category_name()

        self.adapter.save(
            target=identifier.Stream(
                category=other_event_category,
                stream=random_event_stream_name(),
            ),
            events=[NewEventBuilder().build()],
        )

        scanned_events = list(
            self.adapter.scan(
                target=identifier.Category(category=scan_event_category)
            )
        )

        assert scanned_events == []

    def test_category_scan_scans_single_event_in_single_stream(self):
        event_category = random_event_category_name()
        event_stream = random_event_stream_name()

        stored_events = self.adapter.save(
            target=identifier.Stream(
                category=event_category, stream=event_stream
            ),
            events=[NewEventBuilder().build()],
        )

        scanned_events = list(
            self.adapter.scan(
                target=identifier.Category(category=event_category)
            )
        )

        assert scanned_events == stored_events

    def test_category_scan_scans_multiple_events_in_single_stream(self):
        event_category = random_event_category_name()
        event_stream = random_event_stream_name()

        stored_events = self.adapter.save(
            target=identifier.Stream(
                category=event_category,
                stream=event_stream,
            ),
            events=[
                NewEventBuilder().build(),
                NewEventBuilder().build(),
                NewEventBuilder().build(),
            ],
        )

        scanned_events = list(
            self.adapter.scan(
                target=identifier.Category(category=event_category)
            )
        )

        assert scanned_events == stored_events

    def test_category_scan_scans_events_across_streams_in_sequence_order(self):
        event_category = random_event_category_name()
        event_stream_1 = random_event_stream_name()
        event_stream_2 = random_event_stream_name()

        stored_events_1 = self.adapter.save(
            target=identifier.Stream(
                category=event_category, stream=event_stream_1
            ),
            events=[
                NewEventBuilder().build(),
                NewEventBuilder().build(),
            ],
        )
        stored_events_2 = self.adapter.save(
            target=identifier.Stream(
                category=event_category, stream=event_stream_1
            ),
            events=[NewEventBuilder().build()],
        )
        stored_events_3 = self.adapter.save(
            target=identifier.Stream(
                category=event_category, stream=event_stream_2
            ),
            events=[
                NewEventBuilder().build(),
                NewEventBuilder().build(),
            ],
        )
        stored_events_4 = self.adapter.save(
            target=identifier.Stream(
                category=event_category, stream=event_stream_2
            ),
            events=[NewEventBuilder().build()],
        )

        stored_events = (
            list(stored_events_1)
            + list(stored_events_2)
            + list(stored_events_3)
            + list(stored_events_4)
        )
        scanned_events = list(
            self.adapter.scan(
                target=identifier.Category(category=event_category)
            )
        )

        assert scanned_events == stored_events

    def test_category_scan_ignores_events_in_other_categories(self):
        event_category_1 = random_event_category_name()
        event_category_2 = random_event_category_name()
        event_stream_1 = random_event_stream_name()
        event_stream_2 = random_event_stream_name()

        stored_events_1 = self.adapter.save(
            target=identifier.Stream(
                category=event_category_1, stream=event_stream_1
            ),
            events=[
                NewEventBuilder().build(),
                NewEventBuilder().build(),
            ],
        )
        self.adapter.save(
            target=identifier.Stream(
                category=event_category_2, stream=event_stream_2
            ),
            events=[
                NewEventBuilder().build(),
                NewEventBuilder().build(),
            ],
        )
        stored_events_3 = self.adapter.save(
            target=identifier.Stream(
                category=event_category_1, stream=event_stream_1
            ),
            events=[NewEventBuilder().build()],
        )
        self.adapter.save(
            target=identifier.Stream(
                category=event_category_2, stream=event_stream_2
            ),
            events=[NewEventBuilder().build()],
        )

        stored_events = list(stored_events_1) + list(stored_events_3)
        scanned_events = list(
            self.adapter.scan(
                target=identifier.Category(category=event_category_1)
            )
        )

        assert scanned_events == stored_events

    def test_stream_scan_scans_no_events_when_store_empty(self):
        scanned_events = list(
            self.adapter.scan(
                target=identifier.Stream(
                    category=random_event_category_name(),
                    stream=random_event_stream_name(),
                )
            )
        )

        assert scanned_events == []

    def test_stream_scan_scans_no_events_when_stream_empty(self):
        event_category = random_event_category_name()
        scan_event_stream = random_event_stream_name()
        other_event_stream = random_event_stream_name()

        self.adapter.save(
            target=identifier.Stream(
                category=event_category, stream=other_event_stream
            ),
            events=[NewEventBuilder().build()],
        )

        scanned_events = list(
            self.adapter.scan(
                target=identifier.Stream(
                    category=event_category,
                    stream=scan_event_stream,
                )
            )
        )

        assert scanned_events == []

    def test_stream_scan_scans_single_event_in_single_stream(self):
        event_category = random_event_category_name()
        event_stream = random_event_stream_name()

        stored_events = self.adapter.save(
            target=identifier.Stream(
                category=event_category, stream=event_stream
            ),
            events=[NewEventBuilder().build()],
        )

        scanned_events = list(
            self.adapter.scan(
                target=identifier.Stream(
                    category=event_category, stream=event_stream
                )
            )
        )

        assert scanned_events == stored_events

    def test_stream_scan_scans_multiple_events_in_single_stream(self):
        event_category = random_event_category_name()
        event_stream = random_event_stream_name()

        stored_events = self.adapter.save(
            target=identifier.Stream(
                category=event_category, stream=event_stream
            ),
            events=[
                NewEventBuilder().build(),
                NewEventBuilder().build(),
                NewEventBuilder().build(),
            ],
        )

        scanned_events = list(
            self.adapter.scan(
                target=identifier.Stream(
                    category=event_category, stream=event_stream
                )
            )
        )

        assert scanned_events == stored_events

    def test_stream_scan_scans_events_within_stream_in_sequence_order(self):
        event_category = random_event_category_name()
        event_stream = random_event_stream_name()

        stored_events_1 = self.adapter.save(
            target=identifier.Stream(
                category=event_category, stream=event_stream
            ),
            events=[
                NewEventBuilder().build(),
                NewEventBuilder().build(),
            ],
        )
        stored_events_2 = self.adapter.save(
            target=identifier.Stream(
                category=event_category, stream=event_stream
            ),
            events=[NewEventBuilder().build()],
        )
        stored_events_3 = self.adapter.save(
            target=identifier.Stream(
                category=event_category, stream=event_stream
            ),
            events=[
                NewEventBuilder().build(),
                NewEventBuilder().build(),
            ],
        )
        stored_events_4 = self.adapter.save(
            target=identifier.Stream(
                category=event_category, stream=event_stream
            ),
            events=[NewEventBuilder().build()],
        )

        stored_events = (
            list(stored_events_1)
            + list(stored_events_2)
            + list(stored_events_3)
            + list(stored_events_4)
        )
        scanned_events = list(
            self.adapter.scan(
                target=identifier.Stream(
                    category=event_category, stream=event_stream
                )
            )
        )

        assert scanned_events == stored_events

    def test_stream_scan_ignores_events_in_other_streams(self):
        event_category = random_event_category_name()
        event_stream_1 = random_event_stream_name()
        event_stream_2 = random_event_stream_name()

        stored_events_1 = self.adapter.save(
            target=identifier.Stream(
                category=event_category, stream=event_stream_1
            ),
            events=[
                NewEventBuilder().build(),
                NewEventBuilder().build(),
            ],
        )
        self.adapter.save(
            target=identifier.Stream(
                category=event_category, stream=event_stream_2
            ),
            events=[
                NewEventBuilder().build(),
                NewEventBuilder().build(),
            ],
        )
        stored_events_3 = self.adapter.save(
            target=identifier.Stream(
                category=event_category, stream=event_stream_1
            ),
            events=[NewEventBuilder().build()],
        )
        self.adapter.save(
            target=identifier.Stream(
                category=event_category, stream=event_stream_2
            ),
            events=[NewEventBuilder().build()],
        )

        stored_events = list(stored_events_1) + list(stored_events_3)
        scanned_events = list(
            self.adapter.scan(
                target=identifier.Stream(
                    category=event_category, stream=event_stream_1
                )
            )
        )

        assert scanned_events == stored_events

    def test_stream_scan_ignores_events_in_other_categories(self):
        event_category_1 = random_event_category_name()
        event_category_2 = random_event_category_name()
        event_stream = random_event_stream_name()

        stored_events_1 = self.adapter.save(
            target=identifier.Stream(
                category=event_category_1, stream=event_stream
            ),
            events=[
                NewEventBuilder().build(),
                NewEventBuilder().build(),
            ],
        )
        self.adapter.save(
            target=identifier.Stream(
                category=event_category_2, stream=event_stream
            ),
            events=[
                NewEventBuilder().build(),
                NewEventBuilder().build(),
            ],
        )
        stored_events_3 = self.adapter.save(
            target=identifier.Stream(
                category=event_category_1, stream=event_stream
            ),
            events=[NewEventBuilder().build()],
        )
        self.adapter.save(
            target=identifier.Stream(
                category=event_category_2, stream=event_stream
            ),
            events=[NewEventBuilder().build()],
        )

        stored_events = list(stored_events_1) + list(stored_events_3)
        scanned_events = list(
            self.adapter.scan(
                target=identifier.Stream(
                    category=event_category_1, stream=event_stream
                )
            )
        )

        assert scanned_events == stored_events


class StorageAdapterCases(SaveCases, WriteConditionCases, ScanCases, ABC):
    pass
