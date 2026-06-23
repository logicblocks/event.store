# Event Metadata Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an optional, generically-typed `metadata` field to events, folded into the existing `payload` JSONB column as a reserved `__metadata` sibling key — no schema migration, non-breaking for events that carry no metadata.

**Architecture:** A third generic parameter `Metadata` is added to `NewEvent` and `StoredEvent` (mirroring `Projection[State, Metadata]`). Pure wrap/unwrap/guard helper functions live in the types layer. Both storage adapters (memory + Postgres) call those helpers: on write they fold non-empty metadata into the payload object as `__metadata`; on read they split it back out. New no-metadata rows and legacy pre-feature rows are byte-identical, so a single detect-on-read path covers both.

**Tech Stack:** Python 3.13, `psycopg`/`psycopg_pool` (Postgres adapter), `pytest`/`pytest-asyncio`, `mise` task runner.

---

## Storage Shape Reference

| Case | Stored `payload` JSONB | Read result |
|---|---|---|
| No metadata (default) | `{prop1, prop2}` (unchanged from today) | `payload={prop1,prop2}`, `metadata={}` |
| Metadata present | `{"__metadata": {...}, prop1, prop2}` | `payload={prop1,prop2}`, `metadata={...}` |
| Legacy row (pre-feature) | `{prop1, prop2}` | `payload={prop1,prop2}`, `metadata={}` |

**Write guards (raise only when metadata is supplied):**
1. Non-object payload + metadata → raise.
2. Payload already contains `__metadata` key + metadata → raise.

---

## File Structure

**New files:**
- `src/logicblocks/event/types/metadata.py` — pure helpers (`RESERVED_METADATA_KEY`, `fold_metadata_into_payload`, `split_metadata_from_payload`) + `InvalidEventMetadataError` exception.

**Modified files:**
- `src/logicblocks/event/types/event.py` — add `Metadata` generic + `metadata` field to `NewEvent` and `StoredEvent`; update `serialise`, `summarise`, `__repr__`, `serialise_stored_event`.
- `src/logicblocks/event/types/__init__.py` — export new symbols.
- `src/logicblocks/event/store/adapters/memory/adapter.py` — fold metadata on save, split on the serialised-event construction.
- `src/logicblocks/event/store/adapters/postgres/adapter.py` — fold metadata into the insert payload value; custom row factory to split metadata on read; carry metadata through the `RETURNING *` mapping.
- `src/logicblocks/event/testing/builders.py` — add `metadata` to `NewEventBuilder` and `StoredEventBuilder`.

**Test files:**
- `tests/unit/logicblocks/event/types/test_metadata.py` — helper + guard unit tests (new).
- `tests/unit/logicblocks/event/types/test_event.py` — event field/serialise tests (modify).
- `tests/unit/logicblocks/event/testing/test_builders.py` — builder tests (modify).
- `tests/shared/logicblocks/event/testcases/store/adapters.py` — shared adapter behaviour (modify; runs against both memory + Postgres).

---

## Task 1: Metadata helper module + exception

**Files:**
- Create: `src/logicblocks/event/types/metadata.py`
- Test: `tests/unit/logicblocks/event/types/test_metadata.py`

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/logicblocks/event/types/test_metadata.py`:

```python
import pytest

from logicblocks.event.types.metadata import (
    RESERVED_METADATA_KEY,
    InvalidEventMetadataError,
    fold_metadata_into_payload,
    split_metadata_from_payload,
)


class TestFoldMetadataIntoPayload:
    def test_returns_payload_unchanged_when_metadata_empty(self):
        payload = {"amount": 100, "currency": "USD"}

        result = fold_metadata_into_payload(payload, {})

        assert result == {"amount": 100, "currency": "USD"}

    def test_injects_metadata_as_sibling_key_when_present(self):
        payload = {"amount": 100, "currency": "USD"}

        result = fold_metadata_into_payload(payload, {"actor": "user-123"})

        assert result == {
            "amount": 100,
            "currency": "USD",
            RESERVED_METADATA_KEY: {"actor": "user-123"},
        }

    def test_does_not_mutate_input_payload(self):
        payload = {"amount": 100}

        fold_metadata_into_payload(payload, {"actor": "user-123"})

        assert payload == {"amount": 100}

    def test_raises_when_metadata_supplied_for_non_object_payload(self):
        with pytest.raises(InvalidEventMetadataError):
            fold_metadata_into_payload([1, 2, 3], {"actor": "user-123"})

    def test_raises_when_payload_already_contains_reserved_key(self):
        payload = {"amount": 100, RESERVED_METADATA_KEY: {"x": 1}}

        with pytest.raises(InvalidEventMetadataError):
            fold_metadata_into_payload(payload, {"actor": "user-123"})

    def test_allows_non_object_payload_when_metadata_empty(self):
        result = fold_metadata_into_payload([1, 2, 3], {})

        assert result == [1, 2, 3]


class TestSplitMetadataFromPayload:
    def test_returns_empty_metadata_when_no_reserved_key(self):
        stored = {"amount": 100, "currency": "USD"}

        payload, metadata = split_metadata_from_payload(stored)

        assert payload == {"amount": 100, "currency": "USD"}
        assert metadata == {}

    def test_extracts_metadata_when_reserved_key_present(self):
        stored = {
            "amount": 100,
            "currency": "USD",
            RESERVED_METADATA_KEY: {"actor": "user-123"},
        }

        payload, metadata = split_metadata_from_payload(stored)

        assert payload == {"amount": 100, "currency": "USD"}
        assert metadata == {"actor": "user-123"}

    def test_returns_empty_metadata_for_non_object_payload(self):
        payload, metadata = split_metadata_from_payload([1, 2, 3])

        assert payload == [1, 2, 3]
        assert metadata == {}

    def test_does_not_mutate_input(self):
        stored = {
            "amount": 100,
            RESERVED_METADATA_KEY: {"actor": "user-123"},
        }

        split_metadata_from_payload(stored)

        assert RESERVED_METADATA_KEY in stored
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mise run test:unit[TestFoldMetadataIntoPayload]`
Expected: FAIL with `ModuleNotFoundError: No module named 'logicblocks.event.types.metadata'`

- [ ] **Step 3: Write the implementation**

Create `src/logicblocks/event/types/metadata.py`:

```python
from collections.abc import Mapping

from .json import JsonValue, is_json_object

RESERVED_METADATA_KEY = "__metadata"


class InvalidEventMetadataError(Exception):
    def __init__(self, message: str):
        super().__init__(message)
        self.message = message

    def __repr__(self) -> str:
        return f"InvalidEventMetadataError({self.message})"


def fold_metadata_into_payload(
    payload: JsonValue, metadata: Mapping[str, JsonValue]
) -> JsonValue:
    if not metadata:
        return payload

    if not is_json_object(payload):
        raise InvalidEventMetadataError(
            "metadata can only be attached to object payloads"
        )

    if RESERVED_METADATA_KEY in payload:
        raise InvalidEventMetadataError(
            f"payload must not contain reserved key '{RESERVED_METADATA_KEY}'"
        )

    return {**payload, RESERVED_METADATA_KEY: dict(metadata)}


def split_metadata_from_payload(
    stored: JsonValue,
) -> tuple[JsonValue, JsonValue]:
    if not is_json_object(stored) or RESERVED_METADATA_KEY not in stored:
        return stored, {}

    payload = {
        key: value
        for key, value in stored.items()
        if key != RESERVED_METADATA_KEY
    }
    metadata = stored[RESERVED_METADATA_KEY]

    return payload, metadata
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mise run test:unit[TestFoldMetadataIntoPayload]` then `mise run test:unit[TestSplitMetadataFromPayload]`
Expected: PASS (all tests)

---

## Task 2: Export metadata symbols from types package

**Files:**
- Modify: `src/logicblocks/event/types/__init__.py`

- [ ] **Step 1: Add imports and exports**

In `src/logicblocks/event/types/__init__.py`, after the `from .json import (...)` block add:

```python
from .metadata import (
    RESERVED_METADATA_KEY,
    InvalidEventMetadataError,
    fold_metadata_into_payload,
    split_metadata_from_payload,
)
```

And add these four names to the `__all__` list (keep alphabetical grouping consistent with the file's existing ordering):

```python
    "InvalidEventMetadataError",
    "RESERVED_METADATA_KEY",
    "fold_metadata_into_payload",
    "split_metadata_from_payload",
```

- [ ] **Step 2: Verify the package imports cleanly**

Run: `mise run test:unit[TestFoldMetadataIntoPayload]`
Expected: PASS (imports resolve via package root too)

---

## Task 3: Add `metadata` to `NewEvent`

**Files:**
- Modify: `src/logicblocks/event/types/event.py`
- Test: `tests/unit/logicblocks/event/types/test_event.py`

- [ ] **Step 1: Write the failing tests**

Append to the `TestNewEvent` class in `tests/unit/logicblocks/event/types/test_event.py`:

```python
    def test_defaults_metadata_to_empty_mapping(self):
        event = NewEvent(name="something-happened", payload={"foo": "bar"})

        assert event.metadata == {}

    def test_uses_metadata_when_provided(self):
        event = NewEvent(
            name="something-happened",
            payload={"foo": "bar"},
            metadata={"actor": "user-123"},
        )

        assert event.metadata == {"actor": "user-123"}

    def test_serialise_includes_metadata(self):
        observed_at = datetime(2024, 1, 1, tzinfo=UTC)
        event = NewEvent(
            name="something-happened",
            payload={"foo": "bar"},
            metadata={"actor": "user-123"},
            observed_at=observed_at,
            occurred_at=observed_at,
        )

        assert event.serialise() == {
            "name": "something-happened",
            "payload": {"foo": "bar"},
            "metadata": {"actor": "user-123"},
            "observed_at": observed_at.isoformat(),
            "occurred_at": observed_at.isoformat(),
        }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mise run test:unit[TestNewEvent]`
Expected: FAIL — `TypeError: __init__() got an unexpected keyword argument 'metadata'` / `AttributeError: ... has no attribute 'metadata'`

- [ ] **Step 3: Update `NewEvent`**

In `src/logicblocks/event/types/event.py`, change the `NewEvent` class declaration, fields, `__init__`, `serialise`, `summarise`, and `__repr__`:

```python
@dataclass(frozen=True)
class NewEvent[Name = str, Payload = JsonValue, Metadata = JsonValue](
    JsonValueSerialisable
):
    name: Name
    payload: Payload
    metadata: Metadata
    observed_at: datetime
    occurred_at: datetime

    def __init__(
        self,
        *,
        name: Name,
        payload: Payload,
        metadata: Metadata | None = None,
        observed_at: datetime | None = None,
        occurred_at: datetime | None = None,
        clock: Clock = SystemClock(),
    ):
        if observed_at is None:
            observed_at = clock.now(UTC)
        if occurred_at is None:
            occurred_at = observed_at

        object.__setattr__(self, "name", name)
        object.__setattr__(self, "payload", payload)
        object.__setattr__(
            self, "metadata", metadata if metadata is not None else {}
        )
        object.__setattr__(self, "observed_at", observed_at)
        object.__setattr__(self, "occurred_at", occurred_at)

    def serialise(
        self,
        fallback: Callable[
            [object], JsonValue
        ] = default_serialisation_fallback,
    ) -> JsonValue:
        return {
            "name": serialise_to_json_value(self.name, fallback),
            "payload": serialise_to_json_value(self.payload, fallback),
            "metadata": serialise_to_json_value(self.metadata, fallback),
            "observed_at": self.observed_at.isoformat(),
            "occurred_at": self.occurred_at.isoformat(),
        }

    def summarise(self):
        return {
            "name": self.name,
            "metadata": serialise_to_json_value(self.metadata),
            "observed_at": self.observed_at.isoformat(),
            "occurred_at": self.occurred_at.isoformat(),
        }

    def __repr__(self):
        return (
            f"NewEvent("
            f"name={self.name}, "
            f"payload={repr(self.payload)}, "
            f"metadata={repr(self.metadata)}, "
            f"observed_at={self.observed_at}, "
            f"occurred_at={self.occurred_at})"
        )

    def __hash__(self):
        return hash(repr(self))
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mise run test:unit[TestNewEvent]`
Expected: PASS (new and existing tests)

---

## Task 4: Add `metadata` to `StoredEvent`

**Files:**
- Modify: `src/logicblocks/event/types/event.py`
- Test: `tests/unit/logicblocks/event/types/test_event.py`

- [ ] **Step 1: Write the failing tests**

Add a `TestStoredEvent` class to `tests/unit/logicblocks/event/types/test_event.py` (place after `TestNewEvent`):

```python
class TestStoredEvent:
    def _event(self, **overrides):
        defaults = dict(
            id="event-1",
            name="something-happened",
            stream="stream-1",
            category="category-1",
            position=0,
            sequence_number=0,
            payload={"foo": "bar"},
            observed_at=datetime(2024, 1, 1, tzinfo=UTC),
            occurred_at=datetime(2024, 1, 1, tzinfo=UTC),
        )
        defaults.update(overrides)
        return StoredEvent(**defaults)

    def test_defaults_metadata_to_empty_mapping(self):
        event = self._event()

        assert event.metadata == {}

    def test_uses_metadata_when_provided(self):
        event = self._event(metadata={"actor": "user-123"})

        assert event.metadata == {"actor": "user-123"}

    def test_serialise_includes_metadata(self):
        event = self._event(metadata={"actor": "user-123"})

        assert event.serialise()["metadata"] == {"actor": "user-123"}

    def test_summarise_includes_metadata(self):
        event = self._event(metadata={"actor": "user-123"})

        assert event.summarise()["metadata"] == {"actor": "user-123"}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mise run test:unit[TestStoredEvent]`
Expected: FAIL — `TypeError` / `KeyError: 'metadata'`

- [ ] **Step 3: Update `StoredEvent`**

In `src/logicblocks/event/types/event.py`, replace the `StoredEvent` class. Add the `Metadata` generic, a `metadata` field with a default of an empty dict, plus `metadata` in `serialise`, `summarise`, and `__repr__`. Because the dataclass is frozen and uses field defaults (not a custom `__init__`), give `metadata` a `field(default_factory=dict)` default so existing positional/keyword construction keeps working:

```python
from dataclasses import dataclass, field
```

```python
@dataclass(frozen=True)
class StoredEvent[Name = str, Payload = JsonValue, Metadata = JsonValue](
    JsonValueSerialisable, Event
):
    id: str
    name: Name
    stream: str
    category: str
    position: int
    sequence_number: int
    payload: Payload
    observed_at: datetime
    occurred_at: datetime
    metadata: Metadata = field(default_factory=dict)

    def serialise(
        self,
        fallback: Callable[
            [object], JsonValue
        ] = default_serialisation_fallback,
    ) -> JsonValue:
        return {
            "id": self.id,
            "name": serialise_to_json_value(self.name, fallback),
            "stream": self.stream,
            "category": self.category,
            "position": self.position,
            "sequence_number": self.sequence_number,
            "payload": serialise_to_json_value(self.payload, fallback),
            "metadata": serialise_to_json_value(self.metadata, fallback),
            "observed_at": self.observed_at.isoformat(),
            "occurred_at": self.occurred_at.isoformat(),
        }

    def summarise(self) -> JsonValue:
        return {
            "id": self.id,
            "name": serialise_to_json_value(self.name),
            "stream": self.stream,
            "category": self.category,
            "position": self.position,
            "sequence_number": self.sequence_number,
            "metadata": serialise_to_json_value(self.metadata),
            "observed_at": self.observed_at.isoformat(),
            "occurred_at": self.occurred_at.isoformat(),
        }

    def __repr__(self):
        return (
            f"StoredEvent("
            f"id={self.id}, "
            f"name={repr(self.name)}, "
            f"stream={self.stream}, "
            f"category={self.category}, "
            f"position={self.position}, "
            f"sequence_number={self.sequence_number}, "
            f"payload={repr(self.payload)}, "
            f"metadata={repr(self.metadata)}, "
            f"observed_at={self.observed_at}, "
            f"occurred_at={self.occurred_at})"
        )

    def __hash__(self):
        return hash(repr(self))
```

Note: the field is placed last with a `default_factory` so all existing call sites that pass the other nine fields by keyword (memory/postgres adapters, builders) continue to work without supplying `metadata`.

- [ ] **Step 4: Update `serialise_stored_event`**

In the same file, update `serialise_stored_event` to carry metadata through (add the param to its signature type and pass it):

```python
def serialise_stored_event(
    event: StoredEvent[JsonPersistable, JsonPersistable],
    fallback: Callable[[object], JsonValue] = default_serialisation_fallback,
) -> StoredEvent[JsonValue, JsonValue]:
    return StoredEvent[JsonValue, JsonValue](
        id=event.id,
        name=serialise_to_json_value(event.name, fallback),
        stream=event.stream,
        category=event.category,
        position=event.position,
        sequence_number=event.sequence_number,
        payload=serialise_to_json_value(event.payload, fallback),
        observed_at=event.observed_at,
        occurred_at=event.occurred_at,
        metadata=serialise_to_json_value(event.metadata, fallback),
    )
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `mise run test:unit[TestStoredEvent]` then `mise run test:unit[TestNewEvent]`
Expected: PASS

---

## Task 5: Add `metadata` to test builders

**Files:**
- Modify: `src/logicblocks/event/testing/builders.py`
- Test: `tests/unit/logicblocks/event/testing/test_builders.py`

- [ ] **Step 1: Write the failing tests**

Add to `tests/unit/logicblocks/event/testing/test_builders.py` (match the file's existing test-class style; if it uses bare functions, mirror that instead):

```python
from logicblocks.event.testing import NewEventBuilder, StoredEventBuilder


class TestNewEventBuilderMetadata:
    def test_defaults_metadata_to_empty_mapping(self):
        event = NewEventBuilder().build()

        assert event.metadata == {}

    def test_with_metadata_sets_metadata(self):
        event = NewEventBuilder().with_metadata({"actor": "user-1"}).build()

        assert event.metadata == {"actor": "user-1"}


class TestStoredEventBuilderMetadata:
    def test_defaults_metadata_to_empty_mapping(self):
        event = StoredEventBuilder().build()

        assert event.metadata == {}

    def test_with_metadata_sets_metadata(self):
        event = StoredEventBuilder().with_metadata({"actor": "user-1"}).build()

        assert event.metadata == {"actor": "user-1"}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mise run test:unit[TestNewEventBuilderMetadata]`
Expected: FAIL — `AttributeError: 'NewEventBuilder' object has no attribute 'with_metadata'`

- [ ] **Step 3: Update `NewEventBuilder`**

In `src/logicblocks/event/testing/builders.py`, update `NewEventBuilderParams` and `NewEventBuilder`:

Add to `NewEventBuilderParams` (after `payload`):

```python
    metadata: Metadata
```

Change `NewEventBuilder` to carry `metadata` (note the third generic). Update the class generics to `[Name = str, Payload = JsonValue, Metadata = JsonValue]`, add the field, the `__init__` param + assignment, `_clone`, a `with_metadata`, and pass it in `build`:

```python
@dataclass(frozen=True)
class NewEventBuilder[Name = str, Payload = JsonValue, Metadata = JsonValue]:
    name: Name
    payload: Payload
    metadata: Metadata
    occurred_at: datetime | None
    observed_at: datetime | None

    def __init__(
        self,
        *,
        name: Name | None = None,
        payload: Payload | None = None,
        metadata: Metadata | None = None,
        occurred_at: datetime | None = None,
        observed_at: datetime | None = None,
    ):
        object.__setattr__(self, "name", name or random_event_name())
        object.__setattr__(self, "payload", payload or random_event_payload())
        object.__setattr__(
            self, "metadata", metadata if metadata is not None else {}
        )
        object.__setattr__(self, "occurred_at", occurred_at)
        object.__setattr__(self, "observed_at", observed_at)

    def _clone(self, **kwargs: Unpack[NewEventBuilderParams[Name, Payload]]):
        name = kwargs.get("name", self.name)
        payload = kwargs.get("payload", self.payload)
        metadata = kwargs.get("metadata", self.metadata)
        occurred_at = kwargs.get("occurred_at", self.occurred_at)
        observed_at = kwargs.get("observed_at", self.observed_at)

        return NewEventBuilder(
            name=name,
            payload=payload,
            metadata=metadata,
            occurred_at=occurred_at,
            observed_at=observed_at,
        )

    def with_name(self, name: Name):
        return self._clone(name=name)

    def with_payload(self, payload: Payload):
        return self._clone(payload=payload)

    def with_metadata(self, metadata: Metadata):
        return self._clone(metadata=metadata)

    def with_occurred_at(self, occurred_at: datetime | None):
        return self._clone(occurred_at=occurred_at)

    def with_observed_at(self, observed_at: datetime | None):
        return self._clone(observed_at=observed_at)

    def build(self):
        return NewEvent[Name, Payload](
            name=self.name,
            payload=self.payload,
            metadata=self.metadata,
            occurred_at=self.occurred_at,
            observed_at=self.observed_at,
        )
```

- [ ] **Step 4: Update `StoredEventBuilder`**

Add `metadata: Metadata` to `StoredEventBuilderParams`. Update `StoredEventBuilder` generics to `[Name = str, Payload = JsonValue, Metadata = JsonValue]`, add a `metadata` field, an `__init__` assignment defaulting to `{}`, the `_clone` line, a `with_metadata`, and pass it in `build`:

In `__init__`, after the `payload` assignment, add:

```python
        object.__setattr__(
            self, "metadata", metadata if metadata is not None else {}
        )
```

and add `metadata: Metadata | None = None` to the `__init__` signature.

In `_clone`, add:

```python
        metadata = kwargs.get("metadata", self.metadata)
```

and pass `metadata=metadata` to the `StoredEventBuilder(...)` constructor call.

Add the method:

```python
    def with_metadata(self, metadata: Metadata):
        return self._clone(metadata=metadata)
```

In `build`, add `metadata=self.metadata` to the `StoredEvent[Name, Payload](...)` call.

Also update `from_new_event` to carry metadata across:

```python
    def from_new_event(self, event: NewEvent[Name, Payload]):
        return self._clone(
            name=event.name,
            payload=event.payload,
            metadata=event.metadata,
            occurred_at=event.occurred_at,
            observed_at=event.observed_at,
        )
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `mise run test:unit[TestNewEventBuilderMetadata]` then `mise run test:unit[TestStoredEventBuilderMetadata]`
Expected: PASS

---

## Task 6: Memory adapter — fold + split metadata

**Files:**
- Modify: `src/logicblocks/event/store/adapters/memory/adapter.py`
- Test: `tests/shared/logicblocks/event/testcases/store/adapters.py`

The memory adapter stores a *serialised* `StoredEvent[str, JsonValue]` in its transaction (the `serialised_stored_event` built at lines ~212-222 and ~293-301) and returns the un-serialised `StoredEvent` to the caller. To make memory behave like Postgres (which round-trips through a stored payload blob), the serialised event's `payload` must be the folded value, and the returned event's `payload`/`metadata` must come from splitting that folded value back out.

- [ ] **Step 1: Write the failing shared adapter tests**

Add to the shared adapter test base class in `tests/shared/logicblocks/event/testcases/store/adapters.py` (place near `test_stores_single_event_for_later_retrieval`). These run against BOTH memory and Postgres:

```python
    async def test_stores_and_retrieves_event_metadata(self):
        adapter = self.construct_storage_adapter()

        category = data.random_event_category_name()
        stream = data.random_event_stream_name()

        new_event = (
            NewEventBuilder()
            .with_payload({"amount": 100})
            .with_metadata({"actor": "user-123"})
            .build()
        )

        stored_events = await adapter.save(
            target=StreamIdentifier(category=category, stream=stream),
            events=[new_event],
        )

        assert stored_events[0].payload == {"amount": 100}
        assert stored_events[0].metadata == {"actor": "user-123"}

        retrieved = await self.retrieve_events(adapter=adapter)
        assert retrieved[0].payload == {"amount": 100}
        assert retrieved[0].metadata == {"actor": "user-123"}

    async def test_defaults_metadata_to_empty_mapping_on_retrieval(self):
        adapter = self.construct_storage_adapter()

        category = data.random_event_category_name()
        stream = data.random_event_stream_name()

        new_event = NewEventBuilder().with_payload({"amount": 100}).build()

        await adapter.save(
            target=StreamIdentifier(category=category, stream=stream),
            events=[new_event],
        )

        retrieved = await self.retrieve_events(adapter=adapter)
        assert retrieved[0].payload == {"amount": 100}
        assert retrieved[0].metadata == {}
```

(Confirm `NewEventBuilder`, `StreamIdentifier`, and `data` are already imported at the top of this file — `NewEventBuilder` and `data` are; add `StreamIdentifier` to the existing `from logicblocks.event.types import (...)` import if not already present.)

- [ ] **Step 2: Run tests to verify they fail for memory**

Run: `mise run test:unit[InMemoryEventStorageAdapterTestCase]`

(Use the actual memory test-case class name in `tests/unit/logicblocks/event/store/adapters/test_memory.py` — find it with `grep -n "class .*TestCase\|class Test" tests/unit/logicblocks/event/store/adapters/test_memory.py`.)
Expected: FAIL — `retrieved[0].metadata` is `{}` instead of `{"actor": "user-123"}` (metadata not persisted yet).

- [ ] **Step 3: Update the memory adapter save paths**

In `src/logicblocks/event/store/adapters/memory/adapter.py`, add to the types import block:

```python
from logicblocks.event.types import (
    ...
    fold_metadata_into_payload,
)
```

In `_save_to_stream` (the block around lines 199-224), change the `serialised_stored_event` construction so its `payload` is the folded value:

```python
                serialised_stored_event = StoredEvent[str, JsonValue](
                    id=new_stored_event.id,
                    name=serialise_to_string(new_stored_event.name),
                    stream=new_stored_event.stream,
                    category=new_stored_event.category,
                    position=new_stored_event.position,
                    sequence_number=new_stored_event.sequence_number,
                    payload=fold_metadata_into_payload(
                        serialise_to_json_value(new_stored_event.payload),
                        serialise_to_json_value(new_event.metadata),
                    ),
                    observed_at=new_stored_event.observed_at,
                    occurred_at=new_stored_event.occurred_at,
                )
```

Also set `metadata` on the returned `new_stored_event` so callers get it (add `metadata=new_event.metadata` to the `StoredEvent[Name, Payload](...)` constructed at line ~201).

Apply the identical change to the category save path (`_save_to_category`, around lines 282-301): same `fold_metadata_into_payload` on the serialised event's payload, and `metadata=new_event.metadata` on the returned `StoredEvent`.

- [ ] **Step 4: Update the memory adapter read paths to split**

The memory adapter's `scan` and `latest` (around lines 316-329) return the stored `StoredEvent[str, JsonValue]` whose `payload` is the folded value. Wrap each returned stored event so the folded payload is split back into `payload` + `metadata`.

Add to the import block:

```python
    split_metadata_from_payload,
```

Add a private helper method to the adapter class:

```python
    def _split_stored_event(
        self, event: StoredEvent[str, JsonValue]
    ) -> StoredEvent[str, JsonValue]:
        payload, metadata = split_metadata_from_payload(event.payload)
        return StoredEvent[str, JsonValue](
            id=event.id,
            name=event.name,
            stream=event.stream,
            category=event.category,
            position=event.position,
            sequence_number=event.sequence_number,
            payload=payload,
            observed_at=event.observed_at,
            occurred_at=event.occurred_at,
            metadata=metadata,
        )
```

In `latest`, return `self._split_stored_event(event)` (or `None`). In `scan`, yield `self._split_stored_event(event)` for each event. Locate the exact yield/return sites with `grep -n "yield\|return await\|return None\|return self._db" src/logicblocks/event/store/adapters/memory/adapter.py` within `scan`/`latest`.

- [ ] **Step 5: Run tests to verify they pass for memory**

Run: `mise run test:unit[<MemoryTestCaseClassName>]`
Expected: PASS (including the two new metadata tests)

---

## Task 7: Postgres adapter — fold metadata on write

**Files:**
- Modify: `src/logicblocks/event/store/adapters/postgres/adapter.py`

- [ ] **Step 1: Confirm the failing test for Postgres**

The shared metadata tests from Task 6 also run against Postgres. Run them now to see the write side fail:

Run: `mise run test:integration[<PostgresTestCaseClassName>]`

(Find the class name with `grep -n "class .*TestCase\|class Test" tests/integration/logicblocks/event/store/adapters/test_postgres.py`.)
Expected: FAIL on the metadata tests.

- [ ] **Step 2: Fold metadata into the inserted payload value**

In `src/logicblocks/event/store/adapters/postgres/adapter.py`, add to the types import block (line ~31-42):

```python
    fold_metadata_into_payload,
```

In `insert_batch_query` (the value list built around line 419-430), replace the payload value line:

```python
                    Jsonb(serialise_to_json_value(event.payload)),
```

with:

```python
                    Jsonb(
                        fold_metadata_into_payload(
                            serialise_to_json_value(event.payload),
                            serialise_to_json_value(event.metadata),
                        )
                    ),
```

- [ ] **Step 3: Carry metadata through the `RETURNING *` mapping**

In the function that maps returned rows back to `StoredEvent[Name, Payload]` (around lines 581-595), the returned events use the *original* `event.payload`/`event.metadata` (not the DB row's folded payload). Add `metadata=event.metadata` to the `StoredEvent[Name, Payload](...)` constructor:

```python
                StoredEvent[Name, Payload](
                    id=stored_event.id,
                    name=event.name,
                    stream=stored_event.stream,
                    category=stored_event.category,
                    position=stored_event.position,
                    sequence_number=stored_event.sequence_number,
                    payload=event.payload,
                    observed_at=stored_event.observed_at,
                    occurred_at=stored_event.occurred_at,
                    metadata=event.metadata,
                )
```

Note: `stored_event` here is the folded DB row; we deliberately use the caller's original `event.payload`/`event.metadata` for the return value, which is already separated. This matches the memory adapter's behaviour of returning the un-folded event to the caller of `save`.

- [ ] **Step 4: Run write-path tests**

Run: `mise run test:integration[<PostgresTestCaseClassName>] -- -k test_stores_and_retrieves_event_metadata`

(If the test runner doesn't support `-k` passthrough, run the full class.)
Expected: The `save` return value now has correct metadata, but `retrieve_events` (a read) still fails until Task 8 — the `test_defaults_metadata_to_empty_mapping_on_retrieval` and the retrieval half of `test_stores_and_retrieves_event_metadata` will still fail. That's expected; the read split comes next.

---

## Task 8: Postgres adapter — split metadata on read via custom row factory

**Files:**
- Modify: `src/logicblocks/event/store/adapters/postgres/adapter.py`

The Postgres read paths (`latest`, `scan`, and the read used inside save for locking/last-event) construct `StoredEvent` directly from DB columns via `class_row(StoredEvent[str, JsonValue])`. The `payload` column holds the folded value. We need a custom row factory that splits the folded payload into `payload` + `metadata` after `class_row` builds the event.

- [ ] **Step 1: Confirm failing read tests**

Run: `mise run test:integration[<PostgresTestCaseClassName>]`
Expected: FAIL on `test_defaults_metadata_to_empty_mapping_on_retrieval` and the retrieval assertions of `test_stores_and_retrieves_event_metadata`.

- [ ] **Step 2: Add a metadata-splitting row factory**

In `src/logicblocks/event/store/adapters/postgres/adapter.py`, add to the types import block:

```python
    split_metadata_from_payload,
```

Add a module-level row factory near the other query/row helpers (after the imports, before the adapter class):

```python
def stored_event_row_factory(cursor: AsyncCursor):
    base_factory = class_row(StoredEvent[str, JsonValue])(cursor)

    def make_row(values):
        event = base_factory(values)
        payload, metadata = split_metadata_from_payload(event.payload)
        return StoredEvent[str, JsonValue](
            id=event.id,
            name=event.name,
            stream=event.stream,
            category=event.category,
            position=event.position,
            sequence_number=event.sequence_number,
            payload=payload,
            observed_at=event.observed_at,
            occurred_at=event.occurred_at,
            metadata=metadata,
        )

    return make_row
```

(If the `cursor` parameter needs a precise type, use the existing `AsyncCursor[StoredEvent[str, JsonValue]]` annotation pattern already imported in this file.)

- [ ] **Step 3: Use the custom row factory in read paths**

Replace every `row_factory=class_row(StoredEvent[str, JsonValue])` used for **reading events back** with `row_factory=stored_event_row_factory`. These are at approximately:
- `latest` (line ~859)
- `scan` (line ~877)
- the save-path read cursors that fetch existing events (lines ~718 and ~773) — verify each of these is reading events (not just inserting). The insert cursor that uses `RETURNING *` (around the `insert_events` call) returns folded rows but those return values are NOT used for the caller-facing payload (Task 7 uses the original `event.payload`), so leaving its factory as-is is fine; however, if any latest-event read for write conditions uses `class_row`, switch it to `stored_event_row_factory` so write-condition logic sees split payloads.

Find all occurrences:

```bash
grep -n "class_row(StoredEvent" src/logicblocks/event/store/adapters/postgres/adapter.py
```

For each occurrence used to read events for return or for condition checks, replace with `stored_event_row_factory`.

- [ ] **Step 4: Run the full Postgres adapter test class**

Run: `mise run test:integration[<PostgresTestCaseClassName>]`
Expected: PASS (all tests, including both metadata tests and all pre-existing tests).

---

## Task 9: Legacy-row and guard coverage in shared adapter tests

**Files:**
- Modify: `tests/shared/logicblocks/event/testcases/store/adapters.py`

- [ ] **Step 1: Write the guard + legacy tests**

Add to the shared adapter test base class:

```python
    async def test_raises_when_metadata_attached_to_non_object_payload(self):
        adapter = self.construct_storage_adapter()
        category = data.random_event_category_name()
        stream = data.random_event_stream_name()

        new_event = (
            NewEventBuilder()
            .with_payload([1, 2, 3])
            .with_metadata({"actor": "user-1"})
            .build()
        )

        with pytest.raises(InvalidEventMetadataError):
            await adapter.save(
                target=StreamIdentifier(category=category, stream=stream),
                events=[new_event],
            )

    async def test_raises_when_payload_contains_reserved_metadata_key(self):
        adapter = self.construct_storage_adapter()
        category = data.random_event_category_name()
        stream = data.random_event_stream_name()

        new_event = (
            NewEventBuilder()
            .with_payload({"__metadata": {"x": 1}})
            .with_metadata({"actor": "user-1"})
            .build()
        )

        with pytest.raises(InvalidEventMetadataError):
            await adapter.save(
                target=StreamIdentifier(category=category, stream=stream),
                events=[new_event],
            )
```

Add the imports at the top of the file if missing:

```python
import pytest
from logicblocks.event.types import InvalidEventMetadataError
```

- [ ] **Step 2: Run for both adapters**

Run: `mise run test:unit[<MemoryTestCaseClassName>]` then `mise run test:integration[<PostgresTestCaseClassName>]`
Expected: PASS (the guards already exist via `fold_metadata_into_payload` from Task 1; these tests confirm both adapters surface them).

If the Postgres adapter batches the fold inside query construction such that the exception is raised at query-build time, confirm it propagates out of `save` (it should, since `insert_batch_query` is called synchronously within `save`). If a guard does NOT raise for one adapter, fix that adapter's save path to call `fold_metadata_into_payload` before persisting.

---

## Task 10: Changelog fragment

**Files:**
- Create: a changelog fragment under `changelog.d/`

- [ ] **Step 1: Generate the fragment**

Run: `mise run changelog:fragment:create`

Follow the prompt (type/category) to create a fragment describing the addition. If the task is non-interactive, inspect existing files in `changelog.d/` for the naming convention (e.g. `<id>.added.md`) and create one matching it with content:

```
Add optional `metadata` field to events for recording cross-cutting context
such as the actor responsible for an event. Metadata is folded into the event
payload under a reserved key and is fully optional; events without metadata are
unaffected.
```

---

## Task 11: Full build verification

- [ ] **Step 1: Run the full build**

Run: `mise run`
Expected: PASS — linting, type checking, formatting, build, and all tests (unit, integration, component) green.

- [ ] **Step 2: Fix any type/lint issues**

If `mise run type:check` flags the new `Metadata` generic anywhere it's threaded through (`publish`, adapter signatures), add the third generic param consistently. If `mise run lint:fix` / `mise run format:fix` make changes, include them.

---

## Self-Review Notes

- **Spec coverage:** generic `Metadata` param (Tasks 3-4), default `{}` (Tasks 3-4), fold-on-write/split-on-read sibling-key model (Tasks 1, 6-8), both guards (Tasks 1, 9), legacy detect-on-read (Task 1 `split` + Task 9 test), serialise/summarise include metadata (Tasks 3-4), changelog fragment (Task 10), shared adapter coverage for memory + Postgres (Tasks 6-9). All spec sections map to tasks.
- **Type consistency:** helper names `fold_metadata_into_payload` / `split_metadata_from_payload` / `RESERVED_METADATA_KEY` / `InvalidEventMetadataError` are used identically across all tasks.
- **Open verification points flagged for the implementer:** exact test-case class names (memory/postgres) and the precise set of `class_row(StoredEvent...)` read sites must be confirmed via the provided `grep` commands before editing — they are environment-stable but named explicitly to avoid guesswork.
