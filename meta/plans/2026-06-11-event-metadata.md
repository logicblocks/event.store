# Event Metadata Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an optional, generically-typed `metadata` field to events, persisted in a dedicated `metadata JSONB NOT NULL` column on the `events` table (mirroring `projections.metadata`). This is a breaking schema change shipped as a DDL update plus a changelog migration note.

**Architecture:** A third generic parameter `Metadata` is added to `NewEvent` and `StoredEvent` (mirroring `Projection[State, Metadata]`), defaulting to an empty mapping. Both storage adapters persist and read `metadata` as a plain field in its own column — no fold/split, no reserved key, no custom row factory, no write guards. The `payload` column is untouched.

**Baseline:** Implement **clean from `main`**. Any uncommitted fold-into-payload work in the working tree must be discarded first (Task 0). Do not adapt it.

**Tech Stack:** Python 3.13, `psycopg`/`psycopg_pool` (Postgres adapter), `pytest`/`pytest-asyncio`, `mise` task runner.

---

## Test Runner Reference

The `mise run test:unit[ClassName]` parametrised syntax works in an interactive
shell but NOT from a non-interactive subshell. From scripts/subshells use:

- Unit (no DB): `mise exec -- invoke test.unit` ; targeted:
  `mise exec -- invoke test.unit -t "-k '<expr>'"` (wrap `-k` value in single
  quotes when it contains spaces/`or`).
- Integration (needs Postgres): `DB_PORT=<port> mise exec -- invoke test.integration`.
  The `database:test:provision` mise task starts the test DB on host port 5432
  via docker-compose. If 5432 is occupied, start a Postgres 16.3 container
  (`POSTGRES_DB=some-database POSTGRES_USER=admin POSTGRES_PASSWORD=super-secret`)
  on an alternate host port and pass `DB_PORT=<port>` — the integration tests
  read `DB_HOST`/`DB_PORT` env vars (default `localhost:5432`).
- Type check: `mise exec -- invoke types.check`
- Lint: `mise exec -- invoke lint.fix` ; Format: `mise exec -- invoke format.fix`
- Full build: `mise run` (lint, types, format, build, all tests).

**Commits:** Do NOT commit. Leave all changes uncommitted; the human commits.

---

## Storage Shape Reference

The `events` table gains a `metadata JSONB NOT NULL` column. `payload` is
unchanged in all cases.

| Case | Stored `payload` | Stored `metadata` | Read result |
|---|---|---|---|
| No metadata (default) | `{prop1, prop2}` | `{}` | `payload={prop1,prop2}`, `metadata={}` |
| Metadata present | `{prop1, prop2}` | `{"actor": "u1"}` | `payload={prop1,prop2}`, `metadata={"actor": "u1"}` |

No reserved key, no fold/split, no guards. Any payload shape coexists with
metadata.

---

## File Structure

**Modified files:**
- `sql/create_events_table.sql` — add the `metadata JSONB NOT NULL` column.
- `src/logicblocks/event/types/event.py` — add `Metadata` generic + `metadata`
  field to `NewEvent` and `StoredEvent`; update `serialise`, `summarise`,
  `__repr__`, `serialise_stored_event`.
- `src/logicblocks/event/testing/builders.py` — add `metadata` to
  `NewEventBuilder` and `StoredEventBuilder`.
- `src/logicblocks/event/store/adapters/memory/adapter.py` — store + return
  `metadata` as a plain field on both event constructions.
- `src/logicblocks/event/store/adapters/postgres/adapter.py` — add `metadata`
  to the INSERT column list + bound values; carry `metadata` through the
  RETURNING mapping. (Reads map automatically via `class_row`.)

**No new source files.** There is NO `metadata.py`, NO fold/split helpers, NO
`RESERVED_METADATA_KEY`, NO `InvalidEventMetadataError`, NO custom row factory.

**Test files:**
- `tests/unit/logicblocks/event/types/test_event.py` — event field/serialise
  tests (modify).
- `tests/unit/logicblocks/event/testing/test_builders.py` — builder tests
  (modify).
- `tests/shared/logicblocks/event/testcases/store/adapters.py` — shared adapter
  round-trip behaviour (modify; runs against both memory + Postgres).

**Changelog:**
- A fragment under `changelog.d/` describing the addition and the required
  migration.

---

## Task 0: Reset working tree to clean baseline

**Files:** working tree (no specific file)

- [ ] **Step 1: Confirm what is uncommitted**

Run: `git status --short` and `git stash list`
Expected: a set of modified/untracked files from the prior fold-based attempt
(e.g. `src/logicblocks/event/types/metadata.py`, modified `event.py`, adapters,
tests). The `meta/specs/...` and `meta/plans/...` docs are the only changes to
KEEP.

- [ ] **Step 2: Preserve the design docs, discard the fold-based code**

The spec and plan under `meta/` are the source of truth and must be kept. The
implementation changes must be discarded. Do this precisely:

```bash
# Discard tracked-file modifications EXCEPT the meta/ docs
git restore --staged --worktree \
  src/logicblocks/event/types/event.py \
  src/logicblocks/event/types/__init__.py \
  src/logicblocks/event/store/adapters/memory/adapter.py \
  src/logicblocks/event/store/adapters/postgres/adapter.py \
  src/logicblocks/event/store/adapters/postgres/__init__.py \
  src/logicblocks/event/testing/builders.py \
  tests/integration/logicblocks/event/store/adapters/test_postgres.py \
  tests/shared/logicblocks/event/testcases/store/adapters.py \
  tests/unit/logicblocks/event/testing/test_builders.py \
  tests/unit/logicblocks/event/types/test_event.py

# Remove untracked fold-based files (NOT the meta/ docs, NOT the changelog if you want to keep it — but the old changelog fragment was fold-specific, so remove it and recreate in Task 8)
rm -f src/logicblocks/event/types/metadata.py \
      tests/unit/logicblocks/event/types/test_metadata.py \
      changelog.d/20260612_000000_bivav_event_metadata.md
```

- [ ] **Step 3: Verify clean baseline**

Run: `git status --short`
Expected: ONLY `meta/specs/2026-06-11-event-metadata-design.md` and
`meta/plans/2026-06-11-event-metadata.md` appear as changes (one tracked-modified,
one already committed/modified). No `src/` or `tests/` changes remain.

Run: `mise exec -- invoke test.unit` → all pass (baseline green).
Run: `mise exec -- invoke types.check` → 0 errors.

---

## Task 1: Add the `metadata` column to the events DDL

**Files:**
- Modify: `sql/create_events_table.sql`

- [ ] **Step 1: Add the column**

Current file:
```sql
CREATE TABLE events (
    id TEXT NOT NULL,
    name TEXT NOT NULL,
    stream TEXT NOT NULL,
    category TEXT NOT NULL,
    position INT NOT NULL,
    payload JSONB NOT NULL,
    sequence_number BIGSERIAL NOT NULL,
    observed_at TIMESTAMP WITH TIME ZONE NOT NULL,
    occurred_at TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (id),
    UNIQUE (id)
);
```

Add `metadata JSONB NOT NULL` directly after `payload`:
```sql
CREATE TABLE events (
    id TEXT NOT NULL,
    name TEXT NOT NULL,
    stream TEXT NOT NULL,
    category TEXT NOT NULL,
    position INT NOT NULL,
    payload JSONB NOT NULL,
    metadata JSONB NOT NULL,
    sequence_number BIGSERIAL NOT NULL,
    observed_at TIMESTAMP WITH TIME ZONE NOT NULL,
    occurred_at TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (id),
    UNIQUE (id)
);
```

- [ ] **Step 2: Verify**

This DDL is exercised by the integration test fixtures (`create_table(pool,
"events")` reads this file). No standalone run here — Task 6/7 integration tests
will confirm the column works end-to-end. Confirm the file parses by eye
(matches the `projections.metadata` style in `sql/create_projections_table.sql`).

---

## Task 2: Add `metadata` to `NewEvent`

**Files:**
- Modify: `src/logicblocks/event/types/event.py`
- Test: `tests/unit/logicblocks/event/types/test_event.py`

- [ ] **Step 1: Write the failing tests**

Append to the EXISTING `TestNewEvent` class in
`tests/unit/logicblocks/event/types/test_event.py` (`NewEvent`, `datetime`,
`UTC` are already imported):

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

Also update any EXISTING `TestNewEvent` tests that assert the FULL
`serialise()` / `summarise()` / `__repr__()` output to include the new
`metadata` entry (the no-metadata default is `{}` / `metadata={}`). Find them
by running the suite in Step 4 and adding `"metadata": {}` (dict) or
`metadata={}` (repr) to each broken expected value.

- [ ] **Step 2: Run to verify failure**

Run: `mise exec -- invoke test.unit -t "-k TestNewEvent"`
Expected: FAIL — `__init__() got an unexpected keyword argument 'metadata'` /
`AttributeError: ... 'metadata'`.

- [ ] **Step 3: Update `NewEvent`**

`NewEvent` has a custom `__init__`. Add the `Metadata` generic, the field, the
optional kwarg defaulting to `{}`, and include metadata in serialise/summarise/
repr:

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

- [ ] **Step 4: Run to verify pass**

Run: `mise exec -- invoke test.unit -t "-k TestNewEvent"` → pass (new + updated
existing).
Run: `mise exec -- invoke types.check` → 0 errors.

---

## Task 3: Add `metadata` to `StoredEvent`

**Files:**
- Modify: `src/logicblocks/event/types/event.py`
- Test: `tests/unit/logicblocks/event/types/test_event.py`

`StoredEvent` uses a dataclass-generated `__init__` (no custom init) and is
constructed by keyword in the adapters. The `metadata` field MUST be added LAST
with a default so existing constructions keep working.

IMPORTANT (type soundness): a plain `field(default_factory=dict)` FAILS the type
checker with `dict[Unknown, Unknown] is not assignable to Metadata`. Use a cast:
`field(default_factory=cast(Callable[[], Metadata], lambda: {}))`. Add `field`
to the `dataclasses` import and `cast` + `Callable` to typing imports as needed.

- [ ] **Step 1: Write failing tests**

Add a `TestStoredEvent` class (after `TestNewEvent`) using full-equality
assertions consistent with the file's existing StoredEvent tests:

```python
class TestStoredEvent:
    def test_defaults_metadata_to_empty_mapping(self):
        event = StoredEvent(
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

        assert event.metadata == {}

    def test_uses_metadata_when_provided(self):
        event = StoredEvent(
            id="event-1",
            name="something-happened",
            stream="stream-1",
            category="category-1",
            position=0,
            sequence_number=0,
            payload={"foo": "bar"},
            observed_at=datetime(2024, 1, 1, tzinfo=UTC),
            occurred_at=datetime(2024, 1, 1, tzinfo=UTC),
            metadata={"actor": "user-123"},
        )

        assert event.metadata == {"actor": "user-123"}

    def test_serialise_includes_metadata(self):
        event = StoredEvent(
            id="event-1",
            name="something-happened",
            stream="stream-1",
            category="category-1",
            position=0,
            sequence_number=0,
            payload={"foo": "bar"},
            observed_at=datetime(2024, 1, 1, tzinfo=UTC),
            occurred_at=datetime(2024, 1, 1, tzinfo=UTC),
            metadata={"actor": "user-123"},
        )

        assert event.serialise() == {
            "id": "event-1",
            "name": "something-happened",
            "stream": "stream-1",
            "category": "category-1",
            "position": 0,
            "sequence_number": 0,
            "payload": {"foo": "bar"},
            "metadata": {"actor": "user-123"},
            "observed_at": datetime(2024, 1, 1, tzinfo=UTC).isoformat(),
            "occurred_at": datetime(2024, 1, 1, tzinfo=UTC).isoformat(),
        }

    def test_summarise_includes_metadata(self):
        event = StoredEvent(
            id="event-1",
            name="something-happened",
            stream="stream-1",
            category="category-1",
            position=0,
            sequence_number=0,
            payload={"foo": "bar"},
            observed_at=datetime(2024, 1, 1, tzinfo=UTC),
            occurred_at=datetime(2024, 1, 1, tzinfo=UTC),
            metadata={"actor": "user-123"},
        )

        assert event.summarise() == {
            "id": "event-1",
            "name": "something-happened",
            "stream": "stream-1",
            "category": "category-1",
            "position": 0,
            "sequence_number": 0,
            "metadata": {"actor": "user-123"},
            "observed_at": datetime(2024, 1, 1, tzinfo=UTC).isoformat(),
            "occurred_at": datetime(2024, 1, 1, tzinfo=UTC).isoformat(),
        }
```

- [ ] **Step 2: Run to verify failure**

Run: `mise exec -- invoke test.unit -t "-k TestStoredEvent"`
Expected: FAIL — unexpected kwarg / KeyError 'metadata'.

- [ ] **Step 3: Update `StoredEvent`**

Change the import line:
```python
from dataclasses import dataclass, field
from typing import Callable, Protocol, cast
```

Update the class — add the generic, the LAST field with the cast default, and
metadata in serialise/summarise/repr:

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
    metadata: Metadata = field(
        default_factory=cast(Callable[[], Metadata], lambda: {})
    )

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

- [ ] **Step 4: Update `serialise_stored_event`**

Carry metadata through and thread the third generic:

```python
def serialise_stored_event(
    event: StoredEvent[JsonPersistable, JsonPersistable, JsonPersistable],
    fallback: Callable[[object], JsonValue] = default_serialisation_fallback,
) -> StoredEvent[JsonValue, JsonValue, JsonValue]:
    return StoredEvent[JsonValue, JsonValue, JsonValue](
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

- [ ] **Step 5: Run + fix existing full-equality tests**

Run: `mise exec -- invoke test.unit` (full suite).
Some EXISTING tests across the suite assert full `StoredEvent` serialise/
summarise/repr — add `"metadata": {}` / `metadata={}` to each broken expected
value (default-empty). Only update expected values; do not change behaviour.
Run: `mise exec -- invoke types.check` → 0 errors.

---

## Task 4: Add `metadata` to the test builders

**Files:**
- Modify: `src/logicblocks/event/testing/builders.py`
- Test: `tests/unit/logicblocks/event/testing/test_builders.py`

- [ ] **Step 1: Write failing tests**

Add to `tests/unit/logicblocks/event/testing/test_builders.py` (confirm
`NewEventBuilder`, `StoredEventBuilder` imports):

```python
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

    def test_from_new_event_propagates_metadata(self):
        new_event = NewEventBuilder().with_metadata({"actor": "user-1"}).build()
        event = StoredEventBuilder().from_new_event(new_event).build()

        assert event.metadata == {"actor": "user-1"}
```

- [ ] **Step 2: Run to verify failure**

Run: `mise exec -- invoke test.unit -t "-k 'Metadata and Builder'"`
Expected: FAIL — `with_metadata` does not exist.

- [ ] **Step 3: Update `NewEventBuilder`**

Add `metadata: Metadata` to `NewEventBuilderParams`. Update generics to
`[Name = str, Payload = JsonValue, Metadata = JsonValue]`, add the field, the
`__init__` param + `object.__setattr__(self, "metadata", metadata if metadata
is not None else {})`, thread through `_clone`, add `with_metadata`, and pass
`metadata=self.metadata` in `build()` (constructing `NewEvent[Name, Payload,
Metadata]`).

```python
    def with_metadata(self, metadata: Metadata):
        return self._clone(metadata=metadata)
```

- [ ] **Step 4: Update `StoredEventBuilder`**

Same pattern: add `metadata: Metadata` to `StoredEventBuilderParams`, update
generics, add field, `__init__` default `{}`, `_clone` threading,
`with_metadata`, and `metadata=self.metadata` in `build()` (constructing
`StoredEvent[Name, Payload, Metadata]`). Also update `from_new_event` to carry
`metadata=event.metadata` and thread the `Metadata` generic on its `NewEvent`
parameter type:

```python
    def from_new_event(self, event: NewEvent[Name, Payload, Metadata]):
        return self._clone(
            name=event.name,
            payload=event.payload,
            metadata=event.metadata,
            occurred_at=event.occurred_at,
            observed_at=event.observed_at,
        )
```

Do NOT touch the `ProjectionBuilder` family (they already have their own
metadata).

- [ ] **Step 5: Run to verify pass + types**

Run: `mise exec -- invoke test.unit -t "-k 'Metadata and Builder'"` → pass.
Run: `mise exec -- invoke test.unit` (full) → pass.
Run: `mise exec -- invoke types.check` → 0 errors.

---

## Task 5: Memory adapter — store + return metadata

**Files:**
- Modify: `src/logicblocks/event/store/adapters/memory/adapter.py`

The memory adapter builds two events per save in `_save_to_stream` AND
`_save_to_category`: the caller-returned `StoredEvent[Name, Payload]` and the
serialised `StoredEvent[str, JsonValue]` stored internally. Both need metadata.
Because `metadata` is a real field that `class_row`/scan returns directly, there
is NO split needed on read — `latest`/`scan` already return the stored events
whole. (The memory `scan`/`latest` on `main` return snapshot events directly.)

- [ ] **Step 1: Confirm shared tests exist (they're added in Task 6)**

Task 6 adds the shared round-trip tests that exercise BOTH adapters. If doing
Task 5 before Task 6, you can temporarily assert via a quick local check, but
the intended flow is: Task 6 writes the shared tests (they fail for memory
under unit), then this task makes memory pass. If you prefer, do Task 6 Step 1
first to have failing tests, then return here.

- [ ] **Step 2: Update both save paths**

In `_save_to_stream` (≈ lines 199-224) and `_save_to_category` (≈ lines
282-303):
- Add `metadata=new_event.metadata` to the caller-returned
  `StoredEvent[Name, Payload](...)`.
- Add `metadata=serialise_to_json_value(new_stored_event.metadata)` to the
  serialised `StoredEvent[str, JsonValue](...)`.

Example for the stream path:
```python
                new_stored_event = StoredEvent[Name, Payload](
                    id=uuid4().hex,
                    name=new_event.name,
                    stream=target.stream,
                    category=target.category,
                    position=last_stream_position + count + 1,
                    sequence_number=next(self._sequence),
                    payload=new_event.payload,
                    observed_at=new_event.observed_at,
                    occurred_at=new_event.occurred_at,
                    metadata=new_event.metadata,
                )
                serialised_stored_event = StoredEvent[str, JsonValue](
                    id=new_stored_event.id,
                    name=serialise_to_string(new_stored_event.name),
                    stream=new_stored_event.stream,
                    category=new_stored_event.category,
                    position=new_stored_event.position,
                    sequence_number=new_stored_event.sequence_number,
                    payload=serialise_to_json_value(new_stored_event.payload),
                    observed_at=new_stored_event.observed_at,
                    occurred_at=new_stored_event.occurred_at,
                    metadata=serialise_to_json_value(
                        new_stored_event.metadata
                    ),
                )
```

`latest` and `scan` need NO change — they return the stored events (which now
carry metadata) directly.

- [ ] **Step 3: Run**

Run the memory shared tests (after Task 6 adds them):
`mise exec -- invoke test.unit -t "-k 'event_metadata'"` → pass.
Run: `mise exec -- invoke test.unit` (full) → pass.
Run: `mise exec -- invoke types.check` → 0 errors.

---

## Task 6: Postgres adapter — persist + read metadata column

**Files:**
- Modify: `src/logicblocks/event/store/adapters/postgres/adapter.py`
- Test: `tests/shared/logicblocks/event/testcases/store/adapters.py`

- [ ] **Step 1: Write the failing shared round-trip tests**

Add to the shared stream-save test class `StreamSaveCases` in
`tests/shared/logicblocks/event/testcases/store/adapters.py`. Conventions
(verify by reading the file): build with `NewEventBuilder()`, names via
`random_event_category_name()` / `random_event_stream_name()`, target via
`identifier.StreamIdentifier(...)`, retrieve via
`await self.retrieve_events(adapter=adapter)`.

```python
    async def test_save_returns_event_with_metadata(self):
        adapter = self.construct_storage_adapter()

        event_category = random_event_category_name()
        event_stream = random_event_stream_name()

        new_event = (
            NewEventBuilder()
            .with_payload({"amount": 100})
            .with_metadata({"actor": "user-123"})
            .build()
        )

        stored_events = await adapter.save(
            target=identifier.StreamIdentifier(
                category=event_category, stream=event_stream
            ),
            events=[new_event],
        )

        assert stored_events[0].metadata == {"actor": "user-123"}

    async def test_stores_and_retrieves_event_metadata(self):
        adapter = self.construct_storage_adapter()

        event_category = random_event_category_name()
        event_stream = random_event_stream_name()

        new_event = (
            NewEventBuilder()
            .with_payload({"amount": 100})
            .with_metadata({"actor": "user-123"})
            .build()
        )

        await adapter.save(
            target=identifier.StreamIdentifier(
                category=event_category, stream=event_stream
            ),
            events=[new_event],
        )

        retrieved = await self.retrieve_events(adapter=adapter)
        assert retrieved[0].payload == {"amount": 100}
        assert retrieved[0].metadata == {"actor": "user-123"}

    async def test_defaults_metadata_to_empty_mapping_on_retrieval(self):
        adapter = self.construct_storage_adapter()

        event_category = random_event_category_name()
        event_stream = random_event_stream_name()

        new_event = NewEventBuilder().with_payload({"amount": 100}).build()

        await adapter.save(
            target=identifier.StreamIdentifier(
                category=event_category, stream=event_stream
            ),
            events=[new_event],
        )

        retrieved = await self.retrieve_events(adapter=adapter)
        assert retrieved[0].payload == {"amount": 100}
        assert retrieved[0].metadata == {}
```

- [ ] **Step 2: Run to verify failure for postgres**

Find the postgres test class: `grep -n "class .*Cases\|class Test"
tests/integration/logicblocks/event/store/adapters/test_postgres.py`.
Run: `DB_PORT=<port> mise exec -- invoke test.integration -t "-k 'event_metadata'"`
Expected: FAIL — the `metadata` column doesn't exist yet in the INSERT, or the
inserted row is missing metadata. (Memory may also fail until Task 5; that's
fine — these are shared tests.)

- [ ] **Step 3: Add metadata to the INSERT**

In `insert_batch_query` (≈ lines 407-462):
- Change the row placeholder from 8 to 9 columns:
  `rows.append(sql.SQL("(%s, %s, %s, %s, %s, %s, %s, %s, %s)"))`
- Add the metadata value right after the payload value in the `values.extend([...])`:
  ```python
                    Jsonb(serialise_to_json_value(event.payload)),
                    Jsonb(serialise_to_json_value(event.metadata)),
                    event.observed_at,
                    event.occurred_at,
  ```
- Add `metadata` to the INSERT column list (after `payload`):
  ```python
                INSERT INTO {0} (id,
                                 name,
                                 stream,
                                 category,
                                 position,
                                 payload,
                                 metadata,
                                 observed_at,
                                 occurred_at)
  ```

- [ ] **Step 4: Carry metadata through the RETURNING mapping**

In the function that maps inserted rows back to caller-facing
`StoredEvent[Name, Payload]` (≈ lines 575-600), add `metadata=event.metadata`
(from the ORIGINAL event, mirroring how `payload=event.payload` is used):

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

- [ ] **Step 5: Reads need no change**

The read paths use `class_row(StoredEvent[str, JsonValue])`. Once the
`metadata` column exists and `StoredEvent` has a `metadata` field, psycopg maps
the column to the field automatically. Do NOT add a custom row factory. (If a
read maps `metadata` to a JSON string rather than a dict, confirm the column is
`JSONB` — psycopg returns JSONB as parsed Python objects.)

- [ ] **Step 6: Run to verify pass**

Run: `DB_PORT=<port> mise exec -- invoke test.integration -t "-k 'event_metadata'"`
→ all metadata tests pass.
Run: `DB_PORT=<port> mise exec -- invoke test.integration` (full) → all pass.
Run: `mise exec -- invoke test.unit` (full) → all pass (memory shared tests too,
assuming Task 5 done).
Run: `mise exec -- invoke types.check` → 0 errors.

---

## Task 7: Changelog fragment

**Files:**
- Create: a fragment under `changelog.d/`

- [ ] **Step 1: Create the fragment**

Match the existing naming convention `YYYYMMDD_HHMMSS_author_slug.md` (see
existing files in `changelog.d/`). Create
`changelog.d/<timestamp>_<author>_event_metadata.md` with both an `### Added`
section and a migration note:

```markdown
### Added

- Events now support an optional `metadata` field for recording cross-cutting
  context (such as the actor responsible for an event). `NewEvent` and
  `StoredEvent` gain a third generic parameter, `Metadata`, defaulting to an
  empty mapping when omitted. Metadata is persisted in a dedicated
  `metadata JSONB NOT NULL` column on the `events` table, mirroring the existing
  `projections.metadata` column.

### Changed

- The `events` table has a new `metadata JSONB NOT NULL` column. This requires
  a migration for existing deployments:

  ```sql
  ALTER TABLE events ADD COLUMN metadata JSONB NOT NULL DEFAULT '{}';
  ALTER TABLE events ALTER COLUMN metadata DROP DEFAULT;
  ```

  The transient default backfills existing rows, then is dropped so the column
  matches the canonical schema in `sql/create_events_table.sql`.
```

- [ ] **Step 2: Verify it assembles (optional)**

If convenient: `mise run changelog:assemble` (or inspect existing fragments to
confirm format). Otherwise eyeball against a sibling fragment.

---

## Task 8: Full build verification

- [ ] **Step 1: Run the full build**

Run: `DB_PORT=<port> mise run`
Expected: PASS — lint, types, format, build, and all tests (unit, integration,
component) green.

- [ ] **Step 2: Fix any lint/type/format issues**

If `mise exec -- invoke lint.fix` / `format.fix` make changes, keep them. If
`types.check` flags the `Metadata` generic anywhere it's threaded (publish,
adapter signatures), add the third generic consistently.

---

## Self-Review Notes

- **Spec coverage:** column DDL (Task 1), `metadata` generic + field + default
  on NewEvent/StoredEvent (Tasks 2-3), serialise/summarise/repr +
  serialise_stored_event (Tasks 2-3), builders (Task 4), memory persist/return
  (Task 5), postgres INSERT + RETURNING + automatic read (Task 6), shared
  round-trip tests for both adapters (Task 6), migration + changelog (Task 7),
  full build (Task 8). All spec sections map to tasks.
- **No fold/split, no guards, no reserved key, no custom row factory, no
  `metadata.py`** — explicitly removed in Task 0 and absent from all tasks.
- **Type soundness:** the `cast(Callable[[], Metadata], lambda: {})` default on
  StoredEvent and the `metadata if metadata is not None else {}` defaults on
  NewEvent/builders are the established patterns; `serialise_stored_event` and
  `from_new_event` thread the third generic.
- **Clean baseline:** Task 0 discards all prior fold-based work and verifies a
  green starting point before any new code.
- **Verification points flagged for the implementer:** exact postgres/memory
  test-class names and the precise line numbers of the INSERT and RETURNING
  sites must be confirmed via `grep` before editing (line numbers above are
  approximate, against `main`).
