---
type: "pr-review"
id: "137-review-1"
title: "Reset cached consumer state after a rejected save so it reloads instead of staying stale"
date: "2026-10-13T15:23:58+00:00"
author: "Toby Clemson"
producer: "review-pr"
status: "complete"
target: "pr:137"
reviewer: "Toby Clemson"
verdict: "COMMENT"
lenses: ["correctness", "test-coverage", "code-quality", "architecture", "safety", "standards"]
review_number: 1
pr_number: 137
tags: []
last_updated: "2026-10-13T15:23:58+00:00"
last_updated_by: "Toby Clemson"
schema_version: 1
---

## Code Review: #137 - Reset cached consumer state after a rejected save so it reloads instead of staying stale

**Verdict:** COMMENT

This is a small, well-targeted fix for a real liveness failure. The rejection is still raised, only the affected partition is invalidated, and regression tests at three layers (store, source consumer, subscription) fail on `main` and pass here. Every lens found the change safe to merge as an improvement. The main concerns are about how far the recovery goes, not whether it works. Invalidating by deleting the cache entries makes a rejected partition look "never loaded", so a caller that records progress before calling `load()` gets a save that silently skips the conflict check. The rebalance scenario is also handled by failing a write rather than at the subscriber's `withdraw`/`accept` boundary. Finally, a few test gaps let parts of `_invalidate` change without any test failing.

### Cross-Cutting Themes

- **Invalidating by deletion lets the next save skip the conflict check** (flagged by: architecture, correctness, safety). After `_invalidate`, the partition is indistinguishable from one that was never loaded. If `record_processed` runs before `load()` (for example a `ManagedEventIteratorProcessor` that catches the commit error, or another task sharing the store), the next `save()` conditions on `latest()` and overwrites the other writer's state. That conflict was detected before this change. The simplest fix may be to keep the stale `_positions` entry when invalidating, so that `load()` still reloads but a save without a reload is rejected again. Alternatives are an explicit "stale" marker per partition, or at least documenting that callers must call `load()` again after this error.
- **Recovery should also happen at the ownership boundary** (flagged by: architecture, correctness). `make_subscriber` shares one state store across `withdraw`/`accept`, so every rebalance back to an earlier owner costs a duplicate pass, a rejected write and a `publish-failed` warning before recovery. The subscription test asserts this cost as the expected behaviour. Resetting the cache on `withdraw`/`accept`, or creating the store per delegate, would avoid it, and the save-time invalidation would remain the fallback for genuinely concurrent writers.
- **Invalidation is silent** (flagged by: code-quality, safety, architecture). This answers open question 1 with a yes. Overlapping writers now alternate between rejecting and reprocessing for as long as the overlap lasts, instead of failing loudly, so operators need a dedicated signal to tell this apart from transient noise.
- **`try/except: pass` in the test arrangement** (flagged by: code-quality, standards, test-coverage). If the arranged conflict ever stops happening, `test_retains_other_partitions…` passes without testing anything.
- **Duplicated conflict setup across the four store tests** (flagged by: code-quality, test-coverage).

### Tradeoff Analysis

- **Where to log (store vs consumer layer)**: Code quality and safety suggest an injected logger on `EventConsumerStateStore`. Architecture prefers logging in `EventSourceConsumer` or `StateStoreEventProcessorManager.commit`, which keeps the store free of observability and puts source and subscriber context on the log event. Recommendation: log from the consumer layer, where that context is available.
- **`pytest.raises` vs `contextlib.suppress` in the arrangement**: Standards points to the existing `suppress` idiom (`test_store.py:759`). Test coverage and code quality want `pytest.raises` so that a missing conflict fails the test. Recommendation: use `pytest.raises`, because the rejection is a required precondition here, not an optional one.
- **Shared vs per-file `CapturingEventProcessor`**: Code quality flags this as the fourth copy. Standards notes that per-file test doubles are the existing convention. A low-priority follow-up at most.

### Strengths

- ✅ Catches only `UnmetWriteConditionError` and re-raises it, so error handlers and `publish-failed` logging keep their meaning. Only what the store remembers afterwards changes.
- ✅ Invalidation is limited to the rejected partition, so other partitions keep their unsaved progress, and a guard test checks this.
- ✅ `save(partition=None)` iterates over a list copy of the keys, so `_invalidate` removing entries during the loop is safe.
- ✅ Duplicate side-effects are now limited to roughly one window per conflict, instead of the old behaviour of repeating the same batch forever.
- ✅ Making the fix the default, rather than opt-in, is right (open question 3): the old behaviour can never recover.
- ✅ Regression tests at three layers use the real in-memory adapter and assert the complete processed-event sequence, including the deliberate re-processing. They follow the existing naming style, import ordering (ruff isort) and scriv fragment conventions.
- ✅ The changelog fragment states plainly that events will be processed again after recovery.

### General Findings

- 🔵 **Architecture**: The store uses the same cache as the progress cursor, as a read-through cache and as the optimistic-concurrency token, and has no public refresh. Adding a small public `invalidate(partition)` (or `load(..., refresh=True)`) would give one explicit recovery entry point for both this path and the subscription lifecycle.
- 🔵 **Safety**: Overlapping live writers now keep running side by side and keep producing duplicate side-effects, with immediate retries under the default `RetryErrorHandler`. Consider recommending idempotent processors and a backoff strategy in the docs.
- 🔵 **Test Coverage**: The existing `test_consistent_concurrent_execution_*` tests don't check the store's in-memory state after the losing save invalidates. Adding an assertion that `load()` returns the winner's state, or that a follow-up `save()` succeeds, would pin down the same-instance behaviour.
- 🔵 **Architecture**: On open question 2, now that partitions are isolated, `save(partition=None)` could try every partition and raise an `ExceptionGroup`. This is fine as a follow-up, but callers that catch the error would need to switch to `except*`.
- 🔵 **Test Coverage**: Optionally, add a Postgres-backed integration reproduction of the #136/#73 scenario next to the distributed broker integration tests.

### Additional Findings

- 🔵 `src/logicblocks/event/processing/consumers/state/base.py:168` — `save()` loop body keeps growing; consider extracting `_save_partition` (code-quality)
- 🔵 `tests/unit/logicblocks/event/processing/consumers/test_state.py:1519` — Conflict setup duplicated across four new tests; extract a helper (code-quality, test-coverage)
- 🔵 `tests/unit/logicblocks/event/processing/consumers/test_subscription.py:33` — Another copy of the `CapturingEventProcessor` test double (code-quality)

## Inline Comments

### `tests/unit/logicblocks/event/processing/consumers/test_subscription.py:247-258` — Rebalance recovery belongs at the subscriber's accept/withdraw boundary
**Severity**: major | **Confidence**: high | **Lens**: architecture, correctness

🟡 **Architecture**

This test expects every rebalance that hands a source back to a previous owner to process at least one batch, fail the save, and then process the same batch again (`*publish_3_events` twice). `publish_2_events` is processed by both subscribers too. The cause is in `make_subscriber`: it builds one `EventConsumerStateStore` per subscriber and shares it through `delegate_factory`, so the cached position outlives `withdraw()` and is reused after `accept()`. `withdraw`/`accept` is exactly where ownership changes, and at that point the subscriber knows its cache may be stale. The store only finds out by failing a write.

**Impact**: Every `DistributedEventBroker` rebalance back to an earlier owner costs a guaranteed failed write, a `publish-failed` warning, an error-handler cycle and duplicate side-effects. The test now records this as expected behaviour rather than as a fallback path.

**Suggestion**: Reset cached state at the lifecycle boundary. Either create the state store inside `delegate_factory` so each accepted source gets a fresh one, or expose a public `invalidate(partition)` and call it from `withdraw`. Keep this PR's save-time invalidation as the fallback for genuinely concurrent writers. Then this test could assert that a re-accepted subscriber resumes from the other subscriber's position with no rejected save and no duplicates. This could reasonably be a follow-up PR.

---

### `tests/unit/logicblocks/event/processing/consumers/test_state.py:1676-1683` — Retained partitions' unsaved lag and the `save(partition=None)` path are untested
**Severity**: major | **Confidence**: high | **Lens**: test-coverage

🟡 **Test Coverage**

`test_retains_other_partitions_when_save_rejected_by_another_writer` only asserts that `load(partition="partition2")` returns the cached state, which comes straight from `_states`. If `_invalidate` were widened to `self._persistence_lags.clear()`, this test would still pass, yet partition2's unsaved progress would never be persisted, because `save()` iterates `_persistence_lags.keys()`. No test covers a rejection under `save(partition=None)`, which invalidates while iterating and stops at the first failure.

**Impact**: Over-eager invalidation could silently drop other partitions' unsaved progress, and the stop-at-first-failure behaviour of `save()` could change without any test noticing.

**Suggestion**: After the rejection, call `await state_store.save()` and assert the full contents of the partition2 state stream (e.g. `[{"state": {"last_sequence_number": 4}}]`). Add a sibling test where `save()` (no partition) runs while partition1 conflicts, and assert the behaviour you've chosen for open question 2.

---

### `src/logicblocks/event/processing/consumers/state/base.py:193-195` — After invalidation, `record_processed` before `load()` lets the next save skip the conflict check
**Severity**: major | **Confidence**: medium | **Lens**: architecture, correctness, safety

🟡 **Architecture / Correctness / Safety**

`_invalidate` removes the partition from all three maps, so afterwards it looks as if it had never been loaded. Two existing behaviours then combine badly:
- when `_positions` has no entry, `save()` reads `latest()` and uses that as the write condition;
- `load()` only reads the stream when `_states[partition]` is `None`.

If a caller calls `record_processed` after the rejected save and before `load()`, `load()` returns the stale in-memory state, and the next `save()` conditions on whatever is latest. That save succeeds and silently overwrites the other writer's state, which can move `last_sequence_number` backwards. Plausible callers are a `ManagedEventIteratorProcessor` that catches the error from `manager.commit()` and keeps iterating, or another task sharing the store. Before this change, that write would have been rejected.

**Impact**: The optimistic concurrency guard that detected the conflict is bypassed on the next write. Recovery is only correct if every caller stops and reloads after the exception, and the store does nothing to enforce that.

**Suggestion**: The simplest option may be to keep the stale `_positions` entry rather than removing it (see the comment on lines 194-195). Alternatively, track a per-partition `_stale` marker instead of only removing entries. `load()` re-reads a stale partition even if a state is cached, and `save()` refuses to fall back to `latest()` until the partition has been reloaded. At minimum, document that callers must `load()` after `UnmetWriteConditionError`. A store-level test of `record_processed` → `save` after a rejection (without `load`) would pin down whichever contract you choose.

---

### `tests/unit/logicblocks/event/processing/consumers/test_state.py:1549-1552` — `try/except: pass` lets the tests pass when no rejection happens
**Severity**: minor | **Confidence**: high | **Lens**: code-quality, standards, test-coverage

🔵 **Code Quality / Test Coverage / Standards**

Three of the new store tests set up the conflict with `try: await state_store.save() except UnmetWriteConditionError: pass` (here, at ~1588 and at ~1676). If that setup ever stops producing a conflict, the block passes silently. `test_retains_other_partitions_when_save_rejected_by_another_writer` would keep passing even though the scenario it guards never happens. This style also differs from the new consumer and subscription tests in this PR, which use `pytest.raises`.

**Suggestion**: Use `with pytest.raises(UnmetWriteConditionError): await state_store.save(...)` in the setup, so a missing conflict fails the test. (Standards notes that the suite already uses `contextlib.suppress` in `test_store.py:759`. That would be tidier than `try/except/pass` but still wouldn't check the precondition, so `pytest.raises` is the better choice here.)

---

### `src/logicblocks/event/processing/consumers/state/base.py:194-195` — Two of the three `_invalidate` pops can be removed without any test failing
**Severity**: minor | **Confidence**: high | **Lens**: test-coverage

🔵 **Test Coverage**

Every new test calls `load()` after the rejection, and `load()` overwrites `_positions[partition]` whenever there is no cached state, so deleting `self._positions.pop(...)` changes nothing. Every test also calls `record_processed` before the next `save`, so deleting `self._persistence_lags.pop(...)` (which leaves a stale non-zero lag) isn't caught either.

**Impact**: A refactor could drop either line without any test noticing. A stale lag would cause a redundant re-publish of the reloaded state and skew `save_if_needed` thresholds.

**Suggestion**: For the lag, add a test asserting that after the rejection and a `load()`, a `save()` with no new progress writes nothing. For the position, test `record_processed` → `save()` without `load()` and assert the exact persisted events (this ties in with the conflict-check comment above).

**Note: keeping `_positions` may be the simplest fix for the conflict-check comment above.** Writing that position test shows that `self._positions.pop(...)` is what enables the conflict-check bypass. If `_invalidate` dropped only `_states` and `_persistence_lags` and kept the stale `_positions` entry:
- `load()` still reloads, because it checks `_states`, and it overwrites the position with the persisted one, so the normal recovery path is unchanged.
- `record_processed` → `save()` without `load()` conditions on the stale position and is rejected again. That is loud and safe, rather than silently overwriting the other writer.

So removing line 194 and adding a test that pins this down may be enough, without needing a separate stale marker.

---

### `src/logicblocks/event/processing/consumers/state/base.py:181-183` — State invalidation and rewind happen silently
**Severity**: minor | **Confidence**: medium | **Lens**: safety, code-quality, architecture

🔵 **Safety / Code Quality / Architecture**

When a save is rejected, the store now quietly drops its cached state, and the consumer rewinds to the other writer's position and processes the gap again. The only signal is the generic `event.stream.publish-failed` warning, which doesn't say that a rewind and re-processing will follow, or for which partition.

**Impact**: A persistent dual-writer misconfiguration used to fail loudly and permanently. Now it self-heals every cycle while possibly repeating non-idempotent side-effects, and an operator can't tell that apart from a one-off conflict.

**Suggestion**: Yes to open question 1. Emit a warning such as `event.consumer.state-conflict` / `state-invalidated` with partition, stale position and state category, so a sustained rate can be alerted on. Architecture suggests logging from `EventSourceConsumer` (or `StateStoreEventProcessorManager.commit`) rather than adding a logger to the store, since source and subscriber context is already bound there.

---

### `src/logicblocks/event/processing/consumers/state/base.py:182` — Invalidation can discard concurrent in-memory progress, and the next save then quietly does nothing
**Severity**: suggestion | **Confidence**: medium | **Lens**: correctness

🔵 **Correctness** (suggestion)

Nothing in the library shares a store instance across concurrent tasks today (`EventSubscriptionConsumer.consume_all` runs its delegates one after another), so this only matters for user code that does so.

The `await` on `publish()` lets other tasks sharing this store instance run, and `EventStream.publish` also awaits `logger.awarning("event.stream.publish-failed")` before re-raising, which makes the gap longer. If another task calls `record_processed(partition=p)` in that window, `_invalidate(p)` discards that newer state as well. That task's next `save(p)` sees `self._states.get(p) is None`, hits `continue` and returns successfully without persisting anything.

**Impact**: Delivery is still at-least-once, because `load()` re-reads the persisted state later. But a caller gets a "successful" save that persisted nothing, and the extra reprocessing is hard to explain.

**Suggestion**: Capture the state object before publishing and only invalidate if `self._states.get(partition) is state`. Otherwise, drop only `_positions[partition]`. Alternatively, document that a single store instance must not be shared by concurrent writers of the same partition.

---

### `changelog.d/20261012_153641_leo.andreev_stale_consumer_state.md:6-11` — Overlapping writers now take turns failing instead of staying stuck; say so
**Severity**: minor | **Confidence**: medium | **Lens**: architecture

🔵 **Architecture**

The fragment presents the rolling-deploy overlap as fixed. `SingletonEventBroker` has no ownership enforcement or fencing, so two overlapping instances still both consume the source. With this change they take turns: one's save is rejected, it reloads, reprocesses from the other's position, and races again, for as long as the overlap lasts. Only liveness is restored. Single ownership (likely the real subject of #73) is still unaddressed.

**Suggestion**: Describe this as a recovery fallback: overlapping consumers will alternate and reprocess until one stops, and preventing overlap is still the broker's job. Consider a follow-up issue for lease or fencing semantics in the singleton broker.

---

### `tests/unit/logicblocks/event/processing/consumers/source/test_source_event_processor.py:417-418` — Consumer-level recovery is only tested at the final forced save
**Severity**: minor | **Confidence**: medium | **Lens**: test-coverage

🔵 **Test Coverage**

With the default persistence interval of 100 and only three events, the rejection always comes from the forced `save()` at the end of consumption, never from `save_if_needed()` part-way through. Only the plain `EventProcessor` path is covered. The `ManagedEventIteratorProcessor` and `AutoCommitEventIteratorProcessor` paths aren't, and the managed path is the most plausible route to the `record_processed`-before-`load` issue.

**Suggestion**: Add a variant with `persistence_interval=EventCount(1)` so the rejection is raised part-way through the stream. Consider one managed-processor test that commits after the other consumer has saved, asserting the full processed and persisted sequences.

---

### `src/logicblocks/event/processing/consumers/state/base.py:188-192` — Generic method name relies on a comment to explain intent
**Severity**: minor | **Confidence**: medium | **Lens**: code-quality, standards

🔵 **Code Quality / Standards**

CLAUDE.md reserves comments for extremely non-obvious, risky behaviour and prefers intent-revealing names. The 4-line comment mostly explains what `_invalidate` does and why `save()` calls it, which a better name could say. The rest of the module has no inline comments.

**Suggestion**: Rename to something like `_discard_stale_partition(partition)` / `_forget_partition(partition)` and drop the comment. If you keep one, make it a single line in the `except` block about the risky part (that callers must `load()` again before saving).

---

## Per-Lens Results

### Correctness

**Summary**: Sound for the target scenario. Invalidation ends the permanent save-failure loop, and the error still reaches `consume_all`'s callers. Remaining concerns are edge cases: invalidation can discard concurrent in-memory progress (the next save then quietly does nothing), `record_processed` before `load()` lets the next save skip the conflict check and overwrite the other writer, and a re-accepted subscriber only discovers it is stale after a duplicate pass.

**Strengths**:
- `save(partition=None)` copies the keys into a list, so popping during the loop is safe.
- Only the rejected partition is invalidated.
- Re-raising stops the current pass, and retry handlers start the next one via `load()`.
- There is no await between catching the error and invalidating, and `pop(..., None)` is safe. The defaultdict recreates the lag.
- Same-instance concurrent saves still end up in a consistent state.

**Comments**:
- `base.py:181-183` (minor, medium): invalidation can discard concurrent in-memory progress, and the next save quietly does nothing.
- `base.py:193-195` (minor, medium): `record_processed` before `load()` lets the next save overwrite the other writer's state.

**General Findings**:
- (minor) A re-accepted subscriber starts from its stale cached state, and the conflict is only detected after a duplicate pass. Invalidate on `withdraw`/`accept`, or create the store per delegate.

### Test Coverage

**Summary**: Good regression coverage at three layers, with complete expected sequences. Gaps: some `_invalidate` statements can be removed without failures, the multi-partition `save()` path and retained-partition lag are untested, and one guard test can pass without a rejection.

**Strengths**:
- The headline tests fail on `main`.
- Guard tests against over-invalidating and against swallowing the error.
- Complete processed-sequence assertions.
- Real in-memory adapter, with the Postgres conflict behaviour covered by the shared adapter cases.
- The amount of testing is proportionate to the risk.

**Comments**:
- `base.py:193-195` (minor, high): two of the three pops can be removed without any test failing.
- `test_state.py:1676-1683` (major, high): retained partitions' lag and `save(None)` are untested.
- `test_state.py:1676-1679` (minor, high): `try/except-pass` lets the tests pass without a rejection.
- `test_source_event_processor.py:417-418` (minor, medium): only the final forced save is tested.
- `test_state.py:1519` (suggestion, medium): conflict setup is duplicated.

**General Findings**:
- (minor) The concurrent same-instance tests don't check the cache after invalidation.
- (suggestion) Optional Postgres-backed reproduction.

### Code Quality

**Summary**: The production change is small and focused. Concerns: `save()` loop growth, silent invalidation, duplicated test setup, and `try/except: pass` in tests.

**Strengths**:
- Narrow except clause that re-raises.
- Invalidation extracted into a helper.
- Partition-scoped invalidation.
- Behaviour-describing test names with one complete equality assertion each.

**Comments**:
- `base.py:168-183` (minor, medium): extract `_save_partition`.
- `base.py:188-192` (suggestion, low): generic name relies on a comment.
- `base.py:181-183` (minor, medium): invalidation is silent.
- `test_state.py:1549-1552` (minor, high): `try/except: pass` swallows the error.
- `test_state.py:1558-1593` (minor, medium): setup is duplicated across four tests.
- `test_subscription.py:33-38` (suggestion, medium): another copy of `CapturingEventProcessor`.

**General Findings**: none.

### Architecture

**Summary**: Small and well-scoped, with no new public interface, and it fixes a real liveness failure. But it treats a symptom at the lowest layer. The stale cache comes from the subscription lifecycle and from the lack of broker ownership enforcement, and recovery relies on an unstated "always `load()` first" rule.

**Strengths**:
- Partition-scoped invalidation.
- Exception re-raised, so error handling keeps its meaning.
- No public interface change.
- Tests at three layers.
- Default-on rather than opt-in is correct.

**Comments**:
- `base.py:193-195` (major, medium): invalidating by deletion lets the next save skip the conflict check.
- `test_subscription.py:247-258` (major, high): rebalance recovery belongs at `accept`/`withdraw`.
- `base.py:181-183` (minor, medium): emit the invalidation signal from the consumer layer.
- `changelog.d/...:6-11` (minor, medium): state the alternating-failure tradeoff.

**General Findings**:
- (minor) The store uses one cache for three jobs and has no explicit refresh. Add a public `invalidate`/`refresh`.
- (suggestion) `save(partition=None)`: try every partition and raise an `ExceptionGroup`.

### Safety

**Summary**: A clear safety improvement. Duplicates are now limited per conflict instead of repeating forever, and the persisted stream stays protected by optimistic concurrency. Residual risks: silent recovery, a swallowed rejection becoming a near-blind overwrite, and overlapping writers now coexisting indefinitely.

**Strengths**:
- Still re-raises.
- Partition-scoped.
- Reloads the other writer's state rather than forcing its own.
- Duplicates are limited per conflict.
- The changelog is honest about re-processing.

**Comments**:
- `base.py:181-183` (minor, medium): invalidation and rewind are silent.
- `base.py:193-195` (minor, low): a swallowed rejection plus a cleared position can lead to a blind overwrite.

**General Findings**:
- (minor) Overlapping live writers now coexist and produce ongoing duplicate side-effects. Recommend idempotent processors and backoff.

### Standards

**Summary**: Follows conventions closely: import ordering matches ruff isort, the scriv fragment naming and category are correct, and test names match their classes. Minor points: the explanatory comment goes against the comments policy, and `try/except/pass` is used instead of the existing `suppress` idiom.

**Strengths**:
- `import pytest` placement is consistent with isort and existing files.
- Fragment naming and `### Fixed` category.
- Descriptive test names.
- Per-file capturing test double follows existing convention.
- Private underscore helper; imports the public `UnmetWriteConditionError` export.

**Comments**:
- `base.py:189-192` (minor, medium): replace the explanatory comment with an intention-revealing method name. *(The agent anchored this at lines 200-204, outside the diff hunk. It was re-anchored to the comment's real location and merged with the code-quality naming comment.)*
- `test_state.py:1549-1552` (suggestion, medium): use `contextlib.suppress` instead of `try/except/pass`.

**General Findings**: none.

---
*Review generated by /accelerator:review-pr*
