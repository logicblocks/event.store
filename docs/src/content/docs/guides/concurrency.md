---
title: Concurrency and write conditions
description: Use write conditions, retries and serialisation guarantees to handle concurrent writers safely.
---

Most writes follow the same pattern: read a stream, decide what happened,
then publish new events. If two writers do this at the same time, both can
make a decision based on the same state, and one of them will be wrong.

The store handles this with optimistic concurrency. You publish with a
_write condition_ that describes the state you based your decision on. The
storage adapter checks the condition while holding its write lock, and
rejects the write if the stream has changed.

The examples on this page use `InMemoryEventStorageAdapter`, so they run
without a database. For production storage, see the
[PostgreSQL guide](/event.store/guides/postgres/).

## Write conditions

Pass a condition to `publish` with `condition=`. The conditions are in
[`logicblocks.event.store.conditions`](/event.store/reference/store/#logicblocks.event.store.conditions):

- [`stream_is_empty()`](/event.store/reference/store/#logicblocks.event.store.conditions.stream_is_empty)
  is met when the stream has no events. Use it when creating something that
  must only be created once.
- [`position_is(position)`](/event.store/reference/store/#logicblocks.event.store.conditions.position_is)
  is met when the latest event in the stream is at `position`. Use it with
  the position of the last event you read. `position_is(None)` is met only
  when the stream is empty.
- [`NoCondition()`](/event.store/reference/store/#logicblocks.event.store.conditions.NoCondition)
  is always met. It is the default, so `publish` without `condition=` always
  writes.

If the condition is not met, `publish` raises
[`UnmetWriteConditionError`](/event.store/reference/store/#logicblocks.event.store.UnmetWriteConditionError)
and none of the events are written. The error's `message` says which
condition failed.

```python
import asyncio

from logicblocks.event.store import (
    EventStore,
    UnmetWriteConditionError,
    adapters,
    conditions,
)
from logicblocks.event.types import NewEvent


def event(name):
    return NewEvent(name=name, payload={}, metadata=None)


async def main():
    store = EventStore(adapters.InMemoryEventStorageAdapter())
    order = store.stream(category="orders", stream="order-1")

    await order.publish(
        events=[event("order-placed")],
        condition=conditions.stream_is_empty(),
    )

    try:
        await order.publish(
            events=[event("order-placed")],
            condition=conditions.stream_is_empty(),
        )
    except UnmetWriteConditionError as error:
        print(error.message)
    # stream is not empty

    latest = await order.latest()
    await order.publish(
        events=[event("item-added")],
        condition=conditions.position_is(latest.position),
    )

    try:
        await order.publish(
            events=[event("item-added")],
            condition=conditions.position_is(latest.position),
        )
    except UnmetWriteConditionError as error:
        print(error.message)
    # unexpected stream position

    print([stored.name for stored in await order.read()])
    # ['order-placed', 'item-added']


asyncio.run(main())
```

The second `position_is` write fails because the first one moved the stream
on from position 0 to position 1.

Conditions apply to a single stream. When you
[publish to several streams in a category](/event.store/guides/streams-categories-and-logs/#publishing-to-several-streams-at-once),
each stream has its own condition, and if any of them is not met, nothing is
written.

## Combining conditions

Combine conditions with `&`, where all must be met, and `|`, where at least
one must be met:

```python
from logicblocks.event.store import conditions

either = conditions.position_is(3) | conditions.stream_is_empty()
both = conditions.position_is(3) & conditions.NoCondition()
```

`&` gives an
[`AndCondition`](/event.store/reference/store/#logicblocks.event.store.conditions.AndCondition)
and `|` gives an
[`OrCondition`](/event.store/reference/store/#logicblocks.event.store.conditions.OrCondition).
Chaining the same operator gives one flat condition, so `a | b | c` is a
single `OrCondition` of three conditions.

The built-in conditions all check the position of the latest event in the
stream, so `|` is the more useful operator with them. It lets a write go
ahead when the stream is in any of several acceptable states.

## Retrying on conflict

When a write is rejected, the usual response is to read the stream again,
make the decision again and publish again. The
[`retry_on_unmet_condition_error`](/event.store/reference/store/#logicblocks.event.store.retry_on_unmet_condition_error)
decorator does this for you. It calls the decorated function again when it
raises `UnmetWriteConditionError`, up to `tries` times in total.

Put the read, the decision and the publish inside the decorated function,
so that each attempt works from fresh state. In this example, two
withdrawals race against the same account:

```python
import asyncio

from logicblocks.event.store import (
    EventStore,
    EventStream,
    adapters,
    conditions,
    retry_on_unmet_condition_error,
)
from logicblocks.event.types import NewEvent


class InsufficientFundsError(Exception):
    pass


async def read_balance(account: EventStream) -> tuple[int, int | None]:
    balance = 0
    position = None
    async for event in account:
        match event.name:
            case "funds-deposited":
                balance += event.payload["amount"]
            case "funds-withdrawn":
                balance -= event.payload["amount"]
        position = event.position
    return balance, position


@retry_on_unmet_condition_error(tries=3)
async def withdraw(account: EventStream, amount: int) -> None:
    balance, position = await read_balance(account)
    print(f"withdrawing {amount} from a balance of {balance}")

    if amount > balance:
        raise InsufficientFundsError(f"cannot withdraw {amount}")

    await account.publish(
        events=[
            NewEvent(
                name="funds-withdrawn", payload={"amount": amount}, metadata=None
            )
        ],
        condition=conditions.position_is(position),
    )


async def main():
    store = EventStore(adapters.InMemoryEventStorageAdapter())
    account = store.stream(category="accounts", stream="acc-1")
    await account.publish(
        events=[
            NewEvent(name="funds-deposited", payload={"amount": 100}, metadata=None)
        ]
    )

    results = await asyncio.gather(
        withdraw(account, 60),
        withdraw(account, 50),
        return_exceptions=True,
    )
    print(results)
    print(await read_balance(account))


asyncio.run(main())
```

Which withdrawal wins the race can vary between runs. One run printed:

```text
withdrawing 60 from a balance of 100
withdrawing 50 from a balance of 100
withdrawing 60 from a balance of 50
[InsufficientFundsError('cannot withdraw 60'), None]
(50, 1)
```

Both withdrawals read a balance of 100. The withdrawal of 50 was published
first, so the withdrawal of 60 was rejected. On its second attempt it read a
balance of 50 and refused to overdraw the account. Without the condition,
both withdrawals would have been written, leaving a balance of -10.

A few things to keep in mind:

- `position` is `None` when the stream is empty, and `position_is(None)`
  is met only by an empty stream, so the same code works for the first
  write.
- Only `UnmetWriteConditionError` is retried. Other exceptions, such as
  `InsufficientFundsError` here, propagate straight away.
- If every attempt is rejected, the error from the last attempt propagates.
- Retries happen immediately, with no delay between attempts.
- `EventStream.publish` logs a warning, `event.stream.publish-failed`, each
  time a condition is not met.

## Ignoring conflicts

Sometimes a rejected write means the work has already been done, and there
is nothing to retry. The
[`ignore_on_unmet_condition_error`](/event.store/reference/store/#logicblocks.event.store.ignore_on_unmet_condition_error)
decorator suppresses `UnmetWriteConditionError` and makes the function
return `None` instead. Note that it is instantiated with `()`:

```python
import asyncio

from logicblocks.event.store import (
    EventStore,
    EventStream,
    adapters,
    conditions,
    ignore_on_unmet_condition_error,
)
from logicblocks.event.types import NewEvent


@ignore_on_unmet_condition_error()
async def open_account(account: EventStream, owner: str):
    return await account.publish(
        events=[
            NewEvent(name="account-opened", payload={"owner": owner}, metadata=None)
        ],
        condition=conditions.stream_is_empty(),
    )


async def main():
    store = EventStore(adapters.InMemoryEventStorageAdapter())
    account = store.stream(category="accounts", stream="acc-1")

    first = await open_account(account, "Joe")
    second = await open_account(account, "Jane")

    print(first[0].payload, second)
    # {'owner': 'Joe'} None


asyncio.run(main())
```

## Writing your own transaction decorators

The decorators above are built on
[`event_store_transaction`](/event.store/reference/store/#logicblocks.event.store.event_store_transaction).
To retry or ignore a different exception, subclass
[`retry_on_error`](/event.store/reference/store/#logicblocks.event.store.retry_on_error)
or
[`ignore_on_error`](/event.store/reference/store/#logicblocks.event.store.ignore_on_error)
and set the `exception` class attribute. Exceptions that are subclasses of
`exception` are also caught:

```python
from logicblocks.event.store import ignore_on_error, retry_on_error


class retry_on_timeout(retry_on_error):
    exception = TimeoutError


class ignore_on_lookup_error(ignore_on_error):
    exception = LookupError
```

For other behaviour, subclass `event_store_transaction` and implement
`wrap_handler`. It receives a zero-argument callable that runs the decorated
function with its original arguments. This decorator waits a little longer
after each conflict before trying again:

```python
import asyncio
from collections.abc import Awaitable, Callable

from logicblocks.event.store import (
    EventStore,
    EventStream,
    UnmetWriteConditionError,
    adapters,
    conditions,
    event_store_transaction,
)
from logicblocks.event.types import NewEvent


class retry_with_backoff(event_store_transaction):
    def __init__(self, tries: int, delay: float):
        self.tries = tries
        self.delay = delay

    async def wrap_handler[R](self, handler: Callable[[], Awaitable[R]]) -> R:
        for attempt in range(1, self.tries):
            try:
                return await handler()
            except UnmetWriteConditionError:
                print(f"attempt {attempt} conflicted")
                await asyncio.sleep(self.delay * attempt)
        return await handler()


@retry_with_backoff(tries=3, delay=0.1)
async def publish_at_wrong_position(account: EventStream):
    await account.publish(
        events=[NewEvent(name="funds-deposited", payload={}, metadata=None)],
        condition=conditions.position_is(99),
    )


async def main():
    store = EventStore(adapters.InMemoryEventStorageAdapter())
    account = store.stream(category="accounts", stream="acc-1")

    try:
        await publish_at_wrong_position(account)
    except UnmetWriteConditionError as error:
        print("gave up:", error.message)
    # attempt 1 conflicted
    # attempt 2 conflicted
    # gave up: unexpected stream position


asyncio.run(main())
```

## Serialisation guarantees

Write conditions are checked while the storage adapter holds a write lock,
so no other write can change the stream between the check and the insert.
An
[`EventSerialisationGuarantee`](/event.store/reference/store/#logicblocks.event.store.adapters.EventSerialisationGuarantee)
sets how much of the store that lock covers:

| Guarantee  | Writes that are serialised with each other    |
| ---------- | --------------------------------------------- |
| `LOG`      | All writes to the store. This is the default. |
| `CATEGORY` | Writes to the same category.                  |
| `STREAM`   | Writes to the same stream.                    |

Pass the guarantee when you construct the adapter:

```python
from logicblocks.event.store import adapters
from logicblocks.event.store.adapters import EventSerialisationGuarantee

adapter = adapters.InMemoryEventStorageAdapter(
    serialisation_guarantee=EventSerialisationGuarantee.STREAM
)
```

`PostgresEventStorageAdapter` takes the same `serialisation_guarantee=`
argument. It uses PostgreSQL advisory locks, so the guarantee holds across
processes. The in-memory adapter's locks only apply within one adapter
instance.

Every guarantee covers the stream being written to, so write conditions
work correctly whichever you choose. The choice is a trade-off between write
throughput and the order in which readers see events:

- A narrower guarantee lets more writes happen at the same time. With
  `LOG`, every write waits for every other write. With `STREAM`, writes to
  different streams don't wait for each other.
- With a narrower guarantee, events are not always committed in sequence
  number order. With `CATEGORY`, a reader of the log can see an event with
  a later sequence number before an event from another category with an
  earlier one has been committed. With `STREAM`, the same applies to readers
  of a category.

The second point matters for anything that remembers the last sequence
number it read and later asks for events
[after it](/event.store/guides/streams-categories-and-logs/#reading-from-a-point-with-constraints),
because it can skip an event that was committed late. Reading a single
stream is safe under every guarantee. Readers of a category are safe under
`LOG` and `CATEGORY`, and readers of the whole log are only safe under
`LOG`. If you consume a category or the log with an
[event processor](/event.store/guides/event-processing/), choose a guarantee
at least as wide as what it reads. For how this plays out on PostgreSQL,
including switching guarantee on a live service, see
[Running PostgreSQL in production](/event.store/guides/postgres-in-production/#choose-the-serialisation-guarantee).

When you publish to several streams in a category at once, the adapter takes
one lock under `LOG` or `CATEGORY`: the lock for the whole store or for the
category. Under `STREAM`, it takes the lock for each stream being written,
in name order.
