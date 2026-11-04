---
title: Services and error handling
description: Run long-lived processes with ServiceManager, track their health and decide what happens when they fail.
---

An application that processes events usually runs several long-lived tasks
side by side: an event broker, one or more subscribers polling for new
events, and perhaps some housekeeping. The
[`logicblocks.event.processing`](/event.store/reference/processing/)
package gives you a small set of building blocks for this:

- a [`Service`](/event.store/reference/processing/#logicblocks.event.processing.Service)
  is a unit of work with an async `execute` method;
- a [`ServiceManager`](/event.store/reference/processing/#logicblocks.event.processing.ServiceManager)
  starts and stops a collection of services;
- wrapper services add polling, status tracking and error handling to any
  other service;
- error handlers decide what happens when a service raises.

Everything on this page imports from `logicblocks.event.processing`. The
examples run without a database.

## Services

Subclass `Service` and implement `execute`. The type parameter is the type
`execute` returns:

```python
class Greeter(Service[str]):
    def __init__(self, name: str):
        self._name = name

    async def execute(self) -> str:
        return f"Hello, {self._name}"
```

If you already have a zero-argument async function, wrap it in a
[`CallableService`](/event.store/reference/processing/#logicblocks.event.processing.CallableService)
instead of writing a class:

```python
async def heartbeat() -> None:
    while True:
        await asyncio.sleep(1)


service = CallableService(heartbeat)
```

`CallableService.from_maybe_callable` accepts either a service or a
callable. It returns a service unchanged and wraps a callable in a
`CallableService`.

## Running services with ServiceManager

Register each service with a `ServiceManager`, then start it. `register`
returns the manager, so calls can be chained:

```python
import asyncio

from logicblocks.event.processing import (
    CallableService,
    ExecutionMode,
    Service,
    ServiceManager,
)


class Greeter(Service[str]):
    def __init__(self, name: str):
        self._name = name

    async def execute(self) -> str:
        return f"Hello, {self._name}"


async def heartbeat() -> None:
    while True:
        await asyncio.sleep(0.1)


async def main():
    manager = (
        ServiceManager()
        .register(
            Greeter("Joe"),
            name="greeter",
            execution_mode=ExecutionMode.FOREGROUND,
        )
        .register(CallableService(heartbeat), name="heartbeat")
    )

    await manager.start()
    print(await manager.service("greeter").future)
    # Hello, Joe

    await asyncio.sleep(0.25)
    await manager.stop()

    print(manager.service("heartbeat").future.done())
    # True


asyncio.run(main())
```

Each service has a name. Pass `name=` to choose one, or leave it out and
the manager generates a random one. Registering two services with the same
name raises a `ValueError`.

### Execution modes

[`ExecutionMode`](/event.store/reference/processing/#logicblocks.event.processing.ExecutionMode)
controls whether `start()` waits for a service:

- `BACKGROUND`, the default, starts the service and moves on. Use it for
  services that run until they are stopped.
- `FOREGROUND` makes `start()` wait until the service has finished. Use it
  for one-off work, such as a migration, that must complete before the
  rest of your code carries on.

`start()` doesn't raise if a `FOREGROUND` service fails. The exception is
kept on that service's future instead (see
[Inspecting services](#inspecting-services)).

### Isolation modes

[`IsolationMode`](/event.store/reference/processing/#logicblocks.event.processing.IsolationMode)
controls which thread and event loop a service runs on:

- `MAIN_THREAD`, the default, runs the service as a task on the event loop
  that called `start()`.
- `SHARED_THREAD` runs the service on a second event loop in a separate
  thread. All `SHARED_THREAD` services share that thread.
- `DEDICATED_THREAD` gives the service its own thread and event loop.

Use a separate thread for a service that blocks its event loop, for
example with CPU-heavy or synchronous work, so that it doesn't hold up
the others. The other threads use [uvloop](https://github.com/MagicStack/uvloop)
event loops. Objects tied to an event loop, such as an `asyncio.Lock` or a
database connection pool, must be used from the loop that created them.

### Starting and stopping

`start()` starts every registered service. `stop()` cancels every running
service, on every thread, waits for them to finish and shuts down the
threads. A manager is used once: you can't start it again after stopping
it.

`ServiceManager` is also an async context manager. It starts on entry and
stops on exit, including when the body raises:

```python
import asyncio

from logicblocks.event.processing import (
    CallableService,
    ExecutionMode,
    IsolationMode,
    ServiceManager,
)


async def fail() -> None:
    raise RuntimeError("boom")


async def forever() -> None:
    await asyncio.Event().wait()


async def main():
    manager = (
        ServiceManager()
        .register(
            CallableService(fail),
            name="failing",
            execution_mode=ExecutionMode.FOREGROUND,
        )
        .register(
            CallableService(forever),
            name="worker",
            isolation_mode=IsolationMode.DEDICATED_THREAD,
        )
    )

    async with manager:
        failing = manager.service("failing")
        print(failing.future.done(), repr(failing.future.exception()))
        # True RuntimeError('boom')

        worker = manager.service("worker")
        print(worker.isolation_mode, worker.execution_mode)
        # IsolationMode.DEDICATED_THREAD ExecutionMode.BACKGROUND

    print(worker.future.done())
    # True


asyncio.run(main())
```

### Stopping on signals

`stop_on` installs handlers for the given signals when the manager starts,
and stops the manager when one arrives. A typical entry point starts the
manager and then waits for all of the services to finish:

```python
import asyncio
import os
import signal

from logicblocks.event.processing import (
    CallableService,
    IsolationMode,
    ServiceManager,
)


async def worker() -> None:
    await asyncio.Event().wait()


async def send_sigterm_soon() -> None:
    await asyncio.sleep(0.2)
    os.kill(os.getpid(), signal.SIGTERM)


async def main():
    manager = (
        ServiceManager()
        .register(
            CallableService(worker),
            name="worker",
            isolation_mode=IsolationMode.SHARED_THREAD,
        )
        .register(CallableService(send_sigterm_soon), name="signaller")
        .stop_on([signal.SIGINT, signal.SIGTERM])
    )

    await manager.start()
    await asyncio.gather(
        *(state.future for state in manager.services.values()),
        return_exceptions=True,
    )
    print("all services finished")


asyncio.run(main())
```

The `signaller` service is only there to make the example end on its own.

:::caution
`stop()` can only be called once. Don't combine `stop_on` with
`async with` or your own call to `stop()`, because the second stop fails
with `RuntimeError: Event loop is closed`. If you want the context
manager, handle the signals yourself instead:

```python
shutdown = asyncio.Event()
loop = asyncio.get_running_loop()
for sig in [signal.SIGINT, signal.SIGTERM]:
    loop.add_signal_handler(sig, shutdown.set)

async with manager:
    await shutdown.wait()
```

Calling `stop()` on a manager that was never started doesn't return.
:::

### Inspecting services

`manager.services` maps each name to a
[`ManagedServiceState`](/event.store/reference/processing/#logicblocks.event.processing.ManagedServiceState),
and `manager.service(name)` returns one state, or `None` if there is no
service with that name. A state has:

- `service`, `name`, `execution_mode` and `isolation_mode`, as
  registered;
- `service_status`, the service's
  [`ProcessStatus`](#process-status-and-health), or `UNKNOWN` if the
  service doesn't report one;
- `future`, a
  [`DeferredFuture`](/event.store/reference/processing/#logicblocks.event.processing.DeferredFuture)
  for the service's result.

You can get a `future` as soon as the service is registered. It supports
`await`, `done()`, `result()` and `exception()`. Before the manager has
started, `done()` returns `False`, and awaiting it, `result()` and
`exception()` raise `RuntimeError: Service '<name>' has not been scheduled
yet.` After `stop()`, the futures of cancelled services are cancelled, so
`result()` and `exception()` raise `CancelledError`.

## Polling

[`PollingService`](/event.store/reference/processing/#logicblocks.event.processing.PollingService)
runs another service over and over, forever. `poll_interval` is the delay
between one run finishing and the next starting, and defaults to 200
milliseconds:

```python
poller = PollingService(
    CallableService(subscriber.consume_all),
    poll_interval=timedelta(seconds=1),
)
```

If the wrapped service raises, polling stops and the exception propagates.
To keep polling through errors, wrap the inner service in an
[`ErrorHandlingService`](#handling-errors), as in the
[composed example](#putting-it-together) below.

## Process status and health

[`ProcessStatus`](/event.store/reference/processing/#logicblocks.event.processing.ProcessStatus)
is a string enum describing where a process is in its life cycle:
`INITIALISED`, `STARTING`, `WAITING`, `RUNNING`, `STOPPING`, `STOPPED`,
`ERRORED` and `UNKNOWN`.

Anything with a `status` property that returns a `ProcessStatus` counts as
having a status. Subclass the
[`Process`](/event.store/reference/processing/#logicblocks.event.processing.Process)
base class to declare it explicitly, or rely on the runtime-checkable
[`HasProcessStatus`](/event.store/reference/processing/#logicblocks.event.processing.HasProcessStatus)
protocol, which matches any object with a `status` attribute:

```python
from logicblocks.event.processing import (
    HasProcessStatus,
    Process,
    ProcessStatus,
)


class Worker(Process):
    @property
    def status(self) -> ProcessStatus:
        return ProcessStatus.WAITING


class Duck:
    status = ProcessStatus.RUNNING


print(
    isinstance(Worker(), HasProcessStatus),
    isinstance(Duck(), HasProcessStatus),
    isinstance(object(), HasProcessStatus),
)
# True True False
```

`ManagedServiceState.service_status` uses this check, so any registered
service with a `status` reports it through the manager.

### Tracking a service's status

Most services don't track their own status.
[`StatusTrackingService`](/event.store/reference/processing/#logicblocks.event.processing.StatusTrackingService)
wraps a service and does it for you. Its status is:

- `INITIALISED` before it runs;
- `RUNNING` while the wrapped service runs;
- `STOPPED` once the wrapped service returns or is cancelled;
- `ERRORED` if the wrapped service raises anything else.

Only the outermost service is visible to the manager. A
`PollingService` or `ErrorHandlingService` wrapped around a
`StatusTrackingService` hides its status, so put `StatusTrackingService`
on the outside.

If you write your own wrapper service, mix in
[`DelegateServiceStatusTrackingMixin`](/event.store/reference/processing/#logicblocks.event.processing.DelegateServiceStatusTrackingMixin).
It adds a `status` property that reports the status of the service stored
in `self._service`, or `UNKNOWN` if that service doesn't have one:

```python
class Timed(Service, DelegateServiceStatusTrackingMixin):
    def __init__(self, service: Service):
        self._service = service

    async def execute(self):
        started = time.monotonic()
        try:
            return await self._service.execute()
        finally:
            print(f"took {time.monotonic() - started:.2f}s")
```

`Timed(StatusTrackingService(some_service))` then reports the inner
service's status to the manager.

### Combining statuses

[`determine_multi_process_status`](/event.store/reference/processing/#logicblocks.event.processing.determine_multi_process_status)
combines several statuses into one, for example for a health check
endpoint. It ignores `UNKNOWN`. If the remaining statuses are all the
same, it returns that status. Otherwise, in order:

- any `ERRORED` gives `ERRORED`;
- any `STOPPING` or `STOPPED` gives `STOPPING`;
- any `STARTING` gives `STARTING`;
- a mix of `RUNNING` and `WAITING` gives `RUNNING`;
- a mix that includes `INITIALISED` gives `STARTING`.

With no known statuses it returns `UNKNOWN`.

```python
from logicblocks.event.processing import (
    ProcessStatus,
    determine_multi_process_status,
)

print(
    determine_multi_process_status(
        ProcessStatus.RUNNING, ProcessStatus.WAITING
    )
)
# running
print(
    determine_multi_process_status(
        ProcessStatus.RUNNING, ProcessStatus.INITIALISED
    )
)
# starting
print(
    determine_multi_process_status(
        ProcessStatus.RUNNING, ProcessStatus.STOPPED
    )
)
# stopping
print(
    determine_multi_process_status(
        ProcessStatus.RUNNING, ProcessStatus.UNKNOWN
    )
)
# running
```

## Handling errors

[`ErrorHandlingService`](/event.store/reference/processing/#logicblocks.event.processing.ErrorHandlingService)
wraps a service and passes anything it raises to an
[`ErrorHandler`](/event.store/reference/processing/#logicblocks.event.processing.ErrorHandler).
The handler returns a decision, and the service acts on it:

| Decision | What `ErrorHandlingService` does |
| --- | --- |
| [`RaiseErrorHandlerDecision(exception)`](/event.store/reference/processing/#logicblocks.event.processing.RaiseErrorHandlerDecision) | Raises `exception`. |
| [`ContinueErrorHandlerDecision(value)`](/event.store/reference/processing/#logicblocks.event.processing.ContinueErrorHandlerDecision) | Returns `value` as the result. |
| [`RetryErrorHandlerDecision(wait_before_retry)`](/event.store/reference/processing/#logicblocks.event.processing.RetryErrorHandlerDecision) | Waits for `wait_before_retry`, if set, then runs the service again. |
| [`ExitErrorHandlerDecision(exit_code)`](/event.store/reference/processing/#logicblocks.event.processing.ExitErrorHandlerDecision) | Raises `SystemExit(exit_code)`. |

The [`ErrorHandlerDecision`](/event.store/reference/processing/#logicblocks.event.processing.ErrorHandlerDecision)
base class has a factory for each decision: `raise_exception(exception)`,
`continue_execution(value=...)`, `retry_execution(wait_before_retry=...)`
and `exit_fatally(exit_code=...)`. Use them when you write your own
handler.

### Built-in error handlers

- [`RaiseErrorHandler`](/event.store/reference/processing/#logicblocks.event.processing.RaiseErrorHandler)
  raises the exception. Pass `exception_factory` to raise a different
  exception instead, for example to wrap it.
- [`ContinueErrorHandler`](/event.store/reference/processing/#logicblocks.event.processing.ContinueErrorHandler)
  returns the result of `value_factory(exception)` in place of the
  service's result.
- [`RetryErrorHandler`](/event.store/reference/processing/#logicblocks.event.processing.RetryErrorHandler)
  runs the service again. Without a `retry_strategy` it retries
  immediately, with no limit. See [Retry strategies](#retry-strategies).
- [`ExitErrorHandler`](/event.store/reference/processing/#logicblocks.event.processing.ExitErrorHandler)
  exits the process. `exit_code_factory(exception)` gives the exit code,
  which defaults to 1.
- [`TypeMappingErrorHandler`](/event.store/reference/processing/#logicblocks.event.processing.TypeMappingErrorHandler)
  picks one of the above based on the exception's type. See
  [Handling errors by type](#handling-errors-by-type).

```python
import asyncio
from datetime import timedelta

from logicblocks.event.processing import (
    CallableService,
    ConstantRetryStrategy,
    ContinueErrorHandler,
    ErrorHandlingService,
    RetryErrorHandler,
)

attempts = 0


async def flaky_fetch() -> str:
    global attempts
    attempts += 1
    if attempts < 3:
        raise ConnectionError(f"attempt {attempts} failed")
    return f"fetched on attempt {attempts}"


async def broken_fetch() -> str:
    raise ValueError("bad response")


async def main():
    retrying = ErrorHandlingService(
        CallableService(flaky_fetch),
        error_handler=RetryErrorHandler(
            retry_strategy=ConstantRetryStrategy(
                time=timedelta(milliseconds=50)
            )
        ),
    )
    print(await retrying.execute())
    # fetched on attempt 3

    continuing = ErrorHandlingService(
        CallableService(broken_fetch),
        error_handler=ContinueErrorHandler(
            value_factory=lambda exception: f"fallback after {exception!r}"
        ),
    )
    print(await continuing.execute())
    # fallback after ValueError('bad response')


asyncio.run(main())
```

`ErrorHandlingService.apply_error_handling(run, error_handler)` is a class
method that applies the same logic to any zero-argument async callable,
if you need error handling without a service.

### Fatal exceptions

`ErrorHandlingService` passes every exception to its handler, including
ones that don't subclass `Exception`, such as `asyncio.CancelledError`,
`KeyboardInterrupt` and `SystemExit`. Cancellation is how `ServiceManager`
stops services, so these need care.

`ContinueErrorHandler` and `RetryErrorHandler` only act on `Exception`
subclasses, and raise anything else. Since 0.1.8 this includes
`ContinueErrorHandler`, which used to continue on fatal exceptions too.
`RaiseErrorHandler` raises everything, so it is safe as well, as long as
its `exception_factory` doesn't replace a cancellation with something
else.

`ExitErrorHandler` doesn't make this distinction. Used directly, it turns
a cancellation into an exit:

```python
import asyncio

from logicblocks.event.processing import (
    ContinueErrorHandler,
    ExitErrorHandler,
    RetryErrorHandler,
    TypeMappingErrorHandler,
    error_handler_type_mappings,
)

continuing = ContinueErrorHandler(value_factory=lambda exception: None)
print(continuing.handle(ValueError("bad")))
# ContinueErrorHandlerDecision(value=None)
print(continuing.handle(asyncio.CancelledError()))
# RaiseErrorHandlerDecision(exception=CancelledError())
print(RetryErrorHandler().handle(KeyboardInterrupt()))
# RaiseErrorHandlerDecision(exception=KeyboardInterrupt())

print(ExitErrorHandler().handle(asyncio.CancelledError()))
# ExitErrorHandlerDecision(exit_code=1)

exit_on_errors = TypeMappingErrorHandler(
    error_handler_type_mappings(exit_fatally=[Exception])
)
print(exit_on_errors.handle(ValueError("bad")))
# ExitErrorHandlerDecision(exit_code=1)
print(exit_on_errors.handle(asyncio.CancelledError()))
# RaiseErrorHandlerDecision(exception=CancelledError())
```

To exit on errors, prefer a `TypeMappingErrorHandler` that maps
`Exception` to exit, as in the last part of the example. Cancellation then
falls through to the default handler, which raises it.

:::caution
`SystemExit` only ends the process when the service runs on the main
thread. A service on a `SHARED_THREAD` or `DEDICATED_THREAD` that decides
to exit stops that thread's event loop instead. The process keeps
running, the service's future holds the `SystemExit`, and
`ServiceManager.stop()` no longer returns. Use exit decisions only for
`MAIN_THREAD` services.
:::

### Handling errors by type

`TypeMappingErrorHandler` takes a mapping from exception types to
handlers, and a `default_error_handler` for unmapped types, which defaults
to `RaiseErrorHandler()`. It walks the exception's class hierarchy and uses
the most specific type in the mapping, so a handler for `KeyError` wins
over one for `LookupError`.

Build the mapping with
[`error_handler_type_mappings`](/event.store/reference/processing/#logicblocks.event.processing.error_handler_type_mappings).
It takes four arguments, one for each kind of decision:
`exit_fatally`, `raise_exception`, `continue_execution` and
`retry_execution`. Each is either a list of exception types, which uses
the default behaviour, or the result of the matching helper:

- [`exit_fatally_type_mapping(types, exit_code_factory=..., callback=...)`](/event.store/reference/processing/#logicblocks.event.processing.exit_fatally_type_mapping)
- [`raise_exception_type_mapping(types, exception_factory, callback=...)`](/event.store/reference/processing/#logicblocks.event.processing.raise_exception_type_mapping)
- [`continue_execution_type_mapping(types, value_factory, callback=...)`](/event.store/reference/processing/#logicblocks.event.processing.continue_execution_type_mapping)
- [`retry_execution_type_mapping(types, callback=..., retry_strategy=...)`](/event.store/reference/processing/#logicblocks.event.processing.retry_execution_type_mapping)

`continue_execution` has no default value to continue with, so it only
accepts the helper's result. Each helper's `callback` is called with the
exception before the handler runs, which is a good place to log it. If the
same type appears under more than one argument, the later argument in the
order above wins.

```python
import asyncio
from datetime import timedelta

from logicblocks.event.processing import (
    CallableService,
    ConstantRetryStrategy,
    ErrorHandlingService,
    TypeMappingErrorHandler,
    continue_execution_type_mapping,
    error_handler_type_mappings,
    exit_fatally_type_mapping,
    raise_exception_type_mapping,
    retry_execution_type_mapping,
)


class ConfigurationError(Exception):
    pass


class ProcessingFailed(Exception):
    pass


handler = TypeMappingErrorHandler(
    type_mappings=error_handler_type_mappings(
        exit_fatally=exit_fatally_type_mapping(
            types=[ConfigurationError],
            exit_code_factory=lambda exception: 78,
        ),
        raise_exception=raise_exception_type_mapping(
            types=[KeyError],
            exception_factory=lambda exception: ProcessingFailed(
                f"missing key {exception}"
            ),
        ),
        continue_execution=continue_execution_type_mapping(
            types=[LookupError],
            value_factory=lambda exception: None,
            callback=lambda exception: print(f"skipping: {exception!r}"),
        ),
        retry_execution=retry_execution_type_mapping(
            types=[ConnectionError],
            retry_strategy=ConstantRetryStrategy(
                time=timedelta(milliseconds=50)
            ),
        ),
    ),
)


def raising(exception: BaseException) -> CallableService:
    async def run():
        raise exception

    return CallableService(run)


async def main():
    print(handler.handle(ConfigurationError("no database url")))
    # ExitErrorHandlerDecision(exit_code=78)
    print(handler.handle(KeyError("id")))
    # RaiseErrorHandlerDecision(exception=ProcessingFailed("missing key 'id'"))
    print(handler.handle(IndexError("out of range")))
    # skipping: IndexError('out of range')
    # ContinueErrorHandlerDecision(value=None)
    print(handler.handle(ConnectionRefusedError("refused")))
    # RetryErrorHandlerDecision(wait_before_retry=datetime.timedelta(microseconds=50000))
    print(handler.handle(RuntimeError("unexpected")))
    # RaiseErrorHandlerDecision(exception=RuntimeError('unexpected'))

    service = ErrorHandlingService(
        raising(KeyError("id")), error_handler=handler
    )
    try:
        await service.execute()
    except ProcessingFailed as exception:
        print(f"raised: {exception!r}")
        # raised: ProcessingFailed("missing key 'id'")

    service = ErrorHandlingService(
        raising(ConfigurationError("no database url")), error_handler=handler
    )
    try:
        await service.execute()
    except SystemExit as exception:
        print(f"exit code: {exception.code}")
        # exit code: 78


asyncio.run(main())
```

`IndexError` is a `LookupError`, so it continues. `KeyError` is a
`LookupError` too, but its own mapping is more specific, so it raises.

### Retry strategies

A [`RetryStrategy`](/event.store/reference/processing/#logicblocks.event.processing.RetryStrategy)
tells `RetryErrorHandler` how to retry. Its `calculate(exception)` method
returns a
[`RetryStrategyDecision`](/event.store/reference/processing/#logicblocks.event.processing.RetryStrategyDecision):

- [`RetryImmediatelyDecision`](/event.store/reference/processing/#logicblocks.event.processing.RetryImmediatelyDecision),
  from `RetryStrategyDecision.retry_immediately()`, retries without
  waiting;
- [`WaitRetryStrategyDecision`](/event.store/reference/processing/#logicblocks.event.processing.WaitRetryStrategyDecision),
  from `RetryStrategyDecision.wait(duration)`, waits for `duration` and
  then retries;
- [`OverrideRetryStrategyDecision`](/event.store/reference/processing/#logicblocks.event.processing.OverrideRetryStrategyDecision),
  from `RetryStrategyDecision.override(error_handler_decision)`, stops
  retrying and uses the given error handler decision instead, for example
  to raise or exit.

The library includes three strategies:

- [`ConstantRetryStrategy(time)`](/event.store/reference/processing/#logicblocks.event.processing.ConstantRetryStrategy)
  always waits `time`.
- [`IncludeExceptionsRetryStrategy(delegate, include_list)`](/event.store/reference/processing/#logicblocks.event.processing.IncludeExceptionsRetryStrategy)
  uses `delegate` for exceptions that match `include_list`, including
  subclasses.
- [`ExcludeExceptionsRetryStrategy(delegate, exclude_list)`](/event.store/reference/processing/#logicblocks.event.processing.ExcludeExceptionsRetryStrategy)
  uses `delegate` for exceptions that don't match `exclude_list`.

The include and exclude strategies don't decide _whether_ to retry.
Exceptions that the delegate doesn't apply to are still retried, just
immediately. To stop retrying some exceptions, map them to a different
handler with `TypeMappingErrorHandler`, or write a strategy that returns
an override decision. This one gives up after a number of attempts:

```python
import asyncio
from datetime import timedelta

from logicblocks.event.processing import (
    CallableService,
    ConstantRetryStrategy,
    ErrorHandlerDecision,
    ErrorHandlingService,
    ExcludeExceptionsRetryStrategy,
    IncludeExceptionsRetryStrategy,
    RetryErrorHandler,
    RetryStrategy,
    RetryStrategyDecision,
)

wait_a_second = ConstantRetryStrategy(time=timedelta(seconds=1))

include = IncludeExceptionsRetryStrategy(
    delegate=wait_a_second, include_list=[ConnectionError]
)
exclude = ExcludeExceptionsRetryStrategy(
    delegate=wait_a_second, exclude_list=[ConnectionError]
)

print(include.calculate(ConnectionResetError()))
# WaitRetryStrategyDecision(duration=datetime.timedelta(seconds=1))
print(include.calculate(ValueError()))
# RetryImmediatelyDecision()
print(exclude.calculate(ConnectionResetError()))
# RetryImmediatelyDecision()
print(exclude.calculate(ValueError()))
# WaitRetryStrategyDecision(duration=datetime.timedelta(seconds=1))


class GiveUpAfter(RetryStrategy):
    def __init__(self, max_attempts: int, wait: timedelta):
        self._max_attempts = max_attempts
        self._wait = wait
        self._attempts = 0

    def calculate(self, exception: Exception) -> RetryStrategyDecision:
        self._attempts += 1
        if self._attempts >= self._max_attempts:
            return RetryStrategyDecision.override(
                ErrorHandlerDecision.raise_exception(exception)
            )
        return RetryStrategyDecision.wait(self._wait)


async def always_fails() -> None:
    print("trying")
    raise ConnectionError("unreachable")


async def main():
    service = ErrorHandlingService(
        CallableService(always_fails),
        error_handler=RetryErrorHandler(
            retry_strategy=GiveUpAfter(
                max_attempts=3, wait=timedelta(milliseconds=50)
            )
        ),
    )
    try:
        await service.execute()
    except ConnectionError as exception:
        print(f"gave up: {exception}")
    # trying
    # trying
    # trying
    # gave up: unreachable


asyncio.run(main())
```

A strategy instance keeps its state for as long as it lives, so
`GiveUpAfter` counts attempts across every run of the service. Reset the
count when the service succeeds if you need a per-run limit.

## Putting it together

The wrapper services compose, so you can build the behaviour you need
from small pieces. The order matters:

- `PollingService(ErrorHandlingService(...))` handles each poll's errors
  and keeps polling.
- `ErrorHandlingService(PollingService(...), error_handler=RetryErrorHandler(...))`
  restarts the whole polling loop after an error.
- `StatusTrackingService(...)` goes on the outside, so that the manager
  can see the status.

```python
import asyncio
from datetime import timedelta

from logicblocks.event.processing import (
    CallableService,
    ContinueErrorHandler,
    ErrorHandlingService,
    PollingService,
    ServiceManager,
    StatusTrackingService,
    determine_multi_process_status,
)

polls = 0


async def poll_inbox() -> None:
    global polls
    polls += 1
    if polls % 2 == 0:
        raise TimeoutError(f"poll {polls} timed out")


async def fail_to_connect() -> None:
    raise ConnectionError("database unavailable")


def log_and_continue(exception: BaseException) -> None:
    print(f"ignoring: {exception}")


async def main():
    inbox = StatusTrackingService(
        PollingService(
            ErrorHandlingService(
                CallableService(poll_inbox),
                error_handler=ContinueErrorHandler(
                    value_factory=log_and_continue
                ),
            ),
            poll_interval=timedelta(milliseconds=50),
        )
    )
    reporter = StatusTrackingService(CallableService(fail_to_connect))

    async with (
        ServiceManager()
        .register(inbox, name="inbox")
        .register(reporter, name="reporter")
    ) as manager:
        await asyncio.sleep(0.12)

        statuses = {
            name: state.service_status
            for name, state in manager.services.items()
        }
        print(statuses)
        # {'inbox': <ProcessStatus.RUNNING: 'running'>,
        #  'reporter': <ProcessStatus.ERRORED: 'errored'>}
        print(determine_multi_process_status(*statuses.values()))
        # errored
        print(repr(manager.service("reporter").future.exception()))
        # ConnectionError('database unavailable')


asyncio.run(main())
```

The example prints `ignoring: poll 2 timed out` before the statuses.

## The event broker as a service

An [`EventBroker`](/event.store/reference/processing/#logicblocks.event.processing.EventBroker)
is a `Service` and a `Process`. It runs until it is stopped, handing event
sources to the subscribers registered with it, and it reports a `status`.
Register it with a `ServiceManager` like any other service, alongside a
`PollingService` for each subscriber's `consume_all`. Brokers and
subscribers are covered in the
[event processing guide](/event.store/guides/event-processing/).

Both broker types wrap their own work in an `ErrorHandlingService`. Pass
`error_handler=` to
[`make_event_broker`](/event.store/reference/processing/#logicblocks.event.processing.make_event_broker)
to choose how the broker handles errors. The default is a
`RetryErrorHandler` with no retry strategy, which retries immediately and
without limit. While storage is unavailable, that retries in a tight loop,
so give it a retry strategy with a wait:

```python
import asyncio
import logging
from datetime import timedelta

import structlog

from logicblocks.event.processing import (
    CallableService,
    ConstantRetryStrategy,
    EventBrokerStorageType,
    EventBrokerType,
    EventProcessor,
    PollingService,
    RetryErrorHandler,
    ServiceManager,
    SingletonEventBrokerSettings,
    make_event_broker,
    make_event_store_subscriber,
)
from logicblocks.event.store import EventStore, adapters
from logicblocks.event.types import CategoryIdentifier, NewEvent, StoredEvent

structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(logging.WARNING)
)


class PrintingProcessor(EventProcessor[StoredEvent]):
    async def process_event(self, event: StoredEvent) -> None:
        print(f"processed {event.name} from {event.stream}")


async def main():
    adapter = adapters.InMemoryEventStorageAdapter()
    store = EventStore(adapter)

    broker = make_event_broker(
        node_id="node-1",
        broker_type=EventBrokerType.Singleton,
        storage_type=EventBrokerStorageType.InMemory,
        settings=SingletonEventBrokerSettings(
            distribution_interval=timedelta(milliseconds=100)
        ),
        adapter=adapter,
        error_handler=RetryErrorHandler(
            retry_strategy=ConstantRetryStrategy(time=timedelta(seconds=5))
        ),
    )

    subscriber = make_event_store_subscriber(
        subscriber_group="profile-printers",
        subscription_request=CategoryIdentifier(category="profiles"),
        subscriber_state_category=store.category(category="subscribers"),
        event_processor=PrintingProcessor(),
    )
    await broker.register(subscriber)

    manager = (
        ServiceManager()
        .register(broker, name="event-broker")
        .register(
            PollingService(
                CallableService(subscriber.consume_all),
                poll_interval=timedelta(milliseconds=100),
            ),
            name="profile-printer",
        )
    )

    async with manager:
        await store.stream(category="profiles", stream="joe.bloggs").publish(
            events=[
                NewEvent(name="profile-created", payload={}, metadata=None)
            ]
        )
        await asyncio.sleep(0.5)
        print(manager.service("event-broker").service_status)

    # processed profile-created from joe.bloggs
    # running


asyncio.run(main())
```

The `structlog.configure` call hides the broker's info-level logs. See the
[logging guide](/event.store/guides/logging/) for more.

Because the broker reports its own status, `service_status` works without
a `StatusTrackingService`. A distributed broker combines the statuses of
its coordinator and observer with `determine_multi_process_status`.

## Deprecated APIs

These still work but emit a `DeprecationWarning`:

- Passing a bare callable to `PollingService` or `ErrorHandlingService`,
  either positionally or as `callable=`. Wrap it in a `CallableService`
  and pass it as the first argument or `service=`.
- [`ErrorHandlingServiceMixin`](/event.store/reference/processing/#logicblocks.event.processing.ErrorHandlingServiceMixin),
  which applies an error handler to a subclass's `_do_execute` method.
  Wrap the service in an `ErrorHandlingService` instead.
