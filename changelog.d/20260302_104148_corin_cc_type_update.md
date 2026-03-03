### Changed

- Update the return type of `event_store_transaction` (including `retry_on_unmet_condition_error` and `ignore_on_unmet_condition_error`) to return `Coroutine` instead of `Awaitable` in order to be more compatible with methods such as `asyncio.create_task`.
