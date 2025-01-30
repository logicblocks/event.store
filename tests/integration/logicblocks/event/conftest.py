import pytest

pytest.register_assert_rewrite("logicblocks.event.testcases.store.adapters")
pytest.register_assert_rewrite(
    "logicblocks.event.testcases.projection.store.adapters"
)
pytest.register_assert_rewrite(
    "logicblocks.event.testcases.processing.subscribers.store"
)
