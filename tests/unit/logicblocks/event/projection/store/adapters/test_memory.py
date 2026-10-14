from collections.abc import Sequence

import pytest
from logicblocks.event.testcases.projection.store.adapters import (
    ProjectionStorageAdapterCases,
    Thing,
    ThingProjectionBuilder,
)

from logicblocks.event.projection import InMemoryProjectionStorageAdapter
from logicblocks.event.projection.store import ProjectionStorageAdapter
from logicblocks.event.query import FilterClause, Operator, Path, Search
from logicblocks.event.types import JsonValue, Projection


class TestInMemoryProjectionStorageAdapter(ProjectionStorageAdapterCases):
    def construct_storage_adapter(self) -> ProjectionStorageAdapter:
        return InMemoryProjectionStorageAdapter()

    async def clear_storage(self) -> None:
        pass

    async def retrieve_projections(
        self,
        *,
        adapter: ProjectionStorageAdapter,
    ) -> Sequence[Projection[JsonValue]]:
        return await adapter.find_many(search=Search())


class TestInMemoryProjectionStorageAdapterAbsentFieldBehaviour:
    """Asserts the DESIRED behaviour when filtering on an absent field.

    We have chosen "raise a ``ValueError``" as the unified behaviour. This
    adapter already behaves this way; the Postgres adapter does NOT (it
    silently excludes the projection), so its mirror test
    ``TestPostgresProjectionStorageAdapterAbsentFieldBehaviour`` in
    ``tests/integration/.../adapters/test_postgres.py`` is expected to fail
    until the adapter is aligned.
    """

    @pytest.mark.parametrize("operator", [Operator.EQUAL, Operator.NOT_EQUAL])
    async def test_filtering_on_absent_field_raises(
        self, operator: Operator
    ) -> None:
        adapter = InMemoryProjectionStorageAdapter()
        await adapter.save(
            projection=ThingProjectionBuilder()
            .with_state(Thing(value_1=1))
            .build()
        )

        with pytest.raises(ValueError):
            await adapter.find_many(
                search=Search(
                    filters=[
                        FilterClause(operator, Path("state", "value_5"), 42)
                    ]
                ),
                state_type=Thing,
            )
