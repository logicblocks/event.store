from collections.abc import Sequence
from typing import Any

from psycopg import AsyncConnection, abc, sql
from psycopg_pool import AsyncConnectionPool

from .settings import ConnectionSettings
from .settings import TableSettings as TableSettings

type ConnectionSource = (
    ConnectionSettings | AsyncConnectionPool[AsyncConnection]
)
"""Settings from which to open connections, or an existing pool to use."""

type SqlFragment = sql.Composable | None

type ParameterisedQuery = tuple[abc.Query, Sequence[Any]]
"""A SQL query paired with the parameters to bind into it."""
type ParameterisedQueryFragment = tuple[SqlFragment, Sequence[Any]]
"""A SQL fragment, possibly empty, paired with its parameters."""
