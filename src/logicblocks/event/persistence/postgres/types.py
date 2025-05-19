from collections.abc import Sequence
from typing import Any

from psycopg import AsyncConnection, abc, sql
from psycopg_pool import AsyncConnectionPool

from .settings import ConnectionSettings
from .settings import TableSettings as TableSettings

type ConnectionSource = (
    ConnectionSettings | AsyncConnectionPool[AsyncConnection]
)

type SqlFragment = sql.Composable | None

type ParameterisedQuery = tuple[abc.Query, Sequence[Any]]
type ParameterisedQueryFragment = tuple[SqlFragment, Sequence[Any]]
