from dataclasses import dataclass

from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool


@dataclass(frozen=True)
class PostgresConnectionSettings:
    host: str
    port: int
    dbname: str
    user: str
    password: str

    def __init__(
        self, *, host: str, port: int, dbname: str, user: str, password: str
    ):
        object.__setattr__(self, "host", host)
        object.__setattr__(self, "port", port)
        object.__setattr__(self, "dbname", dbname)
        object.__setattr__(self, "user", user)
        object.__setattr__(self, "password", password)

    def __repr__(self):
        return (
            f"PostgresConnectionSettings("
            f"host={self.host}, "
            f"port={self.port}, "
            f"dbname={self.dbname}, "
            f"user={self.user}, "
            f"password={"*" * len(self.password)})"
        )

    def to_connection_string(self) -> str:
        userspec = f"{self.user}:{self.password}"
        hostspec = f"{self.host}:{self.port}"
        return f"postgresql://{userspec}@{hostspec}/{self.dbname}"


PostgresConnectionSource = (
    PostgresConnectionSettings | AsyncConnectionPool[AsyncConnection]
)
