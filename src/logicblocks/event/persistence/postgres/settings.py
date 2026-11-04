from dataclasses import dataclass


@dataclass(frozen=True)
class TableSettings:
    """Settings for the Postgres table that an adapter reads and writes.

    Attributes:
        table_name: The name of the table.
    """

    table_name: str


@dataclass(frozen=True)
class ConnectionSettings:
    """Settings for connecting to a Postgres database.

    The password is masked in the `repr`.

    Attributes:
        host: The hostname or address of the database server.
        port: The port on which the database server listens.
        dbname: The name of the database to connect to.
        user: The user to connect as.
        password: The password for the user.
    """

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
            f"ConnectionSettings("
            f"host={self.host}, "
            f"port={self.port}, "
            f"dbname={self.dbname}, "
            f"user={self.user}, "
            f"password={'*' * len(self.password)})"
        )

    def to_connection_string(self) -> str:
        """Build a `postgresql://` connection URI from these settings.

        The user and password are included without URL encoding.

        Returns:
            the connection URI, including the password.
        """
        userspec = f"{self.user}:{self.password}"
        hostspec = f"{self.host}:{self.port}"
        return f"postgresql://{userspec}@{hostspec}/{self.dbname}"
