from dataclasses import dataclass


@dataclass(frozen=True)
class TableSettings:
    table_name: str
