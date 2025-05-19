from typing import Any, Sequence, TypeGuard

from psycopg.types.json import Jsonb

from logicblocks.event import query as genericquery
from logicblocks.event.persistence.postgres import query as postgresquery


def expression_for_path(path: genericquery.Path) -> postgresquery.Expression:
    if path.is_nested():
        arguments = [
            postgresquery.ColumnReference(field=path.top_level),
            *[
                postgresquery.Constant(value=sub_level)
                for sub_level in path.sub_levels
            ],
        ]
        return postgresquery.FunctionApplication(
            function_name="jsonb_extract_path", arguments=arguments
        )
    else:
        return postgresquery.ColumnReference(field=path.top_level)


def expression_for_function(
    function: genericquery.Function,
) -> postgresquery.Expression:
    return postgresquery.ColumnReference(field=function.alias)


def expression_for_field(
    field: genericquery.Path | genericquery.Function,
) -> postgresquery.Expression:
    match field:
        case genericquery.Path():
            return expression_for_path(field)
        case genericquery.Function():
            return expression_for_function(field)
        case _:  # pragma: no cover
            raise ValueError(f"Unsupported field type: {type(field)}")


def value_for_path(
    value: Any, path: genericquery.Path
) -> postgresquery.Expression:
    if path == genericquery.Path("source"):
        return postgresquery.Constant(
            Jsonb(value.serialise()),
        )
    elif path.is_nested():
        expression = postgresquery.Constant(value)
        if isinstance(value, str):
            expression = postgresquery.Cast(
                expression=expression, typename="text"
            )
        return postgresquery.FunctionApplication(
            function_name="to_jsonb", arguments=[expression]
        )
    else:
        return postgresquery.Constant(value)


def is_multi_valued(value: Any) -> TypeGuard[Sequence[Any]]:
    return (
        not isinstance(value, str)
        and not isinstance(value, bytes)
        and isinstance(value, Sequence)
    )
