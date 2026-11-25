# taken from https://github.com/Automattic/target-parquet/blob/master/target_parquet/utils/parquet.py
import re
import warnings
from collections.abc import Iterator
from dataclasses import dataclass

import pyarrow as pa

from target_ducklake.duckdb_types import (
    DuckDBList,
    DuckDBScalar,
    DuckDBStruct,
    DuckDBTypeTree,
    field_path,
    parse_type,
    quote_identifier,
    render_type,
)
from target_ducklake.type_hints import decimal_hint

# JSON Schema "number" maps to DECIMAL; DuckDB supports precision up to 38.
DECIMAL_MAX_PRECISION = 38
DECIMAL_DEFAULT_SCALE = 9

# value_type_checking: what staging does with a value its column would round.
STRICT = "strict"
PERMISSIVE = "permissive"
VALUE_TYPE_CHECKING_MODES = (PERMISSIVE, STRICT)


def number_precision_and_scale(jsonschema: dict) -> tuple[int, int]:
    """Return the (precision, scale) used for a decimal column.

    Shared by the DuckLake column DDL and the parquet staging schema so both
    always agree. An ``x-sql-datatype`` of ``decimal(p,s)`` or ``numeric(p,s)``
    wins; otherwise the schema's ``precision``/``scale`` are used when present
    (scale defaulting to 9 and never exceeding the precision), otherwise
    DECIMAL(38, 9). A precision above 38 is capped by ``_capped_to_38``.
    """
    precision, scale = decimal_hint(jsonschema) or (
        jsonschema.get("precision"),
        jsonschema.get("scale"),
    )
    if precision is None:
        return DECIMAL_MAX_PRECISION, DECIMAL_DEFAULT_SCALE
    scale = min(DECIMAL_DEFAULT_SCALE if scale is None else scale, precision)
    if precision > DECIMAL_MAX_PRECISION:
        return _capped_to_38(precision, scale)
    return precision, scale


def _capped_to_38(precision: int, scale: int) -> tuple[int, int]:
    """Fit DECIMAL(precision, scale) into DuckDB's 38 digits, with a warning.

    Fraction digits are dropped first, so the integer digits a value needs
    still fit: ``numeric(50,45)`` becomes DECIMAL(38, 33), not DECIMAL(38, 38),
    which can't hold 1. They are never dropped below ``min(scale, 9)``, the
    default scale, so a money column keeps its cents (``numeric(40,2)`` gives
    DECIMAL(38, 2)) and a capped type never keeps fewer fraction digits than
    the DECIMAL(38, 9) default. A value with more integer digits than are left
    fails the batch at staging (``ValueError`` in ``_decimal_column``), as
    it would with DECIMAL(38, 9).
    """
    integer_digits = precision - scale
    fraction_floor = min(scale, DECIMAL_DEFAULT_SCALE)
    capped_scale = min(
        scale, max(DECIMAL_MAX_PRECISION - integer_digits, fraction_floor)
    )
    warnings.warn(
        f"DECIMAL({precision}, {scale}) exceeds DuckDB's 38-digit DECIMAL; "
        f"using DECIMAL({DECIMAL_MAX_PRECISION}, {capped_scale}).",
        UserWarning,
        stacklevel=3,
    )
    return DECIMAL_MAX_PRECISION, capped_scale


# DuckDB column types staged as int64, float64 or binary. Every other type
# except DECIMAL and BOOLEAN (VARCHAR, JSON, UUID, temporal types) is staged as
# text, which DuckDB parses on insert.
_INTEGER_COLUMN_TYPES = {"TINYINT", "SMALLINT", "INT", "INTEGER", "BIGINT"}
_FLOAT_COLUMN_TYPES = {"REAL", "FLOAT", "FLOAT4", "DOUBLE", "FLOAT8"}
# DuckDB's names for BLOB; PRAGMA table_info reports BLOB.
_BINARY_COLUMN_TYPES = {"BLOB", "BYTEA", "BINARY", "VARBINARY"}
_DECIMAL_COLUMN_TYPE = re.compile(
    r"^(?:DECIMAL|NUMERIC)\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)$"
)


def staging_type(column_type: str) -> pa.DataType:
    """Return the parquet type a column of this DuckDB type is staged as.

    A STRUCT or list is staged as an Arrow struct or list built from its
    parts, each leaf as a column of its type would be (see
    ``_scalar_staging_type``), so DuckLake casts the staged struct to the
    table's field by field. A type that doesn't parse is staged as text.
    """
    try:
        tree = parse_type(column_type)
    except ValueError:
        return pa.string()
    return _tree_staging_type(tree)


def _tree_staging_type(tree: DuckDBTypeTree) -> pa.DataType:
    """The Arrow type a parsed DuckDB type is staged as."""
    if isinstance(tree, DuckDBStruct):
        return pa.struct(
            [pa.field(name, _tree_staging_type(part)) for name, part in tree.fields]
        )
    if isinstance(tree, DuckDBList):
        return pa.list_(pa.field("element", _tree_staging_type(tree.element)))
    return _scalar_staging_type(tree.name)


def is_nested_type(column_type: str) -> bool:
    """Whether a DuckDB column type is a STRUCT or a list."""
    staged = staging_type(column_type)
    return pa.types.is_struct(staged) or pa.types.is_list(staged)


def nested_leaf_paths(name: str, column_type: str) -> Iterator[tuple[str, str]]:
    """Each leaf of a nested column as (path, DuckDB type), depth first.

    Paths name struct fields after a dot and list elements with ``[]``:
    ``o.items[].price``. A scalar column yields nothing.
    """
    try:
        tree = parse_type(column_type)
    except ValueError:
        return
    if isinstance(tree, DuckDBScalar):
        return
    yield from _leaf_paths(name, tree)


def _leaf_paths(path: str, tree: DuckDBTypeTree) -> Iterator[tuple[str, str]]:
    if isinstance(tree, DuckDBStruct):
        for field_name, part in tree.fields:
            yield from _leaf_paths(field_path(path, field_name), part)
    elif isinstance(tree, DuckDBList):
        yield from _leaf_paths(f"{path}[]", tree.element)
    else:
        yield path, tree.name


@dataclass(frozen=True)
class NestedFieldAddition:
    """A field to add to an existing STRUCT, in place (``ADD COLUMN o.f T``).

    ``path`` names it in messages (``o.items[].f``); ``sql_path`` is the
    quoted path ALTER TABLE takes (``"o"."items"."element"."f"``).
    """

    path: str
    sql_path: str
    column_type: str


@dataclass(frozen=True)
class NestedTypeConflict:
    """A field whose table type the stream's type can't be staged into."""

    path: str
    table_type: str
    stream_type: str


@dataclass(frozen=True)
class NestedSchemaChanges:
    """How a stream's nested column differs from the table's (DUCKLKD-109)."""

    additions: tuple[NestedFieldAddition, ...] = ()
    conflicts: tuple[NestedTypeConflict, ...] = ()


# Scalar types that hold any value as its text (or its JSON text).
_TEXT_TYPES = {"VARCHAR", "TEXT", "STRING", "JSON"}


def nested_schema_changes(
    name: str, stream_type: str, table_type: str
) -> NestedSchemaChanges:
    """Compare a stream's STRUCT or LIST column with the table's, field by field.

    Fields only the stream declares are additions; fields only the table has
    stay (and get NULL). Fields match case-insensitively, as DuckDB's do. A
    leaf whose type changed is staged at the table's type when that keeps
    its meaning (see ``_leaf_fits``); otherwise, or when a field changed
    between scalar, STRUCT and list, it is a conflict.
    """
    additions: list[NestedFieldAddition] = []
    conflicts: list[NestedTypeConflict] = []
    _diff(
        name,
        quote_identifier(name),
        parse_type(stream_type),
        parse_type(table_type),
        additions,
        conflicts,
    )
    return NestedSchemaChanges(tuple(additions), tuple(conflicts))


def _diff(  # noqa: PLR0913 (the walk's position and its two results)
    path: str,
    sql_path: str,
    ours: DuckDBTypeTree,
    theirs: DuckDBTypeTree,
    additions: list[NestedFieldAddition],
    conflicts: list[NestedTypeConflict],
) -> None:
    if isinstance(ours, DuckDBStruct) and isinstance(theirs, DuckDBStruct):
        have = {name.lower(): part for name, part in theirs.fields}
        for name, part in ours.fields:
            child = field_path(path, name)
            child_sql = f"{sql_path}.{quote_identifier(name)}"
            if name.lower() not in have:
                additions.append(
                    NestedFieldAddition(child, child_sql, render_type(part))
                )
                continue
            _diff(child, child_sql, part, have[name.lower()], additions, conflicts)
        return
    if isinstance(ours, DuckDBList) and isinstance(theirs, DuckDBList):
        _diff(
            f"{path}[]",
            f'{sql_path}."element"',
            ours.element,
            theirs.element,
            additions,
            conflicts,
        )
        return
    if not _leaf_fits(ours, theirs):
        conflicts.append(
            NestedTypeConflict(path, render_type(theirs), render_type(ours))
        )


def _leaf_fits(ours: DuckDBTypeTree, theirs: DuckDBTypeTree) -> bool:
    """Whether a field the stream types as ``ours`` stages into the table's ``theirs``.

    Staging follows the table's type, with the scalar rules: a number fits
    any numeric type (rounded with a warning, or failing under strict
    ``value_type_checking``), anything fits a text or JSON field (an object
    as its JSON text), and text fits a field staged as text (DuckDB parses a
    date-time, a date or a UUID). Text doesn't fit a number, a boolean or a
    BLOB (whose text would have to be base64), and a STRUCT or a list
    doesn't fit a field of another shape.
    """
    if isinstance(theirs, DuckDBScalar):
        if " ".join(theirs.name.upper().split()) in _TEXT_TYPES:
            return True
        if not isinstance(ours, DuckDBScalar):
            return False
        mine, table = _scalar_staging_type(ours.name), _scalar_staging_type(theirs.name)
        numeric = (pa.types.is_integer, pa.types.is_decimal, pa.types.is_floating)
        return (
            mine == table
            or (any(f(mine) for f in numeric) and any(f(table) for f in numeric))
            or (pa.types.is_string(mine) and pa.types.is_string(table))
        )
    return False


def _scalar_staging_type(column_type: str) -> pa.DataType:
    """Return the parquet type a column of this scalar DuckDB type is staged as.

    Staging follows the column's own type, so every value the column can hold
    stages, and a value the column can't hold fails the batch at staging
    instead of being changed: a DECIMAL column is staged at its exact
    precision and scale, a REAL or DOUBLE column as float64 (decimal staging
    would round 1.5e-12 to 0), and a VARCHAR column, such as one declared with
    several JSON types, as text.
    """
    name = " ".join(column_type.upper().split())
    if match := _DECIMAL_COLUMN_TYPE.match(name):
        return pa.decimal128(int(match.group(1)), int(match.group(2)))
    if name in _INTEGER_COLUMN_TYPES:
        return pa.int64()
    if name in _FLOAT_COLUMN_TYPES:
        return pa.float64()
    if name == "BOOLEAN":
        return pa.bool_()
    if name in _BINARY_COLUMN_TYPES:
        return pa.binary()
    return pa.string()


# The DuckDB types of the Arrow types staging uses, other than decimals and
# nested types.
_STAGING_SQL_TYPES = {
    pa.int64(): "BIGINT",
    pa.float64(): "DOUBLE",
    pa.bool_(): "BOOLEAN",
    pa.binary(): "BLOB",
    pa.string(): "VARCHAR",
}


def staging_sql_type(column_type: str) -> str:
    """The DuckDB type of what ``staging_type`` stages a column of this type as.

    A BATCH file is staged by DuckDB (``batch_sql``), at the types the RECORD
    path stages at, so both kinds of staged file load alike.
    """
    return _sql_type(staging_type(column_type))


def _sql_type(staged: pa.DataType) -> str:
    if pa.types.is_struct(staged):
        fields = ", ".join(
            f"{quote_identifier(field.name)} {_sql_type(field.type)}"
            for field in staged
        )
        return f"STRUCT({fields})"
    if pa.types.is_list(staged):
        return f"{_sql_type(staged.value_type)}[]"
    if pa.types.is_decimal(staged):
        return f"DECIMAL({staged.precision}, {staged.scale})"
    return _STAGING_SQL_TYPES[staged]


def date_time_paths(flattened_schema: dict) -> list[str]:
    """The columns and nested leaves declared ``format: date-time``, by path.

    A top-level column by its name, a nested leaf by its path through
    ``properties`` and ``items``: ``o.at``, ``o.items[].at``.
    """
    return [
        path
        for name, prop in flattened_schema.items()
        for path in _date_time_paths(name, prop)
    ]


def _date_time_paths(path: str, prop: object) -> Iterator[str]:
    if not isinstance(prop, dict):
        return
    if prop.get("format") == "date-time":
        yield path
    properties = prop.get("properties")
    if isinstance(properties, dict):
        for name, part in properties.items():
            yield from _date_time_paths(field_path(path, name), part)
    yield from _date_time_paths(f"{path}[]", prop.get("items"))
