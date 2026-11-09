"""Text staged for a column of another type, and what its cast does to it.

A string property whose column an existing table has as another type, such
as BIGINT, is loaded by DuckDB's cast of the staged text (DuckLake can't make
the column VARCHAR: DUCKLKD-137). This module builds the one query a batch
is checked with before it is loaded:

- **uncastable** text, which fails the cast (``TRY_CAST`` is NULL), and with
  it the INSERT, or a merge's key match;
- **changed** text, which casts only by losing part of it: a fraction an
  integer or DECIMAL column can't hold (``'1.5'`` into BIGINT is 2), text a
  number's own parse doesn't read (``'0x10'`` into BIGINT is 16), or a time
  of day for a DATE column;
- **merged keys**: distinct key texts that cast to one key (``'7'`` and
  ``'7.0'``), loaded as two rows with the same key.

Column types come from the catalog (DESCRIBE), never from config, so they
are interpolated into the SQL as DuckDB wrote them.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING

from target_ducklake.duckdb_types import quote_identifier

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

# Integer column types and the decimal digits their range needs.
_INTEGER_DIGITS = {
    "TINYINT": 3,
    "UTINYINT": 3,
    "SMALLINT": 5,
    "USMALLINT": 5,
    "INT": 10,
    "INTEGER": 10,
    "UINTEGER": 10,
    "BIGINT": 19,
    "UBIGINT": 20,
    "HUGEINT": 38,
    "UHUGEINT": 38,
}
_DECIMAL = re.compile(r"^(?:DECIMAL|NUMERIC)\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)$")
_MAX_PRECISION = 38


@dataclass(frozen=True)
class Found:
    """How many values a check found, and a few of them (distinct, sorted)."""

    count: int
    sample: list[str]


@dataclass(frozen=True)
class CastFindings:
    """What a batch's text does when cast to its columns' types.

    ``uncastable`` and ``changed`` are by column, holding only the columns
    with any. ``merged_keys`` counts the key texts lost to another text of
    the same key, and samples them as groups (``'7' = '7.0'``).
    """

    uncastable: dict[str, Found]
    changed: dict[str, Found]
    merged_keys: Found | None


def _numeric_digits(column_type: str) -> tuple[int, int] | None:
    """The integer and fraction digits a numeric column holds, if it is one."""
    name = " ".join(column_type.upper().split())
    if name in _INTEGER_DIGITS:
        return _INTEGER_DIGITS[name], 0
    if match := _DECIMAL.match(name):
        precision, scale = int(match.group(1)), int(match.group(2))
        return precision - scale, scale
    return None


def changed_condition(value: str, column_type: str) -> str | None:
    """SQL that is true where castable text loses part of itself in the cast.

    For an integer or DECIMAL column: the cast differs from the text read as
    an exact decimal with as many fraction digits as 38 digits leave, or the
    text, in plain notation, has a non-zero digit past the column's scale
    (which a DECIMAL(38, 9) column leaves no headroom to compare), or the
    text has an exponent or an underscore and reads as a different DOUBLE
    than the cast (``'1.0000000005e0'`` into DECIMAL(38, 9)). Text a
    decimal doesn't read, such as hexadecimal, differs too. For a DATE
    column: the text has a time of day. None for other types, whose casts
    aren't inspected (as for numbers sent as numbers).
    """
    if (digits := _numeric_digits(column_type)) is not None:
        integer, scale = digits
        fraction = _MAX_PRECISION - min(integer, _MAX_PRECISION)
        exact = f"DECIMAL({_MAX_PRECISION}, {fraction})"
        cast = f"TRY_CAST({value} AS {column_type})"
        past_scale = rf"^\s*[+-]?[0-9]*\.[0-9]{{{scale}}}[0-9]*[1-9][0-9]*\s*$"
        return (
            f"(TRY_CAST({value} AS {exact}) IS DISTINCT FROM "
            f"TRY_CAST({cast} AS {exact}) "
            f"OR regexp_matches({value}, '{past_scale}') "
            f"OR (regexp_matches({value}, '[eE_]') AND "
            f"TRY_CAST({value} AS DOUBLE) IS DISTINCT FROM "
            f"TRY_CAST({cast} AS DOUBLE)))"
        )
    if " ".join(column_type.upper().split()) == "DATE":
        return (
            f"TRY_CAST({value} AS TIMESTAMP) IS DISTINCT FROM "
            f"TRY_CAST(TRY_CAST({value} AS DATE) AS TIMESTAMP)"
        )
    return None


def _sampled(condition: str, value: str, sample: int) -> list[str]:
    """A count of the rows matching ``condition``, and a sample of ``value``."""
    return [
        f"count(*) FILTER (WHERE {condition})",
        f"list_sort(list(DISTINCT {value}) FILTER (WHERE {condition}))[1:{sample}]",
    ]


def findings_query(  # noqa: PLR0913 (the batch and its columns, then the options)
    source: str,
    column_types: Mapping[str, str],
    key_columns: Sequence[str],
    live: str | None,
    sample: int,
    *,
    merge_keys: bool,
) -> str:
    """The query over ``source`` (a staged file) that ``read_findings`` reads.

    ``column_types`` maps each text column to check to its table type;
    ``key_columns`` are the stream's keys. ``live`` is the SQL condition of
    the rows a load inserts (None: every row): a key column is checked over
    every row, as a merge or a delete casts each staged key, any other only
    over the rows inserted. With ``merge_keys`` (a load that matches rows by
    key), key texts that cast to one key are counted over the rows inserted.
    """
    keys = {key.lower() for key in key_columns}
    selects: list[str] = []
    for column, column_type in column_types.items():
        value = quote_identifier(column)
        scope = "" if column.lower() in keys or live is None else f"({live}) AND "
        castable = f"TRY_CAST({value} AS {column_type})"
        selects += _sampled(
            f"{scope}{value} IS NOT NULL AND {castable} IS NULL", value, sample
        )
        changed = changed_condition(value, column_type) or "false"
        selects += _sampled(
            f"{scope}{castable} IS NOT NULL AND {changed}", value, sample
        )
    if merge_keys and _cast_keys(column_types, key_columns):
        rows = "" if live is None else f" FILTER (WHERE {live})"
        selects.append(
            f"count(DISTINCT {_key_row(column_types, key_columns, cast=False)}){rows}"
            f" - count(DISTINCT {_key_row(column_types, key_columns, cast=True)})"
            f"{rows}"
        )
    return f"SELECT {', '.join(selects)} FROM {source}"  # noqa: S608 (identifiers and catalog types)


def read_findings(row: Sequence, column_types: Mapping[str, str]) -> CastFindings:
    """The findings in ``findings_query``'s row; ``merged_keys`` without a sample."""
    uncastable, changed = {}, {}
    for index, column in enumerate(column_types):
        bad_count, bad_sample, changed_count, changed_sample = row[
            4 * index : 4 * index + 4
        ]
        if bad_count:
            uncastable[column] = Found(bad_count, bad_sample)
        if changed_count:
            changed[column] = Found(changed_count, changed_sample)
    merged = row[4 * len(column_types)] if len(row) > 4 * len(column_types) else 0
    return CastFindings(uncastable, changed, Found(merged, []) if merged else None)


def merged_keys_query(
    source: str,
    column_types: Mapping[str, str],
    key_columns: Sequence[str],
    live: str | None,
    sample: int,
) -> str:
    """A sample of the key texts that cast to one key, a group per key.

    Each group is the texts of one key, as ``'7' = '7.0'``.
    """
    texts = _key_row(column_types, key_columns, cast=False)
    # A composite key's texts as one, '(7, a)'; a single key's are text.
    label = texts if len(key_columns) == 1 else f"CAST({texts} AS VARCHAR)"
    where = "" if live is None else f" WHERE {live}"
    quoted = "text -> '''' || text || ''''"
    return (
        f"SELECT list_sort(list(texts))[1:{sample}] FROM ("  # noqa: S608
        f"SELECT array_to_string(list_transform("
        f"list_sort(list(DISTINCT {label})), {quoted}), ' = ') AS texts "
        f"FROM {source}{where} "
        f"GROUP BY {_key_row(column_types, key_columns, cast=True)} "
        f"HAVING count(DISTINCT {texts}) > 1)"
    )


def _cast_keys(column_types: Mapping[str, str], key_columns: Sequence[str]) -> bool:
    """Whether any key column is cast from text."""
    cast = {column.lower() for column in column_types}
    return any(key.lower() in cast for key in key_columns)


def _key_row(
    column_types: Mapping[str, str], key_columns: Sequence[str], *, cast: bool
) -> str:
    """The key as one value: the texts, or each cast to its column's type.

    A single key is the value itself; a composite key a ``row(...)``. A
    NULL in it compares equal to NULL, as a merge's IS NOT DISTINCT FROM
    does, so ``('7', NULL)`` and ``('7.0', NULL)`` are one key once cast.
    """
    types = {
        column.lower(): column_type for column, column_type in column_types.items()
    }
    parts = []
    for key in key_columns:
        value = quote_identifier(key)
        column_type = types.get(key.lower())
        parts.append(
            f"TRY_CAST({value} AS {column_type})" if cast and column_type else value
        )
    return parts[0] if len(parts) == 1 else f"row({', '.join(parts)})"
