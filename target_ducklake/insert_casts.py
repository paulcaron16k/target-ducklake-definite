"""Values staged for a column of another type, and what the INSERT's cast does.

A property whose column an existing table has as another type, such as a
string property of a BIGINT column (DUCKLKD-137) or an integer property of a
BOOLEAN column (DUCKLKD-142), loads by DuckDB's cast of its staged values to
the column's type: DuckLake changes a column's type in place only to widen
it. ``insert_cast`` says which pairs of declared and column types are cast,
``InsertCast.never_loads`` which of them no value can load (DuckDB has no
cast from a number to a DATE: DUCKLKD-145), and ``InsertCast.lossless``
which can neither fail nor change a value. A batch is checked before it is
loaded by one query (``findings_query``) for:

- **uncastable** values, which fail the cast (``TRY_CAST`` is NULL), and with
  it the INSERT, or a merge's key match: any value of a pair that never
  loads;
- **changed** values, which cast only by losing part of them: a fraction an
  integer or DECIMAL column can't hold (``'1.5'`` into BIGINT is 2), text a
  number's own parse doesn't read (``'0x10'`` into BIGINT is 16), a time of
  day for a DATE column, a date for a TIME column, or a number other than 0
  or 1 for a BOOLEAN column (5 is true);
- **merged keys**: distinct key values that cast to one key (``'7'`` and
  ``'7.0'``), loaded as two rows with the same key.

Column types come from the catalog (DESCRIBE) and declared types from the
stream's column DDL, never from config, so they are interpolated into the
SQL as DuckDB and the target wrote them.
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
_FLOATS = frozenset({"REAL", "FLOAT", "FLOAT4", "DOUBLE", "FLOAT8"})
_TIMESTAMPTZ = frozenset({"TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE"})
_TIMESTAMPS = _TIMESTAMPTZ | {
    "TIMESTAMP",
    "DATETIME",
    "TIMESTAMP_S",
    "TIMESTAMP_MS",
    "TIMESTAMP_NS",
}
_MAX_PRECISION = 38

# Type families. A value moves within one as a widening or a resize the
# staging rules handle, not as a change of type.
TEXT = "text"
INTEGER = "integer"
FRACTION = "fraction"
BOOLEAN = "boolean"
DATE = "date"
DATE_TIME = "date-time"
TIME = "time"
# Staged as numbers or booleans; DuckDB casts these only to each other and to
# text.
_NUMBERS = frozenset({INTEGER, FRACTION, BOOLEAN})
# Staged as their ISO text; it reads only as a date, a time or a timestamp.
_TEMPORAL = frozenset({DATE, DATE_TIME, TIME})
# Each type name's family; a DECIMAL(p, s) is a fraction too.
_FAMILIES = {
    "VARCHAR": TEXT,
    "BOOLEAN": BOOLEAN,
    "DATE": DATE,
    "TIME": TIME,
    **dict.fromkeys(_INTEGER_DIGITS, INTEGER),
    **dict.fromkeys(_FLOATS, FRACTION),
    **dict.fromkeys(_TIMESTAMPS, DATE_TIME),
}


def _name(column_type: str) -> str:
    """A type as one upper-case spelling, its spaces collapsed."""
    return " ".join(column_type.upper().split())


def is_timestamptz(column_type: str) -> bool:
    """Whether a DuckDB column type is TIMESTAMPTZ, by either of its names."""
    return _name(column_type) in _TIMESTAMPTZ


def type_family(column_type: str) -> str | None:
    """The family of a scalar DuckDB type; None for any other (JSON, UUID, ...)."""
    name = _name(column_type)
    return FRACTION if _DECIMAL.match(name) else _FAMILIES.get(name)


@dataclass(frozen=True)
class InsertCast:
    """A column whose staged values the INSERT casts to the table's type.

    ``declared`` is the stream's type for it (the column DDL it is staged
    by), ``column`` the table's.
    """

    declared: str
    column: str

    @property
    def _families(self) -> tuple[str | None, str | None]:
        return type_family(self.declared), type_family(self.column)

    @property
    def never_loads(self) -> bool:
        """Whether no value of the declared type can load into the column.

        DuckDB has no cast from a number or a boolean to a date, timestamp
        or time ("Unimplemented type for cast (BIGINT -> DATE)"), and a
        date, timestamp or time is staged as ISO text, which no number,
        boolean, or (for a time) date or timestamp cast reads.
        """
        declared, column = self._families
        return (
            (declared in _NUMBERS and column in _TEMPORAL)
            or (declared in _TEMPORAL and column in _NUMBERS)
            or (declared == TIME and column in {DATE, DATE_TIME})
        )

    @property
    def lossless(self) -> bool:
        """Whether the cast can neither fail nor change a value.

        A date into a timestamp column (its midnight), and a boolean into an
        integer column, or a number column with an integer digit (0 or 1).
        """
        declared, column = self._families
        if declared == DATE:
            return column == DATE_TIME
        if declared != BOOLEAN or column not in {INTEGER, FRACTION}:
            return False
        digits = _numeric_digits(self.column)
        return digits is None or digits[0] >= 1

    @property
    def staged_as_utc(self) -> bool:
        """Whether staging must write the values as UTC text.

        An instant (a TIMESTAMPTZ property) into a DATE or TIME column is cast
        from its UTC date and time. DuckDB's cast of staged text to a DATE or
        TIME doesn't apply its offset ('2024-01-01 23:00:00-05:00' is
        2024-01-01 as a DATE and 23:00:00 as a TIME), so the text is the UTC
        time first. A date into a TIMESTAMPTZ column needs
        nothing: the UTC session (``SESSION_TIME_ZONE``) casts it to midnight
        UTC (DUCKLKD-144, 148).
        """
        return is_timestamptz(self.declared) and self._families[1] in {DATE, TIME}

    @property
    def change(self) -> str:
        """What the cast does to a changed value, for messages."""
        declared, column = self._families
        if column == BOOLEAN and declared in _NUMBERS:
            return "aren't 0 or 1, so they load as true"
        if column == DATE:
            return "lose their time of day in its cast"
        if column == TIME:
            return "lose their date in its cast"
        return (
            "lose part of their text in its cast (a fraction it doesn't hold, or "
            "text a number isn't written as)"
        )

    def changed_condition(self, value: str) -> str | None:
        """SQL that is true where a castable ``value`` loses part of itself.

        Staged text (a string's, or a date's or a time's ISO text) is
        inspected by the column's type:

        - an integer or DECIMAL column: the cast differs from the text read
          as an exact decimal with as many fraction digits as 38 digits
          leave, or the text, in plain notation, has a non-zero digit past
          the column's scale (which a DECIMAL(38, 9) column leaves no
          headroom to compare), or the text has an exponent or an underscore
          and reads as a different DOUBLE than the cast (``'1.0000000005e0'``
          into DECIMAL(38, 9)). Text a decimal doesn't read, such as
          hexadecimal, differs too;
        - a DATE column: the text has a time of day;
        - a TIME column: the text has a date.

        A number into a BOOLEAN column: one other than 0 and 1. None for the
        other pairs, whose casts aren't inspected (as a number into a number
        column of its own family isn't).
        """
        declared, column = self._families
        if declared in _NUMBERS:
            return f"{value} NOT IN (0, 1)" if column == BOOLEAN else None
        if (digits := _numeric_digits(self.column)) is not None:
            integer, scale = digits
            fraction = _MAX_PRECISION - min(integer, _MAX_PRECISION)
            exact = f"DECIMAL({_MAX_PRECISION}, {fraction})"
            cast = f"TRY_CAST({value} AS {self.column})"
            past_scale = rf"^\s*[+-]?[0-9]*\.[0-9]{{{scale}}}[0-9]*[1-9][0-9]*\s*$"
            return (
                f"(TRY_CAST({value} AS {exact}) IS DISTINCT FROM "
                f"TRY_CAST({cast} AS {exact}) "
                f"OR regexp_matches({value}, '{past_scale}') "
                f"OR (regexp_matches({value}, '[eE_]') AND "
                f"TRY_CAST({value} AS DOUBLE) IS DISTINCT FROM "
                f"TRY_CAST({cast} AS DOUBLE)))"
            )
        if column == DATE:
            return (
                f"TRY_CAST({value} AS TIMESTAMP) IS DISTINCT FROM "
                f"TRY_CAST(TRY_CAST({value} AS DATE) AS TIMESTAMP)"
            )
        if column == TIME:
            return f"TRY_CAST({value} AS DATE) IS NOT NULL"
        return None


def insert_cast(declared: str, column: str) -> InsertCast | None:
    """The cast the INSERT applies to a column staged as ``declared``, if any.

    None where the column takes the value as it is: a VARCHAR column (as its
    text), or a column of the declared type's own family (a widening, or a
    DECIMAL or timestamp the staging rules handle). None too for the pairs
    left to DuckDB as before: a declared type outside the families (JSON,
    UUID, ...), or, unless the property is a string, a column type outside
    them. A string property's text is checked for any column but VARCHAR
    (DUCKLKD-137).
    """
    declared_family, column_family = type_family(declared), type_family(column)
    if (
        column_family == TEXT
        or declared_family is None
        or declared_family == column_family
        or (column_family is None and declared_family != TEXT)
    ):
        return None
    return InsertCast(declared, column)


@dataclass(frozen=True)
class Found:
    """How many values a check found, and a few of them (distinct, sorted)."""

    count: int
    sample: list[str]


@dataclass(frozen=True)
class CastFindings:
    """What a batch's values do when cast to their columns' types.

    ``uncastable`` and ``changed`` are by column, holding only the columns
    with any. ``merged_keys`` counts the key values lost to another value of
    the same key, and samples them as groups (``'7' = '7.0'``).
    """

    uncastable: dict[str, Found]
    changed: dict[str, Found]
    merged_keys: Found | None


def _numeric_digits(column_type: str) -> tuple[int, int] | None:
    """The integer and fraction digits a numeric column holds, if it is one."""
    name = _name(column_type)
    if name in _INTEGER_DIGITS:
        return _INTEGER_DIGITS[name], 0
    if match := _DECIMAL.match(name):
        precision, scale = int(match.group(1)), int(match.group(2))
        return precision - scale, scale
    return None


def _sampled(condition: str, value: str, sample: int) -> list[str]:
    """A count of the rows matching ``condition``, and a sample of ``value``.

    The sample is text, whatever the staged type.
    """
    text = f"CAST({value} AS VARCHAR)"
    return [
        f"count(*) FILTER (WHERE {condition})",
        f"list_sort(list(DISTINCT {text}) FILTER (WHERE {condition}))[1:{sample}]",
    ]


def findings_query(  # noqa: PLR0913 (the batch and its columns, then the options)
    source: str,
    casts: Mapping[str, InsertCast],
    key_columns: Sequence[str],
    live: str | None,
    sample: int,
    *,
    merge_keys: bool,
) -> str:
    """The query over ``source`` (a staged file) that ``read_findings`` reads.

    ``casts`` maps each column to check to its cast; ``key_columns`` are the
    stream's keys. ``live`` is the SQL condition of the rows a load inserts
    (None: every row): a key column is checked over every row, as a merge or
    a delete casts each staged key, any other only over the rows inserted.
    With ``merge_keys`` (a load that matches rows by key), key values that
    cast to one key are counted over the rows inserted.
    """
    keys = {key.lower() for key in key_columns}
    selects: list[str] = []
    for column, cast in casts.items():
        value = quote_identifier(column)
        scope = "" if column.lower() in keys or live is None else f"({live}) AND "
        castable = f"TRY_CAST({value} AS {cast.column})"
        selects += _sampled(
            f"{scope}{value} IS NOT NULL AND {castable} IS NULL", value, sample
        )
        changed = cast.changed_condition(value) or "false"
        selects += _sampled(
            f"{scope}{castable} IS NOT NULL AND {changed}", value, sample
        )
    if merge_keys and _cast_keys(casts, key_columns):
        rows = "" if live is None else f" FILTER (WHERE {live})"
        selects.append(
            f"count(DISTINCT {_key_row(casts, key_columns, cast=False)}){rows}"
            f" - count(DISTINCT {_key_row(casts, key_columns, cast=True)})"
            f"{rows}"
        )
    return f"SELECT {', '.join(selects)} FROM {source}"  # noqa: S608 (identifiers and catalog types)


def read_findings(row: Sequence, casts: Mapping[str, InsertCast]) -> CastFindings:
    """The findings in ``findings_query``'s row; ``merged_keys`` without a sample."""
    uncastable, changed = {}, {}
    for index, column in enumerate(casts):
        bad_count, bad_sample, changed_count, changed_sample = row[
            4 * index : 4 * index + 4
        ]
        if bad_count:
            uncastable[column] = Found(bad_count, bad_sample)
        if changed_count:
            changed[column] = Found(changed_count, changed_sample)
    merged = row[4 * len(casts)] if len(row) > 4 * len(casts) else 0
    return CastFindings(uncastable, changed, Found(merged, []) if merged else None)


def merged_keys_query(
    source: str,
    casts: Mapping[str, InsertCast],
    key_columns: Sequence[str],
    live: str | None,
    sample: int,
) -> str:
    """A sample of the key values that cast to one key, a group per key.

    Each group is the values of one key as text, as ``'7' = '7.0'``.
    """
    values = _key_row(casts, key_columns, cast=False)
    # A composite key's values as one text, '(7, a)'.
    label = f"CAST({values} AS VARCHAR)"
    where = "" if live is None else f" WHERE {live}"
    quoted = "text -> '''' || text || ''''"
    return (
        f"SELECT list_sort(list(texts))[1:{sample}] FROM ("  # noqa: S608
        f"SELECT array_to_string(list_transform("
        f"list_sort(list(DISTINCT {label})), {quoted}), ' = ') AS texts "
        f"FROM {source}{where} "
        f"GROUP BY {_key_row(casts, key_columns, cast=True)} "
        f"HAVING count(DISTINCT {values}) > 1)"
    )


def _cast_keys(casts: Mapping[str, InsertCast], key_columns: Sequence[str]) -> bool:
    """Whether any key column is cast."""
    cast = {column.lower() for column in casts}
    return any(key.lower() in cast for key in key_columns)


def _key_row(
    casts: Mapping[str, InsertCast], key_columns: Sequence[str], *, cast: bool
) -> str:
    """The key as one value: as staged, or each cast to its column's type.

    A single key is the value itself; a composite key a ``row(...)``. A
    NULL in it compares equal to NULL, as a merge's IS NOT DISTINCT FROM
    does, so ``('7', NULL)`` and ``('7.0', NULL)`` are one key once cast.
    """
    types = {column.lower(): insert.column for column, insert in casts.items()}
    parts = []
    for key in key_columns:
        value = quote_identifier(key)
        column_type = types.get(key.lower())
        parts.append(
            f"TRY_CAST({value} AS {column_type})" if cast and column_type else value
        )
    return parts[0] if len(parts) == 1 else f"row({', '.join(parts)})"
