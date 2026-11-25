"""SQL that stages a file's rows for loading, read by DuckDB (DUCKLKD-119, 157).

The file is a BATCH message's, or a batch of records the sink wrote as JSON
lines (``record_lines``); its rows are never read into Python. One SELECT
per file converts its rows to the columns' staging types
(``staging_sql_type``), column by column, so every staged Parquet file
loads, merges, deletes and is checked alike:

- a flattened column is the struct field (or JSON member) at its property's
  path; a STRUCT or LIST column is rebuilt field by field, element by
  element, each leaf converted as a column of its type;
- a value is cast to its column's type by DuckDB, and a value the cast
  refuses fails the file, naming the stream, the file, the column and the
  value (DuckDB's own error names none of them);
- a value the cast would change without failing (a number rounded to its
  column's scale, an object key a STRUCT doesn't declare, dropped) is a
  ``Finding``: the caller counts them, and warns or fails by
  ``value_type_checking``.

A source is a Parquet file, whose columns have their own DuckDB types, or a
JSONL file, parsed once into a table (``batch_files``): a text, date or time
column's property as ``VARCHAR`` (a string as itself), a number or boolean
column's as its text beside its JSON type (``JsonText``), and the others as
``JSON``. A JSON number is a double to DuckDB's JSON reader, so its text
holds at most 17 significant digits; a number with more than 15 is a
finding, as digits past them may have changed (record lines hold such
numbers as text). A column is found by its property's exact name, in both
formats, and a STRUCT field by its key's.
"""

from __future__ import annotations

import itertools
import json
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Union

import pyarrow as pa

from target_ducklake.duckdb_types import (
    DuckDBList,
    DuckDBScalar,
    DuckDBStruct,
    DuckDBTypeTree,
    field_path,
    parse_type,
    quote_identifier,
    quote_literal,
    render_type,
)
from target_ducklake.insert_casts import (
    DATE,
    FRACTION,
    TIME,
    InsertCast,
    text_rounded,
    type_family,
)
from target_ducklake.parquet_utils import staging_sql_type, staging_type

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping, Sequence

    from target_ducklake.flatten import JsonTextRule

__all__ = [
    "JSON",
    "JSON_TEXT",
    "UUID_BYTES",
    "VARCHAR",
    "BatchColumn",
    "Finding",
    "JsonText",
    "Projection",
    "Source",
    "SourceType",
    "StagingContext",
    "jsonl_read_types",
    "projection",
]

JSON = DuckDBScalar("JSON")
VARCHAR = DuckDBScalar("VARCHAR")
# A Parquet column of the Arrow ``uuid`` type, which DuckDB reads as the
# UUID's 16 bytes when the writer didn't mark it with Parquet's UUID type.
UUID_BYTES = DuckDBScalar("UUID_BYTES")
_UUID = DuckDBScalar("UUID")
_BIGINT = DuckDBScalar("BIGINT")
_NULL = DuckDBScalar("NULL")
# A property the file has no column for.
_ABSENT = DuckDBScalar("ABSENT")
# How a JSONL file's property is read for a number or boolean column: its
# text, beside its JSON type (``JsonText``).
JSON_TEXT = "JSON_TEXT"


@dataclass(frozen=True)
class JsonText:
    """A JSON scalar's text (``->> '$'``), its JSON type in a column beside it.

    Attributes:
        kind: The SQL of the value's ``json_type``.
    """

    kind: str


# A source column's type: DuckDB's, or a JSON scalar read as its text.
SourceType = Union[DuckDBTypeTree, JsonText]  # noqa: UP007

# Column types whose values are text, which a value of any type is staged as.
_TEXT_TYPES = frozenset({"VARCHAR", "TEXT", "STRING", "CHAR", "BPCHAR"})
_JSON_KINDS = {"object": "OBJECT", "array": "ARRAY"}
# Base64 text, as ``from_base64`` reads it (strictly, as staging records does).
_BASE64 = quote_literal(
    "(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?"
)
# The UTC text of an instant, as a record's line holds it
# (``record_lines.datetime_text``).
_UTC_TEXT = "'%Y-%m-%d %H:%M:%S.%fZ'"
# The JSON types of numbers, as json_type gives them.
_JSON_NUMBERS = "'BIGINT', 'UBIGINT', 'DOUBLE'"
# The length of a date's text, ``YYYY-MM-DD``: a date with a time of day is
# longer (a finding's guard).
_DATE_LENGTH = 8
# The significant digits a double holds exactly.
_DOUBLE_DIGITS = 15
# The widest DECIMAL DuckDB casts text to quickly (see ``_try_cast``).
_NARROW_DECIMAL = 18
# How much of a refused value a message shows.
_SHOWN = 40


@dataclass(frozen=True)
class BatchColumn:
    """A staged column, and where each row's value for it comes from.

    Attributes:
        name: The column's name, as staged and loaded.
        column_type: The DuckDB type its values are cast to: the column's
            type, or the type the stream's values are staged at
            (``ducklakeSink._staged_columns``).
        path: The property path of its value in a row: ``("o", "a")`` for
            ``o__a`` flattened from object ``o``. None for a value that isn't
            in the file (``value``).
        flattened: The column's name before conforming and truncation; a
            Parquet file holding flattened columns has it.
        value: The value of every row, when ``path`` is None: an ``_sdc_*``
            metadata value (text, a number or None).
        json_text: Where the strings of the property at the path's head are
            JSON text (``flatten.json_text_rule``), from there down.
    """

    name: str
    column_type: str
    path: tuple[str, ...] | None = None
    flattened: str | None = None
    value: str | int | None = None
    json_text: JsonTextRule | None = None


@dataclass(frozen=True)
class Source:
    """A BATCH file as DuckDB reads it.

    Attributes:
        url: The file, as the manifest names it, for messages.
        reader: The table function reading it (``read_parquet(...)``).
        columns: Its columns, by name, and their types.
        row: The SQL of a row's position in the file, from 1.
    """

    url: str
    reader: str
    columns: Mapping[str, SourceType]
    row: str


@dataclass(frozen=True)
class Finding:
    """Values a column's cast changes without failing.

    Attributes:
        column: The column, or nested path, holding them.
        change: What happened to them, for messages.
        values: SQL giving, for each row, the list (``VARCHAR[]``) of its
            changed values as text, or of the object keys dropped.
        key: Whether the column is a key column.
        guard: SQL true for a row that may hold such a value, cheap to
            test; None to count every row. A file none of whose rows passes
            isn't counted (``batch_files._count``): the regular expressions
            of ``values`` cost most of a wide file's staging.
    """

    column: str
    change: str
    values: str
    key: bool = False
    guard: str | None = None


@dataclass(frozen=True)
class Projection:
    """How a file's rows are staged.

    Attributes:
        selects: Each staged column's expression, aliased.
        findings: The values the casts would change, of every column.
        absent: The columns whose properties the file doesn't have (a
            Parquet file's schema says so).
    """

    selects: list[str]
    findings: list[Finding]
    absent: list[str]


@dataclass(frozen=True)
class StagingContext:
    """What the staging rules say of a stream's columns.

    Attributes:
        keys: The key columns, whose findings say so.
        utc_paths: The lowercased columns and nested leaf paths whose
            date-times stage as UTC text (``ducklakeSink._utc_paths``).
        cast_checked: The lowercased columns whose staged values
            ``ducklakeSink._check_staged_values`` checks for their INSERT's
            cast; the projection leaves those to it.
        convert_tz_paths: The lowercased columns and nested leaf paths whose
            date-times with an offset other than UTC's stage as UTC text, a
            date-time without one as it is (``convert_tz_to_utc``).
        record_lines: Whether the source is a batch of records the target
            wrote (``record_lines``): its numbers are exact, as a number with
            a fraction is written as text, and each value of a JSON column or
            leaf is a string holding its JSON text.
    """

    keys: frozenset[str] = frozenset()
    utc_paths: frozenset[str] = frozenset()
    cast_checked: frozenset[str] = frozenset()
    convert_tz_paths: frozenset[str] = frozenset()
    record_lines: bool = False


@dataclass(frozen=True)
class _At:
    """Where a value is, for messages, and what applies to it there."""

    where: str
    path: str
    rule: JsonTextRule | None
    context: StagingContext = StagingContext()
    row: str = "NULL"
    # What a refused value keeps from loading, for messages.
    unit: str = "file"
    depth: int = 0
    # Numbers the lambda variables that bind values (``_bind``), so no two
    # nested ones share a name.
    names: Iterator[int] = field(default_factory=itertools.count, compare=False)

    def variable(self) -> str:
        """A new name for a lambda variable."""
        return f"b{next(self.names)}"

    @property
    def utc(self) -> bool:
        """Whether a date-time here stages as UTC text."""
        return self.path.lower() in self.context.utc_paths

    @property
    def converts_tz(self) -> bool:
        """Whether a date-time with an offset here stages as UTC text."""
        return self.path.lower() in self.context.convert_tz_paths

    def field(self, name: str) -> _At:
        rule = self.rule.properties.get(name) if self.rule is not None else None
        return replace(self, path=field_path(self.path, name), rule=rule)

    def element(self) -> _At:
        rule = self.rule.items if self.rule is not None else None
        return replace(self, path=f"{self.path}[]", rule=rule, depth=self.depth + 1)


def jsonl_read_types(
    columns: Sequence[BatchColumn], *, record_lines: bool = False
) -> dict[str, str]:
    """How each property the columns read from a JSONL file is read.

    A property read for a text, date, time or other column staged as text
    is ``VARCHAR``: its string, or the JSON text of another value. One read
    for a number or boolean column is ``JSON_TEXT``: its text, and beside it
    its JSON type, which tells a number's text from a string's, and ``true``
    from ``"true"``. Any other is ``JSON``: one flattened into columns, or
    read for a STRUCT, LIST or JSON column, converted as its shape is
    checked. With ``record_lines`` (``StagingContext.record_lines``), a JSON
    column's property is ``VARCHAR``: its string is the JSON text.
    """
    types: dict[str, str] = {}
    for column in columns:
        if column.path is None:
            continue
        target = parse_type(column.column_type)
        head = column.path[0]
        if types.get(head) == JSON.name:
            continue
        if record_lines and len(column.path) == 1 and _is_json(target):
            types[head] = VARCHAR.name
        elif (
            len(column.path) > 1
            or not isinstance(target, DuckDBScalar)
            or _is_json(target)
        ):
            types[head] = JSON.name
        elif staging_sql_type(target.name) in {"VARCHAR", "BLOB"}:
            types[head] = VARCHAR.name
        else:
            types[head] = JSON_TEXT
    return types


def projection(
    source: Source,
    columns: Sequence[BatchColumn],
    *,
    where: str,
    context: StagingContext,
    unit: str = "file",
) -> Projection:
    """How ``source``'s rows are staged as ``columns``.

    Args:
        source: The file.
        columns: The staged columns, in order.
        where: Who the rows are for, for messages: the stream and the file.
        context: The staging rules of the stream's columns.
        unit: What a refused value keeps from loading, for messages: the
            ``file``, or the ``batch`` of records.
    """
    selects: list[str] = []
    findings: list[Finding] = []
    absent: list[str] = []
    names = itertools.count()
    for column in columns:
        target = parse_type(column.column_type)
        found: list[Finding] = []
        expr, tree, rule = _source_value(source, column)
        if tree == _ABSENT:
            absent.append(column.name)
            expr, tree = "NULL", _NULL
        at = _At(where, column.name, rule, context, source.row, unit, names=names)
        value = _convert(expr, tree, target, at, found)
        selects.append(f"{value} AS {quote_identifier(column.name)}")
        is_key = column.name.lower() in context.keys
        findings += [replace(finding, key=is_key) for finding in found]
    return Projection(selects, findings, absent)


def _source_value(
    source: Source, column: BatchColumn
) -> tuple[str, SourceType, JsonTextRule | None]:
    """The SQL of a column's value in the file, its type, and its JSON text rule.

    The property at the column's path; a Parquet file's column of the
    flattened name; or, for a value the file doesn't have, ``_ABSENT`` (no
    such column) or NULL (no such member of an object).
    """
    if column.path is None:
        value = column.value
        if value is None:
            return "NULL", _NULL, None
        if isinstance(value, int):
            return str(value), _BIGINT, None
        return quote_literal(value), VARCHAR, None
    head, *rest = column.path
    rule = column.json_text
    name = head
    if head not in source.columns and column.flattened is not None and rest:
        name, rest, rule = column.flattened, [], None
    if name not in source.columns:
        return "NULL", _ABSENT, None
    expr, tree = quote_identifier(name), source.columns[name]
    names = itertools.count()
    for name in rest:
        at = _At("", "", rule, names=names)
        expr, tree = _member(expr, tree, name, at)
        rule = at.field(name).rule
    return expr, tree, rule


def _member(expr: str, tree: SourceType, name: str, at: _At) -> tuple[str, SourceType]:
    """A flattened object's property: a struct's field, or a JSON member.

    A value that isn't an object has no members (NULL), as a record whose
    object property holds something else gives its flattened columns none.
    JSON text of an object is that object, where ``json_text`` says so.
    """
    if isinstance(tree, DuckDBStruct):
        fields = dict(tree.fields)
        if name not in fields:
            return "NULL", _NULL
        return f"{expr}.{quote_identifier(name)}", fields[name]
    member = quote_literal(json.dumps({name: "JSON"}))
    parent = at.variable()
    obj = _decoded(parent, at)
    member_value = f"json_transform({obj}, {member}).{quote_identifier(name)}"
    return _bind(_as_json(expr, tree), parent, member_value), JSON


def _as_json(expr: str, tree: SourceType) -> str:
    """A value as JSON: JSON text parsed (NULL if it isn't), anything else written."""
    if tree == JSON:
        return expr
    if isinstance(tree, JsonText) or _is_text(tree):
        return f"TRY_CAST({expr} AS JSON)"
    return f"to_json({expr})"


def _convert(
    expr: str,
    source: SourceType,
    target: DuckDBTypeTree,
    at: _At,
    found: list[Finding],
) -> str:
    """SQL converting a value of type ``source`` to ``target``'s staging type."""
    if source == _NULL:
        return f"NULL::{staging_sql_type(render_type(target))}"
    if isinstance(source, JsonText):
        if isinstance(target, DuckDBScalar) and not _is_text(target):
            return _from_json(expr, source.kind, target.name, at, found)
        source = VARCHAR
    if isinstance(target, DuckDBStruct):
        return _to_struct(expr, source, target, at, found)
    if isinstance(target, DuckDBList):
        return _to_list(expr, source, target, at, found)
    return _to_scalar(expr, source, target.name, at, found)


def _to_scalar(
    expr: str,
    source: DuckDBTypeTree,
    column_type: str,
    at: _At,
    found: list[Finding],
) -> str:
    """SQL converting a value of type ``source`` to a scalar column's staging type."""
    target = DuckDBScalar(column_type)
    if source == UUID_BYTES:
        expr, source = f"CAST(hex({expr}) AS UUID)", _UUID
    if _is_json(target):
        return _to_json_text(expr, source, at)
    if source == JSON and staging_sql_type(target.name) != "VARCHAR":
        text, kind = f"({expr} ->> '$')", f"json_type({expr})"
        return _from_json(text, kind, target.name, at, found)
    if source == JSON:
        expr, source = f"({expr} ->> '$')", VARCHAR
    if _is_text(target):
        return _zoned(expr, _to_text(expr, source), at)
    if staging_sql_type(target.name) == "VARCHAR":
        return _to_text_typed(expr, source, target.name, at, found)
    return _to_typed(expr, source, target.name, at, found)


def _from_json(
    text: str, kind: str, column_type: str, at: _At, found: list[Finding]
) -> str:
    """A JSON scalar into a number, boolean or BLOB column, by its JSON type.

    ``text`` is its text (``->> '$'``), ``kind`` its ``json_type``. A JSON
    boolean into a number column, and a JSON number into a BOOLEAN one, are
    cast as DuckDB casts a boolean or a number (``true`` to BIGINT is 1, 2
    to BOOLEAN is true); anything else is read as its text. A JSON number
    DuckDB read as a double with more than 15 significant digits is a
    finding for an integer or DECIMAL column, where it isn't already one
    for being rounded: digits past them may have changed. Not in record
    lines, whose JSON numbers are exact: a whole number (DuckDB keeps the
    text of one too wide for 64 bits), or a float, whose shortest text the
    double holds.
    """
    typed = _to_typed(text, VARCHAR, column_type, at, found)
    staged = staging_sql_type(column_type)
    if _rounds(column_type) and not at.context.record_lines:
        rounded = _rounding(
            text, VARCHAR, column_type, _try_cast(text, VARCHAR, column_type)
        )
        found.append(
            Finding(
                at.path,
                f"is {column_type}, and these JSON numbers have more than "
                f"{_DOUBLE_DIGITS} significant digits, which DuckDB reads as a "
                "double, so digits past them may have changed (send such a "
                "number as text)",
                f"CASE WHEN {kind} = 'DOUBLE' AND "
                f"{_significant_digits(text)} > {_DOUBLE_DIGITS} "
                f"AND NOT coalesce({rounded}, false) "
                f"THEN [{text}] ELSE []::VARCHAR[] END",
                guard=f"{kind} = 'DOUBLE'",
            )
        )
    if staged == "BLOB":
        return typed
    if staged == "BOOLEAN":
        number = f"CAST(TRY_CAST({text} AS DOUBLE) AS BOOLEAN)"
        return f"CASE WHEN {kind} IN ({_JSON_NUMBERS}) THEN {number} ELSE {typed} END"
    boolean = f"CAST(CAST({text} AS BOOLEAN) AS {staged})"
    return f"CASE WHEN {kind} = 'BOOLEAN' THEN {boolean} ELSE {typed} END"


def _significant_digits(text: str) -> str:
    """SQL counting the significant digits of a number's text, exponent aside."""
    mantissa = f"regexp_extract({text}, '^[+-]?([0-9.]*)', 1)"
    return f"length(trim(replace({mantissa}, '.', ''), '0'))"


def _to_text_typed(
    expr: str,
    source: DuckDBTypeTree,
    column_type: str,
    at: _At,
    found: list[Finding],
) -> str:
    """A date, time, timestamp, UUID or other column staged as text, as text.

    For the INSERT to cast: the text as sent (a typed value as DuckDB
    writes it), or its instant as UTC text where the staging rules say
    (``_zoned``). A value the column's cast
    refuses fails the file. Text that loses part of itself in a DATE or TIME
    column's cast (a time of day, a date) is a finding, as
    ``InsertCast.changed_condition`` has it. A column whose INSERT cast is
    checked after staging (``StagingContext.cast_checked``) is staged as its
    text: that check reports both.
    """
    text = expr if _is_text(source) else f"CAST({expr} AS VARCHAR)"
    text = _zoned(expr, text, at)
    family = type_family(column_type)
    if at.path.lower() in at.context.cast_checked:
        # Checked after staging, which names the column's and the declared
        # types (``ducklakeSink._check_staged_values``).
        return text
    if family in {DATE, TIME}:
        cast = InsertCast(VARCHAR.name, column_type)
        if (changed := cast.changed_condition(text)) is not None:
            found.append(
                Finding(
                    at.path,
                    f"is {column_type}, and these values {cast.change}",
                    f"CASE WHEN {changed} THEN [{text}] ELSE []::VARCHAR[] END",
                    guard=f"length({text}) > {_DATE_LENGTH if family == DATE else 0}",
                )
            )
    refusal = _refusal(text, column_type, at)
    return (
        f"CASE WHEN {text} IS NULL OR TRY_CAST({text} AS {column_type}) "
        f"IS NOT NULL THEN {text} ELSE {refusal} END"
    )


def _zoned(expr: str, text: str, at: _At) -> str:
    """``text``, the text of ``expr``, as UTC text where the staging rules say.

    Where a date-time stages as UTC text (``StagingContext.utc_paths``), the
    instant of ``expr`` in UTC, one without an offset read as UTC; where a
    date-time with an offset does (``StagingContext.convert_tz_paths``),
    that instant in UTC, one without an offset as it is. Text that isn't a
    date-time stays as it is.
    """
    if at.utc:
        instant = f"strftime(TRY_CAST({expr} AS TIMESTAMPTZ), {_UTC_TEXT})"
        return f"coalesce({instant}, {text})"
    if at.converts_tz:
        # Its offset is other than UTC's where the instant differs from the
        # wall-clock time read as UTC (a TIMESTAMP cast drops the offset).
        instant = f"TRY_CAST({expr} AS TIMESTAMPTZ)"
        wall = f"CAST(TRY_CAST({expr} AS TIMESTAMP) AS TIMESTAMPTZ)"
        return (
            f"CASE WHEN {instant} <> {wall} THEN strftime({instant}, {_UTC_TEXT}) "
            f"ELSE {text} END"
        )
    return text


def _to_struct(
    expr: str,
    source: DuckDBTypeTree,
    target: DuckDBStruct,
    at: _At,
    found: list[Finding],
) -> str:
    """A STRUCT rebuilt field by field; a field the file lacks is NULL.

    Fields match by exact name, as an object's keys do in JSON (or JSON
    text), and in a record. A field the target doesn't declare is dropped:
    a finding, for each value it held. From JSON, the value must be an
    object.
    """
    if not isinstance(source, DuckDBStruct):
        return _json_to_struct(_as_json(expr, source), target, at, found)
    have = dict(source.fields)
    parts = []
    for name, part in target.fields:
        if name not in have:
            value = f"NULL::{staging_sql_type(render_type(part))}"
        else:
            child = f"{expr}.{quote_identifier(name)}"
            value = _convert(child, have[name], part, at.field(name), found)
        parts.append(f"{quote_identifier(name)} := {value}")
    declared = {name for name, _ in target.fields}
    dropped = [
        f"CASE WHEN {expr}.{quote_identifier(name)} IS NOT NULL "
        f"THEN {quote_literal(field_path(at.path, name))} END"
        for name, _ in source.fields
        if name not in declared
    ]
    if dropped:
        found.append(
            _dropped(at, f"list_filter([{', '.join(dropped)}], {_not_null(at)})")
        )
    return (
        f"CASE WHEN {expr} IS NULL THEN NULL ELSE struct_pack({', '.join(parts)}) END"
    )


def _json_to_struct(
    value: str, target: DuckDBStruct, at: _At, found: list[Finding]
) -> str:
    """A STRUCT from a JSON object, its members converted field by field.

    The object of its members is computed once and bound to a name
    (``_bind``), which each field's conversion refers to: a field's value is
    that name's field, so the SQL grows with the depth of nesting, not as a
    power of it, and each level adds one lambda. The findings of the fields
    are bound alike, as they are evaluated on their own. ``value`` is used
    several times, so it must be short: a column, a name, or a bound
    object's field.
    """
    json_value = _decoded(value, at)
    obj = at.variable()
    members = quote_literal(json.dumps({name: "JSON" for name, _ in target.fields}))
    transform = f"json_transform({json_value}, {members})"
    inner: list[Finding] = []
    parts = [
        f"{quote_identifier(name)} := "
        + _convert(f"{obj}.{quote_identifier(name)}", JSON, part, at.field(name), inner)
        for name, part in target.fields
    ]
    names = ", ".join(quote_literal(name) for name, _ in target.fields)
    key = f"k{at.depth}"
    prefix = quote_literal(f"{at.path}.")
    found.append(
        _dropped(
            at,
            f"CASE WHEN json_type({json_value}) = 'OBJECT' THEN list_transform("
            f"list_filter(json_keys({json_value}), "
            f"lambda {key}: {key} NOT IN ({names})), "
            f"lambda {key}: {prefix} || {key}) ELSE []::VARCHAR[] END",
        )
    )
    found += [
        replace(finding, values=_bind(transform, obj, finding.values), guard=None)
        for finding in inner
    ]
    struct = _bind(transform, obj, f"struct_pack({', '.join(parts)})")
    return (
        f"CASE WHEN {json_value} IS NULL OR json_type({json_value}) = 'NULL' "
        f"THEN NULL WHEN json_type({json_value}) = 'OBJECT' THEN {struct} "
        f"ELSE {_refusal(json_value, 'an object', at)} END"
    )


def _bind(value: str, name: str, body: str) -> str:
    """``body`` with ``name`` standing for ``value``, which is computed once.

    DuckDB has no ``LET``; a one-element list's lambda binds the value, so
    SQL that uses it several times holds it once.
    """
    return f"list_transform([{value}], lambda {name}: {body})[1]"


def _to_list(
    expr: str,
    source: DuckDBTypeTree,
    target: DuckDBList,
    at: _At,
    found: list[Finding],
) -> str:
    """A list rebuilt element by element; from JSON, the value must be an array."""
    element_at = at.element()
    var = f"e{at.depth}"
    inner: list[Finding] = []
    if isinstance(source, DuckDBList):
        converted = _convert(var, source.element, target.element, element_at, inner)
        result = f"list_transform({expr}, lambda {var}: {converted})"
        found += [
            replace(
                finding,
                values=(
                    f"flatten(list_transform({expr}, lambda {var}: {finding.values}))"
                ),
                guard=None,
            )
            for finding in inner
        ]
        return result
    value = _decoded(_as_json(expr, source), at)
    elements = (
        f"CASE WHEN json_type({value}) = 'ARRAY' THEN CAST({value} AS JSON[]) "
        "ELSE []::JSON[] END"
    )
    converted = _convert(var, JSON, target.element, element_at, inner)
    found += [
        replace(
            finding,
            values=(
                f"flatten(list_transform({elements}, lambda {var}: {finding.values}))"
            ),
            guard=None,
        )
        for finding in inner
    ]
    return (
        f"CASE WHEN {value} IS NULL OR json_type({value}) = 'NULL' THEN NULL "
        f"WHEN json_type({value}) = 'ARRAY' THEN list_transform("
        f"CAST({value} AS JSON[]), lambda {var}: {converted}) "
        f"ELSE {_refusal(value, 'an array', at)} END"
    )


def _to_json_text(expr: str, source: DuckDBTypeTree, at: _At) -> str:
    """A JSON column's value as its JSON text.

    JSON is kept as it is, and JSON text where the property is declared an
    object or array (``json_text``) is that object or array; any other
    value is written as JSON (text as a JSON string, bytes as base64 text).
    In record lines, the value is a string holding its JSON text, which is
    kept as the target wrote it.
    """
    if at.context.record_lines:
        return expr if _is_text(source) else f"({expr} ->> '$')"
    if source == JSON:
        return f"CAST({_decoded(expr, at)} AS VARCHAR)"
    if _is_text(source):
        written = f"CAST(to_json({expr}) AS VARCHAR)"
        if kinds := _decodes(at):
            return (
                f"CASE WHEN {_json_type(expr)} IN ({kinds}) "
                f"THEN CAST(json({expr}) AS VARCHAR) ELSE {written} END"
            )
        return written
    if _is_blob(source):
        expr = f"to_base64({expr})"
    return f"CAST(to_json({expr}) AS VARCHAR)"


def _to_text(expr: str, source: DuckDBTypeTree) -> str:
    """A text column's value: text as it is, anything else as its text.

    Bytes as base64 text (the JSON form of binary), objects and arrays as
    JSON text, and other values as DuckDB writes them.
    """
    if _is_text(source):
        return expr
    if _is_blob(source):
        return f"to_base64({expr})"
    if not isinstance(source, DuckDBScalar) or source.name.upper().startswith("MAP"):
        return f"CAST(to_json({expr}) AS VARCHAR)"
    return f"CAST({expr} AS VARCHAR)"


def _to_typed(
    expr: str,
    source: DuckDBTypeTree,
    column_type: str,
    at: _At,
    found: list[Finding],
) -> str:
    """A value cast to its column's type, then to that type's staging type.

    A value the cast refuses fails the file. A number the cast rounds is a
    finding: text, a float, or a decimal with more fraction digits than an
    integer or DECIMAL column holds. A float is read as its shortest text, as
    JSON writes it (a float32 0.1 is 0.1), so DuckDB's cast of a binary
    fraction doesn't add digits it never had.
    """
    staged = staging_sql_type(column_type)
    if staged == "BLOB":
        if _is_blob(source):
            return expr
        valid = f"regexp_full_match({expr}, {_BASE64})"
        decoded = f"CASE WHEN {valid} THEN from_base64({expr}) END"
        return _checked(decoded, expr, "base64 text", at)
    if _rounds(column_type) and _is_float(source):
        expr, source = f"CAST({expr} AS VARCHAR)", VARCHAR
    cast = _try_cast(expr, source, column_type)
    value = _checked(cast, expr, column_type, at)
    if (changed := _rounding(expr, source, column_type, cast)) is not None:
        found.append(
            Finding(
                at.path,
                f"is {column_type}, and these values were rounded to fit it",
                f"CASE WHEN {changed} THEN [CAST({expr} AS VARCHAR)] "
                "ELSE []::VARCHAR[] END",
                guard=_fraction_or_exponent(expr) if _is_text(source) else None,
            )
        )
    return (
        value
        if staged == render_type(parse_type(column_type))
        else f"CAST({value} AS {staged})"
    )


def _try_cast(expr: str, source: DuckDBTypeTree, column_type: str) -> str:
    """``TRY_CAST`` to the column's type; for text and a wide DECIMAL, a fast one.

    DuckDB 1.5.2 casts text to a DECIMAL of more than 18 digits about a
    thousand times slower than to one of 18 or fewer (about 40 µs a value
    for DECIMAL(38, 9), a JSON number's default column), so text is cast to
    DECIMAL(18, s) first, rounding alike, and to the column's own type only
    when it doesn't fit.
    """
    staged = staging_type(column_type)
    if (
        _is_text(source)
        and pa.types.is_decimal(staged)
        and staged.precision > _NARROW_DECIMAL
        and staged.scale <= _NARROW_DECIMAL
    ):
        narrow = f"DECIMAL({_NARROW_DECIMAL}, {staged.scale})"
        return (
            f"coalesce(TRY_CAST({expr} AS {narrow}), TRY_CAST({expr} AS {column_type}))"
        )
    return f"TRY_CAST({expr} AS {column_type})"


def _rounding(
    expr: str, source: DuckDBTypeTree, column_type: str, cast: str
) -> str | None:
    """SQL true where ``cast``, of ``expr`` to an integer or DECIMAL column, rounds.

    Text in plain notation with a non-zero digit past the column's scale;
    text with an exponent whose double differs from the cast's; a float or a
    decimal with more fraction digits than the column holds, that the cast
    changes. None when the column isn't one, or the source can't hold more
    digits.
    """
    if not _rounds(column_type):
        return None
    if _is_text(source):
        staged = staging_type(column_type)
        scale = staged.scale if pa.types.is_decimal(staged) else 0
        return text_rounded(expr, cast, scale)
    if (
        isinstance(source, DuckDBScalar)
        and type_family(source.name) == FRACTION
        and not _fits_scale(source.name, column_type)
    ):
        return f"{cast} <> {expr}"
    return None


def _fraction_or_exponent(text: str) -> str:
    """SQL true for number text that may round: with a point, an exponent or ``_``.

    What ``text_rounded`` needs to find a change, in one cheap test (DuckDB
    evaluates a disjunction in an aggregate far more slowly).
    """
    return f"regexp_matches({text}, '[.eE_]')"


def _rounds(column_type: str) -> bool:
    """Whether a column's cast rounds a fraction: an integer or DECIMAL column."""
    staged = staging_type(column_type)
    return pa.types.is_integer(staged) or pa.types.is_decimal(staged)


def _is_float(tree: DuckDBTypeTree) -> bool:
    return isinstance(tree, DuckDBScalar) and pa.types.is_floating(
        staging_type(tree.name)
    )


def _fits_scale(source_type: str, column_type: str) -> bool:
    """Whether a DECIMAL source has no more fraction digits than a DECIMAL column.

    A float may have any number, and an integer column holds none.
    """
    source, column = staging_type(source_type), staging_type(column_type)
    return (
        pa.types.is_decimal(source)
        and pa.types.is_decimal(column)
        and source.scale <= column.scale
    )


def _checked(value: str, source: str, column_type: str, at: _At) -> str:
    """``value``, failing the file where it is NULL for a non-NULL ``source``."""
    refusal = _refusal(source, column_type, at)
    return f"coalesce({value}, CASE WHEN {source} IS NULL THEN NULL ELSE {refusal} END)"


def _refusal(source: str, what: str, at: _At) -> str:
    """SQL failing the query, naming the file, the row, the column and the value."""
    prefix = quote_literal(f"{at.where}, row ")
    column = quote_literal(f", column {at.path!r}: value '")
    suffix = quote_literal(
        f"' can't be loaded as {what}, so the {at.unit} wasn't loaded."
    )
    shown = f"left(CAST({source} AS VARCHAR), {_SHOWN})"
    return (
        f"error({prefix} || CAST({at.row} AS VARCHAR) || {column} || {shown} "
        f"|| {suffix})"
    )


def _decoded(value: str, at: _At) -> str:
    """A JSON value, its JSON text replaced by what it encodes where ``json_text`` says.

    A JSON string whose text is the JSON of an object or array the property
    is declared (and can't be a string) is that object or array, as a
    record's is (``flatten.decode_json_text``).
    """
    if not (kinds := _decodes(at)):
        return value
    text = f"({value} ->> '$')"
    return (
        f"CASE WHEN json_type({value}) = 'VARCHAR' AND {_json_type(text)} "
        f"IN ({kinds}) THEN json({text}) ELSE {value} END"
    )


def _json_type(text: str) -> str:
    """SQL of the JSON type of text that is JSON; NULL for text that isn't.

    ``json_type`` fails on text that isn't JSON, and ``TRY`` can't be used
    in the lambdas a list's elements are converted in.
    """
    return f"(CASE WHEN json_valid({text}) THEN json_type({text}) END)"


def _decodes(at: _At) -> str:
    """The JSON types of the containers JSON text decodes to here, as SQL."""
    if at.rule is None or not at.rule.decodes:
        return ""
    return ", ".join(
        quote_literal(_JSON_KINDS[kind]) for kind in sorted(at.rule.decodes)
    )


def _dropped(at: _At, values: str) -> Finding:
    return Finding(
        at.path,
        "is a STRUCT without these fields of the objects sent, whose values "
        "were dropped",
        values,
    )


def _not_null(at: _At) -> str:
    var = f"k{at.depth}"
    return f"lambda {var}: {var} IS NOT NULL"


def _is_json(tree: DuckDBTypeTree) -> bool:
    return isinstance(tree, DuckDBScalar) and tree.name.upper() == "JSON"


def _is_text(tree: DuckDBTypeTree) -> bool:
    return isinstance(tree, DuckDBScalar) and (
        tree.name.upper().split("(")[0] in _TEXT_TYPES
    )


def _is_blob(tree: DuckDBTypeTree) -> bool:
    return isinstance(tree, DuckDBScalar) and staging_sql_type(tree.name) == "BLOB"
