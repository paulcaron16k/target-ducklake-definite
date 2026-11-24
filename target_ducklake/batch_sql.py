"""SQL that stages a BATCH file's rows for loading, read by DuckDB (DUCKLKD-119).

A BATCH file is never read into Python. One SELECT per file converts its
rows to the staging types the RECORD path writes (``staging_sql_type``),
column by column, so the staged Parquet file loads, merges, deletes and is
checked exactly as a RECORD batch's is:

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
JSONL file, read with ``read_json``: a scalar column as its JSON text
(``VARCHAR``: a string as itself, a number as written), so DuckDB's cast
reads a value as it reads text; an object or array as ``JSON``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING

import pyarrow as pa

from target_ducklake.duckdb_types import (
    DuckDBList,
    DuckDBScalar,
    DuckDBStruct,
    DuckDBTypeTree,
    field_path,
    is_nested,
    parse_type,
    quote_identifier,
    quote_literal,
    render_type,
)
from target_ducklake.insert_casts import FRACTION, type_family
from target_ducklake.parquet_utils import staging_sql_type, staging_type

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence

    from target_ducklake.flatten import JsonTextRule

__all__ = [
    "JSON",
    "UUID_BYTES",
    "VARCHAR",
    "BatchColumn",
    "Finding",
    "Source",
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

# Column types whose values are text, which a value of any type is staged as.
_TEXT_TYPES = frozenset({"VARCHAR", "TEXT", "STRING", "CHAR", "BPCHAR"})
_JSON_KINDS = {"object": "OBJECT", "array": "ARRAY"}
# Base64 text, as ``from_base64`` reads it (strictly, as staging records does).
_BASE64 = quote_literal(
    "(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?"
)
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
            type, or the type the RECORD path stages it at
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
        columns: Its columns, by lowercased name: the name and type.
        row: The SQL of a row's position in the file.
        exact_names: Whether a column is found by its exact name (JSON
            keys) rather than ignoring case (Parquet, as DuckDB binds it).
    """

    url: str
    reader: str
    columns: Mapping[str, tuple[str, DuckDBTypeTree]]
    row: str
    exact_names: bool = False

    def column(self, name: str) -> tuple[str, DuckDBTypeTree] | None:
        """The file's column for a property, if it has one."""
        found = self.columns.get(name.lower())
        if found is None or (self.exact_names and found[0] != name):
            return None
        return found


@dataclass(frozen=True)
class Finding:
    """Values a column's cast changes without failing.

    Attributes:
        column: The column, or nested path, holding them.
        change: What happened to them, for messages.
        values: SQL giving, for each row, the list (``VARCHAR[]``) of its
            changed values as text, or of the object keys dropped.
        key: Whether the column is a key column.
    """

    column: str
    change: str
    values: str
    key: bool = False


@dataclass(frozen=True)
class _At:
    """Where a value is, for messages, and what applies to it there."""

    where: str
    path: str
    rule: JsonTextRule | None
    depth: int = 0

    def field(self, name: str) -> _At:
        rule = self.rule.properties.get(name) if self.rule is not None else None
        return replace(self, path=field_path(self.path, name), rule=rule)

    def element(self) -> _At:
        rule = self.rule.items if self.rule is not None else None
        return replace(self, path=f"{self.path}[]", rule=rule, depth=self.depth + 1)


def jsonl_read_types(columns: Sequence[BatchColumn]) -> dict[str, str]:
    """The ``read_json`` type of each property the columns read from a JSONL file.

    A property read for a scalar column is ``VARCHAR``, its JSON text; one
    flattened into columns, or read for a STRUCT, LIST or JSON column, is
    ``JSON``, converted as its shape is checked.
    """
    types: dict[str, str] = {}
    for column in columns:
        if column.path is None:
            continue
        target = parse_type(column.column_type)
        whole = len(column.path) == 1 and not (is_nested(target) or _is_json(target))
        head = column.path[0]
        if types.get(head) != JSON.name:
            types[head] = VARCHAR.name if whole else JSON.name
    return types


def projection(
    source: Source,
    columns: Sequence[BatchColumn],
    *,
    where: str,
    keys: Sequence[str] = (),
) -> tuple[list[str], list[Finding]]:
    """The SELECT list staging ``source``'s rows as ``columns``, and its findings.

    Args:
        source: The file.
        columns: The staged columns, in order.
        where: Who the rows are for, for messages: the stream and the file.
        keys: The key columns, whose findings say so.

    Returns:
        Each column's expression, aliased, and the findings of every column.
    """
    selects: list[str] = []
    findings: list[Finding] = []
    lowered_keys = {key.lower() for key in keys}
    for column in columns:
        target = parse_type(column.column_type)
        found: list[Finding] = []
        expr, tree, rule = _source_value(source, column)
        value = _convert(expr, tree, target, _At(where, column.name, rule), found)
        selects.append(f"{value} AS {quote_identifier(column.name)}")
        is_key = column.name.lower() in lowered_keys
        findings += [replace(finding, key=is_key) for finding in found]
    return selects, findings


def _source_value(
    source: Source, column: BatchColumn
) -> tuple[str, DuckDBTypeTree, JsonTextRule | None]:
    """The SQL of a column's value in the file, its type, and its JSON text rule.

    The property at the column's path; a Parquet file's column of the
    flattened name; or NULL, for a value the file doesn't have.
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
    found = source.column(head)
    if found is None and column.flattened is not None and rest:
        found, rest, rule = source.column(column.flattened), [], None
    if found is None:
        return "NULL", _NULL, None
    expr, tree = quote_identifier(found[0]), found[1]
    for name in rest:
        at = _At("", "", rule)
        expr, tree = _member(expr, tree, name, at)
        rule = at.field(name).rule
    return expr, tree, rule


def _member(
    expr: str, tree: DuckDBTypeTree, name: str, at: _At
) -> tuple[str, DuckDBTypeTree]:
    """A flattened object's property: a struct's field, or a JSON member.

    A value that isn't an object has no members (NULL), as a record whose
    object property holds something else gives its flattened columns none.
    JSON text of an object is that object, where ``json_text`` says so.
    """
    if isinstance(tree, DuckDBStruct):
        fields = {field.lower(): (field, part) for field, part in tree.fields}
        if (found := fields.get(name.lower())) is None:
            return "NULL", _NULL
        return f"{expr}.{quote_identifier(found[0])}", found[1]
    obj = _decoded(_as_json(expr, tree), at)
    member = quote_literal(json.dumps({name: "JSON"}))
    return f"json_transform({obj}, {member}).{quote_identifier(name)}", JSON


def _as_json(expr: str, tree: DuckDBTypeTree) -> str:
    """A value as JSON: JSON text parsed (NULL if it isn't), anything else written."""
    if tree == JSON:
        return expr
    if _is_text(tree):
        return f"TRY_CAST({expr} AS JSON)"
    return f"to_json({expr})"


def _convert(
    expr: str,
    source: DuckDBTypeTree,
    target: DuckDBTypeTree,
    at: _At,
    found: list[Finding],
) -> str:
    """SQL converting a value of type ``source`` to ``target``'s staging type."""
    if source == _NULL:
        return f"NULL::{staging_sql_type(render_type(target))}"
    if isinstance(target, DuckDBStruct):
        return _to_struct(expr, source, target, at, found)
    if isinstance(target, DuckDBList):
        return _to_list(expr, source, target, at, found)
    if source == UUID_BYTES:
        expr, source = f"CAST(hex({expr}) AS UUID)", _UUID
    if _is_json(target):
        return _to_json_text(expr, source, at)
    if source == JSON:
        expr, source = f"({expr} ->> '$')", VARCHAR
    if _is_text(target):
        return _to_text(expr, source)
    return _to_typed(expr, source, target.name, at, found)


def _to_struct(
    expr: str,
    source: DuckDBTypeTree,
    target: DuckDBStruct,
    at: _At,
    found: list[Finding],
) -> str:
    """A STRUCT rebuilt field by field; a field the file lacks is NULL.

    From a struct, fields match by name ignoring case, as DuckDB's do, and a
    field the target doesn't declare is dropped (a finding, for each value
    it held). From JSON (or JSON text), the value must be an object, and its
    members match by exact name, as JSON's keys do.
    """
    if not isinstance(source, DuckDBStruct):
        return _json_to_struct(_as_json(expr, source), target, at, found)
    have = {name.lower(): (name, part) for name, part in source.fields}
    parts = []
    for name, part in target.fields:
        field_at = at.field(name)
        if (field := have.get(name.lower())) is None:
            value = f"NULL::{staging_sql_type(render_type(part))}"
        else:
            child = f"{expr}.{quote_identifier(field[0])}"
            value = _convert(child, field[1], part, field_at, found)
        parts.append(f"{quote_identifier(name)} := {value}")
    declared = {name.lower() for name, _ in target.fields}
    dropped = [
        f"CASE WHEN {expr}.{quote_identifier(name)} IS NOT NULL "
        f"THEN {quote_literal(field_path(at.path, name))} END"
        for name, _ in source.fields
        if name.lower() not in declared
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
    """A STRUCT from a JSON object, its members converted field by field."""
    value = _decoded(value, at)
    members = json.dumps({name: "JSON" for name, _ in target.fields})
    obj = f"json_transform({value}, {quote_literal(members)})"
    parts = [
        f"{quote_identifier(name)} := "
        + _convert(f"{obj}.{quote_identifier(name)}", JSON, part, at.field(name), found)
        for name, part in target.fields
    ]
    names = ", ".join(quote_literal(name) for name, _ in target.fields)
    key = f"k{at.depth}"
    prefix = quote_literal(f"{at.path}.")
    found.append(
        _dropped(
            at,
            f"CASE WHEN json_type({value}) = 'OBJECT' THEN list_transform("
            f"list_filter(json_keys({value}), lambda {key}: {key} NOT IN ({names})), "
            f"lambda {key}: {prefix} || {key}) ELSE []::VARCHAR[] END",
        )
    )
    return (
        f"CASE WHEN {value} IS NULL OR json_type({value}) = 'NULL' THEN NULL "
        f"WHEN json_type({value}) = 'OBJECT' THEN struct_pack({', '.join(parts)}) "
        f"ELSE {_refusal(value, 'an object', at)} END"
    )


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
        elements = expr
        converted = _convert(var, source.element, target.element, element_at, inner)
        result = f"list_transform({expr}, lambda {var}: {converted})"
    else:
        value = _decoded(_as_json(expr, source), at)
        elements = (
            f"CASE WHEN json_type({value}) = 'ARRAY' THEN CAST({value} AS JSON[]) "
            "ELSE []::JSON[] END"
        )
        converted = _convert(var, JSON, target.element, element_at, inner)
        result = (
            f"CASE WHEN {value} IS NULL OR json_type({value}) = 'NULL' THEN NULL "
            f"WHEN json_type({value}) = 'ARRAY' THEN list_transform("
            f"CAST({value} AS JSON[]), lambda {var}: {converted}) "
            f"ELSE {_refusal(value, 'an array', at)} END"
        )
    found += [
        replace(
            finding,
            values=(
                f"flatten(list_transform({elements}, lambda {var}: {finding.values}))"
            ),
        )
        for finding in inner
    ]
    return result


def _to_json_text(expr: str, source: DuckDBTypeTree, at: _At) -> str:
    """A JSON column's value as its JSON text.

    JSON is kept as it is, and JSON text where the property is declared an
    object or array (``json_text``) is that object or array; any other
    value is written as JSON (text as a JSON string, bytes as base64 text).
    """
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
        past_scale = quote_literal(
            rf"^\s*[+-]?[0-9]*\.[0-9]{{{scale}}}[0-9]*[1-9][0-9]*\s*$"
        )
        return (
            f"(regexp_matches({expr}, {past_scale}) OR "
            f"(regexp_matches({expr}, '[eE]') AND TRY_CAST({expr} AS DOUBLE) "
            f"IS DISTINCT FROM TRY_CAST({cast} AS DOUBLE)))"
        )
    if (
        isinstance(source, DuckDBScalar)
        and type_family(source.name) == FRACTION
        and not _fits_scale(source.name, column_type)
    ):
        return f"{cast} <> {expr}"
    return None


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
    """SQL failing the query, naming the value, the column and the file."""
    prefix = quote_literal(f"{at.where}, column {at.path!r}: value '")
    suffix = quote_literal(f"' can't be loaded as {what}, so the file wasn't loaded.")
    shown = f"left(CAST({source} AS VARCHAR), {_SHOWN})"
    return f"error({prefix} || {shown} || {suffix})"


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
        "is a STRUCT without these fields of the file's objects, whose values "
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
