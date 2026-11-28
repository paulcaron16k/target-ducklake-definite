"""A batch of records written as JSON lines, for DuckDB to stage (DUCKLKD-157).

Each record, once the singer-sdk's per-record steps have run (stream maps,
``_sdc_*`` metadata, validation, its date-time parsing, ``type_source``),
is appended as one line to its batch's file. At drain, the file is staged
as a BATCH JSONL file is (``batch_files.stage_records``), so records and
BATCH files load through the same SQL.

A line holds each value so that DuckDB reads what the record held, as the
Arrow staging the lines replace read it:

- a number with a fraction (the singer-sdk parses one as a ``Decimal``) as
  its exact text, ``1.50`` as ``"1.50"``: DuckDB's JSON reader would read it
  as a double, with at most 17 significant digits. A whole number stays a
  JSON number, which DuckDB reads exactly at any width;
- a date-like value as text DuckDB reads as the same instant, date or time
  of day: as sent, in the forms ``sent_form`` gives for its column, else
  written from the value the singer-sdk parsed (``datetime_text``:
  ``...000000Z`` in UTC, its own offset otherwise, none when it had none,
  read as UTC where its column needs an instant; a date and a time of day
  as ISO text);
- the value of a property declared an object or array that loads into a
  JSON column, or of a JSON field of a STRUCT column, as its JSON text in a
  string (``ValueRules``), written as before (``flatten.dump_json_value``),
  so its numbers keep their digits; an object or array in a text or JSON
  column or field alike (text in a JSON column is JSON text), and a float
  in one as Python's text for it;
- text for an integer column (top-level or flattened) as the number it
  names by PostgreSQL's integer input (DUCKLKD-162): surrounding whitespace,
  a sign, then decimal digits or a ``0x``/``0o``/``0b`` prefix and its
  digits, with single underscores between digits. Other text (``"7.0"``,
  ``"1e3"``, ``""``) fails the batch, as PostgreSQL refuses it; a JSON
  number with a fraction is cast as before (rounded, a finding);
- a BOOLEAN column's value as a JSON boolean (DUCKLKD-163): text, trimmed
  and in any case, ``true t yes y on 1`` or ``false f no n off 0``; a
  number, or text that is one, true when greater than 0. Anything else
  fails the batch;
- an object for a STRUCT with its keys renamed to the fields they match
  ignoring case, as the Arrow staging matched them (a BATCH file's keys
  match exactly).

The lines are held in memory and appended to the file a megabyte at a time,
so no file stays open between records.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, NamedTuple

from target_ducklake.duckdb_types import (
    DuckDBList,
    DuckDBScalar,
    DuckDBStruct,
    DuckDBTypeTree,
    field_path,
    parse_type,
)
from target_ducklake.flatten import dump_json_value

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

__all__ = [
    "BOOLEAN_RULE",
    "LineColumn",
    "RecordLines",
    "ValueRules",
    "boolean",
    "datetime_text",
    "sent_form",
    "value_rules",
]

# Column types holding text, whose objects and arrays are their JSON text.
_TEXT_TYPES = frozenset({"VARCHAR", "TEXT", "STRING"})
_JSON = "JSON"
_BOOLEAN = "BOOLEAN"
_INTEGER_TYPES = frozenset(
    {
        "TINYINT",
        "SMALLINT",
        "INTEGER",
        "BIGINT",
        "HUGEINT",
        "UTINYINT",
        "USMALLINT",
        "UINTEGER",
        "UBIGINT",
        "UHUGEINT",
    }
)
# PostgreSQL's integer text input (16 and later): whitespace, a sign, then
# decimal digits or a base prefix and its digits, an underscore only between
# digits (or after the prefix). Range is the cast's to check.
_INTEGER_TEXT = re.compile(
    r"[ \t\n\r\v\f]*(?P<sign>[+-]?)(?:"
    r"0[xX](?P<hex>_?[0-9a-fA-F]+(?:_[0-9a-fA-F]+)*)"
    r"|0[oO](?P<oct>_?[0-7]+(?:_[0-7]+)*)"
    r"|0[bB](?P<bin>_?[01]+(?:_[01]+)*)"
    r"|(?P<dec>[0-9]+(?:_[0-9]+)*)"
    r")[ \t\n\r\v\f]*"
)
_BASES = {"hex": 16, "oct": 8, "bin": 2, "dec": 10}
# Booleans by their text, trimmed and lowercased; other text is read as a
# number, true when greater than 0.
_BOOLEAN_WORDS = {
    **dict.fromkeys(("true", "t", "yes", "y", "on", "1"), True),
    **dict.fromkeys(("false", "f", "no", "n", "off", "0"), False),
}
_NUMBER_TEXT = re.compile(r"[+-]?(?:[0-9]+\.?[0-9]*|\.[0-9]+)(?:[eE][+-]?[0-9]+)?")


class _RefusedError(ValueError):
    """A value its column can't hold; ``RecordLines`` names its row and column."""


# The text of a date-like value (by the singer-sdk's kinds) that a line
# holds as sent: DuckDB reads each of these forms as the value the singer-sdk
# parses it as, which ``datetime_text`` (or ``isoformat``) would write, into
# the column types in ``_SENT_TYPES`` (DUCKLKD-157). Not the basic or week
# forms, a time without seconds before an offset, an offset with seconds,
# more than six fraction digits, or a ``,`` before them: DuckDB refuses them
# or reads them otherwise.
_SENT_FORMS = {
    "date-time": re.compile(
        r"[0-9]{4}-[0-9]{2}-[0-9]{2}"
        r"(?:[T ][0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?"
        r"(?:Z|[+-][0-9]{2}:[0-9]{2})?)?"
    ),
    "date": re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}"),
    "time": re.compile(r"[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?"),
}
# The column types a date-like value's text is sent to as it is. A date in
# its form is the text ``isoformat`` writes, into any column. A date-time's
# or a time's text is another than the one written, so not into a text or
# JSON column, which keeps it, nor a key column, whose text the staging's
# dedup compares.
_SENT_TYPES = {
    "date-time": frozenset({"TIMESTAMP", "TIMESTAMPTZ", "DATE", "TIME"}),
    "time": frozenset({"TIME"}),
}


def sent_form(
    kind: str, column_types: Iterable[str], *, key: bool
) -> re.Pattern[str] | None:
    """The forms of a date-like property's text that its line holds as sent.

    None when every value is written from its parsed value, as before.

    Args:
        kind: The property's kind, as the singer-sdk reads it: ``date-time``,
            ``date`` or ``time``.
        column_types: Its staged column's type, and the table's column's.
        key: Whether its column is a key column.
    """
    if kind == "date":
        return _SENT_FORMS[kind]
    allowed = _SENT_TYPES.get(kind)
    if allowed is None or key:
        return None
    names = {_type_name(column_type) for column_type in column_types}
    return _SENT_FORMS[kind] if names <= allowed else None


def _type_name(column_type: str) -> str:
    """A scalar type's name without its precision; TIMESTAMPTZ for its long name."""
    name = column_type.upper().split("(")[0].strip()
    return "TIMESTAMPTZ" if name == "TIMESTAMP WITH TIME ZONE" else name


def datetime_text(value: datetime) -> str:
    """Render a datetime as ISO text that DuckDB reads exactly.

    Naive: '2024-01-01 10:00:00.000000'; UTC: '...000000Z'; a fixed offset:
    its own local time and offset, '...000000+02:00' (with seconds,
    '+02:00:30', when the offset has them). A named zone's local time that
    doesn't exist (a DST gap) is normalised through UTC first, as pyarrow
    does; fixed offsets have no gaps, so they skip that round trip, which
    would overflow at the edges of the year range.
    """
    zone = value.tzinfo
    if zone is timezone.utc:
        return value.replace(tzinfo=None).isoformat(" ", "microseconds") + "Z"
    if zone is None or isinstance(zone, timezone):
        # isoformat writes a fixed offset as '+HH:MM', or '+HH:MM:SS'.
        return value.isoformat(" ", "microseconds")
    local = value
    try:
        local = value.astimezone(timezone.utc).astimezone(zone)
    except OverflowError:
        local = value
    return _wall_clock_text(local) + _offset_text(local.utcoffset())


def _wall_clock_text(value: datetime) -> str:
    """'YYYY-MM-DD HH:MM:SS.ffffff' with a zero-padded year (strftime's %Y isn't)."""
    return value.replace(tzinfo=None).isoformat(sep=" ", timespec="microseconds")


def _offset_text(offset: timedelta | None) -> str:
    """'+HH:MM', or '+HH:MM:SS' when the offset has seconds (DuckDB reads both)."""
    seconds = int((offset or timedelta(0)).total_seconds())
    sign = "-" if seconds < 0 else "+"
    hours, rest = divmod(abs(seconds), 3600)
    minutes, secs = divmod(rest, 60)
    text = f"{sign}{hours:02d}:{minutes:02d}"
    return f"{text}:{secs:02d}" if secs else text


def _text(value: object) -> str:
    """The text a line holds for a value JSON has no type for.

    Raises:
        TypeError: For a value that isn't a Decimal, a datetime, a date or a
            time, as ``json.dumps`` raises for one.
    """
    if isinstance(value, Decimal):
        # Exact digits, without an exponent: 1E+2 is '100', as a text column
        # stores it.
        return format(value, "f")
    if isinstance(value, datetime):
        return datetime_text(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, time):
        return value.isoformat(timespec="microseconds")
    msg = f"Object of type {type(value).__name__} is not JSON serializable"
    raise TypeError(msg)


# The stdlib's C encoder: about three times faster than simplejson's for
# these records (DUCKLKD-157). NaN and infinity are written as such, which
# DuckDB's JSON reader reads.
_ENCODER = json.JSONEncoder(
    default=_text, ensure_ascii=False, check_circular=False, separators=(",", ":")
)


# A batch's lines are kept in memory up to this many bytes, then appended to
# its file, which is open only while they are: a run with many streams holds
# no descriptor per stream between records.
_FLUSH_BYTES = 1 << 20
# How much of a value a message shows.
_SHOWN = 40


class LineColumn(NamedTuple):
    """A staged column, as ``value_rules`` reads it.

    Attributes:
        path: Its property's path in a record: ``("o", "a")`` for a column
            flattened from object ``o``.
        name: The column's name, for messages.
        column_type: Its staged DuckDB type.
        container: Whether its property is declared an object or array.
    """

    path: tuple[str, ...]
    name: str
    column_type: str
    container: bool


@dataclass(frozen=True)
class ValueRules:
    """What a record's values become in its line, where they need more than JSON.

    Attributes:
        label: The column or nested field, for messages (``o.items[].a``).
        whole: A JSON column's or field's value: any value but None is
            written as its JSON text.
        text: A text column's or field's value: an object or array is written
            as its JSON text, a float as Python's text for it (``nan``, as
            the Arrow staging stored it).
        boolean: A BOOLEAN column's value: written as a JSON boolean by
            ``boolean``, or refused.
        integer: An integer column's type: its text is written as the number
            PostgreSQL's integer input reads it as (``_integer``), or refused.
        fields: The object's members that need rules, by exact name.
        struct: For a STRUCT, its fields by lowercased name: a record's keys
            match them ignoring case, as the Arrow staging matched them.
        declared: A STRUCT's field names: an object whose keys are all among
            them needs no matching.
        texts_only: Whether every field with rules is a text field, so an
            object holding only plain scalars needs nothing done.
        element: The array's elements, when they need rules.
        shape: ``object`` for a STRUCT, ``array`` for a LIST: a number with
            a fraction there is refused as the staging would refuse it
            (``RecordLines._not_a_shape``), since its line holds it as text.
    """

    label: str = ""
    whole: bool = False
    text: bool = False
    boolean: bool = False
    integer: str = ""
    fields: Mapping[str, ValueRules] = field(default_factory=dict)
    struct: Mapping[str, str] | None = None
    declared: frozenset[str] = frozenset()
    texts_only: bool = False
    element: ValueRules | None = None
    shape: str = ""


def value_rules(columns: Iterable[LineColumn]) -> dict[str, ValueRules]:
    """The rules of a stream's records, by top-level property, from its columns.

    Empty when no column needs any: every value is then written as JSON.
    """
    root: dict[str, ValueRules] = {}
    for column in columns:
        tree = parse_type(column.column_type)
        if isinstance(tree, DuckDBScalar):
            node = _scalar_rules(tree, column)
        else:
            node = _nested_rules(tree, column.name)
        if node is not None:
            _place(root, column.path, node)
    return root


def _scalar_rules(tree: DuckDBScalar, column: LineColumn) -> ValueRules | None:
    """A top-level (or flattened) scalar column's rules; None if it needs none.

    A property declared an object or array is JSON text whatever its value
    (a string too), in a JSON or text column; a text column's other values
    follow ``ValueRules.text``, as do a JSON column's.
    """
    name = tree.name.upper().split("(")[0]
    if name == _BOOLEAN:
        return ValueRules(label=column.name, boolean=True)
    if name in _INTEGER_TYPES:
        return ValueRules(label=column.name, integer=name)
    if name not in _TEXT_TYPES and name != _JSON:
        return None
    if column.container:
        return ValueRules(label=column.name, whole=True)
    return ValueRules(label=column.name, text=True)


def _nested_rules(tree: DuckDBTypeTree, label: str) -> ValueRules | None:
    """A STRUCT's or LIST's (or a field's) rules; None for a field that needs none.

    A STRUCT always has rules, to match its keys ignoring case, and a LIST
    to refuse a number with a fraction (``shape``); a JSON field is JSON
    text, and a text field holds an object or array as JSON text.
    """
    if isinstance(tree, DuckDBStruct):
        fields = {
            name: child
            for name, part in tree.fields
            if (child := _nested_rules(part, field_path(label, name))) is not None
        }
        struct = {name.lower(): name for name, _ in tree.fields}
        return ValueRules(
            label=label,
            fields=fields,
            struct=struct,
            declared=frozenset(name for name, _ in tree.fields),
            texts_only=all(child.text for child in fields.values()),
            shape="object",
        )
    if isinstance(tree, DuckDBList):
        element = _nested_rules(tree.element, f"{label}[]")
        return ValueRules(label=label, element=element, shape="array")
    name = tree.name.upper().split("(")[0]
    if name == _JSON:
        return ValueRules(label=label, whole=True)
    if name in _TEXT_TYPES:
        return ValueRules(label=label, text=True)
    return None


def _place(
    root: dict[str, ValueRules], path: tuple[str, ...], node: ValueRules
) -> None:
    """Put ``node`` at ``path`` in the tree of properties, merging objects."""
    head, *rest = path
    if not rest:
        root[head] = node
        return
    parent = root.get(head, ValueRules())
    fields = dict(parent.fields)
    _place(fields, tuple(rest), node)
    root[head] = ValueRules(fields=fields)


def _scalar(value: object, node: ValueRules) -> object:
    """A JSON, text, BOOLEAN or integer column's (or field's) value, by its rules.

    Raises:
        _RefusedError: For a value a BOOLEAN or integer column can't hold.
    """
    if node.boolean:
        return boolean(value)
    if node.integer:
        return _integer(value) if isinstance(value, str) else value
    if node.whole:
        return dump_json_value(value)
    if node.text:
        if isinstance(value, (dict, list)):
            return dump_json_value(value)
        return repr(value) if isinstance(value, float) else value
    return value


def boolean(value: object) -> bool:
    """A BOOLEAN column's value: a word, or a number true when greater than 0.

    Raises:
        ValueError: For any other value (``_RefusedError``).
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        text = value.strip(" \t\n\r\v\f").lower()
        word = _BOOLEAN_WORDS.get(text)
        if word is not None:
            return word
        if _NUMBER_TEXT.fullmatch(text):
            return Decimal(text) > 0
    elif isinstance(value, int | Decimal | float) and not _is_nan(value):
        return value > 0
    raise _RefusedError(BOOLEAN_RULE)


def _is_nan(value: float | Decimal) -> bool:
    return value != value  # noqa: PLR0124 (NaN is the one value unequal to itself)


def _integer(text: str) -> int:
    """The integer PostgreSQL's integer input reads ``text`` as.

    Raises:
        _RefusedError: For text it refuses (a fraction, an exponent, no digits).
    """
    match = _INTEGER_TEXT.fullmatch(text)
    if match is None:
        raise _RefusedError(_INTEGER_RULE)
    base = match.lastgroup or "dec"
    number = int(match[base].replace("_", ""), _BASES[base])
    return -number if match["sign"] == "-" else number


BOOLEAN_RULE = (
    "a boolean is true, t, yes, y, on or 1, or false, f, no, n, off or 0 (any "
    "case), or a number: true when greater than 0"
)
_INTEGER_RULE = (
    "integer text is digits, or a 0x, 0o or 0b prefix and its digits, with an "
    "optional sign and underscores between digits, as PostgreSQL reads it; "
    "round a fraction or an exponent before sending it as text"
)


# The types of a text field's values that it holds as they are.
_PLAIN = frozenset({str, int, bool, type(None)})
# The types JSON decodes to, but objects and arrays: a value of one of these
# that isn't a Decimal is held as it is by any node without ``whole``,
# ``text``, ``boolean`` or ``integer`` (``_kept``).
_SCALARS = frozenset({str, int, bool, float, Decimal, type(None)})


def _kept(node: ValueRules) -> frozenset[type]:
    """The types of a property's values that its line holds as they are.

    A record's value of one of them is written as it is, without walking
    ``node``'s rules: text in a text column, a boolean in a BOOLEAN one, any
    value but text in an integer one, and in a STRUCT or LIST column any
    scalar but a Decimal (which the staging then refuses), and an array whose
    elements need no rules. None, or no value, always.
    """
    if node.whole:
        return frozenset({type(None)})
    if node.text:
        return _PLAIN
    if node.boolean:
        return frozenset({bool, type(None)})
    if node.integer:
        return (_SCALARS - {str}) | {dict, list}
    if node.shape == "array" and node.element is None:
        return (_SCALARS - {Decimal}) | {dict, list}
    return _SCALARS - {Decimal}


def _shallow(node: ValueRules) -> bool:
    """Whether an object or array whose own values are plain needs nothing here.

    A STRUCT's object whose keys are all its fields' names, and whose fields
    with rules are text fields; a LIST's array whose elements are text.
    """
    if node.struct is not None:
        return node.texts_only
    return node.element is not None and node.element.text


def _plain_members(value: object, node: ValueRules) -> bool:
    """Whether ``value``, an object or array, needs nothing (``_shallow``)."""
    if type(value) is dict:
        return value.keys() <= node.declared and _PLAIN.issuperset(
            map(type, value.values())
        )
    return type(value) is list and _PLAIN.issuperset(map(type, value))


def _encodes(value: object) -> bool:
    """Whether text, or a value's JSON text, is valid UTF-8 (no lone surrogate)."""
    try:
        (value if isinstance(value, str) else _ENCODER.encode(value)).encode()
    except UnicodeEncodeError:
        return False
    return True


def _shown(value: object) -> str:
    text = repr(value)
    return text if len(text) <= _SHOWN else text[:_SHOWN] + "..."


class RecordLines:
    """A batch's records, appended as JSON lines to its file."""

    def __init__(
        self,
        path: str,
        rules: Mapping[str, ValueRules],
        *,
        stream: str,
        strict: bool = False,
        logger: logging.Logger | None = None,
    ) -> None:
        """Start an empty batch; its file is written as lines accumulate.

        Args:
            path: The file.
            rules: The stream's ``value_rules``.
            stream: The stream, for messages.
            strict: Whether ``value_type_checking`` is strict: two keys of one
                object for one STRUCT field fail rather than warn.
            logger: The stream's logger, for that warning.
        """
        self.path = path
        # The top-level properties with rules, the types of their values that
        # need none (_kept), and whether a plain object or array needs none
        # (_shallow): most records then skip the walk of the rules.
        self._rules = [
            (name, node, _kept(node), _shallow(node)) for name, node in rules.items()
        ]
        self._stream = stream
        self._strict = strict
        self._logger = logger or logging.getLogger(__name__)
        self._lines: list[str] = []
        self._size = 0
        # The records written, so a refusal names its row in the batch.
        self._rows = 0
        Path(path).write_text("")

    def write(self, record: dict) -> None:
        """Append a record as one line; only its top-level dict is changed.

        Raises:
            ValueError: For a string UTF-8 can't encode (a lone surrogate),
                naming the stream and the property; or, under strict, for
                two keys of one object that name one STRUCT field.
        """
        for name, node, kept, shallow in self._rules:
            value = record.get(name)
            if type(value) in kept or (shallow and _plain_members(value, node)):
                continue
            record[name] = self._prepared(value, node)
        line = _ENCODER.encode(record) + "\n"
        if not line.isascii() and not _encodes(line):
            raise self._not_utf8(record)
        self._lines.append(line)
        self._rows += 1
        self._size += len(line)
        if self._size >= _FLUSH_BYTES:
            self._flush()

    def close(self) -> None:
        """Append the lines still held to the file."""
        self._flush()

    def discard(self) -> None:
        """Forget the batch, and remove its file."""
        self._lines, self._size = [], 0
        Path(self.path).unlink(missing_ok=True)

    def _flush(self) -> None:
        if self._lines:
            with Path(self.path).open("a", encoding="utf-8") as file:
                file.write("".join(self._lines))
            self._lines, self._size = [], 0

    def _prepared(self, value: object, node: ValueRules) -> object:
        """``value`` as its line holds it, by ``node``'s rules.

        Objects and arrays on the way are copied when changed, never changed
        in place: a stream map's copies of a record share them.
        """
        if value is None:
            return None
        if node.whole or node.text or node.boolean or node.integer:
            return self._scalar(value, node)
        if node.shape and type(value) is Decimal:
            raise self._not_a_shape(value, node)
        if isinstance(value, dict):
            return self._object(value, node)
        if node.element is not None and isinstance(value, list):
            if node.element.text and _PLAIN.issuperset(map(type, value)):
                return value
            items = [self._prepared(item, node.element) for item in value]
            changed = any(new is not old for new, old in zip(items, value, strict=True))
            return items if changed else value
        return value

    def _scalar(self, value: object, node: ValueRules) -> object:
        """``_scalar``, with a refusal naming the row, the column and the value."""
        try:
            return _scalar(value, node)
        except _RefusedError as refused:
            column_type = node.integer or _BOOLEAN
            msg = (
                f"stream {self._stream!r}, batch of records, row {self._rows + 1}, "
                f"column {node.label!r}: value {_shown(value)} can't be loaded as "
                f"{column_type}, so the batch wasn't loaded: {refused}."
            )
            raise ValueError(msg) from None

    def _object(self, value: dict, node: ValueRules) -> dict:
        """An object's members by ``node``'s rules, its keys matched to a STRUCT's."""
        if node.struct is not None and not value.keys() <= node.declared:
            value = self._matched(value, node)
        if not node.fields:
            return value
        if node.texts_only and _PLAIN.issuperset(map(type, value.values())):
            return value
        copied = None
        for name, child in node.fields.items():
            if name not in value:
                continue
            item = value[name]
            prepared = self._prepared(item, child)
            if prepared is not item:
                copied = dict(value) if copied is None else copied
                copied[name] = prepared
        return value if copied is None else copied

    def _matched(self, value: dict, node: ValueRules) -> dict:
        """The object with its keys renamed to the STRUCT's fields, ignoring case.

        A key no field matches is kept, for the staging to report as dropped.
        Two keys for one field: the later one's value is kept, with a
        warning, or the batch fails under strict ``value_type_checking``.
        """
        struct = node.struct or {}
        matched: dict = {}
        given: dict[str, str] = {}
        for key, item in value.items():
            name = struct.get(str(key).lower())
            if name is None:
                matched[key] = item
                continue
            if name in given:
                self._two_keys(node.label, given[name], str(key), name)
            given[name] = str(key)
            matched[name] = item
        return matched

    def _two_keys(self, label: str, first: str, second: str, name: str) -> None:
        """Two keys of one object name the same STRUCT field, ignoring case.

        Raises:
            ValueError: Under strict ``value_type_checking``.
        """
        message = (
            f"stream {self._stream!r}: column {label!r} got keys {first!r} and "
            f"{second!r} for field {name!r}, which matches names case-insensitively"
        )
        if self._strict:
            msg = (
                f"{message}; one value would be lost. value_type_checking is "
                "strict; set it to permissive to keep the later key's value with "
                "a warning."
            )
            raise ValueError(msg)
        self._logger.warning("%s; keeping the value of %r.", message, second)

    def _not_a_shape(self, value: Decimal, node: ValueRules) -> ValueError:
        """The error for a number with a fraction where an object or array is due.

        The staging refuses any number there, as for a BATCH file; but the
        line would hold this one as its exact text, which the staging would
        report as a string. So it is refused here, with the staging's words.
        """
        what = "an object" if node.shape == "object" else "an array"
        return ValueError(
            f"stream {self._stream!r}, batch of records, row {self._rows + 1}, "
            f"column {node.label!r}: value '{format(value, 'f')[:_SHOWN]}' "
            f"(a number) can't be loaded as {what}, so the batch wasn't loaded."
        )

    def _not_utf8(self, record: dict) -> ValueError:
        """The error for a record holding a string UTF-8 can't encode."""
        for name, value in record.items():
            if not _encodes(value):
                return ValueError(
                    f"stream {self._stream!r}, property {name!r}: value "
                    f"{_shown(value)} holds a lone surrogate, which UTF-8 can't "
                    "encode, so the record can't be loaded."
                )
        return ValueError(
            f"stream {self._stream!r}: a record holds a lone surrogate, which "
            "UTF-8 can't encode, so it can't be loaded."
        )
