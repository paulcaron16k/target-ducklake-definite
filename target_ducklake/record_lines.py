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
- a date-time as text DuckDB reads as the same instant: ``...000000Z`` in
  UTC, its own offset otherwise, none when it had none (read as UTC where
  its column needs an instant); a date and a time of day as ISO text;
- the value of a property declared an object or array that loads into a
  JSON column, or of a JSON field of a STRUCT column, as its JSON text in a
  string (``ValueRules``), written as before (``flatten.dump_json_value``),
  so its numbers keep their digits; an object or array in a text or JSON
  column or field alike (text in a JSON column is JSON text), and a float
  in one as Python's text for it;
- a Decimal for a BOOLEAN column as a JSON number, true unless it is 0;
- an object for a STRUCT with its keys renamed to the fields they match
  ignoring case, as the Arrow staging matched them (a BATCH file's keys
  match exactly).

The lines are held in memory and appended to the file a megabyte at a time,
so no file stays open between records.
"""

from __future__ import annotations

import json
import logging
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

__all__ = ["LineColumn", "RecordLines", "ValueRules", "datetime_text", "value_rules"]

# Column types holding text, whose objects and arrays are their JSON text.
_TEXT_TYPES = frozenset({"VARCHAR", "TEXT", "STRING"})
_JSON = "JSON"
_BOOLEAN = "BOOLEAN"


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
        numbers: A BOOLEAN column's value: a Decimal is written as a JSON
            number, which DuckDB's cast reads as true unless it is 0.
        fields: The object's members that need rules, by exact name.
        struct: For a STRUCT, its fields by lowercased name: a record's keys
            match them ignoring case, as the Arrow staging matched them.
        declared: A STRUCT's field names: an object whose keys are all among
            them needs no matching.
        texts_only: Whether every field with rules is a text field, so an
            object holding only plain scalars needs nothing done.
        element: The array's elements, when they need rules.
    """

    label: str = ""
    whole: bool = False
    text: bool = False
    numbers: bool = False
    fields: Mapping[str, ValueRules] = field(default_factory=dict)
    struct: Mapping[str, str] | None = None
    declared: frozenset[str] = frozenset()
    texts_only: bool = False
    element: ValueRules | None = None


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
        return ValueRules(label=column.name, numbers=True)
    if name not in _TEXT_TYPES and name != _JSON:
        return None
    if column.container:
        return ValueRules(label=column.name, whole=True)
    return ValueRules(label=column.name, text=True)


def _nested_rules(tree: DuckDBTypeTree, label: str) -> ValueRules | None:
    """A STRUCT's or LIST's (or a field's) rules; None for a field that needs none.

    A STRUCT always has rules, to match its keys ignoring case; a JSON field
    is JSON text, and a text field holds an object or array as JSON text.
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
        )
    if isinstance(tree, DuckDBList):
        element = _nested_rules(tree.element, f"{label}[]")
        return None if element is None else ValueRules(label=label, element=element)
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
    """A JSON, text or BOOLEAN column's (or field's) value, by its rules."""
    if node.whole:
        return dump_json_value(value)
    if node.text:
        if isinstance(value, (dict, list)):
            return dump_json_value(value)
        return repr(value) if isinstance(value, float) else value
    return float(value) if isinstance(value, Decimal) else value


# The types of a text field's values that it holds as they are.
_PLAIN = frozenset({str, int, bool, type(None)})


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
        self._rules = rules
        self._stream = stream
        self._strict = strict
        self._logger = logger or logging.getLogger(__name__)
        self._lines: list[str] = []
        self._size = 0
        Path(path).write_text("")

    def write(self, record: dict) -> None:
        """Append a record as one line; only its top-level dict is changed.

        Raises:
            ValueError: For a string UTF-8 can't encode (a lone surrogate),
                naming the stream and the property; or, under strict, for
                two keys of one object that name one STRUCT field.
        """
        for name, node in self._rules.items():
            if name in record:
                record[name] = self._prepared(record[name], node)
        line = _ENCODER.encode(record) + "\n"
        if not line.isascii() and not _encodes(line):
            raise self._not_utf8(record)
        self._lines.append(line)
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
        if node.whole or node.text or node.numbers:
            return _scalar(value, node)
        if isinstance(value, dict):
            return self._object(value, node)
        if node.element is not None and isinstance(value, list):
            if node.element.text and _PLAIN.issuperset(map(type, value)):
                return value
            items = [self._prepared(item, node.element) for item in value]
            changed = any(new is not old for new, old in zip(items, value, strict=True))
            return items if changed else value
        return value

    def _object(self, value: dict, node: ValueRules) -> dict:
        """An object's members by ``node``'s rules, its keys matched to a STRUCT's."""
        if node.struct is not None and not value.keys() <= node.declared:
            value = self._matched(value, node)
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
