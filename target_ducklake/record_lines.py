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
  string (``JsonValues``), written as before (``flatten.dump_json_value``),
  so its numbers keep their digits; an object or array in a text or JSON
  column alike (text in a JSON column is JSON text).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING

from target_ducklake.duckdb_types import (
    DuckDBList,
    DuckDBScalar,
    DuckDBStruct,
    DuckDBTypeTree,
    parse_type,
)
from target_ducklake.flatten import dump_json_value

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

__all__ = ["JsonValues", "RecordLines", "datetime_text", "json_values"]

# Column types holding text, whose objects and arrays are their JSON text.
_TEXT_TYPES = frozenset({"VARCHAR", "TEXT", "STRING"})
_JSON = "JSON"


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


@dataclass(frozen=True)
class JsonValues:
    """Where a record's values are written as their JSON text.

    Attributes:
        whole: The value here is a JSON column's or field's: any value but
            None is written as its JSON text.
        containers: The value here is a text column's: an object or array is
            written as its JSON text.
        fields: The object's members that hold such values, by exact name.
        element: The array's elements, when they hold such values.
    """

    whole: bool = False
    containers: bool = False
    fields: Mapping[str, JsonValues] = field(default_factory=dict)
    element: JsonValues | None = None


def json_values(
    columns: Iterable[tuple[tuple[str, ...], str, bool]],
) -> dict[str, JsonValues]:
    """Where a stream's records hold JSON text, by property, from its columns.

    Args:
        columns: Each staged column's property path (``("o", "a")`` for a
            column flattened from object ``o``), its staged type, and
            whether its property is declared an object or array.

    Returns:
        The top-level properties that hold such values; empty when none do.
    """
    root: dict[str, JsonValues] = {}
    for path, column_type, container in columns:
        tree = parse_type(column_type)
        if isinstance(tree, DuckDBScalar):
            node = _scalar_values(tree, container=container)
        else:
            node = _column_values(tree)
        if node is not None:
            _place(root, path, node)
    return root


def _scalar_values(tree: DuckDBScalar, *, container: bool) -> JsonValues | None:
    """A top-level text or JSON column's ``JsonValues``; None for any other.

    A property declared an object or array is JSON text whatever its value
    (a string too); otherwise only an object or array is.
    """
    name = tree.name.upper().split("(")[0]
    if name not in _TEXT_TYPES and name != _JSON:
        return None
    return JsonValues(whole=True) if container else JsonValues(containers=True)


def _column_values(tree: DuckDBTypeTree) -> JsonValues | None:
    """A STRUCT or LIST column's (or field's) ``JsonValues``; None when it has none.

    Its JSON fields are JSON text; a text field holds what DuckDB writes.
    """
    if isinstance(tree, DuckDBStruct):
        fields = {
            name: child
            for name, part in tree.fields
            if (child := _column_values(part)) is not None
        }
        return JsonValues(fields=fields) if fields else None
    if isinstance(tree, DuckDBList):
        element = _column_values(tree.element)
        return None if element is None else JsonValues(element=element)
    return JsonValues(whole=True) if tree.name.upper() == _JSON else None


def _place(
    root: dict[str, JsonValues], path: tuple[str, ...], node: JsonValues
) -> None:
    """Put ``node`` at ``path`` in the tree of properties, merging objects."""
    head, *rest = path
    if not rest:
        root[head] = node
        return
    parent = root.get(head, JsonValues())
    fields = dict(parent.fields)
    _place(fields, tuple(rest), node)
    root[head] = JsonValues(fields=fields)


def _with_json_text(value: object, node: JsonValues) -> object:
    """``value`` with the JSON values ``node`` names written as their JSON text.

    Objects and arrays on the way are copied, never changed: a stream map's
    copies of a record share them.
    """
    if value is None:
        return None
    if node.whole or (node.containers and isinstance(value, (dict, list))):
        return dump_json_value(value)
    if node.fields and isinstance(value, dict):
        copied = dict(value)
        for name, child in node.fields.items():
            if name in copied:
                copied[name] = _with_json_text(copied[name], child)
        return copied
    if node.element is not None and isinstance(value, list):
        return [_with_json_text(item, node.element) for item in value]
    return value


class RecordLines:
    """A batch's records, appended as JSON lines to a file."""

    def __init__(self, path: str, json_paths: Mapping[str, JsonValues]) -> None:
        """Create (or empty) the file.

        Args:
            path: The file.
            json_paths: Where the records hold JSON values (``json_values``).
        """
        self.path = path
        self._json_paths = json_paths
        self._file = Path(path).open("w", encoding="utf-8")  # noqa: SIM115 (closed by close)

    def write(self, record: dict) -> None:
        """Append a record, changing only the top-level dict, as one line."""
        for name, node in self._json_paths.items():
            if name in record:
                record[name] = _with_json_text(record[name], node)
        self._file.write(_ENCODER.encode(record) + "\n")

    def close(self) -> None:
        """Flush and close the file."""
        self._file.close()
