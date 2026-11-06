"""Typing an all-string stream from a data dictionary: ``type_source`` (DUCKLKD-43).

A file or CSV tap sends every value as a string, so without help every column
lands as VARCHAR. ``type_source`` is the help: a per-stream dictionary of
column name to type, set in ``stream_defaults`` or ``stream_overrides.<stream>``
as in target-iceberg, so one config types a stream the same way in both
targets::

    stream_overrides:
      orders:
        type_source:
          id: bigint
          amount: money
          shipped: {type: date, x-parse-format: "%d/%m/%Y"}
        on_error: "null"

Nothing is typed without it: the target never guesses a type from values.
With it, each named column is declared its type (the table column is typed
accordingly) and each value is converted before the record is validated,
flattened and staged, so a typed column receives what a typed tap would have
sent. Columns it doesn't name are left alone.

A value that isn't its declared type is handled by ``on_error``: ``raise``
(the default) fails the run naming the stream, column and value; ``null``
stores NULL; ``string`` keeps the raw text, so every typed column is a text
column and the values that do convert are stored as their normalised text.
``null`` and ``string`` warn once per column and report the count when the
stream ends. A value is "its type" only if the column can hold it as it is:
in range, and, under strict ``value_type_checking``, without dropping a
fraction or a time of day.
"""

from __future__ import annotations

import contextlib
import copy
import datetime as dt
import re
import typing as t
from collections import Counter
from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Context, Decimal, InvalidOperation

from target_ducklake.flatten import dump_json_value
from target_ducklake.layout import LayoutError, stream_settings
from target_ducklake.parquet_utils import DECIMAL_DEFAULT_SCALE, DECIMAL_MAX_PRECISION

if t.TYPE_CHECKING:
    import logging
    from collections.abc import Mapping

__all__ = [
    "ON_ERROR_POLICIES",
    "TYPES",
    "Coercer",
    "CoercionError",
    "DeclaredType",
    "TypeSource",
    "check_type_source_config",
    "declared_schema",
    "resolve_type_source",
]

TYPE_SOURCE_KEY = "type_source"
ON_ERROR_KEY = "on_error"
# What a value that isn't its declared type becomes; the first is the default.
ON_ERROR_POLICIES = ("raise", "null", "string")
# The types a dictionary can declare, and the column each one gives: the JSON
# Schema a typed tap would declare, typed by the stream's usual mapping
# (BIGINT, DECIMAL(38, 9), BOOLEAN, DATE, TIME, and date_time_type).
_STRING = "string"
_SCHEMAS: dict[str, dict[str, t.Any]] = {
    _STRING: {"type": ["null", "string"]},
    "integer": {"type": ["null", "integer"]},
    "number": {"type": ["null", "number"]},
    "boolean": {"type": ["null", "boolean"]},
    "date": {"type": ["null", "string"], "format": "date"},
    "time": {"type": ["null", "string"], "format": "time"},
    "date-time": {"type": ["null", "string"], "format": "date-time"},
}
TYPES = tuple(_SCHEMAS)
# The other names target-iceberg reads for each type, compared case-insensitively:
# a dictionary is often copied from a source system's DDL.
_SYNONYMS = {
    _STRING: ("str", "text", "varchar", "json"),
    "integer": ("int", "int32", "int64", "bigint", "smallint"),
    "number": ("float", "double", "real", "numeric", "decimal", "money", "currency"),
    "boolean": ("bool",),
    "date-time": ("datetime", "timestamp", "timestamptz"),
}
_TYPE_NAMES = {
    name: kind for kind in TYPES for name in (kind, *_SYNONYMS.get(kind, ()))
}
# A dictionary entry is a type name, or an object with "type" and, for dates
# and times, an optional strptime pattern.
_PARSE_FORMAT_KEY = "x-parse-format"
# A number as text: an optional sign, an optional currency (a symbol or an
# ISO 4217 code, before or after the number), ASCII digits with thousands
# commas only in valid groups, a point, an exponent. Anything else, such as
# "(5)", "1,5", "12abc" or "NOT 5", isn't a number: target-iceberg drops every
# other character and would read those as 5, 15, 12 and 5.
_CURRENCY = r"[$€£¥₹]|[A-Z]{3}"
# ISO 4217's currency codes (2025), with the recently replaced ANG, HRK, SLL
# and ZWL; without the fund, precious-metal and testing codes, which aren't
# money a source states a price in.
_CURRENCY_CODES = frozenset(
    """
    AED AFN ALL AMD ANG AOA ARS AUD AWG AZN BAM BBD BDT BGN BHD BIF BMD BND BOB
    BRL BSD BTN BWP BYN BZD CAD CDF CHF CLP CNY COP CRC CUC CUP CVE CZK DJF DKK
    DOP DZD EGP ERN ETB EUR FJD FKP GBP GEL GHS GIP GMD GNF GTQ GYD HKD HNL HRK
    HTG HUF IDR ILS INR IQD IRR ISK JMD JOD JPY KES KGS KHR KMF KPW KRW KWD KYD
    KZT LAK LBP LKR LRD LSL LYD MAD MDL MGA MKD MMK MNT MOP MRU MUR MVR MWK MXN
    MYR MZN NAD NGN NIO NOK NPR NZD OMR PAB PEN PGK PHP PKR PLN PYG QAR RON RSD
    RUB RWF SAR SBD SCR SDG SEK SGD SHP SLE SLL SOS SRD SSP STN SVC SYP SZL THB
    TJS TMT TND TOP TRY TTD TWD TZS UAH UGX USD UYU UZS VED VES VND VUV WST XAF
    XCD XCG XOF XPF YER ZAR ZMW ZWG ZWL
    """.split()  # noqa: SIM905 (a table reads better than 160 list items)
)
_NUMBER = re.compile(
    rf"""\s*
    (?P<sign>[+-])?
    (?:(?P<before>{_CURRENCY})\s*)?
    (?P<sign_after_currency>[+-])?
    (?P<digits>\d{{1,3}}(?:,\d{{3}})+(?:\.\d*)?|\d+(?:\.\d*)?|\.\d+)
    (?P<exponent>[eE][+-]?\d+)?
    (?:\s*(?P<after>{_CURRENCY}))?
    \s*""",
    re.VERBOSE | re.ASCII,
)
# The columns a declared integer and number get: BIGINT and DECIMAL(38, 9).
_INT64 = range(-(2**63), 2**63)
_INT64_DIGITS = 19  # a number with more integer digits is out of range
_DECIMAL_INTEGER_DIGITS = DECIMAL_MAX_PRECISION - DECIMAL_DEFAULT_SCALE
_DECIMAL_QUANTUM = Decimal(1).scaleb(-DECIMAL_DEFAULT_SCALE)
# Rounds half away from zero, as DuckDB's cast to DECIMAL does.
_DECIMAL_CONTEXT = Context(prec=DECIMAL_MAX_PRECISION, rounding=ROUND_HALF_UP)
# ISO 8601 date forms fromisoformat reads (extended, basic, week), and what
# follows one: nothing, or T or a space before a time. Python accepts any one
# character there, so "2024-03-01x10:00" would read as a date-time.
_ISO_DATE = re.compile(
    r"(\d{4}-\d{2}-\d{2}|\d{8}|\d{4}-W\d{2}-\d|\d{4}W\d{3})(?P<rest>.*)",
    re.ASCII | re.DOTALL,
)
# A week without its day: fromisoformat reads it as the Monday.
_ISO_WEEK_ONLY = re.compile(r"\d{4}-?W\d{2}", re.ASCII)
# A time's seconds fraction: DuckDB keeps microseconds (6 digits).
_SECONDS_FRACTION = re.compile(r":\d{2}[.,](?P<digits>\d+)", re.ASCII)
_MICROSECOND_DIGITS = 6
_TRUE_WORDS = frozenset({"true", "t", "yes", "y", "1"})
_FALSE_WORDS = frozenset({"false", "f", "no", "n", "0"})


class CoercionError(ValueError):
    """A value isn't its declared type, and the stream's on_error is raise."""


@dataclass(frozen=True)
class DeclaredType:
    """A column's declared type (one of ``TYPES``) and its strptime pattern."""

    kind: str
    parse_format: str | None = None


@dataclass(frozen=True)
class TypeSource:
    """A stream's typed columns and its ``on_error`` policy."""

    columns: Mapping[str, DeclaredType]
    on_error: str


def resolve_type_source(
    config: Mapping[str, t.Any], stream_name: str
) -> TypeSource | None:
    """The stream's ``type_source`` and ``on_error``, or None when nothing is typed.

    Each from ``stream_overrides.<stream>``, else ``stream_defaults``. An
    override's ``type_source`` replaces the defaults' whole, as in
    target-iceberg. A type name this target doesn't know is read as
    ``string``, as target-iceberg reads it.

    Raises:
        LayoutError: If either setting is malformed (see
            ``check_type_source_config``).
    """
    where = f"stream {stream_name!r}" if stream_name else "stream_defaults"
    settings = stream_settings(config, stream_name, where)
    on_error = _on_error(settings.get(ON_ERROR_KEY), where)
    columns, _ = _declared_types(settings.get(TYPE_SOURCE_KEY), where)
    return TypeSource(columns, on_error) if columns else None


def check_type_source_config(
    config: Mapping[str, t.Any], logger: logging.Logger | None = None
) -> None:
    """Check every level's ``type_source`` and ``on_error`` at startup.

    A type name this target doesn't know is read as ``string``, as in
    target-iceberg; it is reported once, where it is set, so a typo doesn't
    go unnoticed. Run after ``check_layout_config``, which has checked
    the shape of ``stream_overrides``.

    Raises:
        LayoutError: If ``type_source`` isn't a mapping of column names to
            type names (or ``{type, x-parse-format}`` objects), or
            ``on_error`` isn't one of ``ON_ERROR_POLICIES``; the message names
            the stream and the value.
    """
    overrides = config.get("stream_overrides") or {}
    for stream_name in ["", *overrides]:
        where = f"stream {stream_name!r}" if stream_name else "stream_defaults"
        resolve_type_source(config, stream_name)
        own = overrides[stream_name] if stream_name else config.get("stream_defaults")
        _, unknown = _declared_types((own or {}).get(TYPE_SOURCE_KEY), where)
        if logger is None:
            continue
        for column, name in unknown.items():
            logger.warning(
                "%s: type_source column %r has type %r, which target-ducklake "
                "doesn't know, so the column is loaded as string, as "
                "target-iceberg loads it. Known type names: %s.",
                where,
                column,
                name,
                ", ".join(sorted(_TYPE_NAMES)),
            )


def _on_error(value: object, where: str) -> str:
    if value is None:
        return ON_ERROR_POLICIES[0]
    if value not in ON_ERROR_POLICIES:
        msg = (
            f"{where}: on_error must be one of {list(ON_ERROR_POLICIES)}, "
            f"not {value!r}."
        )
        raise LayoutError(msg)
    return t.cast("str", value)


def _declared_types(
    value: object, where: str
) -> tuple[dict[str, DeclaredType], dict[str, str]]:
    """A ``type_source`` read: each column's type, and the unknown type names.

    A column with an unknown type is declared ``string``.
    """
    if value is None:
        return {}, {}
    if not isinstance(value, dict):
        msg = (
            f"{where}: type_source must map column names to types, e.g. "
            f"{{id: integer}}, not {value!r}."
        )
        raise LayoutError(msg)
    declared: dict[str, DeclaredType] = {}
    unknown: dict[str, str] = {}
    for column, entry in value.items():
        at = f"{where}: type_source column {column!r}"
        if not column:
            msg = f"{where}: type_source has an empty column name."
            raise LayoutError(msg)
        spec = entry if isinstance(entry, dict) else {"type": entry}
        name, parse_format = spec.get("type", _STRING), spec.get(_PARSE_FORMAT_KEY)
        if not isinstance(name, str) or not name.strip():
            msg = f"{at}: the type must be a name such as integer, not {name!r}."
            raise LayoutError(msg)
        if parse_format is not None and not isinstance(parse_format, str):
            msg = (
                f"{at}: {_PARSE_FORMAT_KEY} must be a strptime pattern, "
                f"not {parse_format!r}."
            )
            raise LayoutError(msg)
        kind = _TYPE_NAMES.get(name.strip().lower())
        if kind is None:
            unknown[column] = name
            kind = _STRING
        declared[column] = DeclaredType(kind, parse_format)
    return declared, unknown


def declared_schema(
    schema: dict[str, t.Any], type_source: TypeSource
) -> dict[str, t.Any]:
    """The stream's schema with each typed column declared its type.

    Under ``on_error: string`` every typed column is declared a string,
    whatever the tap declares, so the raw text of a value that isn't its
    type always fits. A copy: the tap's own schema is what the SDK compares a
    later SCHEMA message with. Columns the stream doesn't have are left out.
    """
    typed = dict(schema)
    properties = dict(schema.get("properties") or {})
    for column, declared in type_source.columns.items():
        if column in properties:
            kind = _STRING if type_source.on_error == "string" else declared.kind
            properties[column] = copy.deepcopy(_SCHEMAS[kind])
    typed["properties"] = properties
    return typed


class _NotThatTypeError(ValueError):
    """A value that isn't its declared type, and why."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class _LossyError(_NotThatTypeError):
    """A value that converts only by dropping part of it: a fraction, a time of day.

    A type in its own right under permissive ``value_type_checking``, converted
    as target-iceberg converts it; not one under strict.
    """

    def __init__(self, reason: str, converted: object, outcome: str) -> None:
        super().__init__(reason)
        self.converted = converted
        self.outcome = outcome


_BLANK_KEY = _NotThatTypeError("it is blank, and a key can't be NULL")


class Coercer:
    """Converts one stream's typed columns, as its ``on_error`` says.

    ``coerce`` changes a record in place, before the SDK validates it. Values
    a ``null`` or ``string`` policy changes, and values converted by dropping
    part of them, are warned about once per column and counted; ``report``
    logs the counts.
    """

    def __init__(
        self,
        stream_name: str,
        type_source: TypeSource,
        *,
        strict: bool,
        logger: logging.Logger,
        key_columns: frozenset[str] = frozenset(),
    ) -> None:
        """Set up for a stream.

        Args:
            stream_name: The stream, named in errors and warnings.
            type_source: Its typed columns and on_error.
            strict: Whether ``value_type_checking`` is strict, under which a
                value converted only by dropping part of it (``7.5`` for an
                integer, a time of day for a date, a tenth fraction digit for
                a number) is not its type.
            logger: Where warnings and counts go.
            key_columns: The stream's key properties, lowercased. A blank
                value there isn't its type: it would be a NULL key.
        """
        self.stream_name = stream_name
        self.type_source = type_source
        self.strict = strict
        self.logger = logger
        self.key_columns = key_columns
        self._counts: Counter[tuple[str, str]] = Counter()
        # Columns whose values are stored as text: all under on_error string.
        self._text_columns: set[str] = (
            set(type_source.columns) if self.keeps_text else set()
        )

    @property
    def keeps_text(self) -> bool:
        """Whether every typed column is a text column (on_error string)."""
        return self.type_source.on_error == "string"

    def store_as_text(self, column: str) -> None:
        """Store a column's converted values as text, for a VARCHAR column."""
        self._text_columns.add(column)

    def coerce(self, record: dict[str, t.Any]) -> None:
        """Convert the record's typed columns in place.

        An empty or blank value is NULL: a CSV's empty cell is missing data.
        In a key column it isn't its type, as a NULL key can't be merged.
        A ``string`` column's values are kept, as text.

        Raises:
            CoercionError: For a value that isn't its type, under ``raise``.
        """
        for column, declared in self.type_source.columns.items():
            value = record.get(column)
            if value is None:
                continue
            if declared.kind == _STRING:
                record[column] = _as_text(value)
                continue
            if isinstance(value, str) and not value.strip():
                record[column] = (
                    self._not_that_type(column, declared, value, _BLANK_KEY)
                    if column.lower() in self.key_columns
                    else None
                )
                continue
            try:
                converted = _convert(declared, value)
            except _LossyError as error:
                if column.lower() in self.key_columns:
                    # Whatever value_type_checking says: 7.5 and 7.2 would
                    # both be key 7, and merge into one row.
                    reason = (
                        f"{error.reason}, and a key can't drop part of its value, "
                        "as distinct keys would merge"
                    )
                    record[column] = self._not_that_type(
                        column, declared, value, _NotThatTypeError(reason)
                    )
                    continue
                if self.strict:
                    record[column] = self._not_that_type(column, declared, value, error)
                    continue
                converted = error.converted
                self._note(
                    column,
                    value,
                    error.reason,
                    f"{error.outcome}, as value_type_checking is permissive",
                )
            except _NotThatTypeError as error:
                record[column] = self._not_that_type(column, declared, value, error)
                continue
            text = column in self._text_columns
            record[column] = _as_text(converted) if text else converted

    def _not_that_type(
        self,
        column: str,
        declared: DeclaredType,
        value: object,
        error: _NotThatTypeError,
    ) -> object:
        """What a value that isn't its type becomes: NULL, its text, or an error.

        A key column's value is never NULL: NULL keys match each other, so
        distinct records would merge into one row, and a delete would remove
        unrelated rows. Under ``null`` it fails as under ``raise``.
        """
        policy = self.type_source.on_error
        is_key = column.lower() in self.key_columns
        if policy == "null" and not is_key:
            self._note(column, value, error.reason, "stored NULL (on_error null)")
            return None
        if policy == "string":
            self._note(column, value, error.reason, "stored as sent (on_error string)")
            return value
        if policy == "null":
            fix = (
                "on_error null doesn't apply to a key column, as a NULL key would "
                "merge or delete other records' rows; set on_error to string to "
                "keep the text, or correct the value or the type_source entry."
            )
        else:
            fix = (
                "Correct the value at the source or the type_source entry, or set "
                "on_error to null or string for this stream."
            )
        msg = (
            f"stream {self.stream_name!r}: column {column!r} is declared "
            f"{declared.kind} in type_source, but a record has {value!r}: "
            f"{error.reason}. {fix}"
        )
        raise CoercionError(msg) from error

    def _note(self, column: str, value: object, reason: str, outcome: str) -> None:
        """Warn about the first value of a column with an outcome; count them all."""
        key = (column, outcome)
        if not self._counts[key]:
            self.logger.warning(
                "Stream %r column %r: %r is not its type_source type (%s); %s. "
                "Further such values are counted and reported when the stream ends.",
                self.stream_name,
                column,
                value,
                reason,
                outcome,
            )
        self._counts[key] += 1

    def report(self) -> None:
        """Log how many values in each column were changed, then start over."""
        for (column, outcome), count in sorted(self._counts.items()):
            self.logger.warning(
                "Stream %r column %r: %d value(s) not of the type_source type: %s.",
                self.stream_name,
                column,
                count,
                outcome,
            )
        self._counts.clear()


def _convert(declared: DeclaredType, value: object) -> object:
    """The value as its declared type, in the form a typed tap would send it.

    An integer is an int, a number a Decimal and a boolean a bool; a date,
    time or date-time is ISO text, which the SDK then parses as it parses a
    typed tap's.

    Raises:
        _NotThatTypeError: If the value isn't the type, or its column (BIGINT,
            DECIMAL(38, 9)) can't hold it.
        _LossyError: If it converts only by dropping part of it.
    """
    if isinstance(value, (dict, list)):
        msg = f"an {type(value).__name__} is not a {declared.kind}"
        raise _NotThatTypeError(msg)
    if declared.kind == "integer":
        return _integer(_number(value))
    if declared.kind == "number":
        return _decimal(_number(value))
    if declared.kind == "boolean":
        return _boolean(value)
    return _temporal(declared, str(value).strip())


def _number(value: object) -> Decimal:
    """A number read by ``_NUMBER``: what remains is exactly the value."""
    if isinstance(value, bool):
        msg = "a boolean is not a number"
        raise _NotThatTypeError(msg)
    match = _NUMBER.fullmatch(str(value))
    if (
        match is None
        or (match["sign"] and match["sign_after_currency"])
        or (match["before"] and match["after"])
        or not _is_currency(match["before"] or match["after"])
    ):
        msg = (
            "it is not a number (digits with an optional sign, point, exponent, "
            "thousands commas in groups of three, and one currency symbol or "
            "ISO 4217 code)"
        )
        raise _NotThatTypeError(msg)
    sign = match["sign"] or match["sign_after_currency"] or ""
    digits = match["digits"].replace(",", "")
    return Decimal(f"{sign}{digits}{match['exponent'] or ''}")


def _is_currency(text: str | None) -> bool:
    """Whether a number's currency is none, a symbol, or an ISO 4217 code."""
    return text is None or len(text) == 1 or text in _CURRENCY_CODES


def _integer(number: Decimal) -> int:
    """A number as a BIGINT; a fraction is dropped toward zero, as target-iceberg."""
    # Checked on the exponent before int() expands it: 1e100000 would take
    # minutes to expand, for a value no BIGINT holds.
    if number and number.adjusted() >= _INT64_DIGITS:
        msg = "it is out of range for a 64-bit integer"
        raise _NotThatTypeError(msg)
    whole = int(number)
    if whole not in _INT64:
        msg = "it is out of range for a 64-bit integer"
        raise _NotThatTypeError(msg)
    if whole != number:
        msg = "it has a fraction, which an integer can't hold"
        raise _LossyError(msg, whole, "dropped the fraction")
    return whole


def _decimal(number: Decimal) -> Decimal:
    """A number as DECIMAL(38, 9) holds it; extra fraction digits are rounded."""
    too_large = f"it has more than {_DECIMAL_INTEGER_DIGITS} integer digits"
    if number and number.adjusted() >= _DECIMAL_INTEGER_DIGITS:
        raise _NotThatTypeError(too_large)
    try:
        # More than 38 digits once rounded: it rounded up to 30 integer digits.
        rounded = number.quantize(_DECIMAL_QUANTUM, context=_DECIMAL_CONTEXT)
    except InvalidOperation:
        raise _NotThatTypeError(too_large) from None
    if rounded != number:
        msg = (
            f"it has more than {DECIMAL_DEFAULT_SCALE} fraction digits, which "
            f"DECIMAL({DECIMAL_MAX_PRECISION}, {DECIMAL_DEFAULT_SCALE}) can't hold"
        )
        raise _LossyError(
            msg, rounded, f"rounded to {DECIMAL_DEFAULT_SCALE} fraction digits"
        )
    return number


def _boolean(value: object) -> bool:
    if isinstance(value, bool):
        return value
    word = str(value).strip().lower()
    if word in _TRUE_WORDS:
        return True
    if word in _FALSE_WORDS:
        return False
    msg = (
        f"it is not one of {sorted(_TRUE_WORDS)} or {sorted(_FALSE_WORDS)} "
        "(in any case)"
    )
    raise _NotThatTypeError(msg)


def _temporal(declared: DeclaredType, text: str) -> str:
    """A date, time or date-time as ISO text.

    Read with ``x-parse-format`` when given, else as ISO 8601, which
    ``datetime.fromisoformat`` reads: extended (``2024-03-01``,
    ``2024-03-01 10:00``, ``...T10:00:00.5+02:00``, ``...Z``), basic
    (``20240301``, ``20240301T101500``) and week dates (``2024-W09-5``),
    all unambiguous; a week without its day isn't a date, and only T or a
    space separates a date from its time (``_check_iso_form``). A time
    without a pattern must have colons. It is read alone (``09:30``) or as a
    date-time's time of day; a date alone isn't a time. Digits are ASCII.
    More than 6 fraction digits of a second are lossy (``_to_microseconds``).
    """
    pattern = declared.parse_format
    if not text.isascii():
        msg = f"it is not a {declared.kind}: it has characters other than ASCII"
        raise _NotThatTypeError(msg)
    if pattern is None:
        _check_iso_form(declared.kind, text)
        text, truncated = _to_microseconds(text)
    else:
        truncated = False
    result = _parsed_temporal(declared, text)
    if truncated and declared.kind != "date":
        msg = (
            f"it has more than {_MICROSECOND_DIGITS} fraction digits of a "
            "second, which a time can't hold"
        )
        raise _LossyError(msg, result, "truncated to microseconds, as DuckDB does")
    return result


def _parsed_temporal(declared: DeclaredType, text: str) -> str:
    """The text read as its date, time or date-time, as ISO text (see _temporal)."""
    pattern = declared.parse_format
    if declared.kind == "time" and pattern is None:
        with contextlib.suppress(ValueError):
            return dt.time.fromisoformat(text).isoformat()
    try:
        parsed = (
            dt.datetime.strptime(text, pattern)  # noqa: DTZ007 (a zone is the text's)
            if pattern
            else dt.datetime.fromisoformat(text)
        )
    except ValueError as error:
        expected = f"the pattern {pattern!r}" if pattern else "ISO 8601"
        msg = f"it is not a {declared.kind} in {expected} ({error})"
        raise _NotThatTypeError(msg) from None
    if declared.kind == "date-time":
        return parsed.isoformat()
    if declared.kind == "time":
        return parsed.timetz().isoformat()
    date = parsed.date().isoformat()
    if parsed.timetz().replace(tzinfo=None) != dt.time():
        msg = "it has a time of day, which a date can't hold"
        raise _LossyError(msg, date, "dropped the time of day")
    return date


def _check_iso_form(kind: str, text: str) -> None:
    """Refuse the ISO 8601 forms fromisoformat reads too leniently.

    A time needs colons: in basic format "20240301" would read as
    20:24:03.01. A week needs its day: "2024-W09" would read as its Monday.
    Only T or a space may separate a date from its time.
    """
    if kind == "time" and ":" not in text:
        msg = "it is not a time written with colons, e.g. 09:30"
        raise _NotThatTypeError(msg)
    if _ISO_WEEK_ONLY.fullmatch(text):
        msg = f"it is not a {kind}: a week without its day isn't a day"
        raise _NotThatTypeError(msg)
    match = _ISO_DATE.match(text)
    if match and match["rest"] and match["rest"][0] not in "T ":
        msg = f"it is not a {kind}: only T or a space may separate a date from its time"
        raise _NotThatTypeError(msg)


def _to_microseconds(text: str) -> tuple[str, bool]:
    """The text with its seconds fraction cut to microseconds, as DuckDB cuts it.

    Returns:
        The text, and whether a non-zero digit was cut.
    """
    match = _SECONDS_FRACTION.search(text)
    if match is None or len(match["digits"]) <= _MICROSECOND_DIGITS:
        return text, False
    start, end = match.span("digits")
    kept, cut = (
        match["digits"][:_MICROSECOND_DIGITS],
        match["digits"][_MICROSECOND_DIGITS:],
    )
    return text[:start] + kept + text[end:], bool(cut.strip("0"))


def _as_text(value: object) -> object:
    """A value as the text a VARCHAR column stores, one form per column.

    Text is kept; a boolean is ``true``/``false``; a number is written out in
    full (no exponent); an object or array is JSON.
    """
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, (dict, list)):
        return dump_json_value(value)
    return str(value)
