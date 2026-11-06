"""type_source: typing an all-string stream from a data dictionary (DUCKLKD-43)."""

from __future__ import annotations

import io
import json
import logging
import time
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import pytest

from target_ducklake.layout import LayoutError
from target_ducklake.target import Targetducklake
from target_ducklake.type_source import (
    Coercer,
    CoercionError,
    DeclaredType,
    TypeSource,
    check_type_source_config,
    declared_schema,
    resolve_type_source,
)

if TYPE_CHECKING:
    from pathlib import Path

    from tests.catalogs import Lake

# A CSV tap's stream: every property is a string.
COLUMNS = ("id", "qty", "price", "paid", "day", "at", "opens", "note")
PROPERTIES = {name: {"type": ["null", "string"]} for name in COLUMNS}
DICTIONARY = {
    "id": "BIGINT",
    "qty": "int",
    "price": "money",
    "paid": "bool",
    "day": {"type": "date", "x-parse-format": "%d/%m/%Y", "description": "x"},
    "at": "timestamptz",
    "opens": "time",
}
ROW = {
    "id": "1",
    "qty": " 1,200 ",
    "price": "$1,234.50",
    "paid": "Yes",
    "day": "04/03/2026",
    "at": "2026-03-04T10:15:00+02:00",
    "opens": "09:30",
    "note": "0042",
}


def _coercer(
    columns: dict[str, Any],
    on_error: str = "raise",
    *,
    strict: bool = False,
    logger: logging.Logger | None = None,
    keys: frozenset[str] = frozenset(),
) -> Coercer:
    source = resolve_type_source(
        {"stream_defaults": {"type_source": columns, "on_error": on_error}}, "t"
    )
    assert source is not None
    return Coercer(
        "t",
        source,
        strict=strict,
        logger=logger or logging.getLogger("test"),
        key_columns=keys,
    )


def _coerced(columns: dict[str, Any], record: dict, **kwargs: Any) -> dict:
    record = dict(record)
    _coercer(columns, **kwargs).coerce(record)
    return record


class TestSettings:
    """Where type_source and on_error are read from, and what is refused."""

    def test_names_are_read_as_target_iceberg_reads_them(self):
        """SQL type names and target-iceberg's spellings name the same types."""
        source = resolve_type_source(
            {"stream_defaults": {"type_source": DICTIONARY}}, "t"
        )
        assert source is not None
        assert {name: d.kind for name, d in source.columns.items()} == {
            "id": "integer",
            "qty": "integer",
            "price": "number",
            "paid": "boolean",
            "day": "date",
            "at": "date-time",
            "opens": "time",
        }
        assert source.columns["day"].parse_format == "%d/%m/%Y"
        assert source.on_error == "raise"

    def test_an_unknown_type_is_read_as_string_with_a_warning(
        self, caplog: pytest.LogCaptureFixture
    ):
        """As target-iceberg reads it; the warning catches a typo."""
        config = {
            "stream_overrides": {"t": {"type_source": {"a": "TEXT", "b": "uuid"}}}
        }
        source = resolve_type_source(config, "t")
        assert source is not None
        assert {name: d.kind for name, d in source.columns.items()} == {
            "a": "string",
            "b": "string",
        }
        with caplog.at_level(logging.WARNING):
            check_type_source_config(config, logging.getLogger("test"))
        assert "stream 't': type_source declares {'b': 'uuid'}" in caplog.text
        assert "loaded as string" in caplog.text

    def test_an_override_replaces_the_defaults_dictionary_whole(self):
        """Merged key by key, as every per-stream setting is."""
        config = {
            "stream_defaults": {"type_source": {"a": "integer"}, "on_error": "null"},
            "stream_overrides": {"t": {"type_source": {"b": "date"}}},
        }
        source = resolve_type_source(config, "t")
        assert source == TypeSource({"b": DeclaredType("date")}, "null")
        other = resolve_type_source(config, "other")
        assert other == TypeSource({"a": DeclaredType("integer")}, "null")

    def test_on_error_without_type_source_is_accepted(self):
        """target-iceberg's config schema puts on_error: raise in stream_defaults."""
        base = {"catalog_url": "x", "data_path": "/x", "storage_type": "local"}
        Targetducklake(config={**base, "stream_defaults": {"on_error": "raise"}})

    @pytest.mark.parametrize(
        ("layout", "message"),
        [
            ({"type_source": ["id"]}, "type_source must map column names to types"),
            ({"type_source": {"id": 5}}, "column 'id': the type must be a name"),
            ({"type_source": {"id": {"type": ""}}}, "the type must be a name"),
            (
                {"type_source": {"d": {"type": "date", "x-parse-format": 1}}},
                "x-parse-format must be a strptime pattern",
            ),
            ({"type_source": {"": "integer"}}, "empty column name"),
            ({"on_error": "skip"}, "on_error must be one of"),
        ],
    )
    def test_a_malformed_setting_is_refused_naming_the_stream(
        self, layout: dict, message: str
    ):
        """Each mistake names the stream and the value."""
        config = {"stream_overrides": {"orders": layout}}
        with pytest.raises(LayoutError, match=message) as error:
            check_type_source_config(config)
        assert "stream 'orders'" in str(error.value)

    def test_the_target_checks_at_startup(self):
        """A bad on_error fails the target before any record arrives."""
        base = {"catalog_url": "x", "data_path": "/x", "storage_type": "local"}
        config = {**base, "stream_defaults": {"type_source": {"id": "integer"}}}
        Targetducklake(config=config)
        config["stream_overrides"] = {"orders": {"on_error": "ignore"}}
        with pytest.raises(LayoutError, match="stream 'orders': on_error .* 'ignore'"):
            Targetducklake(config=config)

    def test_the_schema_declares_the_typed_columns(self):
        """Typed columns get a typed tap's declaration; the rest keep theirs."""
        source = resolve_type_source(
            {"stream_defaults": {"type_source": DICTIONARY}}, "t"
        )
        assert source is not None
        schema = {"type": "object", "properties": PROPERTIES}
        typed = declared_schema(schema, source)["properties"]
        assert typed["id"] == {"type": ["null", "integer"]}
        assert typed["price"] == {"type": ["null", "number"]}
        assert typed["at"] == {"type": ["null", "string"], "format": "date-time"}
        assert typed["note"] == PROPERTIES["note"]
        assert schema["properties"] is PROPERTIES  # the tap's is left alone

    def test_a_string_entry_overrides_the_taps_type(self):
        """As in target-iceberg: the dictionary is authoritative for its columns."""
        source = resolve_type_source(
            {"stream_defaults": {"type_source": {"n": "varchar"}}}, "t"
        )
        assert source is not None
        schema = {"properties": {"n": {"type": "integer"}}}
        assert declared_schema(schema, source)["properties"]["n"] == {
            "type": ["null", "string"]
        }

    def test_on_error_string_declares_every_typed_column_text(self):
        """So the raw text of a value that isn't its type always fits."""
        source = resolve_type_source(
            {"stream_defaults": {"type_source": DICTIONARY, "on_error": "string"}},
            "t",
        )
        assert source is not None
        typed = {"properties": {"id": {"type": "integer"}, "at": {"type": "string"}}}
        properties = declared_schema(typed, source)["properties"]
        assert properties == {
            "id": {"type": ["null", "string"]},
            "at": {"type": ["null", "string"]},
        }


class TestValues:
    """How each type reads its text."""

    def test_a_row_of_text_becomes_typed_values(self):
        """Each value takes the form a typed tap would send."""
        assert _coerced(DICTIONARY, ROW) == {
            "id": 1,
            "qty": 1200,
            "price": Decimal("1234.50"),
            "paid": True,
            "day": "2026-03-04",
            "at": "2026-03-04T10:15:00+02:00",
            "opens": "09:30:00",
            "note": "0042",
        }

    @pytest.mark.parametrize(
        ("kind", "text", "value"),
        [
            ("integer", "-12", -12),
            ("integer", "1e3", 1000),
            ("integer", "9223372036854775807", 9223372036854775807),
            ("number", "€ 2.5e-3", Decimal("2.5e-3")),
            ("number", "0.1234567890", Decimal("0.1234567890")),  # trailing 0
            ("number", "-0.10", Decimal("-0.10")),
            ("boolean", "F", False),
            ("boolean", "no", False),
            ("boolean", "1", True),
            ("date", "2026-12-31", "2026-12-31"),
            ("date", "2026-12-31T00:00:00", "2026-12-31"),
            ("date-time", "2026-01-02 03:04:05Z", "2026-01-02T03:04:05+00:00"),
            ("date-time", "2026-01-02", "2026-01-02T00:00:00"),
            ("time", "2026-01-02T23:59:58.5", "23:59:58.500000"),
            ("time", "07:00+01:00", "07:00:00+01:00"),
            ("string", "  kept as sent ", "  kept as sent "),
            ("string", 5, "5"),
            ("string", Decimal("1E+3"), "1000"),
            ("string", True, "true"),
            ("string", {"k": [1]}, '{"k": [1]}'),
        ],
    )
    def test_each_type(self, kind: str, text: object, value: object):
        """Each type reads the text forms it accepts."""
        assert _coerced({"c": kind}, {"c": text}) == {"c": value}

    @pytest.mark.parametrize("text", ["", "   ", None])
    def test_an_empty_cell_is_null(self, text: str | None):
        """An empty cell is missing data, not a zero or false."""
        assert _coerced({"c": "integer"}, {"c": text}) == {"c": None}

    def test_a_missing_column_stays_missing(self):
        """A record without the column isn't given one."""
        assert _coerced({"c": "integer"}, {"other": "x"}) == {"other": "x"}

    def test_a_parse_format_reads_a_day_first_date(self):
        """x-parse-format reads what ISO 8601 can't."""
        column = {"c": {"type": "date-time", "x-parse-format": "%d.%m.%Y %H:%M"}}
        assert _coerced(column, {"c": "01.02.2026 13:45"}) == {
            "c": "2026-02-01T13:45:00"
        }

    @pytest.mark.parametrize(
        ("kind", "text", "reason"),
        [
            ("integer", "N/A", "is not a number"),
            ("integer", "1.2.3", "is not a number"),
            ("number", "--", "is not a number"),
            ("boolean", "maybe", "is not one of"),
            ("date", "31/12/2026", "is not a date in ISO 8601"),
            ("time", "25:00", "is not a time in ISO 8601"),
            ("time", "2024-03-01", "is a date without a time of day"),
            ("integer", "9223372036854775808", "out of range for a 64-bit integer"),
            ("integer", "-9.3e18", "out of range for a 64-bit integer"),
            ("number", "1e29", "more than 29 integer digits"),
            ("number", "99999999999999999999999999999.9999999999", "29 integer"),
            ("date-time", "yesterday", "is not a date-time in ISO 8601"),
        ],
    )
    def test_a_value_that_is_not_its_type_fails_naming_it(
        self, kind: str, text: str, reason: str
    ):
        """The default on_error fails on the first such value."""
        with pytest.raises(CoercionError) as error:
            _coerced({"c": kind}, {"c": text})
        message = str(error.value)
        assert f"stream 't': column 'c' is declared {kind}" in message
        assert repr(text) in message
        assert reason in message
        assert "on_error" in message

    def test_a_fraction_is_dropped_with_a_warning_when_permissive(
        self, caplog: pytest.LogCaptureFixture
    ):
        """As target-iceberg, toward zero; warned once per column and counted."""
        coercer = _coercer({"c": "integer", "d": "date"})
        records: list[dict[str, Any]] = [
            {"c": "7.9", "d": "2026-01-02T10:00"},
            {"c": "-7.9", "d": None},
        ]
        with caplog.at_level(logging.WARNING):
            for record in records:
                coercer.coerce(record)
            coercer.report()
        assert records == [{"c": 7, "d": "2026-01-02"}, {"c": -7, "d": None}]
        first_and_count = 2 * 2  # each column: its first value, then its count
        assert caplog.text.count("as value_type_checking is permissive") == (
            first_and_count
        )
        assert "column 'c': 2 value(s)" in caplog.text
        assert "column 'd': 1 value(s)" in caplog.text

    @pytest.mark.parametrize(
        ("kind", "text"),
        [
            ("integer", "7.5"),
            ("date", "2026-01-02 10:00"),
            ("number", "0.1234567891234"),
        ],
    )
    def test_strict_value_type_checking_refuses_a_lossy_value(
        self, kind: str, text: str
    ):
        """Under strict, a value that loses a part isn't its type."""
        with pytest.raises(CoercionError, match="can't hold"):
            _coerced({"c": kind}, {"c": text}, strict=True)
        assert _coerced({"c": kind}, {"c": text}, strict=True, on_error="null") == {
            "c": None
        }

    def test_on_error_null_warns_once_per_column_and_counts(
        self, caplog: pytest.LogCaptureFixture
    ):
        """Not one line per row: the first, then a count."""
        coercer = _coercer({"c": "integer"}, "null")
        records = [{"c": "x"}, {"c": "2"}, {"c": "y"}, {"c": "z"}]
        with caplog.at_level(logging.WARNING):
            for record in records:
                coercer.coerce(record)
            assert caplog.text.count("stored NULL") == 1
            coercer.report()
        assert [r["c"] for r in records] == [None, 2, None, None]
        assert "column 'c': 3 value(s) not of the type_source type: stored NULL" in (
            caplog.text
        )

    def test_on_error_string_keeps_text(self):
        """Values that convert become normalised text; the others stay as sent."""
        record = _coerced(
            {"n": "integer", "m": "number", "b": "boolean"},
            {"n": "1,000", "m": "1e2", "b": "Y", "x": "1"},
            on_error="string",
        )
        assert record == {"n": "1000", "m": "100", "b": "true", "x": "1"}
        assert _coerced({"n": "integer"}, {"n": "n/a"}, on_error="string") == {
            "n": "n/a"
        }


class TestNumbers:
    """A number's text must be exactly a number; noise is not stripped blindly."""

    @pytest.mark.parametrize(
        ("text", "value"),
        [
            ("$1,234.50", Decimal("1234.50")),
            ("1,200", Decimal(1200)),
            ("-$5", Decimal(-5)),
            ("$-5", Decimal(-5)),
            ("USD 100", Decimal(100)),
            ("100 EUR", Decimal(100)),
            ("+1,000,000.5", Decimal("1000000.5")),
            ("5.", Decimal(5)),
            (".5", Decimal("0.5")),
            ("1E3", Decimal(1000)),
            (" 42 ", Decimal(42)),
        ],
    )
    def test_number_text_that_is_exactly_a_number(self, text: str, value: Decimal):
        """Currency, surrounding spaces and grouped thousands commas are noise."""
        assert _coerced({"c": "number"}, {"c": text}) == {"c": value}

    @pytest.mark.parametrize(
        "text",
        [
            "(5)",  # target-iceberg: 5, an accounting negative turned positive
            "1,5",  # a decimal comma: 15
            "1.234,50",  # 1.2345
            "abc1def2",  # 100: the e of "def" read as an exponent
            "12abc",
            "12 34",
            "0x10",
            "1_000",
            "12,34",  # not a group of three
            "1,2345",
            "usd 100",  # a currency code is upper case
            "$5 USD",  # one currency
            "--5",
            "-$-5",
            "e5",
            ".",
            "inf",
            "NaN",
        ],
    )
    @pytest.mark.parametrize("kind", ["integer", "number"])
    def test_anything_else_is_not_a_number(self, kind: str, text: str):
        """It isn't changed into another number: on_error decides."""
        with pytest.raises(CoercionError, match="it is not a number"):
            _coerced({"c": kind}, {"c": text})
        assert _coerced({"c": kind}, {"c": text}, on_error="null") == {"c": None}

    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            ("1e100000", {"i": None, "n": None}),  # out of range: NULL
            ("-1e999999999", {"i": None, "n": None}),
            ("1e-100000", {"i": 0, "n": Decimal(0)}),  # a fraction, dropped
        ],
    )
    def test_a_huge_exponent_is_judged_without_expanding_it(
        self, text: str, expected: dict
    ):
        """1e100000 used to take minutes to expand into an int."""
        started = time.monotonic()
        record = _coerced(
            {"i": "integer", "n": "number"}, {"i": text, "n": text}, on_error="null"
        )
        assert time.monotonic() - started < 1
        assert record == expected

    def test_a_tenth_fraction_digit_is_rounded_with_a_warning_when_permissive(
        self, caplog: pytest.LogCaptureFixture
    ):
        """Half away from zero, as DuckDB's cast to DECIMAL(38, 9) rounds."""
        with caplog.at_level(logging.WARNING):
            record = _coerced({"c": "number"}, {"c": "-0.0000000005"})
        assert record == {"c": Decimal("-1E-9")}
        assert "rounded to 9 fraction digits" in caplog.text


class TestKeysAndText:
    """Blank keys, string columns, and on_error string."""

    def test_a_blank_key_is_not_its_type(self):
        """A NULL key can't be merged, so on_error decides, raise by default."""
        with pytest.raises(CoercionError, match="blank, and a key can't be NULL"):
            _coerced({"id": "integer"}, {"id": " "}, keys=frozenset({"id"}))
        assert _coerced(
            {"id": "integer"}, {"id": ""}, keys=frozenset({"id"}), on_error="string"
        ) == {"id": ""}
        assert _coerced(
            {"other": "integer"}, {"other": ""}, keys=frozenset({"id"})
        ) == {"other": None}

    def test_a_time_with_a_parse_format(self):
        """x-parse-format reads a time as it reads a date."""
        column = {"c": {"type": "time", "x-parse-format": "%I:%M %p"}}
        assert _coerced(column, {"c": "07:45 PM"}) == {"c": "19:45:00"}


# ------------------------------------------------------------- through the target


def _messages(records: list[dict], properties: dict | None = None) -> list[dict]:
    schema = {
        "type": "SCHEMA",
        "stream": "t",
        "schema": {"type": "object", "properties": properties or PROPERTIES},
        "key_properties": ["id"],
    }
    return [schema, *({"type": "RECORD", "stream": "t", "record": r} for r in records)]


def _listen(lake: Lake, messages: list[dict], **config: object) -> None:
    target = Targetducklake(config={**lake.config(), **config})
    try:
        target.listen(
            file_input=io.StringIO("".join(json.dumps(m) + "\n" for m in messages))
        )
    finally:
        target.target_connector.close()


def _types(lake: Lake) -> dict[str, str]:
    return dict(
        lake.query(
            "SELECT column_name, data_type FROM duckdb_columns() "
            "WHERE database_name = 'lake' AND table_name = 't'"
        )
    )


TYPED = {
    "id": "BIGINT",
    "qty": "BIGINT",
    "price": "DECIMAL(38,9)",
    "paid": "BOOLEAN",
    "day": "DATE",
    "at": "TIMESTAMP WITH TIME ZONE",
    "opens": "TIME",
    "note": "VARCHAR",
}
SELECT = (
    'SELECT id, qty, price, paid, day::VARCHAR, "at"::VARCHAR, opens::VARCHAR, note'
)
LOADED = (
    1,
    1200,
    Decimal("1234.500000000"),
    True,
    "2026-03-04",
    "2026-03-04 08:15:00+00",
    "09:30:00",
    "0042",
)


class TestLoading:
    """What lands in the table."""

    def test_an_all_string_stream_loads_typed(self, lake: Lake):
        """Typed columns, typed values, and record validation passes."""
        _listen(
            lake,
            _messages([ROW]),
            stream_defaults={"type_source": DICTIONARY},
            validate_records=True,
        )
        assert _types(lake) == TYPED
        assert lake.query(f"{SELECT} FROM lake.s.t") == [LOADED]

    def test_without_type_source_everything_is_text(self, lake: Lake):
        """The default: no type is guessed from values."""
        _listen(lake, _messages([ROW]))
        assert set(_types(lake).values()) == {"VARCHAR"}

    def test_date_time_type_types_a_date_time_column(self, lake: Lake):
        """A declared date-time follows the stream's date_time_type."""
        _listen(
            lake,
            _messages([ROW]),
            stream_overrides={
                "t": {"type_source": DICTIONARY, "date_time_type": "timestamp"}
            },
        )
        assert _types(lake)["at"] == "TIMESTAMP"

    def test_raise_fails_the_run_naming_the_value(self, lake: Lake):
        """Nothing is stored NULL silently."""
        bad = {**ROW, "id": "2", "qty": "twelve"}
        with pytest.raises(CoercionError, match="column 'qty'.*'twelve'"):
            _listen(
                lake, _messages([ROW, bad]), stream_defaults={"type_source": DICTIONARY}
            )

    def test_null_stores_null(self, lake: Lake):
        """Only the bad values are NULL."""
        bad = {**ROW, "id": "2", "qty": "twelve", "paid": "perhaps"}
        _listen(
            lake,
            _messages([ROW, bad]),
            stream_defaults={"type_source": DICTIONARY, "on_error": "null"},
        )
        assert lake.query("SELECT id, qty, paid FROM lake.s.t ORDER BY id") == [
            (1, 1200, True),
            (2, None, None),
        ]

    def test_string_keeps_the_raw_text_in_a_text_column(self, lake: Lake):
        """The bad text is kept, so the column is VARCHAR."""
        bad = {**ROW, "id": "2", "qty": "twelve"}
        _listen(
            lake,
            _messages([ROW, bad]),
            stream_defaults={"type_source": DICTIONARY, "on_error": "string"},
        )
        assert set(_types(lake).values()) == {"VARCHAR"}
        assert lake.query("SELECT id, qty, price, paid FROM lake.s.t ORDER BY id") == [
            ("1", "1200", "1234.50", "true"),
            ("2", "twelve", "1234.50", "true"),
        ]

    def test_an_existing_text_column_stays_text(
        self, lake: Lake, capfd: pytest.CaptureFixture[str]
    ):
        """Typing a column a table already has as VARCHAR doesn't retype it."""
        _listen(lake, _messages([ROW]))
        _listen(
            lake,
            _messages([{**ROW, "id": "2"}]),
            stream_defaults={"type_source": DICTIONARY},
        )
        assert set(_types(lake).values()) == {"VARCHAR"}
        assert (
            "already has it as VARCHAR, which isn't retyped" in capfd.readouterr().err
        )
        rows = lake.query("SELECT id, qty, price, paid, day FROM lake.s.t ORDER BY id")
        assert rows == [
            ("1", " 1,200 ", "$1,234.50", "Yes", "04/03/2026"),
            ("2", "1200", "1234.50", "true", "2026-03-04"),  # as on_error string
        ]

    def test_a_blank_key_fails_the_run(self, lake: Lake):
        """A blank key would load as a NULL key, which can't merge."""
        with pytest.raises(CoercionError, match="column 'id'.*blank, and a key"):
            _listen(
                lake,
                _messages([{**ROW, "id": " "}]),
                stream_defaults={"type_source": DICTIONARY},
            )

    def test_out_of_range_follows_on_error(self, lake: Lake):
        """A value the typed column can't hold is not its type, under every policy."""
        huge = {**ROW, "qty": "99999999999999999999", "price": "1e30"}
        _listen(
            lake,
            _messages([huge]),
            stream_defaults={"type_source": DICTIONARY, "on_error": "null"},
        )
        assert lake.query("SELECT id, qty, price FROM lake.s.t") == [(1, None, None)]

    def test_string_on_a_typed_tap_makes_the_column_text(self, lake: Lake):
        """The tap declares n an integer; on_error string must still keep 'abc'."""
        properties = {"id": {"type": "integer"}, "n": {"type": ["null", "integer"]}}
        _listen(
            lake,
            _messages([{"id": 1, "n": 5}, {"id": 2, "n": "abc"}], properties),
            stream_defaults={"type_source": {"n": "integer"}, "on_error": "string"},
        )
        assert _types(lake) == {"id": "BIGINT", "n": "VARCHAR"}
        assert lake.query("SELECT id, n FROM lake.s.t ORDER BY id") == [
            (1, "5"),
            (2, "abc"),
        ]

    def test_string_refuses_an_existing_typed_column(self, lake: Lake):
        """A BIGINT column can't keep raw text: refused before anything loads."""
        _listen(lake, _messages([ROW]), stream_defaults={"type_source": DICTIONARY})
        with pytest.raises(LayoutError, match="column 'id' must be text"):
            _listen(
                lake,
                _messages([{**ROW, "id": "2"}]),
                stream_defaults={"type_source": DICTIONARY, "on_error": "string"},
            )
        assert lake.query("SELECT count(*) FROM lake.s.t") == [(1,)]

    def test_a_string_entry_types_a_typed_tap_column_as_text(self, lake: Lake):
        """As in target-iceberg, a string entry overrides the tap's type."""
        properties = {"id": {"type": "integer"}, "n": {"type": ["null", "integer"]}}
        _listen(
            lake,
            _messages([{"id": 1, "n": 5}], properties),
            stream_defaults={"type_source": {"n": "text"}},
        )
        assert _types(lake) == {"id": "BIGINT", "n": "VARCHAR"}
        assert lake.query("SELECT n FROM lake.s.t") == [("5",)]

    def test_batch_messages_are_typed_too(self, lake: Lake, tmp_path: Path):
        """BATCH records go through the RECORD path (DUCKLKD-40), coercion included."""
        path = tmp_path / "rows.jsonl"
        path.write_text(json.dumps(ROW) + "\n")
        batch = {
            "type": "BATCH",
            "stream": "t",
            "encoding": {"format": "jsonl", "compression": "none"},
            "manifest": [path.as_uri()],
        }
        _listen(
            lake,
            [*_messages([]), batch],
            stream_defaults={"type_source": DICTIONARY},
        )
        assert _types(lake) == TYPED
        assert lake.query(f"{SELECT} FROM lake.s.t") == [LOADED]

    def test_a_repeated_schema_keeps_the_sink(self, tmp_path: Path):
        """The SDK compares a SCHEMA with the tap's schema, not the typed one."""
        config = {
            "catalog_url": str(tmp_path / "c.sqlite"),
            "catalog_type": "sqlite",
            "data_path": str(tmp_path),
            "storage_type": "local",
            "default_target_schema": "s",
            "stream_defaults": {"type_source": DICTIONARY},
        }
        target = Targetducklake(config=config)
        schema = _messages([])[0]
        target._process_schema_message(schema)  # noqa: SLF001
        sink = target.get_sink("t")
        target._process_schema_message(json.loads(json.dumps(schema)))  # noqa: SLF001
        assert target.get_sink("t") is sink
        assert sink.schema["properties"]["id"] == {"type": ["null", "integer"]}
        target.target_connector.close()
