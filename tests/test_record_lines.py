"""RECORD messages staged by DuckDB from JSON lines (DUCKLKD-157).

Each record is written as one line of its batch's file and staged at drain
with the SQL that stages BATCH files. These tests pin what a record's values
load as: exactly what the Arrow staging the lines replaced stored.
"""

from __future__ import annotations

import io
import json
import re
import subprocess
import sys
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from target_ducklake.record_lines import (
    LineColumn,
    RecordLines,
    ValueRules,
    datetime_text,
    sent_form,
    value_rules,
)
from target_ducklake.target import Targetducklake

if TYPE_CHECKING:
    from tests.catalogs import Lake


def _load(lake: Lake, properties: dict, records: list[dict], **config: object) -> None:
    messages: list[dict[str, Any]] = [
        {
            "type": "SCHEMA",
            "stream": "t",
            "schema": {"type": "object", "properties": properties},
            "key_properties": ["id"],
        },
        *({"type": "RECORD", "stream": "t", "record": r} for r in records),
    ]
    target = Targetducklake(config={**lake.config(), **config})
    try:
        target.listen(
            file_input=io.StringIO("".join(json.dumps(m) + "\n" for m in messages))
        )
    finally:
        target.target_connector.close()


def _load_raw(lake: Lake, properties: dict, records: list[str]) -> None:
    """Load records given as their JSON text, which json.dumps can't always write."""
    schema = {
        "type": "SCHEMA",
        "stream": "t",
        "schema": {"type": "object", "properties": properties},
        "key_properties": ["id"],
    }
    lines = [json.dumps(schema)]
    lines += [f'{{"type": "RECORD", "stream": "t", "record": {r}}}' for r in records]
    target = Targetducklake(config=lake.config())
    try:
        target.listen(file_input=io.StringIO("\n".join(lines) + "\n"))
    finally:
        target.target_connector.close()


def _text(lake: Lake, column: str) -> list[tuple]:
    return lake.query(
        f'SELECT id, CAST("{column}" AS VARCHAR) FROM lake.s.t ORDER BY id'  # noqa: S608
    )


ID = {"id": {"type": "integer"}}


class TestLines:
    """What a line holds for each value."""

    def test_values_json_has_no_type_for(self, tmp_path: Path):
        """Decimals as exact text; date-times, dates and times as ISO text."""
        lines = RecordLines(str(tmp_path / "r.jsonl"), {}, stream="t")
        lines.write(
            {
                "n": Decimal("1.50"),
                "e": Decimal("1E+2"),
                "utc": datetime(2024, 1, 1, 10, tzinfo=timezone.utc),
                "off": datetime(2024, 1, 1, 10, tzinfo=timezone(timedelta(hours=2))),
                "naive": datetime(2024, 1, 1, 10),  # noqa: DTZ001 (naive)
                "d": date(2024, 2, 3),
                "t": time(10, 11, 12),
                "i": 2**70,
            }
        )
        lines.close()
        assert json.loads((tmp_path / "r.jsonl").read_text()) == {
            "n": "1.50",
            "e": "100",
            "utc": "2024-01-01 10:00:00.000000Z",
            "off": "2024-01-01 10:00:00.000000+02:00",
            "naive": "2024-01-01 10:00:00.000000",
            "d": "2024-02-03",
            "t": "10:11:12.000000",
            "i": 2**70,
        }

    def test_value_rules(self, tmp_path: Path):
        """JSON values as their JSON text, a float in text as Python's text."""
        rules = value_rules(
            [
                LineColumn(("j",), "j", "JSON", container=True),
                LineColumn(
                    ("s",), "s", 'STRUCT("x" DECIMAL(38,9), "y" JSON)', container=True
                ),
                LineColumn(("txt",), "txt", "VARCHAR", container=False),
                LineColumn(("b",), "b", "BOOLEAN", container=False),
                LineColumn(("n",), "n", "DECIMAL(38,9)", container=False),
            ]
        )
        assert rules == {
            "j": ValueRules(label="j", whole=True),
            "s": ValueRules(
                label="s",
                fields={"y": ValueRules(label="s.y", whole=True)},
                struct={"x": "x", "y": "y"},
                declared=frozenset({"x", "y"}),
            ),
            "txt": ValueRules(label="txt", text=True),
            "b": ValueRules(label="b", numbers=True),
        }
        nested = {"x": Decimal("1.5"), "y": {"p": Decimal("3.000")}}
        record = {
            "j": {"a": Decimal("1.50")},
            "s": nested,
            "txt": float("nan"),
            "b": Decimal("1.0"),
            "n": None,
        }
        lines = RecordLines(str(tmp_path / "r.jsonl"), rules, stream="t")
        lines.write(record)
        lines.close()
        assert json.loads((tmp_path / "r.jsonl").read_text()) == {
            "j": '{"a": 1.50}',
            "s": {"x": "1.5", "y": '{"p": 3.000}'},
            "txt": "nan",
            "b": 1.0,
            "n": None,
        }
        # The record's nested object, which a stream map's copy may share, is
        # unchanged.
        assert nested["y"] == {"p": Decimal("3.000")}

    def test_no_file_stays_open_between_records(self, tmp_path: Path):
        """Lines are held, then appended a megabyte at a time, the file closed.

        150 streams with a record each run under a limit of 120 descriptors
        (in a child process, so the limit is its own).
        """
        streams = 150
        messages = []
        for i in range(streams):
            messages.append(
                {
                    "type": "SCHEMA",
                    "stream": f"s{i}",
                    "schema": {"type": "object", "properties": ID},
                    "key_properties": ["id"],
                }
            )
            messages.append({"type": "RECORD", "stream": f"s{i}", "record": {"id": i}})
        config = {
            "catalog_type": "sqlite",
            "catalog_url": str(tmp_path / "c.sqlite"),
            "data_path": f"{tmp_path}/data/",
            "storage_type": "local",
            "default_target_schema": "s",
            "temp_file_dir": f"{tmp_path}/st/",
        }
        (tmp_path / "config.json").write_text(json.dumps(config))
        code = (
            "import resource, sys; "
            "resource.setrlimit(resource.RLIMIT_NOFILE, (120, 120)); "
            "from target_ducklake.target import Targetducklake; "
            "Targetducklake.cli()"
        )
        result = subprocess.run(  # noqa: S603 (this interpreter, our own code)
            [sys.executable, "-c", code, "--config", str(tmp_path / "config.json")],
            input="".join(json.dumps(m) + "\n" for m in messages),
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, result.stderr[-2000:]
        assert "Too many open files" not in result.stderr

    def test_lines_are_flushed_in_chunks(self, tmp_path: Path):
        """Past a megabyte, held lines go to the file; the rest at close."""
        lines = RecordLines(str(tmp_path / "r.jsonl"), {}, stream="t")
        lines.write({"s": "x" * 600_000})
        assert (tmp_path / "r.jsonl").read_bytes() == b""
        lines.write({"s": "y" * 600_000})
        assert (tmp_path / "r.jsonl").read_text().split("\n")[2:] == [""]
        lines.write({"s": "z"})
        lines.close()
        assert (tmp_path / "r.jsonl").read_text().endswith('\n{"s":"z"}\n')

    def test_a_lone_surrogate_names_the_stream_and_property(self, tmp_path: Path):
        """UTF-8 can't encode it; nothing of the record reaches the file."""
        lines = RecordLines(str(tmp_path / "r.jsonl"), {}, stream="t")
        lines.write({"id": 1, "s": "ok"})
        with pytest.raises(
            ValueError,
            match=r"stream 't', property 's': value 'a\\ud800b' holds a lone surrogate",
        ):
            lines.write({"id": 2, "s": "a\ud800b"})
        lines.close()
        assert (tmp_path / "r.jsonl").read_text() == '{"id":1,"s":"ok"}\n'


class TestExactValues:
    """Values DuckDB's JSON reader would change, loaded exactly."""

    def test_wide_numbers(self, lake: Lake):
        """Fractions with more than 17 significant digits, and wide integers."""
        number = {"type": ["null", "number"]}
        wide = {**number, "x-sql-datatype": "decimal(38,0)"}
        _load_raw(
            lake,
            {**ID, "n": number, "w": wide},
            [
                '{"id": 1, "n": 12345678901234567890.123456789, '
                '"w": 12345678901234567890123456789012345678}'
            ],
        )
        assert lake.query("SELECT n::VARCHAR, w::VARCHAR FROM lake.s.t") == [
            ("12345678901234567890.123456789", "12345678901234567890123456789012345678")
        ]

    def test_text_column_keeps_a_numbers_digits(self, lake: Lake):
        """A number in a column of several types is stored as its exact text."""
        _load_raw(
            lake,
            {**ID, "v": {"type": ["null", "string", "number"]}},
            ['{"id": 1, "v": 1.50}', '{"id": 2, "v": 1E+2}', '{"id": 3, "v": true}'],
        )
        assert _text(lake, "v") == [(1, "1.50"), (2, "100"), (3, "true")]

    def test_json_column_keeps_its_text(self, lake: Lake):
        """As the target wrote it before: its spaces and its numbers' digits."""
        _load_raw(
            lake,
            {**ID, "j": {"type": ["null", "object"]}},
            ['{"id": 1, "j": {"a": 0.12345678901234567890123, "b": [1, 2.50]}}'],
        )
        assert _text(lake, "j") == [
            (1, '{"a": 0.12345678901234567890123, "b": [1, 2.50]}')
        ]

    @pytest.mark.parametrize(
        ("config", "stored"),
        [
            ({}, ["2024-01-01 08:00:00+00", "2024-01-01 10:00:00+00"]),
            (
                {"date_time_type": "timestamp"},
                ["2024-01-01 10:00:00", "2024-01-01 10:00:00"],
            ),
            (
                {"date_time_type": "timestamp", "convert_tz_to_utc": True},
                ["2024-01-01 08:00:00", "2024-01-01 10:00:00"],
            ),
        ],
        ids=["timestamptz", "timestamp", "timestamp-utc"],
    )
    def test_date_times(self, lake: Lake, config: dict, stored: list[str]):
        """An offset, and none (UTC to a TIMESTAMPTZ column), top-level and nested."""
        at = {"type": ["null", "string"], "format": "date-time"}
        properties = {
            **ID,
            "at": at,
            "o": {"type": ["null", "object"], "properties": {"at": at}},
        }
        records = [
            {
                "id": 1,
                "at": "2024-01-01T10:00:00+02:00",
                "o": {"at": "2024-01-01T10:00:00+02:00"},
            },
            {"id": 2, "at": "2024-01-01T10:00:00", "o": {"at": "2024-01-01T10:00:00"}},
        ]
        _load(lake, properties, records, **config)
        assert [row[1] for row in _text(lake, "at")] == stored
        nested = lake.query("SELECT CAST(o.at AS VARCHAR) FROM lake.s.t ORDER BY id")
        assert [row[0] for row in nested] == stored


class TestStreamMaps:
    """Stream maps change records, as RECORD messages always allowed."""

    def test_transform_and_filter(self, lake: Lake):
        """A property expression and a filter, which BATCH files refuse."""
        _load(
            lake,
            {**ID, "v": {"type": ["null", "number"]}},
            [{"id": 1, "v": 1.5}, {"id": 2, "v": 2.5}, {"id": 3, "v": 3.5}],
            stream_maps={"t": {"double": "v * 2", "__filter__": "id != 2"}},
        )
        assert lake.query(
            "SELECT id, CAST(v AS VARCHAR), CAST(double AS VARCHAR) FROM lake.s.t "
            "ORDER BY id"
        ) == [(1, "1.500000000", "3.0"), (3, "3.500000000", "7.0")]


class TestRefusals:
    """A value DuckDB can't load names the stream, the row, the column and value."""

    def test_a_bad_value_names_its_row_and_column(self, lake: Lake):
        """The record's row in its batch, from 1."""
        with pytest.raises(
            ValueError,
            match=(
                r"stream 't', batch of records, row 2, column 'i': value 'abc' can't "
                r"be loaded as BIGINT, so the batch wasn't loaded\."
            ),
        ):
            _load(
                lake,
                {**ID, "i": {"type": ["null", "integer"]}},
                [{"id": 1, "i": 5}, {"id": 2, "i": "abc"}],
            )
        # The batch's records, and its staged file, aren't left on disk.
        staging = Path(lake.config()["temp_file_dir"])
        assert not [p for p in staging.rglob("*") if p.is_file()]

    def test_a_lone_surrogate_fails_the_run_leaving_no_file(self, lake: Lake):
        """Named by stream and property; the batch's file is removed."""
        with pytest.raises(ValueError, match=r"stream 't', property 's': value"):
            _load(
                lake,
                {**ID, "s": {"type": ["null", "string"]}},
                [{"id": 1, "s": "ok"}, {"id": 2, "s": "a\ud800b"}],
            )
        staging = Path(lake.config()["temp_file_dir"])
        assert not [p for p in staging.rglob("*") if p.is_file()]


def _listen(lake: Lake, messages: list[dict], **config: object) -> None:
    target = Targetducklake(config={**lake.config(), **config})
    try:
        target.listen(
            file_input=io.StringIO("".join(json.dumps(m) + "\n" for m in messages))
        )
    finally:
        target.target_connector.close()


def _staged_files(lake: Lake) -> list[Path]:
    staging = Path(lake.config()["temp_file_dir"])
    return [p for p in staging.rglob("*") if p.is_file()]


# Date-times the singer-sdk parses: the forms a line holds as sent, and
# others (basic and week forms, no seconds before an offset, an offset with
# seconds, more than six fraction digits, a ',' before them, another
# separator), which it holds as written from the parsed value.
DATE_TIMES = [
    "2024-01-01T10:00:00Z",
    "2024-01-01T10:00:00+00:00",
    "2024-01-01T10:00:00-00:00",
    "2024-01-01T10:00:00-05:00",
    "2024-01-01T10:00:00+05:30",
    "2024-01-01 10:00:00",
    "2024-01-01T10:00:00",
    "2024-01-01T10:00:00.1Z",
    "2024-01-01T10:00:00.12+01:00",
    "2024-01-01T10:00:00.123",
    "2024-01-01T10:00:00.12345Z",
    "2024-01-01T10:00:00.123456Z",
    "2024-01-01 10:00:00.5-03:00",
    "2024-01-01",
    "1969-12-31T23:59:59.999999Z",
    "0001-01-01T00:00:00Z",
    "9999-12-31T23:59:59.999999Z",
    "2024-01-01T10:00:00.1234567Z",
    "2024-01-01T10:00:00.123456789Z",
    "2024-01-01T10:00",
    "2024-01-01T10:00Z",
    "2024-01-01T10",
    "20240101T100000",
    "20240101",
    "2024-W01-1",
    "2024-W01-1T10:00:00",
    "2024-01-01T10:00:00+02",
    "2024-01-01T10:00:00+0200",
    "2024-01-01T10:00:00+02:00:30",
    "2024-01-01T10:00:00,5Z",
    "2024-01-01x10:00:00",
    "2024-01-01T10:00:00 +02:00",
]
SENT_DATE_TIMES = DATE_TIMES[: DATE_TIMES.index("2024-01-01T10:00:00.1234567Z")]
DATES = ["2024-05-06", "0001-01-01", "9999-12-31", "20240506", "2024-W19-1"]
TIMES = ["10:00:00", "10:00:00.5", "10:00:00.123456", "10:00", "10:00:00.1234567"]
TIMES += ["10:00:00Z", "10:00:00+02:00", "100000", "T10:00:00", "10:00:00,5"]


class TestDateTexts:
    """A date-like value's text, kept where DuckDB reads it alike (DUCKLKD-157)."""

    def test_sent_forms(self):
        """By kind and column type; a date-time's never for a key or text column."""
        instant = sent_form(
            "date-time", ["TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE"], key=False
        )
        assert instant is not None
        assert [
            form for form in DATE_TIMES if instant.fullmatch(form)
        ] == SENT_DATE_TIMES
        assert sent_form("date-time", ["TIMESTAMP", "DATE"], key=False) is not None
        assert sent_form("date-time", ["TIMESTAMPTZ"], key=True) is None
        assert sent_form("date-time", ["TIMESTAMPTZ", "VARCHAR"], key=False) is None
        assert sent_form("date-time", ["TIMESTAMP_NS"], key=False) is None
        time_form = sent_form("time", ["TIME"], key=False)
        assert time_form is not None
        assert [form for form in TIMES if time_form.fullmatch(form)] == TIMES[:3]
        assert sent_form("time", ["VARCHAR"], key=False) is None
        # A date's sent form is the text isoformat writes, into any column.
        date_form = sent_form("date", ["VARCHAR"], key=True)
        assert date_form is not None
        assert [form for form in DATES if date_form.fullmatch(form)] == DATES[:3]

    @pytest.mark.parametrize(
        ("hint", "config"),
        [
            (None, {}),
            (None, {"date_time_type": "timestamp"}),
            (None, {"date_time_type": "timestamp", "convert_tz_to_utc": True}),
            ("date", {}),
            ("time", {}),
        ],
        ids=["timestamptz", "timestamp", "timestamp-utc", "date-hint", "time-hint"],
    )
    def test_date_times_load_as_parsed(
        self,
        lake: Lake,
        monkeypatch: pytest.MonkeyPatch,
        hint: str | None,
        config: dict,
    ):
        """Each form stores what its parsed value's text stored, sent as is or not.

        ``n`` holds the text a line held for every value before
        (``datetime_text`` of the singer-sdk's parse), which is in a sent form.
        """
        at: dict[str, Any] = {"type": ["null", "string"], "format": "date-time"}
        if hint is not None:
            at["x-sql-datatype"] = hint
        lines = _written_records(monkeypatch)
        _load(
            lake,
            {**ID, "ts": at, "n": at},
            [
                {"id": i, "ts": form, "n": datetime_text(datetime.fromisoformat(form))}
                for i, form in enumerate(DATE_TIMES)
            ],
            **config,
        )
        rows = lake.query(
            "SELECT id, CAST(ts AS VARCHAR), CAST(n AS VARCHAR) FROM lake.s.t "
            "ORDER BY id"
        )
        assert len(rows) == len(DATE_TIMES)
        assert [(i, ts) for i, ts, _ in rows] == [(i, n) for i, _, n in rows]
        # Sent as is in the forms DuckDB reads alike; others as parsed.
        assert [r["ts"] for r in lines if isinstance(r["ts"], str)] == SENT_DATE_TIMES

    def test_dates_and_times_load_as_parsed(
        self, lake: Lake, monkeypatch: pytest.MonkeyPatch
    ):
        """A date and a time of day, as the line held their parsed values' text."""
        day = {"type": ["null", "string"], "format": "date"}
        clock = {"type": ["null", "string"], "format": "time"}
        lines = _written_records(monkeypatch)
        _load(
            lake,
            {**ID, "d": day, "dn": day, "t": clock, "tn": clock},
            [
                {
                    "id": i,
                    "d": DATES[i % len(DATES)],
                    "dn": date.fromisoformat(DATES[i % len(DATES)]).isoformat(),
                    "t": form,
                    "tn": time.fromisoformat(form).isoformat("microseconds"),
                }
                for i, form in enumerate(TIMES)
            ],
        )
        rows = lake.query(
            "SELECT CAST(d AS VARCHAR), CAST(dn AS VARCHAR), CAST(t AS VARCHAR), "
            "CAST(tn AS VARCHAR) FROM lake.s.t ORDER BY id"
        )
        assert len(rows) == len(TIMES)
        assert [(d, t) for d, _, t, _ in rows] == [(dn, tn) for _, dn, _, tn in rows]
        assert [r["t"] for r in lines if isinstance(r["t"], str)] == TIMES[:3]

    def test_a_key_is_written_as_parsed(self, lake: Lake):
        """Two spellings of one key's date-time are one key, as before."""
        at = {"type": "string", "format": "date-time"}
        schema = {"type": "object", "properties": {"ts": at, "v": {"type": "integer"}}}
        _listen(
            lake,
            [
                {
                    "type": "SCHEMA",
                    "stream": "t",
                    "schema": schema,
                    "key_properties": ["ts"],
                },
                {
                    "type": "RECORD",
                    "stream": "t",
                    "record": {"ts": "2024-01-01T10:00:00+02:00", "v": 1},
                },
                {
                    "type": "RECORD",
                    "stream": "t",
                    "record": {"ts": "2024-01-01 10:00:00.000+02:00", "v": 2},
                },
            ],
            date_time_type="timestamp",
        )
        assert lake.query("SELECT CAST(ts AS VARCHAR), v FROM lake.s.t") == [
            ("2024-01-01 10:00:00", 2)
        ]

    @pytest.mark.parametrize(
        "value", ["2024-01-01T24:00:00", "2024-1-1T10:00:00", "2023-02-29T00:00:00"]
    )
    def test_text_the_sdk_refuses_still_fails(self, lake: Lake, value: str):
        """DuckDB reads the first two; the singer-sdk's parse refuses all three."""
        with pytest.raises(
            ValueError, match=f"Could not parse value '{value}' for field 'ts'"
        ):
            _load(
                lake,
                {**ID, "ts": {"type": ["null", "string"], "format": "date-time"}},
                [{"id": 1, "ts": value}],
            )

    def test_a_property_without_schema_is_warned_of(
        self, lake: Lake, warnings: pytest.LogCaptureFixture
    ):
        """As the singer-sdk warns of it, for a value that isn't null."""
        _load(lake, ID, [{"id": 1, "extra": "x"}, {"id": 2, "extra": None}])
        assert [
            r.getMessage()
            for r in warnings.records
            if "No schema for record field" in r.getMessage()
        ] == ["No schema for record field 'extra'"]


def _written_records(monkeypatch: pytest.MonkeyPatch) -> list[dict]:
    """The records ``RecordLines.write`` is given, as given, from now on."""
    written: list[dict] = []
    write = RecordLines.write

    def spy(self: RecordLines, record: dict) -> None:
        written.append(dict(record))
        write(self, record)

    monkeypatch.setattr(RecordLines, "write", spy)
    return written


class TestFailures:
    """A failed run leaves none of its streams' records on disk."""

    def test_another_streams_lines_are_removed(self, lake: Lake):
        """Stream 'a' has flushed lines to its file when a record of 'b' fails."""
        schema = {"type": "object", "properties": {**ID, "s": {"type": "string"}}}
        messages: list[dict] = [
            {"type": "SCHEMA", "stream": s, "schema": schema, "key_properties": ["id"]}
            for s in ("a", "b")
        ]
        messages += [
            {"type": "RECORD", "stream": "a", "record": {"id": i, "s": "x" * 100_000}}
            for i in range(30)
        ]
        messages.append(
            {"type": "RECORD", "stream": "b", "record": {"id": 1, "s": "a\ud800b"}}
        )
        with pytest.raises(ValueError, match="lone surrogate"):
            _listen(lake, messages)
        assert not _staged_files(lake)

    def test_a_failed_drain_removes_the_other_streams_files(self, lake: Lake):
        """'b' fails at the end of the run; 'a', loaded or not, leaves nothing."""
        schema = {"type": "object", "properties": {**ID, "i": {"type": "integer"}}}
        messages: list[dict] = [
            {"type": "SCHEMA", "stream": s, "schema": schema, "key_properties": ["id"]}
            for s in ("b", "a")
        ]
        messages += [
            {"type": "RECORD", "stream": "a", "record": {"id": 1, "i": 1}},
            {"type": "RECORD", "stream": "b", "record": {"id": 1, "i": "x"}},
        ]
        with pytest.raises(ValueError, match="column 'i': value 'x'"):
            _listen(lake, messages)
        assert not _staged_files(lake)


class TestJsonRefusals:
    """A refused JSON value is shown with its JSON type."""

    @pytest.mark.parametrize(
        ("value", "shown"),
        [
            ("12", "'12' (a string)"),
            (12, "'12' (a number)"),
            (True, "'true' (a boolean)"),
        ],
    )
    def test_a_string_and_a_number_read_apart(
        self, lake: Lake, value: object, shown: str
    ):
        """A string "12" and a number 12 where an array is declared."""
        with pytest.raises(
            ValueError, match=re.escape(f"column 'l': value {shown} can't")
        ):
            _load(
                lake,
                {**ID, "l": {"type": ["null", "array"], "items": {"type": "integer"}}},
                [{"id": 1, "l": value}],
            )
