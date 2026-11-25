"""RECORD messages staged by DuckDB from JSON lines (DUCKLKD-157).

Each record is written as one line of its batch's file and staged at drain
with the SQL that stages BATCH files. These tests pin what a record's values
load as: exactly what the Arrow staging the lines replaced stored.
"""

from __future__ import annotations

import io
import json
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import pytest

from target_ducklake.record_lines import JsonValues, RecordLines, json_values
from target_ducklake.target import Targetducklake

if TYPE_CHECKING:
    from pathlib import Path

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
        lines = RecordLines(str(tmp_path / "r.jsonl"), {})
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

    def test_json_values_are_their_json_text(self, tmp_path: Path):
        """A JSON column's value and a STRUCT's JSON field, but no other."""
        paths = json_values(
            [
                (("j",), "JSON", True),
                (("s",), 'STRUCT("x" DECIMAL(38,9), "y" JSON)', True),
                (("txt",), "VARCHAR", False),
                (("n",), "DECIMAL(38,9)", False),
            ]
        )
        assert paths == {
            "j": JsonValues(whole=True),
            "s": JsonValues(fields={"y": JsonValues(whole=True)}),
            "txt": JsonValues(containers=True),
        }
        nested = {"x": Decimal("1.5"), "y": {"p": Decimal("3.000")}}
        record = {"j": {"a": Decimal("1.50")}, "s": nested, "txt": "a", "n": None}
        lines = RecordLines(str(tmp_path / "r.jsonl"), paths)
        lines.write(record)
        lines.close()
        assert json.loads((tmp_path / "r.jsonl").read_text()) == {
            "j": '{"a": 1.50}',
            "s": {"x": "1.5", "y": '{"p": 3.000}'},
            "txt": "a",
            "n": None,
        }
        # The record's nested object, which a stream map's copy may share, is
        # unchanged.
        assert nested["y"] == {"p": Decimal("3.000")}


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
