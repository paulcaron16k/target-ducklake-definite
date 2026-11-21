"""The target's DuckDB session is in UTC, whatever the machine's zone (DUCKLKD-148).

DuckDB gives a new database the process's time zone, read once when DuckDB is
loaded, and reads a date-time without an offset in it wherever a TIMESTAMPTZ
takes one: an INSERT, a merge's key match, a date cast to TIMESTAMPTZ.
``TZ`` set in this test process can't change that, so each test runs the
target in a child process (``tests/zoned_run.py``) with ``TZ`` set to a zone
other than UTC: one west of it, and one with a half-hour offset. Every run
checks that the child's DuckDB default is that zone and the target's session
is UTC. The tables are read here, in UTC.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from tests.test_mid_run_schema import _activate, _records, _schema

if TYPE_CHECKING:
    from tests.catalogs import Lake

ZONES = ["America/Los_Angeles", "Asia/Kolkata"]
LOAD_METHODS = ["append", "merge", "overwrite"]
DATE = {"type": ["null", "string"], "format": "date"}
DATE_TIME = {"type": ["null", "string"], "format": "date-time"}
TIME = {"type": ["null", "string"], "format": "time"}
STRING = {"type": ["null", "string"]}
REPO = Path(__file__).resolve().parents[1]


def _run(lake: Lake, messages: list[dict], **config: Any) -> dict:
    """One target run's payload: ``messages`` with the lake's config and ``config``."""
    return {"config": {**lake.config(), **config}, "messages": messages}


def _zoned(zone: str, *runs: dict) -> None:
    """Run the target once per run, in order, in a child process in ``zone``."""
    child = subprocess.run(  # noqa: S603 - this interpreter, our own module
        [sys.executable, "-m", "tests.zoned_run"],
        input=json.dumps({"runs": list(runs)}),
        capture_output=True,
        text=True,
        cwd=REPO,
        env={**os.environ, "TZ": zone},
        check=False,
    )
    assert child.returncode == 0, child.stderr[-3000:]
    report = json.loads(child.stdout.strip().splitlines()[-1])
    assert report == {"default_zone": zone, "session_zones": ["UTC"] * len(runs)}


def _properties(value: dict, key: dict | None = None) -> dict:
    return {"id": key or {"type": "integer"}, "v": value}


def _earlier(lake: Lake, schema: dict, value: object, **config: Any) -> dict:
    """A run creating table orders with row 7: ``value``."""
    return _run(
        lake,
        [_schema(_properties(schema)), *_records([{"id": 7, "v": value}])],
        **config,
    )


def _later(lake: Lake, schema: dict, values: list, **config: Any) -> dict:
    """A run declaring ``v`` as ``schema``: rows 1, 2, ... with ``values``."""
    rows = [{"id": i, "v": v} for i, v in enumerate(values, 1)]
    return _run(lake, [_schema(_properties(schema)), *_records(rows)], **config)


def _leaf(schema: dict) -> dict:
    """An object property whose field ``d`` is ``schema``: a STRUCT column."""
    return {"type": ["null", "object"], "properties": {"d": schema}}


def _rows(lake: Lake, columns: str = "id, v::VARCHAR") -> list[tuple]:
    return lake.query(f"SELECT {columns} FROM lake.s.orders ORDER BY ALL")  # noqa: S608


def _column_type(lake: Lake) -> str:
    return lake.query(
        "SELECT data_type FROM information_schema.columns "
        "WHERE table_name = 'orders' AND column_name = 'v'"
    )[0][0]


def test_tz_sets_duckdbs_default_zone_only_in_a_new_process():
    """Why the tests run the target in a child process."""
    script = (
        "import os, time, duckdb\n"
        "zone = lambda: duckdb.connect().sql(\"SELECT current_setting('TimeZone')\")"
        ".fetchone()[0]\n"
        "first = zone()\n"
        "os.environ['TZ'] = 'Asia/Tokyo'\n"
        "time.tzset()\n"
        "print(first, zone())\n"
    )
    child = subprocess.run(  # noqa: S603
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        env={**os.environ, "TZ": "Asia/Kolkata"},
        check=True,
    )
    assert child.stdout.split() == ["Asia/Kolkata", "Asia/Kolkata"]


@pytest.mark.parametrize("zone", ZONES)
class TestDateTimesWithoutAnOffsetAreUTC:
    """DUCKLKD-148: with date_time_type timestamp, into a TIMESTAMPTZ column."""

    @pytest.mark.parametrize("load_method", LOAD_METHODS)
    def test_into_an_existing_timestamptz_column(
        self, lake: Lake, zone: str, load_method: str
    ):
        """Read as UTC, a value with an offset as its instant, under each method."""
        _zoned(
            zone,
            _earlier(lake, DATE_TIME, "2024-01-01T00:00:00Z"),
            _later(
                lake,
                DATE_TIME,
                ["2024-01-02T10:00:00", "2024-01-02T10:00:00+05:30", None],
                load_method=load_method,
                date_time_type="timestamp",
            ),
        )
        kept = [] if load_method == "overwrite" else [(7, "2024-01-01 00:00:00+00")]
        assert _rows(lake) == [
            (1, "2024-01-02 10:00:00+00"),
            (2, "2024-01-02 04:30:00+00"),
            (3, None),
            *kept,
        ]
        assert _column_type(lake) == "TIMESTAMP WITH TIME ZONE"

    def test_a_merge_key_matches_its_row(self, lake: Lake, zone: str):
        """The staged key and the stored key are one instant: the row is replaced."""
        schema = _schema(_properties(STRING, key=DATE_TIME))
        _zoned(
            zone,
            _run(lake, [schema, *_records([{"id": "2024-01-02T10:00:00Z", "v": "a"}])]),
            _run(
                lake,
                [schema, *_records([{"id": "2024-01-02T10:00:00", "v": "b"}])],
                load_method="merge",
                date_time_type="timestamp",
            ),
        )
        assert _rows(lake, "id::VARCHAR, v") == [("2024-01-02 10:00:00+00", "b")]

    def test_into_a_hinted_timestamptz_column(self, lake: Lake, zone: str):
        """A new column the hint makes TIMESTAMPTZ."""
        hinted = {**DATE_TIME, "x-sql-datatype": "timestamptz"}
        _zoned(
            zone,
            _later(lake, hinted, ["2024-01-02T10:00:00"], date_time_type="timestamp"),
        )
        assert _rows(lake) == [(1, "2024-01-02 10:00:00+00")]

    def test_a_timestamp_column_keeps_wall_clock_times(self, lake: Lake, zone: str):
        """Each value's own wall-clock time, as before; UTC with convert_tz_to_utc."""
        values = ["2024-01-02T10:00:00", "2024-01-02T10:00:00+05:30"]
        _zoned(
            zone,
            _later(lake, DATE_TIME, values, date_time_type="timestamp"),
        )
        assert _column_type(lake) == "TIMESTAMP"
        assert _rows(lake) == [(1, "2024-01-02 10:00:00"), (2, "2024-01-02 10:00:00")]
        _zoned(
            zone,
            _later(
                lake,
                DATE_TIME,
                values,
                date_time_type="timestamp",
                convert_tz_to_utc=True,
                load_method="overwrite",
            ),
        )
        assert _rows(lake) == [(1, "2024-01-02 10:00:00"), (2, "2024-01-02 04:30:00")]

    @pytest.mark.parametrize("date_time_type", ["timestamptz", "timestamp"])
    def test_deleted_at_is_now(self, lake: Lake, zone: str, date_time_type: str):
        """An ACTIVATE_VERSION's _sdc_deleted_at, in either column type."""
        schema = _schema(_properties(STRING))
        _zoned(
            zone,
            _run(
                lake,
                [
                    schema,
                    _activate(1),
                    *_records([{"id": 1, "v": "a"}], version=1),
                    _activate(2),
                    *_records([{"id": 2, "v": "b"}], version=2),
                    _activate(2),
                ],
                add_record_metadata=True,
                date_time_type=date_time_type,
            ),
        )
        (deleted_at,) = lake.query(
            "SELECT _sdc_deleted_at::VARCHAR FROM lake.s.orders WHERE id = 1"
        )[0]
        utc = datetime.fromisoformat(deleted_at).replace(tzinfo=timezone.utc)
        assert abs(datetime.now(timezone.utc) - utc) < timedelta(minutes=5)


@pytest.mark.parametrize("zone", ZONES)
class TestDatesAndInstantsAcrossTypes:
    """DUCKLKD-143/144: a date into TIMESTAMPTZ, an instant into DATE or TIME."""

    @pytest.mark.parametrize("date_time_type", ["timestamptz", "timestamp"])
    @pytest.mark.parametrize("load_method", LOAD_METHODS)
    def test_a_date_into_timestamptz_is_midnight_utc(
        self, lake: Lake, zone: str, load_method: str, date_time_type: str
    ):
        """Whichever date_time_type the stream has now."""
        _zoned(
            zone,
            _earlier(lake, DATE_TIME, "2024-01-01T00:00:00Z"),
            _later(
                lake,
                DATE,
                ["2024-01-02", None],
                load_method=load_method,
                date_time_type=date_time_type,
                value_type_checking="strict",
            ),
        )
        kept = [] if load_method == "overwrite" else [(7, "2024-01-01 00:00:00+00")]
        assert _rows(lake) == [(1, "2024-01-02 00:00:00+00"), (2, None), *kept]

    @pytest.mark.parametrize("load_method", ["merge", "overwrite"])
    def test_a_date_key_matches_the_row_at_midnight_utc(
        self, lake: Lake, zone: str, load_method: str
    ):
        """And replaces it."""
        _zoned(
            zone,
            _run(
                lake,
                [
                    _schema(_properties(STRING, key=DATE_TIME)),
                    *_records(
                        [
                            {"id": "2024-01-02T00:00:00Z", "v": "old"},
                            {"id": "2024-01-03T00:00:00Z", "v": "kept"},
                        ]
                    ),
                ],
            ),
            _run(
                lake,
                [
                    _schema(_properties(STRING, key=DATE)),
                    *_records([{"id": "2024-01-02", "v": "new"}]),
                ],
                load_method=load_method,
            ),
        )
        kept = (
            [] if load_method == "overwrite" else [("2024-01-03 00:00:00+00", "kept")]
        )
        assert _rows(lake, "id::VARCHAR, v") == [
            ("2024-01-02 00:00:00+00", "new"),
            *kept,
        ]

    def test_a_date_in_a_mid_run_schema(self, lake: Lake, zone: str):
        """Rows on both sides of the change."""
        _zoned(
            zone,
            _run(
                lake,
                [
                    _schema(_properties(DATE_TIME)),
                    *_records([{"id": 1, "v": "2024-01-01T12:00:00Z"}]),
                    _schema(_properties(DATE)),
                    *_records([{"id": 2, "v": "2024-01-02"}]),
                ],
            ),
        )
        assert _rows(lake) == [
            (1, "2024-01-01 12:00:00+00"),
            (2, "2024-01-02 00:00:00+00"),
        ]

    @pytest.mark.parametrize("date_time_type", ["timestamptz", "timestamp"])
    def test_a_date_into_a_timestamptz_leaf_is_midnight_utc(
        self, lake: Lake, zone: str, date_time_type: str
    ):
        """As a top-level column's value is."""
        _zoned(
            zone,
            _earlier(lake, _leaf(DATE_TIME), {"d": "2024-01-01T00:00:00Z"}),
            _later(
                lake, _leaf(DATE), [{"d": "2024-01-02"}], date_time_type=date_time_type
            ),
        )
        assert _rows(lake, "id, v.d::VARCHAR") == [
            (1, "2024-01-02 00:00:00+00"),
            (7, "2024-01-01 00:00:00+00"),
        ]

    @pytest.mark.parametrize(
        ("column", "earlier", "value", "expected"),
        [
            (DATE, "2024-01-01", "2024-01-02T19:00:00-05:00", "2024-01-03"),
            (TIME, "09:00:00", "2024-01-02T19:30:00-05:00", "00:30:00"),
        ],
        ids=["date", "time"],
    )
    def test_an_instant_into_a_date_or_time_column_is_cast_from_utc(  # noqa: PLR0913
        self,
        lake: Lake,
        zone: str,
        column: dict,
        earlier: str,
        value: str,
        expected: str,
    ):
        """2024-01-02T19:00:00-05:00 is 2024-01-03 00:00 UTC."""
        _zoned(
            zone,
            _earlier(lake, column, earlier),
            _later(lake, DATE_TIME, [value], value_type_checking="permissive"),
        )
        assert _rows(lake)[0] == (1, expected)

    def test_an_instant_into_a_date_leaf_is_cast_from_utc(self, lake: Lake, zone: str):
        """As a top-level column's value is."""
        _zoned(
            zone,
            _earlier(lake, _leaf(DATE), {"d": "2024-01-01"}),
            _later(
                lake,
                _leaf(DATE_TIME),
                [{"d": "2024-01-02T19:00:00-05:00"}],
                value_type_checking="permissive",
            ),
        )
        assert _rows(lake, "id, v.d::VARCHAR") == [(1, "2024-01-03"), (7, "2024-01-01")]
