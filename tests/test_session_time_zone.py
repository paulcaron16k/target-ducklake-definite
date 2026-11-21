"""The target's DuckDB session is in UTC, whatever the machine's zone (DUCKLKD-148).

DuckDB gives a new database the process's time zone, read once when DuckDB is
loaded, and reads a date-time without an offset in it wherever a TIMESTAMPTZ
takes one: an INSERT, a merge's key match, a date cast to TIMESTAMPTZ.
``TZ`` set in this test process can't change that, so the target runs in a
child process (``tests/zoned_run.py``) with ``TZ`` set to a zone other than
UTC: one west of it, and one with a half-hour offset.

Each scenario below loads its own catalog. All of them run in one child per
zone and catalog (the ``loaded`` fixture), which checks that the child's
DuckDB default is that zone and the target's session is UTC: after each run,
after a reconnect, and after the catalog is attached again. The tests then
read the tables here, in UTC.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pytest

from tests.catalogs import POSTGRES_ENV, Lake
from tests.test_mid_run_schema import _activate, _records, _schema

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

ZONES = ["America/Los_Angeles", "Asia/Kolkata"]
LOAD_METHODS = ["append", "merge", "overwrite"]
DATE_TIME_TYPES = ["timestamptz", "timestamp"]
DATE = {"type": ["null", "string"], "format": "date"}
DATE_TIME = {"type": ["null", "string"], "format": "date-time"}
HINTED = {**DATE_TIME, "x-sql-datatype": "timestamptz"}
TIME = {"type": ["null", "string"], "format": "time"}
STRING = {"type": ["null", "string"]}
REPO = Path(__file__).resolve().parents[1]


def _run(lake: Lake, messages: list[dict], **config: Any) -> dict:
    """One target run's payload: ``messages`` with the lake's config and ``config``."""
    return {"config": {**lake.config(), **config}, "messages": messages}


def _zoned(zone: str, runs: list[dict]) -> None:
    """Run the target once per run, in order, in a child process in ``zone``."""
    child = subprocess.run(  # noqa: S603 - this interpreter, our own module
        [sys.executable, "-m", "tests.zoned_run"],
        input=json.dumps({"runs": runs}),
        capture_output=True,
        text=True,
        cwd=REPO,
        env={**os.environ, "TZ": zone},
        check=False,
    )
    assert child.returncode == 0, child.stderr[-3000:]
    report = json.loads(child.stdout.strip().splitlines()[-1])
    assert report == {
        "default_zone": zone,
        "session_zones": ["UTC"] * len(runs),
        "reconnected_zones": ["UTC", "UTC"],
    }


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


# Scenarios: each one's runs, given its catalog.


def _into_existing_timestamptz(lake: Lake, load_method: str) -> list[dict]:
    return [
        _earlier(lake, DATE_TIME, "2024-01-01T00:00:00Z"),
        _later(
            lake,
            DATE_TIME,
            ["2024-01-02T10:00:00", "2024-01-02T10:00:00+05:30", None],
            load_method=load_method,
            date_time_type="timestamp",
        ),
    ]


def _naive_merge_key(lake: Lake) -> list[dict]:
    schema = _schema(_properties(STRING, key=DATE_TIME))
    return [
        _run(lake, [schema, *_records([{"id": "2024-01-02T10:00:00Z", "v": "a"}])]),
        _run(
            lake,
            [schema, *_records([{"id": "2024-01-02T10:00:00", "v": "b"}])],
            load_method="merge",
            date_time_type="timestamp",
        ),
    ]


def _into_hinted_timestamptz(lake: Lake) -> list[dict]:
    return [_later(lake, HINTED, ["2024-01-02T10:00:00"], date_time_type="timestamp")]


# One instant spelt three ways, in one batch.
SPELLINGS = [
    {"id": "2024-01-02T10:00:00Z", "v": "a"},
    {"id": "2024-01-02T10:00:00", "v": "b"},
    {"id": "2024-01-02T15:30:00+05:30", "v": "c"},
]


def _spellings_of_a_hinted_key(lake: Lake, date_time_type: str) -> list[dict]:
    return [
        _run(
            lake,
            [_schema(_properties(STRING, key=HINTED)), *_records(SPELLINGS)],
            load_method="merge",
            date_time_type=date_time_type,
        )
    ]


def _spellings_of_an_existing_key(lake: Lake, date_time_type: str) -> list[dict]:
    schema = _schema(_properties(STRING, key=DATE_TIME))
    return [
        _run(lake, [schema, *_records([{"id": "2024-01-02T10:00:00Z", "v": "old"}])]),
        _run(
            lake,
            [schema, *_records(SPELLINGS)],
            load_method="merge",
            date_time_type=date_time_type,
        ),
    ]


WALL_CLOCK_VALUES = ["2024-01-02T10:00:00", "2024-01-02T10:00:00+05:30"]


def _into_timestamp(lake: Lake, *, convert_tz_to_utc: bool) -> list[dict]:
    return [
        _later(
            lake,
            DATE_TIME,
            WALL_CLOCK_VALUES,
            date_time_type="timestamp",
            convert_tz_to_utc=convert_tz_to_utc,
        )
    ]


def _soft_delete(lake: Lake, date_time_type: str) -> list[dict]:
    return [
        _run(
            lake,
            [
                _schema(_properties(STRING)),
                _activate(1),
                *_records([{"id": 1, "v": "a"}], version=1),
                _activate(2),
                *_records([{"id": 2, "v": "b"}], version=2),
                _activate(2),
            ],
            add_record_metadata=True,
            date_time_type=date_time_type,
        )
    ]


def _date_into_timestamptz(
    lake: Lake, load_method: str, date_time_type: str
) -> list[dict]:
    return [
        _earlier(lake, DATE_TIME, "2024-01-01T00:00:00Z"),
        _later(
            lake,
            DATE,
            ["2024-01-02", None],
            load_method=load_method,
            date_time_type=date_time_type,
            value_type_checking="strict",
        ),
    ]


def _date_key(lake: Lake, load_method: str) -> list[dict]:
    return [
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
    ]


def _date_in_a_mid_run_schema(lake: Lake) -> list[dict]:
    return [
        _run(
            lake,
            [
                _schema(_properties(DATE_TIME)),
                *_records([{"id": 1, "v": "2024-01-01T12:00:00Z"}]),
                _schema(_properties(DATE)),
                *_records([{"id": 2, "v": "2024-01-02"}]),
            ],
        )
    ]


def _date_into_a_timestamptz_leaf(lake: Lake, date_time_type: str) -> list[dict]:
    return [
        _earlier(lake, _leaf(DATE_TIME), {"d": "2024-01-01T00:00:00Z"}),
        _later(lake, _leaf(DATE), [{"d": "2024-01-02"}], date_time_type=date_time_type),
    ]


# An instant into a DATE or a TIME column: the column, an earlier value, the
# instant, and what the column holds for it.
INSTANTS = {
    "date": (DATE, "2024-01-01", "2024-01-02T19:00:00-05:00", "2024-01-03"),
    "time": (TIME, "09:00:00", "2024-01-02T19:30:00-05:00", "00:30:00"),
}


def _instant_into(lake: Lake, kind: str) -> list[dict]:
    column, earlier, value, _ = INSTANTS[kind]
    return [
        _earlier(lake, column, earlier),
        _later(lake, DATE_TIME, [value], value_type_checking="permissive"),
    ]


def _instant_into_a_date_leaf(lake: Lake) -> list[dict]:
    return [
        _earlier(lake, _leaf(DATE), {"d": "2024-01-01"}),
        _later(
            lake,
            _leaf(DATE_TIME),
            [{"d": "2024-01-02T19:00:00-05:00"}],
            value_type_checking="permissive",
        ),
    ]


SCENARIOS: dict[str, Callable[[Lake], list[dict]]] = {
    **{
        f"existing-{m}": partial(_into_existing_timestamptz, load_method=m)
        for m in LOAD_METHODS
    },
    "naive-merge-key": _naive_merge_key,
    "hinted": _into_hinted_timestamptz,
    **{
        f"hinted-key-{t}": partial(_spellings_of_a_hinted_key, date_time_type=t)
        for t in DATE_TIME_TYPES
    },
    **{
        f"existing-key-{t}": partial(_spellings_of_an_existing_key, date_time_type=t)
        for t in DATE_TIME_TYPES
    },
    "timestamp": partial(_into_timestamp, convert_tz_to_utc=False),
    "timestamp-converted": partial(_into_timestamp, convert_tz_to_utc=True),
    **{
        f"soft-delete-{t}": partial(_soft_delete, date_time_type=t)
        for t in DATE_TIME_TYPES
    },
    **{
        f"date-{m}-{t}": partial(
            _date_into_timestamptz, load_method=m, date_time_type=t
        )
        for m in LOAD_METHODS
        for t in DATE_TIME_TYPES
    },
    **{
        f"date-key-{m}": partial(_date_key, load_method=m)
        for m in ("merge", "overwrite")
    },
    "date-mid-run": _date_in_a_mid_run_schema,
    **{
        f"date-leaf-{t}": partial(_date_into_a_timestamptz_leaf, date_time_type=t)
        for t in DATE_TIME_TYPES
    },
    **{f"instant-{kind}": partial(_instant_into, kind=kind) for kind in INSTANTS},
    "instant-leaf": _instant_into_a_date_leaf,
}


@pytest.fixture(scope="module", params=ZONES)
def zone(request: pytest.FixtureRequest) -> str:
    """The child process's zone."""
    return request.param


@pytest.fixture(scope="module", params=["sqlite", "postgres"])
def catalog(request: pytest.FixtureRequest) -> str:
    """SQLite, and Postgres when POSTGRES_ENV is set (as the ``lake`` fixture)."""
    if request.param == "postgres" and not os.environ.get(POSTGRES_ENV):
        pytest.skip(f"set {POSTGRES_ENV} to a libpq conninfo to test on Postgres")
    return request.param


@pytest.fixture(scope="module")
def loaded(
    zone: str, catalog: str, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[dict[str, Lake]]:
    """Every scenario's catalog, loaded in one child process in ``zone``."""
    lakes: dict[str, Lake] = {}
    try:
        runs = []
        for name, scenario in SCENARIOS.items():
            lakes[name] = Lake(catalog, tmp_path_factory.mktemp(name))
            runs.extend(scenario(lakes[name]))
        _zoned(zone, runs)
        yield lakes
    finally:
        for lake in lakes.values():
            lake.drop()


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


class TestDateTimesWithoutAnOffsetAreUTC:
    """DUCKLKD-148: with date_time_type timestamp, into a TIMESTAMPTZ column."""

    @pytest.mark.parametrize("load_method", LOAD_METHODS)
    def test_into_an_existing_timestamptz_column(
        self, loaded: dict[str, Lake], load_method: str
    ):
        """Read as UTC, a value with an offset as its instant, under each method."""
        lake = loaded[f"existing-{load_method}"]
        kept = [] if load_method == "overwrite" else [(7, "2024-01-01 00:00:00+00")]
        assert _rows(lake) == [
            (1, "2024-01-02 10:00:00+00"),
            (2, "2024-01-02 04:30:00+00"),
            (3, None),
            *kept,
        ]
        assert _column_type(lake) == "TIMESTAMP WITH TIME ZONE"

    def test_a_merge_key_matches_its_row(self, loaded: dict[str, Lake]):
        """The staged key and the stored key are one instant: the row is replaced."""
        lake = loaded["naive-merge-key"]
        assert _rows(lake, "id::VARCHAR, v") == [("2024-01-02 10:00:00+00", "b")]

    def test_into_a_hinted_timestamptz_column(self, loaded: dict[str, Lake]):
        """A new column the hint makes TIMESTAMPTZ."""
        assert _rows(loaded["hinted"]) == [(1, "2024-01-02 10:00:00+00")]

    @pytest.mark.parametrize("date_time_type", DATE_TIME_TYPES)
    @pytest.mark.parametrize("key", ["hinted-key", "existing-key"])
    def test_one_instant_spelt_three_ways_is_one_key(
        self, loaded: dict[str, Lake], key: str, date_time_type: str
    ):
        """'Z', naive and '+05:30' in one batch: the last record is kept."""
        lake = loaded[f"{key}-{date_time_type}"]
        assert _rows(lake, "id::VARCHAR, v") == [("2024-01-02 10:00:00+00", "c")]

    def test_a_timestamp_column_keeps_wall_clock_times(self, loaded: dict[str, Lake]):
        """Each value's own wall-clock time, as before."""
        lake = loaded["timestamp"]
        assert _column_type(lake) == "TIMESTAMP"
        assert _rows(lake) == [(1, "2024-01-02 10:00:00"), (2, "2024-01-02 10:00:00")]

    def test_convert_tz_to_utc_stores_utc_wall_clock_times(
        self, loaded: dict[str, Lake]
    ):
        """A naive value as given, one with an offset as its UTC time."""
        assert _rows(loaded["timestamp-converted"]) == [
            (1, "2024-01-02 10:00:00"),
            (2, "2024-01-02 04:30:00"),
        ]

    @pytest.mark.parametrize("date_time_type", DATE_TIME_TYPES)
    def test_deleted_at_is_now(self, loaded: dict[str, Lake], date_time_type: str):
        """An ACTIVATE_VERSION's _sdc_deleted_at, in either column type."""
        (deleted_at,) = loaded[f"soft-delete-{date_time_type}"].query(
            "SELECT _sdc_deleted_at::VARCHAR FROM lake.s.orders WHERE id = 1"
        )[0]
        utc = datetime.fromisoformat(deleted_at).replace(tzinfo=timezone.utc)
        assert abs(datetime.now(timezone.utc) - utc) < timedelta(minutes=10)


class TestDatesAndInstantsAcrossTypes:
    """DUCKLKD-143/144: a date into TIMESTAMPTZ, an instant into DATE or TIME."""

    @pytest.mark.parametrize("date_time_type", DATE_TIME_TYPES)
    @pytest.mark.parametrize("load_method", LOAD_METHODS)
    def test_a_date_into_timestamptz_is_midnight_utc(
        self, loaded: dict[str, Lake], load_method: str, date_time_type: str
    ):
        """Whichever date_time_type the stream has now."""
        lake = loaded[f"date-{load_method}-{date_time_type}"]
        kept = [] if load_method == "overwrite" else [(7, "2024-01-01 00:00:00+00")]
        assert _rows(lake) == [(1, "2024-01-02 00:00:00+00"), (2, None), *kept]

    @pytest.mark.parametrize("load_method", ["merge", "overwrite"])
    def test_a_date_key_matches_the_row_at_midnight_utc(
        self, loaded: dict[str, Lake], load_method: str
    ):
        """And replaces it."""
        kept = (
            [] if load_method == "overwrite" else [("2024-01-03 00:00:00+00", "kept")]
        )
        assert _rows(loaded[f"date-key-{load_method}"], "id::VARCHAR, v") == [
            ("2024-01-02 00:00:00+00", "new"),
            *kept,
        ]

    def test_a_date_in_a_mid_run_schema(self, loaded: dict[str, Lake]):
        """Rows on both sides of the change."""
        assert _rows(loaded["date-mid-run"]) == [
            (1, "2024-01-01 12:00:00+00"),
            (2, "2024-01-02 00:00:00+00"),
        ]

    @pytest.mark.parametrize("date_time_type", DATE_TIME_TYPES)
    def test_a_date_into_a_timestamptz_leaf_is_midnight_utc(
        self, loaded: dict[str, Lake], date_time_type: str
    ):
        """As a top-level column's value is."""
        assert _rows(loaded[f"date-leaf-{date_time_type}"], "id, v.d::VARCHAR") == [
            (1, "2024-01-02 00:00:00+00"),
            (7, "2024-01-01 00:00:00+00"),
        ]

    @pytest.mark.parametrize("kind", INSTANTS)
    def test_an_instant_into_a_date_or_time_column_is_cast_from_utc(
        self, loaded: dict[str, Lake], kind: str
    ):
        """2024-01-02T19:00:00-05:00 is 2024-01-03 00:00 UTC."""
        assert _rows(loaded[f"instant-{kind}"])[0] == (1, INSTANTS[kind][3])

    def test_an_instant_into_a_date_leaf_is_cast_from_utc(
        self, loaded: dict[str, Lake]
    ):
        """As a top-level column's value is."""
        assert _rows(loaded["instant-leaf"], "id, v.d::VARCHAR") == [
            (1, "2024-01-03"),
            (7, "2024-01-01"),
        ]
