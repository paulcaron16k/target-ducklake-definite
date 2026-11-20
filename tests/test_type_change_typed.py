"""A typed property whose column an existing table has as another type.

DUCKLKD-142..145, after DUCKLKD-137 (a string property: test_type_change).
DuckLake changes a column's type in place only to widen it, so the INSERT
casts the staged values to the column's type:

- a value the cast changes (a number other than 0 or 1 into BOOLEAN, a time
  of day into DATE, a date into TIME) follows ``value_type_checking``;
- a date into TIMESTAMPTZ is midnight UTC, whatever the session time zone;
- a pair DuckDB has no cast for (a number into DATE) fails at setup.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, ClassVar
from unittest.mock import patch

import pytest

from target_ducklake.connector import DuckLakeConnector
from tests.test_mid_run_schema import DELETED, _records, _run, _schema

if TYPE_CHECKING:
    from collections.abc import Iterator

    import duckdb

    from tests.catalogs import Lake

INTEGER = {"type": ["null", "integer"]}
NUMBER = {"type": ["null", "number"]}
BOOLEAN = {"type": ["null", "boolean"]}
STRING = {"type": ["null", "string"]}
DATE = {"type": ["null", "string"], "format": "date"}
DATE_TIME = {"type": ["null", "string"], "format": "date-time"}
TIME = {"type": ["null", "string"], "format": "time"}
LOAD_METHODS = ["append", "merge", "overwrite"]
SETUP_WARNING = "declares properties whose columns table orders has as another type"
# A session time zone other than UTC (and other than this machine's).
SESSION_ZONE = "America/Toronto"


def _properties(value: dict, key: dict | None = None) -> dict:
    return {"id": key or {"type": "integer"}, "v": value}


def _earlier(lake: Lake, schema: dict, value: object, **config: Any) -> None:
    """A table an earlier run loaded: row 7 with ``value``."""
    _run(
        lake,
        [_schema(_properties(schema)), *_records([{"id": 7, "v": value}])],
        **config,
    )


def _later(lake: Lake, schema: dict, values: list, **config: Any) -> None:
    """A run declaring ``v`` as ``schema``: rows 1, 2, ... with ``values``."""
    rows = [{"id": i, "v": v} for i, v in enumerate(values, 1)]
    _run(lake, [_schema(_properties(schema)), *_records(rows)], **config)


def _rows(lake: Lake) -> list[tuple]:
    return lake.query("SELECT id, v::VARCHAR FROM lake.s.orders ORDER BY id")


def _column_type(lake: Lake, column: str = "v") -> str:
    return lake.query(
        "SELECT data_type FROM information_schema.columns "  # noqa: S608
        f"WHERE table_name = 'orders' AND column_name = '{column}'"
    )[0][0]


@pytest.fixture
def warnings(caplog: pytest.LogCaptureFixture) -> Iterator[pytest.LogCaptureFixture]:
    """The target's warnings, caught on its own logger (the SDK replaces root's)."""
    logger = logging.getLogger("target-ducklake")
    logger.addHandler(caplog.handler)
    caplog.handler.setLevel(logging.WARNING)
    try:
        yield caplog
    finally:
        logger.removeHandler(caplog.handler)


@pytest.fixture
def session_zone(monkeypatch: pytest.MonkeyPatch) -> None:
    """The target's DuckDB session in ``SESSION_ZONE``, not UTC."""
    create = DuckLakeConnector._create_connection  # noqa: SLF001

    def in_zone(connector: DuckLakeConnector) -> duckdb.DuckDBPyConnection:
        connection = create(connector)
        connection.execute(f"SET TimeZone = '{SESSION_ZONE}'")
        return connection

    monkeypatch.setattr(DuckLakeConnector, "_create_connection", in_zone)


def _setup_warnings(warnings: pytest.LogCaptureFixture) -> list[str]:
    return [r.getMessage() for r in warnings.records if SETUP_WARNING in r.getMessage()]


class TestNumbersIntoABooleanColumn:
    """DUCKLKD-142: DuckDB casts any number but 0 to true."""

    # The declared type, and 0 and 1 in it.
    ZERO_ONE: ClassVar[list[tuple[dict, list]]] = [
        (INTEGER, [0, 1]),
        (NUMBER, [0.0, 1.0]),
    ]
    # The declared type, a value other than 0 and 1, and its text in messages.
    OTHER: ClassVar[list[tuple[dict, object, str]]] = [
        (INTEGER, 5, "5"),
        (INTEGER, -1, "-1"),
        (NUMBER, 1.5, "1.5"),
        # DuckDB casts each of these to true; staged as a DECIMAL(38, 9), they
        # failed (NaN, inf) or rounded to 0 or 1 first.
        (NUMBER, float("nan"), "nan"),
        (NUMBER, float("inf"), "inf"),
        (NUMBER, -float("inf"), "-inf"),
        (NUMBER, 1e-320, "1e-320"),
        (NUMBER, 0.9999999999, "0.9999999999"),
    ]

    @pytest.mark.parametrize("load_method", LOAD_METHODS)
    @pytest.mark.parametrize(("schema", "values"), ZERO_ONE)
    def test_0_and_1_load_even_under_strict(
        self,
        lake: Lake,
        schema: dict,
        values: list,
        load_method: str,
        warnings: pytest.LogCaptureFixture,
    ):
        """As false and true; the setup warning names the column."""
        _earlier(lake, BOOLEAN, True)  # noqa: FBT003
        _later(
            lake,
            schema,
            [*values, None],
            load_method=load_method,
            value_type_checking="strict",
        )
        kept = [] if load_method == "overwrite" else [(7, "true")]
        assert _rows(lake) == [(1, "false"), (2, "true"), (3, None), *kept]
        assert _column_type(lake) == "BOOLEAN"
        (warned,) = _setup_warnings(warnings)
        assert "'v' (BOOLEAN, declared " in warned
        assert "changes a column's type in place only to widen it" in warned
        assert "recreate the column as the declared type" in warned
        assert "Loaded as cast" not in warnings.text

    @pytest.mark.parametrize(("schema", "value", "text"), OTHER)
    def test_permissive_loads_other_numbers_as_true_with_a_warning(
        self,
        lake: Lake,
        schema: dict,
        value: object,
        text: str,
        warnings: pytest.LogCaptureFixture,
    ):
        """Counted and sampled, per batch."""
        _earlier(lake, BOOLEAN, False)  # noqa: FBT003
        _later(lake, schema, [value, 0])
        assert _rows(lake) == [(1, "true"), (2, "false"), (7, "false")]
        assert (
            f"column 'v' is BOOLEAN, and 1 value(s) aren't 0 or 1, so they load "
            f"as true: '{text}'. Loaded as cast"
        ) in warnings.text

    @pytest.mark.parametrize("load_method", LOAD_METHODS)
    @pytest.mark.parametrize(("schema", "value", "text"), OTHER)
    def test_strict_fails_the_batch(
        self, lake: Lake, schema: dict, value: object, text: str, load_method: str
    ):
        """Before it is loaded or staged: the table keeps its rows."""
        _earlier(lake, BOOLEAN, False)  # noqa: FBT003
        with pytest.raises(ValueError, match="value_type_checking is strict") as failed:
            _later(
                lake,
                schema,
                [value],
                load_method=load_method,
                value_type_checking="strict",
            )
        assert f"aren't 0 or 1, so they load as true: '{text}'" in str(failed.value)
        assert _rows(lake) == [(7, "false")]

    @pytest.mark.parametrize("load_method", LOAD_METHODS)
    def test_in_a_mid_run_schema(self, lake: Lake, load_method: str):
        """At the batch after the change; an overwrite replaces nothing."""
        _earlier(lake, BOOLEAN, False)  # noqa: FBT003
        with pytest.raises(ValueError, match="aren't 0 or 1"):
            _run(
                lake,
                [
                    _schema(_properties(BOOLEAN)),
                    *_records([{"id": 1, "v": True}]),
                    _schema(_properties(INTEGER)),
                    *_records([{"id": 2, "v": 5}]),
                ],
                load_method=load_method,
                value_type_checking="strict",
            )
        before_change = [] if load_method == "overwrite" else [(1, "true")]
        assert _rows(lake) == [*before_change, (7, "false")]

    @pytest.mark.parametrize("load_method", LOAD_METHODS)
    def test_a_row_marked_deleted_isnt_checked(self, lake: Lake, load_method: str):
        """Its value is never inserted."""
        _earlier(lake, BOOLEAN, False)  # noqa: FBT003
        properties = {**_properties(INTEGER), "_sdc_deleted_at": DATE_TIME}
        _run(
            lake,
            [
                _schema(properties),
                *_records(
                    [{"id": 7, "v": 5, "_sdc_deleted_at": DELETED}, {"id": 1, "v": 1}]
                ),
            ],
            load_method=load_method,
            delete_rows_with_deleted_at=True,
            value_type_checking="strict",
        )
        assert _rows(lake) == [(1, "true")]


class TestDateTimesIntoADateColumn:
    """DUCKLKD-143: the DATE cast drops a date-time's time of day."""

    # A date-time at midnight UTC, and the date it loads as.
    MIDNIGHT: ClassVar[list[tuple[str, str]]] = [
        ("2024-01-02T00:00:00Z", "2024-01-02"),
        ("2024-01-02T00:00:00", "2024-01-02"),
        # Midnight UTC is 19:00 the day before, five hours west.
        ("2024-01-02T19:00:00-05:00", "2024-01-03"),
    ]
    # A date-time with a time of day in UTC, the date it loads as, and the
    # value in messages (as staged, in UTC).
    OTHER: ClassVar[list[tuple[str, str, str]]] = [
        ("2024-01-02T03:04:05Z", "2024-01-02", "2024-01-02 03:04:05.000000Z"),
        ("2024-01-02T00:00:00-05:00", "2024-01-02", "2024-01-02 05:00:00.000000Z"),
        ("2024-01-02T23:30:00+05:00", "2024-01-02", "2024-01-02 18:30:00.000000Z"),
    ]

    @pytest.mark.parametrize("load_method", LOAD_METHODS)
    @pytest.mark.parametrize(("value", "loaded"), MIDNIGHT)
    def test_midnight_utc_loads_even_under_strict(
        self,
        lake: Lake,
        value: str,
        loaded: str,
        load_method: str,
        session_zone: None,  # noqa: ARG002
    ):
        """As its date in UTC."""
        _earlier(lake, DATE, "2024-01-01")
        _later(
            lake,
            DATE_TIME,
            [value],
            load_method=load_method,
            value_type_checking="strict",
        )
        kept = [] if load_method == "overwrite" else [(7, "2024-01-01")]
        assert _rows(lake) == [(1, loaded), *kept]
        assert _column_type(lake) == "DATE"

    @pytest.mark.parametrize(("value", "loaded", "text"), OTHER)
    def test_permissive_loads_the_date_with_a_warning(
        self,
        lake: Lake,
        value: str,
        loaded: str,
        text: str,
        warnings: pytest.LogCaptureFixture,
    ):
        """Its date in UTC; counted and sampled."""
        _earlier(lake, DATE, "2024-01-01")
        _later(lake, DATE_TIME, [value])
        assert _rows(lake)[0] == (1, loaded)
        assert (
            f"column 'v' is DATE, and 1 value(s) lose their time of day in its "
            f"cast: '{text}'. Loaded as cast"
        ) in warnings.text
        (warned,) = _setup_warnings(warnings)
        assert "'v' (DATE, declared TIMESTAMPTZ)" in warned

    @pytest.mark.parametrize("load_method", LOAD_METHODS)
    @pytest.mark.parametrize(("value", "loaded", "text"), OTHER)
    def test_strict_fails_the_batch(
        self, lake: Lake, value: str, loaded: str, text: str, load_method: str
    ):
        """Before it is loaded or staged: the table keeps its rows."""
        _earlier(lake, DATE, "2024-01-01")
        with pytest.raises(ValueError, match="value_type_checking is strict") as failed:
            _later(
                lake,
                DATE_TIME,
                [value],
                load_method=load_method,
                value_type_checking="strict",
            )
        assert f"lose their time of day in its cast: '{text}'" in str(failed.value)
        assert _rows(lake) == [(7, "2024-01-01")]
        assert loaded  # what permissive would have stored

    @pytest.mark.parametrize("load_method", LOAD_METHODS)
    def test_in_a_mid_run_schema(self, lake: Lake, load_method: str):
        """At the batch after the change."""
        _earlier(lake, DATE, "2024-01-01")
        with pytest.raises(ValueError, match="lose their time of day"):
            _run(
                lake,
                [
                    _schema(_properties(DATE)),
                    *_records([{"id": 1, "v": "2024-01-05"}]),
                    _schema(_properties(DATE_TIME)),
                    *_records([{"id": 2, "v": "2024-01-06T12:00:00Z"}]),
                ],
                load_method=load_method,
                value_type_checking="strict",
            )
        before_change = [] if load_method == "overwrite" else [(1, "2024-01-05")]
        assert _rows(lake) == [*before_change, (7, "2024-01-01")]

    def test_wall_clock_times_with_date_time_type_timestamp(self, lake: Lake):
        """A TIMESTAMP property's own wall-clock date: its offset is dropped."""
        _earlier(lake, DATE, "2024-01-01")
        _later(
            lake,
            DATE_TIME,
            ["2024-01-02T00:00:00-05:00"],
            date_time_type="timestamp",
            value_type_checking="strict",
        )
        assert _rows(lake)[0] == (1, "2024-01-02")

    @pytest.mark.parametrize("load_method", LOAD_METHODS)
    def test_a_key_is_checked_on_a_row_marked_deleted(
        self, lake: Lake, load_method: str
    ):
        """Its key is cast to find the row it deletes."""
        _run(
            lake,
            [
                _schema(_properties(STRING, key=DATE)),
                *_records([{"id": "2024-01-02", "v": "a"}]),
            ],
        )
        properties = {
            **_properties(STRING, key=DATE_TIME),
            "_sdc_deleted_at": DATE_TIME,
        }
        with pytest.raises(ValueError, match="column 'id' is DATE, and 1 value"):
            _run(
                lake,
                [
                    _schema(properties),
                    *_records(
                        [
                            {
                                "id": "2024-01-02T03:00:00Z",
                                "v": "b",
                                "_sdc_deleted_at": DELETED,
                            }
                        ]
                    ),
                ],
                load_method=load_method,
                delete_rows_with_deleted_at=True,
                value_type_checking="strict",
            )
        assert lake.query("SELECT id::VARCHAR, v FROM lake.s.orders") == [
            ("2024-01-02", "a")
        ]

    def test_keys_that_cast_to_one_date(
        self, lake: Lake, warnings: pytest.LogCaptureFixture
    ):
        """Two instants of one date load as two rows with that key: warned."""
        _run(
            lake,
            [
                _schema(_properties(STRING, key=DATE)),
                *_records([{"id": "2024-01-01", "v": "a"}]),
            ],
        )
        _run(
            lake,
            [
                _schema(_properties(STRING, key=DATE_TIME)),
                *_records(
                    [
                        {"id": "2024-01-02T01:00:00Z", "v": "b"},
                        {"id": "2024-01-02T02:00:00Z", "v": "c"},
                    ]
                ),
            ],
            load_method="merge",
        )
        assert (
            "1 key value(s) cast to the key of another, so their records load as "
            "separate rows with one key: '2024-01-02 01:00:00.000000Z' = "
            "'2024-01..."
        ) in warnings.text


class TestDatesIntoATimestampColumn:
    """DUCKLKD-144: a date is midnight UTC, whatever the session time zone."""

    TIMESTAMPTZ = "2024-01-02 00:00:00+00"

    @pytest.mark.parametrize("date_time_type", ["timestamptz", "timestamp"])
    @pytest.mark.parametrize("load_method", LOAD_METHODS)
    def test_into_timestamptz(
        self,
        lake: Lake,
        load_method: str,
        date_time_type: str,
        session_zone: None,  # noqa: ARG002
    ):
        """Whichever date_time_type the stream has now."""
        _earlier(lake, DATE_TIME, "2024-01-01T00:00:00Z")
        _later(
            lake,
            DATE,
            ["2024-01-02", None],
            load_method=load_method,
            date_time_type=date_time_type,
            value_type_checking="strict",
        )
        kept = [] if load_method == "overwrite" else [(7, "2024-01-01 00:00:00+00")]
        assert _rows(lake) == [(1, self.TIMESTAMPTZ), (2, None), *kept]
        assert _column_type(lake) == "TIMESTAMP WITH TIME ZONE"

    @pytest.mark.parametrize("load_method", LOAD_METHODS)
    def test_in_a_mid_run_schema(
        self,
        lake: Lake,
        load_method: str,
        session_zone: None,  # noqa: ARG002
    ):
        """The rows on both sides of the change load."""
        _run(
            lake,
            [
                _schema(_properties(DATE_TIME)),
                *_records([{"id": 1, "v": "2024-01-01T12:00:00Z"}]),
                _schema(_properties(DATE)),
                *_records([{"id": 2, "v": "2024-01-02"}]),
            ],
            load_method=load_method,
        )
        assert _rows(lake) == [(1, "2024-01-01 12:00:00+00"), (2, self.TIMESTAMPTZ)]

    @pytest.mark.parametrize("load_method", ["merge", "overwrite"])
    def test_a_date_key_matches_the_row_at_midnight_utc(
        self,
        lake: Lake,
        load_method: str,
        session_zone: None,  # noqa: ARG002
    ):
        """And replaces it."""
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
        )
        _run(
            lake,
            [
                _schema(_properties(STRING, key=DATE)),
                *_records([{"id": "2024-01-02", "v": "new"}]),
            ],
            load_method=load_method,
        )
        kept = (
            [] if load_method == "overwrite" else [("2024-01-03 00:00:00+00", "kept")]
        )
        assert lake.query("SELECT id::VARCHAR, v FROM lake.s.orders ORDER BY id") == [
            (self.TIMESTAMPTZ, "new"),
            *kept,
        ]

    @pytest.mark.parametrize("load_method", LOAD_METHODS)
    def test_into_timestamp(
        self,
        lake: Lake,
        load_method: str,
        session_zone: None,  # noqa: ARG002
    ):
        """Midnight, as before."""
        _earlier(lake, DATE_TIME, "2024-01-01T00:00:00", date_time_type="timestamp")
        _later(
            lake,
            DATE,
            ["2024-01-02"],
            load_method=load_method,
            value_type_checking="strict",
        )
        assert _rows(lake)[0] == (1, "2024-01-02 00:00:00")
        assert _column_type(lake) == "TIMESTAMP"


class TestPairsWithoutACast:
    """DUCKLKD-145: no value loads, so the stream fails at setup."""

    # The earlier type and a value, its column type; the later declared type,
    # a value, and its column type.
    PAIRS: ClassVar[list[tuple[dict, object, str, dict, object, str]]] = [
        (DATE, "2024-01-02", "DATE", INTEGER, 5, "BIGINT"),
        (DATE, "2024-01-02", "DATE", NUMBER, 1.5, "DECIMAL(38, 9)"),
        (DATE, "2024-01-02", "DATE", BOOLEAN, True, "BOOLEAN"),
        (
            DATE_TIME,
            "2024-01-02T00:00:00Z",
            "TIMESTAMP WITH TIME ZONE",
            INTEGER,
            5,
            "BIGINT",
        ),
        (
            DATE_TIME,
            "2024-01-02T00:00:00Z",
            "TIMESTAMP WITH TIME ZONE",
            BOOLEAN,
            True,
            "BOOLEAN",
        ),
        (TIME, "03:04:05", "TIME", NUMBER, 1.5, "DECIMAL(38, 9)"),
        (INTEGER, 5, "BIGINT", DATE, "2024-01-02", "DATE"),
        (
            NUMBER,
            1.5,
            "DECIMAL(38,9)",
            DATE_TIME,
            "2024-01-02T00:00:00Z",
            "TIMESTAMPTZ",
        ),
        (BOOLEAN, True, "BOOLEAN", TIME, "03:04:05", "TIME"),
        (DATE, "2024-01-02", "DATE", TIME, "03:04:05", "TIME"),
        (
            DATE_TIME,
            "2024-01-02T00:00:00Z",
            "TIMESTAMP WITH TIME ZONE",
            TIME,
            "03:04:05",
            "TIME",
        ),
    ]

    @pytest.mark.parametrize("load_method", LOAD_METHODS)
    @pytest.mark.parametrize(
        ("earlier", "value", "column_type", "later", "later_value", "declared"),
        PAIRS,
    )
    def test_a_batch_with_a_value_fails(  # noqa: PLR0913
        self,
        lake: Lake,
        earlier: dict,
        value: object,
        column_type: str,
        later: dict,
        later_value: object,
        declared: str,
        load_method: str,
        warnings: pytest.LogCaptureFixture,
    ):
        """Naming the stream, the column, both types and the fixes.

        Warned at setup; the batch isn't loaded, and an overwrite replaces
        nothing.
        """
        _earlier(lake, earlier, value)
        before = _rows(lake)
        with pytest.raises(ValueError, match="no value of it can load") as refused:
            _later(lake, later, [later_value], load_method=load_method)
        message = str(refused.value)
        assert (
            f"stream 'orders', table orders: column 'v' is {column_type}, and the "
            f"stream declares it {declared}, which DuckDB can't cast to "
            f"{column_type}, so no value of it can load: 1 value(s) in this batch"
        ) in message
        assert "The batch wasn't loaded." in message
        assert "declare each property as its column's type again" in message
        assert "load the stream into a new table" in message
        assert "recreate the column as the declared type" in message
        assert _rows(lake) == before
        assert _column_type(lake) == column_type
        (warned,) = _setup_warnings(warnings)
        assert f"DuckDB can't cast 'v' ({column_type}, declared {declared})" in warned

    @pytest.mark.parametrize("load_method", LOAD_METHODS)
    def test_no_records_and_null_values_load(self, lake: Lake, load_method: str):
        """As before: no value of the column is cast."""
        _earlier(lake, DATE, "2024-01-01")
        kept = [] if load_method == "overwrite" else [(7, "2024-01-01")]
        _run(lake, [_schema(_properties(INTEGER))], load_method=load_method)
        assert _rows(lake) == kept  # an overwrite of no records empties it
        _later(lake, INTEGER, [None, None], load_method=load_method)
        assert _rows(lake) == [(1, None), (2, None), *kept]

    @pytest.mark.parametrize("load_method", LOAD_METHODS)
    def test_at_the_first_batch_with_a_value(self, lake: Lake, load_method: str):
        """Batches of NULLs before it load (or are staged).

        An overwrite replaces nothing.
        """
        _earlier(lake, DATE, "2024-01-01")
        with pytest.raises(ValueError, match="no value of it can load: 1 value"):
            _later(
                lake,
                INTEGER,
                [None, None, 5, None],
                load_method=load_method,
                batch_size_rows=1,
            )
        loaded = [] if load_method == "overwrite" else [(1, None), (2, None)]
        assert _rows(lake) == [*loaded, (7, "2024-01-01")]

    @pytest.mark.parametrize("load_method", LOAD_METHODS)
    def test_in_a_mid_run_schema(self, lake: Lake, load_method: str):
        """At the batch after it.

        Records before it load as an append or merge would.
        """
        _earlier(lake, DATE, "2024-01-01")
        with pytest.raises(ValueError, match="no value of it can load"):
            _run(
                lake,
                [
                    _schema(_properties(DATE)),
                    *_records([{"id": 1, "v": "2024-01-05"}]),
                    _schema(_properties(INTEGER)),
                    *_records([{"id": 2, "v": 5}]),
                ],
                load_method=load_method,
            )
        before_change = [] if load_method == "overwrite" else [(1, "2024-01-05")]
        assert _rows(lake) == [*before_change, (7, "2024-01-01")]

    @pytest.mark.parametrize("load_method", LOAD_METHODS)
    def test_nulls_after_a_mid_run_schema_load(self, lake: Lake, load_method: str):
        """Both sides of the change load."""
        _earlier(lake, DATE, "2024-01-01")
        _run(
            lake,
            [
                _schema(_properties(DATE)),
                *_records([{"id": 1, "v": "2024-01-05"}]),
                _schema(_properties(INTEGER)),
                *_records([{"id": 2, "v": None}]),
            ],
            load_method=load_method,
        )
        kept = [] if load_method == "overwrite" else [(7, "2024-01-01")]
        assert _rows(lake) == [(1, "2024-01-05"), (2, None), *kept]

    def test_every_column_is_named(self, lake: Lake):
        """In one error."""
        properties = {"id": {"type": "integer"}, "a": DATE, "b": TIME}
        _run(
            lake,
            [
                _schema(properties),
                *_records([{"id": 1, "a": "2024-01-02", "b": "03:04:05"}]),
            ],
        )
        numbers = {"id": {"type": "integer"}, "a": INTEGER, "b": BOOLEAN}
        with pytest.raises(ValueError, match="column 'a' is DATE") as refused:
            _run(lake, [_schema(numbers), *_records([{"id": 2, "a": 1, "b": True}])])
        assert "column 'b' is TIME, and the stream declares it BOOLEAN" in str(
            refused.value
        )


class TestOtherChangedValues:
    """Neighbours: other pairs whose cast changes values, or can fail."""

    # The earlier type, a value and its column type; the later declared type,
    # a value, what it loads as, and the reason in messages.
    CHANGED: ClassVar[list[tuple[dict, object, str, dict, object, str, str]]] = [
        (TIME, "01:00:00", "TIME", DATE, "2024-01-02", "00:00:00", "lose their date"),
        (
            TIME,
            "01:00:00",
            "TIME",
            DATE_TIME,
            "2024-01-02T03:04:05Z",
            "03:04:05",
            "lose their date",
        ),
        # A string property into a TIME column (as DUCKLKD-137 checks text).
        (
            TIME,
            "01:00:00",
            "TIME",
            STRING,
            "2024-01-02 03:04:05",
            "03:04:05",
            "lose their date",
        ),
    ]

    @pytest.mark.parametrize(
        ("earlier", "value", "column_type", "later", "later_value", "loaded", "why"),
        CHANGED,
    )
    def test_permissive_loads_it_with_a_warning(  # noqa: PLR0913
        self,
        lake: Lake,
        earlier: dict,
        value: object,
        column_type: str,
        later: dict,
        later_value: object,
        loaded: str,
        why: str,
        warnings: pytest.LogCaptureFixture,
    ):
        """As cast."""
        _earlier(lake, earlier, value)
        _later(lake, later, [later_value])
        assert _rows(lake)[0] == (1, loaded)
        assert f"column 'v' is {column_type}, and 1 value(s) {why} in its cast" in (
            warnings.text
        )

    @pytest.mark.parametrize("load_method", LOAD_METHODS)
    @pytest.mark.parametrize(
        ("earlier", "value", "column_type", "later", "later_value", "loaded", "why"),
        CHANGED,
    )
    def test_strict_fails_the_batch(  # noqa: PLR0913
        self,
        lake: Lake,
        earlier: dict,
        value: object,
        column_type: str,
        later: dict,
        later_value: object,
        loaded: str,
        why: str,
        load_method: str,
    ):
        """The table keeps its rows."""
        _earlier(lake, earlier, value)
        before = _rows(lake)
        with pytest.raises(
            ValueError, match=f"is {column_type}, and 1 .*{why}.*strict"
        ):
            _later(
                lake,
                later,
                [later_value],
                load_method=load_method,
                value_type_checking="strict",
            )
        assert _rows(lake) == before
        assert loaded  # what permissive would have stored

    def test_a_time_loads_into_a_time_column_under_strict(self, lake: Lake):
        """Text without a date isn't changed."""
        _earlier(lake, TIME, "01:00:00")
        _later(lake, STRING, ["03:04:05"], value_type_checking="strict")
        assert _rows(lake)[0] == (1, "03:04:05")

    @pytest.mark.parametrize("load_method", LOAD_METHODS)
    def test_an_integer_a_decimal_column_cant_hold_fails_the_batch(
        self, lake: Lake, load_method: str
    ):
        """In both modes, at its batch: an overwrite replaces nothing."""
        sized = {"type": ["null", "number"], "precision": 4, "scale": 2}
        _earlier(lake, sized, 1.5)
        with pytest.raises(
            ValueError,
            match=r"column 'v' is DECIMAL\(4,2\), and 1 value\(s\) aren't: '100'\. "
            "The stream declares 'v' as BIGINT",
        ):
            _later(lake, INTEGER, [1, 100], load_method=load_method, batch_size_rows=1)
        loaded = [] if load_method == "overwrite" else [(1, "1.00")]
        assert _rows(lake) == [*loaded, (7, "1.50")]

    @pytest.mark.parametrize(
        ("earlier", "value", "later", "later_value", "loaded"),
        [
            (INTEGER, 5, BOOLEAN, True, "1"),
            (NUMBER, 1.5, INTEGER, 7, "7.000000000"),
            (NUMBER, 1.5, BOOLEAN, True, "1.000000000"),
        ],
    )
    def test_numbers_and_booleans_load_into_number_columns(  # noqa: PLR0913
        self,
        lake: Lake,
        earlier: dict,
        value: object,
        later: dict,
        later_value: object,
        loaded: str,
    ):
        """Unchanged, even under strict."""
        _earlier(lake, earlier, value)
        _later(lake, later, [later_value], value_type_checking="strict")
        assert _rows(lake)[0] == (1, loaded)


class TestSetup:
    """One warning per sink; nothing to pay for a stream without such columns."""

    def test_one_warning_names_every_column(
        self, lake: Lake, warnings: pytest.LogCaptureFixture
    ):
        """With its type and the declared one."""
        earlier = {"id": {"type": "integer"}, "a": BOOLEAN, "b": DATE, "c": INTEGER}
        _run(
            lake,
            [
                _schema(earlier),
                *_records([{"id": 1, "a": True, "b": "2024-01-02", "c": 5}]),
            ],
        )
        later = {"id": {"type": "integer"}, "a": INTEGER, "b": DATE_TIME, "c": STRING}
        _run(
            lake,
            [
                _schema(later),
                *_records([{"id": 2, "a": 1, "b": "2024-01-03T00:00:00Z", "c": "6"}]),
            ],
        )
        (warned,) = _setup_warnings(warnings)
        assert (
            "'a' (BOOLEAN, declared BIGINT), 'b' (DATE, declared TIMESTAMPTZ), "
            "'c' (BIGINT, declared VARCHAR)"
        ) in warned

    def test_no_warning_and_no_check_without_such_columns(
        self, lake: Lake, warnings: pytest.LogCaptureFixture
    ):
        """A stream whose properties keep their columns' types pays nothing."""
        properties = {
            "id": {"type": "integer"},
            "a": BOOLEAN,
            "b": DATE,
            "c": DATE_TIME,
        }
        row = {"id": 1, "a": True, "b": "2024-01-02", "c": "2024-01-02T00:00:00Z"}
        _run(lake, [_schema(properties), *_records([row])])
        with patch.object(DuckLakeConnector, "cast_findings") as check:
            _run(lake, [_schema(properties), *_records([{**row, "id": 2}])])
        check.assert_not_called()
        assert _setup_warnings(warnings) == []

    @pytest.mark.parametrize(
        ("prop", "column", "config"),
        [
            ("My Col", "my_col", {"conform_column_names": True}),
            # Truncated to 63 characters on a Postgres catalog.
            ("c" * 70, "c" * 70, {}),
        ],
    )
    def test_columns_are_named_as_the_table_has_them(
        self, lake: Lake, prop: str, column: str, config: dict
    ):
        """By the name the stream loads them as."""
        if lake.kind == "postgres":
            column = column[:63]
        _run(
            lake,
            [
                _schema({"id": {"type": "integer"}, prop: BOOLEAN}),
                *_records([{"id": 7, prop: True}]),
            ],
            **config,
        )
        integers = _schema({"id": {"type": "integer"}, prop: INTEGER})
        _run(lake, [integers, *_records([{"id": 1, prop: 0}])], **config)
        with pytest.raises(ValueError, match=rf"column '{column}' is BOOLEAN, and 1"):
            _run(
                lake,
                [integers, *_records([{"id": 2, prop: 5}])],
                value_type_checking="strict",
                **config,
            )
        dates = _schema({"id": {"type": "integer"}, prop: DATE})
        with pytest.raises(ValueError, match=rf"column '{column}' is BOOLEAN, and the"):
            _run(lake, [dates, *_records([{"id": 3, prop: "2024-01-02"}])], **config)
        query = f'SELECT id, "{column}" FROM lake.s.orders ORDER BY id'  # noqa: S608
        assert lake.query(query) == [(1, False), (7, True)]


class TestNestedLeaves:
    """A STRUCT leaf takes the rules of a top-level column of its type."""

    @pytest.mark.parametrize("date_time_type", ["timestamptz", "timestamp"])
    def test_a_date_into_a_timestamptz_leaf_is_midnight_utc(
        self,
        lake: Lake,
        date_time_type: str,
        session_zone: None,  # noqa: ARG002
    ):
        """Whichever date_time_type the stream has now (DUCKLKD-144)."""

        def leaf(schema: dict) -> dict:
            return {"type": ["null", "object"], "properties": {"d": schema}}

        _earlier(lake, leaf(DATE_TIME), {"d": "2024-01-01T00:00:00Z"})
        _later(lake, leaf(DATE), [{"d": "2024-01-02"}], date_time_type=date_time_type)
        assert lake.query("SELECT id, v.d::VARCHAR FROM lake.s.orders ORDER BY id") == [
            (1, "2024-01-02 00:00:00+00"),
            (7, "2024-01-01 00:00:00+00"),
        ]


class TestPairsThatCantChangeAValue:
    """A date into a timestamp, a boolean into a number: logged, not checked."""

    @pytest.mark.parametrize(
        ("earlier", "value", "later", "later_value", "loaded"),
        [
            (
                DATE_TIME,
                "2024-01-01T00:00:00Z",
                DATE,
                "2024-01-02",
                "2024-01-02 00:00:00+00",
            ),
            (INTEGER, 5, BOOLEAN, True, "1"),
            (NUMBER, 1.5, BOOLEAN, True, "1.000000000"),
        ],
    )
    def test_no_query_and_no_warning(  # noqa: PLR0913
        self,
        lake: Lake,
        earlier: dict,
        value: object,
        later: dict,
        later_value: object,
        loaded: str,
        warnings: pytest.LogCaptureFixture,
    ):
        """The setup message is INFO: no value can change or fail."""
        _earlier(lake, earlier, value)
        warnings.handler.setLevel(logging.INFO)
        with patch.object(DuckLakeConnector, "cast_findings") as check:
            _later(lake, later, [later_value], value_type_checking="strict")
        check.assert_not_called()
        assert _rows(lake)[0] == (1, loaded)
        (logged,) = [r for r in warnings.records if SETUP_WARNING in r.getMessage()]
        assert logged.levelno == logging.INFO

    def test_a_key_column_is_still_checked(self, lake: Lake):
        """For keys that cast to one key."""
        _run(
            lake,
            [
                _schema(_properties(STRING, key=DATE_TIME)),
                *_records([{"id": "2024-01-01T00:00:00Z", "v": "a"}]),
            ],
        )
        with patch.object(
            DuckLakeConnector,
            "cast_findings",
            autospec=True,
            side_effect=DuckLakeConnector.cast_findings,
        ) as check:
            _run(
                lake,
                [
                    _schema(_properties(STRING, key=DATE)),
                    *_records([{"id": "2024-01-02", "v": "b"}]),
                ],
                load_method="merge",
            )
        check.assert_called_once()
