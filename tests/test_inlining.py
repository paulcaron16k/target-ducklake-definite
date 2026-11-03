"""Data inlining (DUCKLKD-39) and its nested-column workaround (DUCKLKD-110)."""

from __future__ import annotations

import io
import json
import re
import subprocess
import sys
from typing import TYPE_CHECKING

import pytest

from target_ducklake.connector import DuckLakeConnector
from target_ducklake.layout import (
    Inlining,
    LayoutError,
    check_layout_config,
    resolve_inlining,
)
from target_ducklake.target import Targetducklake

if TYPE_CHECKING:
    from tests.catalogs import Lake

pytestmark = pytest.mark.ducklake_options

LIST_FILES = "SELECT data_file FROM ducklake_list_files('lake', 't', schema => 's')"


def _run(lake: Lake, records: int, props: dict | None = None, **config: object) -> None:
    """Load ``records`` records into s.t, keyed by id."""
    schema = {
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "v": {"type": "string"},
            **(props or {}),
        },
    }
    messages = [
        {"type": "SCHEMA", "stream": "t", "schema": schema, "key_properties": ["id"]},
        *(
            {"type": "RECORD", "stream": "t", "record": {"id": i, "v": f"r{i}"}}
            for i in range(records)
        ),
    ]
    target = Targetducklake(config={**lake.config(), **config})
    try:
        target.listen(
            file_input=io.StringIO("".join(json.dumps(m) + "\n" for m in messages))
        )
    finally:
        target.target_connector.close()


def _table_option(lake: Lake) -> list[tuple]:
    return lake.query(
        "SELECT value FROM lake.options() WHERE option_name = "
        "'data_inlining_row_limit' AND scope = 'TABLE' AND scope_entry = 's.t'"
    )


class TestInliningSettings:
    """data_inlining_row_limit and inlined_data_flush_rows, resolved per stream."""

    @pytest.mark.parametrize(
        ("config", "expected"),
        [
            ({}, Inlining(0, 10000)),
            ({"data_inlining_row_limit": 50}, Inlining(50, 10000)),
            (
                {
                    "data_inlining_row_limit": 50,
                    "stream_defaults": {"inlined_data_flush_rows": 0},
                    "stream_overrides": {"t": {"data_inlining_row_limit": 7}},
                },
                Inlining(7, 0),
            ),
            (
                {"stream_overrides": {"other": {"data_inlining_row_limit": 9}}},
                Inlining(0, 10000),
            ),
        ],
        ids=["defaults", "top-level", "defaults-and-override", "other-stream"],
    )
    def test_precedence(self, config: dict, expected: Inlining):
        """Override, then stream_defaults, then the top level, then the default."""
        assert resolve_inlining(config, "t") == expected

    @pytest.mark.parametrize(
        "config",
        [
            {"stream_overrides": {"t": {"data_inlining_row_limit": -1}}},
            {"stream_defaults": {"inlined_data_flush_rows": "many"}},
            {"stream_overrides": {"t": {"data_inlining_row_limit": True}}},
            {"stream_overrides": {"t": {"data_inlining_row_limit": 1.5}}},
            {"data_inlining_row_limit": -3},
        ],
        ids=["negative", "text", "boolean", "fraction", "top-level-negative"],
    )
    def test_a_bad_value_is_rejected_at_startup(self, config: dict):
        """Named, with the setting, for every stream."""
        with pytest.raises(LayoutError, match="must be a whole number of at least 0"):
            check_layout_config(config)

    def test_a_duckdb_catalog_is_attached_without_inlining(self):
        """As every other catalog type: tables opt in (it used DuckLake's default)."""
        connector = DuckLakeConnector(
            {"catalog_url": "x", "data_path": "/x", "catalog_type": "duckdb"}
        )
        assert "(DATA_INLINING_ROW_LIMIT 0)" in connector._build_attach_statement()


class TestInliningThroughListen:
    """Through listen() on a real catalog (SQLite, and Postgres when set)."""

    def test_off_by_default(self, lake: Lake):
        """Every batch is a Parquet file, and the table gets no option."""
        _run(lake, 3)
        assert len(lake.query(LIST_FILES)) == 1
        assert _table_option(lake) == []

    def test_small_inserts_are_inlined(self, lake: Lake):
        """A batch of at most the limit is stored in the catalog: no file."""
        for _ in range(2):  # the second run finds the option set and keeps it
            _run(lake, 3, data_inlining_row_limit=10)
        assert lake.query(LIST_FILES) == []
        assert _table_option(lake) == [("10",)]
        assert lake.query("SELECT id, v FROM lake.s.t ORDER BY id") == [
            (0, "r0"),
            (1, "r1"),
            (2, "r2"),
        ]

    def test_a_batch_over_the_limit_is_a_file(self, lake: Lake):
        """Inlining applies per insert."""
        _run(lake, 5, data_inlining_row_limit=4)
        assert len(lake.query(LIST_FILES)) == 1

    def test_per_stream(self, lake: Lake):
        """stream_overrides turns it on for one stream."""
        _run(lake, 3, stream_overrides={"t": {"data_inlining_row_limit": 10}})
        assert lake.query(LIST_FILES) == []

    def test_inlined_rows_are_flushed_once_they_reach_the_threshold(self, lake: Lake):
        """At the end of the stream, to one Parquet file; below it, they stay."""
        _run(lake, 3, data_inlining_row_limit=10, inlined_data_flush_rows=5)
        assert lake.query(LIST_FILES) == []
        _run(lake, 3, data_inlining_row_limit=10, inlined_data_flush_rows=5)
        # Merged on id: 3 rows, 6 inlined versions, 3 of them current.
        assert len(lake.query(LIST_FILES)) == 0
        _run(lake, 6, data_inlining_row_limit=10, inlined_data_flush_rows=5)
        assert len(lake.query(LIST_FILES)) == 1
        assert lake.query("SELECT count(*) FROM lake.s.t") == [(6,)]

    def test_a_threshold_of_zero_never_flushes(self, lake: Lake):
        """Left to maintenance."""
        _run(lake, 9, data_inlining_row_limit=10, inlined_data_flush_rows=0)
        assert lake.query(LIST_FILES) == []

    def test_turning_inlining_off_flushes_what_was_inlined(self, lake: Lake):
        """The option is set back to 0, and no row stays in the catalog."""
        _run(lake, 3, data_inlining_row_limit=10)
        _run(lake, 2)
        assert _table_option(lake) == [("0",)]
        assert len(lake.query(LIST_FILES)) == 2  # the flushed rows, the new batch
        assert lake.query("SELECT count(*) FROM lake.s.t") == [(3,)]

    @pytest.mark.parametrize(
        "prop",
        [
            {
                "type": "object",
                "properties": {"at": {"type": "string", "format": "date-time"}},
            },
            {"type": "array", "items": {"type": "integer"}},
        ],
        ids=["struct", "list"],
    )
    def test_kept_off_for_a_table_with_nested_columns(self, lake: Lake, prop: dict):
        """DUCKLKD-110: the rows go to a file, and read back."""
        props = {"o": prop}
        _run(lake, 3, props, data_inlining_row_limit=10)
        assert len(lake.query(LIST_FILES)) == 1
        assert _table_option(lake) in ([], [("0",)])
        assert lake.query("SELECT count(*) FROM lake.s.t") == [(3,)]

    def test_a_nested_column_added_later_turns_inlining_off(self, lake: Lake):
        """The rows inlined before are flushed, then inlining stays off."""
        _run(lake, 3, data_inlining_row_limit=10)
        props = {"o": {"type": "object", "properties": {"n": {"type": "integer"}}}}
        _run(lake, 2, props, data_inlining_row_limit=10)
        assert _table_option(lake) == [("0",)]
        assert len(lake.query(LIST_FILES)) == 2
        assert lake.query("SELECT count(*), count(o) FROM lake.s.t") == [(3, 0)]


# DuckLake 1.5.2 fails to read these back inlined (probed 2026-11-03). When a
# DuckDB/DuckLake upgrade fixes them, this test fails: narrow or drop the
# workaround in ducklakeSink._apply_inlining (DUCKLKD-110).
UNREADABLE_INLINED = {
    "struct-timestamptz": (
        "STRUCT(a TIMESTAMPTZ)",
        "{'a': '2024-01-01 12:00:00+00'::TIMESTAMPTZ}",
    ),
    "struct-timestamp": (
        "STRUCT(a TIMESTAMP)",
        "{'a': '2024-01-01 12:00:00'::TIMESTAMP}",
    ),
    "struct-interval": ("STRUCT(a INTERVAL)", "{'a': INTERVAL 1 DAY}"),
    "struct-uuid": ("STRUCT(a UUID)", "{'a': gen_random_uuid()}"),
    "json-list": ("JSON[]", "['{\"a\": 1}'::JSON]"),
}


# How each catalog reports the failure: SQLite as a conversion error,
# Postgres as an internal error that invalidates the database.
UNREADABLE_ERROR = (
    r"Failed to read inlined data|Attempted to access index 0 within vector of size 0"
)


# Run in a child process: on Postgres the failure invalidates the DuckDB
# database, which then keeps its pooled Postgres connection (and its open
# transaction) until the process ends, blocking the fixture's DROP SCHEMA.
_READ_BACK = """
import sys, duckdb
attach, column_type, value = sys.argv[1:4]
con = duckdb.connect()
con.execute(attach)
con.execute(f"CREATE TABLE lake.u (c {column_type})")
con.execute(f"INSERT INTO lake.u VALUES ({value})")
try:
    con.execute("SELECT c::VARCHAR FROM lake.u").fetchall()
except duckdb.Error as error:
    print(f"{type(error).__name__}: {error}")
else:
    print("read back")
"""


@pytest.mark.parametrize("case", list(UNREADABLE_INLINED))
def test_ducklake_still_cant_read_these_inlined(lake: Lake, case: str):
    """Pins the upstream bug the nested-column workaround exists for."""
    column_type, value = UNREADABLE_INLINED[case]
    attach = lake.attach_sql().replace(
        "DATA_INLINING_ROW_LIMIT 0", "DATA_INLINING_ROW_LIMIT 10"
    )
    result = subprocess.run(  # noqa: S603 (this interpreter, a fixed script)
        [sys.executable, "-c", _READ_BACK, attach, column_type, value],
        capture_output=True,
        text=True,
        timeout=120,
        check=True,
    )
    assert re.search(UNREADABLE_ERROR, result.stdout), result.stdout
