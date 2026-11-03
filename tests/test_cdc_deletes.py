"""Deleting rows a CDC tap marks with _sdc_deleted_at (DUCKLKD-23)."""

from __future__ import annotations

import io
import json
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from target_ducklake.connector import DuckLakeConnector, DuckLakeConnectorError
from target_ducklake.layout import (
    LayoutError,
    check_layout_config,
    resolve_delete_marked,
)
from target_ducklake.target import Targetducklake

if TYPE_CHECKING:
    from pathlib import Path

    from tests.catalogs import Lake

DELETED = "2024-01-01T00:00:00Z"
DELETED_AT = {"type": ["null", "string"], "format": "date-time"}
ON: dict[str, Any] = {"delete_rows_with_deleted_at": True}


def _run(
    lake: Lake,
    records: list[dict],
    *,
    keys: list[str] | None = None,
    declares_deleted_at: bool = True,
    props: dict | None = None,
    **config: object,
) -> None:
    properties = {
        "id": {"type": "integer"},
        "v": {"type": ["null", "string"]},
        **(props or {}),
    }
    if declares_deleted_at:
        properties["_sdc_deleted_at"] = DELETED_AT
    messages = [
        {
            "type": "SCHEMA",
            "stream": "t",
            "schema": {"type": "object", "properties": properties},
            "key_properties": ["id"] if keys is None else keys,
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


def _rows(lake: Lake, columns: str = "id, v") -> list[tuple]:
    return lake.query(f"SELECT {columns} FROM lake.s.t ORDER BY ALL")  # noqa: S608


def _deleted(record_id: int, **extra: object) -> dict:
    return {"id": record_id, "_sdc_deleted_at": DELETED, **extra}


LOADED = [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}, {"id": 3, "v": "c"}]


class TestSetting:
    """delete_rows_with_deleted_at at the top level and per stream."""

    @pytest.mark.parametrize(
        ("config", "expected"),
        [
            ({}, False),
            (ON, True),
            (
                {
                    **ON,
                    "stream_overrides": {"t": {"delete_rows_with_deleted_at": False}},
                },
                False,
            ),
            ({"stream_defaults": ON}, True),
        ],
        ids=["default", "top-level", "override", "defaults"],
    )
    def test_precedence(self, config: dict, expected: object):
        """Override, then stream_defaults, then the top level, then false."""
        assert resolve_delete_marked(config, "t") == expected

    def test_a_bad_stream_value_is_rejected_at_startup(self):
        """Only true or false."""
        config = {"stream_overrides": {"t": {"delete_rows_with_deleted_at": "yes"}}}
        with pytest.raises(LayoutError, match="must be true or false"):
            check_layout_config(config)


class TestThroughListen:
    """On a real catalog (SQLite, and Postgres when set)."""

    def test_merge_deletes_the_marked_rows(self, lake: Lake):
        """A marked record deletes its key's row; others merge as before."""
        _run(lake, LOADED, **ON)
        _run(lake, [_deleted(2), {"id": 3, "v": "c2"}, {"id": 4, "v": "d"}], **ON)
        assert _rows(lake) == [(1, "a"), (3, "c2"), (4, "d")]

    def test_composite_keys(self, lake: Lake):
        """Each key column is matched: (1, 'x') goes, (1, 'y') stays."""
        props = {"k": {"type": "string"}}
        records = [{"id": 1, "k": "x", "v": "a"}, {"id": 1, "k": "y", "v": "b"}]
        _run(lake, records, keys=["id", "k"], props=props, **ON)
        _run(lake, [_deleted(1, k="x")], keys=["id", "k"], props=props, **ON)
        assert _rows(lake, "id, k, v") == [(1, "y", "b")]

    def test_append_deletes_the_marked_keys_rows(self, lake: Lake):
        """Every row of a marked key goes; other records are appended."""
        _run(lake, LOADED, load_method="append", **ON)
        _run(lake, [{"id": 1, "v": "a2"}], load_method="append", **ON)
        _run(lake, [_deleted(1), {"id": 4, "v": "d"}], load_method="append", **ON)
        assert _rows(lake) == [(2, "b"), (3, "c"), (4, "d")]

    def test_overwrite_leaves_the_marked_rows_out(self, lake: Lake):
        """The table is the batch's live rows; no key is needed."""
        _run(lake, LOADED, keys=[], load_method="overwrite", **ON)
        _run(
            lake,
            [{"id": 1, "v": "a"}, _deleted(2)],
            keys=[],
            load_method="overwrite",
            **ON,
        )
        assert _rows(lake) == [(1, "a")]

    @pytest.mark.parametrize(
        ("records", "expected"),
        [
            ([{"id": 2, "v": "b2"}, _deleted(2)], [(1, "a"), (3, "c")]),
            ([_deleted(2), {"id": 2, "v": "b2"}], [(1, "a"), (2, "b2"), (3, "c")]),
        ],
        ids=["update-then-delete", "delete-then-reinsert"],
    )
    def test_the_last_record_of_a_key_in_a_batch_decides(
        self, lake: Lake, records: list[dict], expected: list[tuple]
    ):
        """The in-batch dedup keeps a key's last record."""
        _run(lake, LOADED, **ON)
        _run(lake, records, **ON)
        assert _rows(lake) == expected

    def test_off_by_default_the_marked_row_is_kept_with_its_mark(self, lake: Lake):
        """As before, but the tap's column is now kept, as in target-iceberg."""
        _run(lake, LOADED)
        _run(lake, [_deleted(2, v="b")])
        assert lake.query(
            "SELECT id, _sdc_deleted_at IS NOT NULL FROM lake.s.t ORDER BY id"
        ) == [(1, False), (2, True), (3, False)]

    def test_a_record_without_the_mark_clears_it(self, lake: Lake):
        """The record is the row's new state: a live row again."""
        _run(lake, [_deleted(1, v="a")])
        _run(lake, [{"id": 1, "v": "a"}])
        assert lake.query("SELECT _sdc_deleted_at FROM lake.s.t") == [(None,)]

    def test_with_add_record_metadata(self, lake: Lake):
        """The SDK's own column works the same way."""
        _run(lake, LOADED, declares_deleted_at=False, add_record_metadata=True, **ON)
        _run(
            lake,
            [_deleted(3)],
            declares_deleted_at=False,
            add_record_metadata=True,
            **ON,
        )
        assert _rows(lake) == [(1, "a"), (2, "b")]

    def test_a_stream_without_the_column_loads_as_before(self, lake: Lake):
        """Nothing is marked, with a warning; the records load."""
        _run(
            lake,
            [{"id": 1, "v": "a"}, _deleted(2, v="b")],
            declares_deleted_at=False,
            **ON,
        )
        assert _rows(lake) == [(1, "a"), (2, "b")]

    @pytest.mark.parametrize("load_method", ["merge", "append"])
    def test_a_stream_without_keys_is_refused(self, lake: Lake, load_method: str):
        """It can't tell which row a delete is for."""
        with pytest.raises(ValueError, match="delete_rows_with_deleted_at needs key"):
            _run(lake, LOADED, keys=[], load_method=load_method, **ON)


class TestOneTransaction:
    """The delete and the batch's insert are one transaction."""

    @pytest.mark.parametrize("method", ["merge", "append"])
    def test_delete_and_insert_share_a_transaction(self, method: str):
        """BEGIN, the DELETE of marked keys, the INSERT of live rows, COMMIT."""
        connector = DuckLakeConnector({"catalog_url": "x", "data_path": "/x"})
        executed: list[str] = []
        connector.execute = executed.append  # type: ignore[method-assign,assignment]
        common: dict[str, Any] = {
            "file_location": "/b.parquet",
            "target_schema_name": "s",
            "table_name": "t",
            "file_columns": ["id", "_sdc_deleted_at"],
            "target_table_columns": ["id", "_sdc_deleted_at"],
        }
        if method == "merge":
            connector.merge_into_table(
                **common, key_properties=["id"], delete_marked=True
            )
        else:
            connector.insert_into_table(**common, delete_marked_keys=["id"])
        (sql,) = executed
        statements = [line.strip() for line in sql.strip().splitlines() if line.strip()]
        assert statements[0] == "BEGIN TRANSACTION;"
        assert statements[-1] == "COMMIT;"
        assert any(line.startswith("DELETE FROM") for line in statements)
        assert '"_sdc_deleted_at" IS NULL' in sql


class TestReview:
    """Cases from the review of PR #115."""

    @pytest.mark.parametrize("keys", [["id"], ["id", "k"]])
    def test_overwrite_honours_a_delete_in_a_later_batch(
        self, lake: Lake, keys: list[str]
    ):
        """Finding 1: the key's last record across every staged file decides."""
        props = {"k": {"type": "string"}}
        records = [
            {"id": 1, "k": "x", "v": "a"},
            {"id": 2, "k": "x", "v": "b"},
            _deleted(1, k="x"),
            {"id": 3, "k": "x", "v": "c"},
            {"id": 2, "k": "x", "v": "b2"},
        ]
        _run(
            lake,
            records,
            keys=keys,
            props=props,
            load_method="overwrite",
            max_batch_size=1,
            **ON,
        )
        assert _rows(lake) == [(2, "b2"), (3, "c")]

    def test_a_keyless_overwrite_leaves_out_only_the_marked_record(self, lake: Lake):
        """Round 2: without a key no delete can cancel an earlier live record."""
        records = [{"id": 1, "v": "a"}, _deleted(1), {"id": 2, "v": "b"}]
        _run(lake, records, keys=[], load_method="overwrite", **ON)
        assert _rows(lake) == [(1, "a"), (2, "b")]

    def test_an_existing_table_gains_the_column(self, lake: Lake):
        """Finding 2: old rows read NULL; the marked row loads with its mark."""
        _run(lake, LOADED, declares_deleted_at=False)
        _run(lake, [_deleted(2, v="b")])
        assert lake.query(
            "SELECT id, _sdc_deleted_at IS NOT NULL FROM lake.s.t ORDER BY id"
        ) == [(1, False), (2, True), (3, False)]

    @pytest.mark.parametrize("load_method", ["merge", "append"])
    def test_a_null_key_component_is_matched(self, lake: Lake, load_method: str):
        """Finding 3: (1, NULL) is deleted by a mark on (1, NULL)."""
        props = {"k": {"type": ["null", "string"]}}
        records: list[dict] = [
            {"id": 1, "k": None, "v": "a"},
            {"id": 1, "k": "y", "v": "b"},
        ]
        common: dict[str, Any] = {
            "keys": ["id", "k"],
            "props": props,
            "load_method": load_method,
            **ON,
        }
        _run(lake, records, **common)
        _run(lake, [_deleted(1, k=None)], **common)
        assert _rows(lake, "id, k, v") == [(1, "y", "b")]

    def test_a_timestamptz_key_on_append(self, lake: Lake):
        """Finding 3: the staged key is cast to the table's type to match."""
        props = {"at": {"type": "string", "format": "date-time"}}
        live = [{"id": 1, "at": "2024-01-01T12:00:00+02:00", "v": "a"}]
        _run(lake, live, keys=["at"], props=props, load_method="append", **ON)
        mark = [{"id": 1, "at": "2024-01-01T10:00:00Z", "_sdc_deleted_at": DELETED}]
        _run(lake, mark, keys=["at"], props=props, load_method="append", **ON)
        assert _rows(lake) == []

    def test_a_failed_insert_rolls_the_delete_back(self, lake: Lake, tmp_path: Path):
        """Finding 3, behaviourally: one transaction, so the marked key's row stays."""
        _run(lake, LOADED, **ON)
        bad = tmp_path / "bad.parquet"
        pq.write_table(
            pa.table(
                {
                    "id": pa.array([2, 9], pa.int64()),
                    "v": pa.array(["gone", "x"]),
                    "_sdc_deleted_at": pa.array([DELETED, None]),
                    "extra": pa.array(["1", "not a number"]),
                }
            ),
            bad,
        )
        connector = DuckLakeConnector(lake.config())
        try:
            types = connector.get_table_column_types("s", "t")
            with pytest.raises(DuckLakeConnectorError):
                connector.insert_into_table(
                    file_location=str(bad),
                    target_schema_name="s",
                    table_name="t",
                    # "extra" stands in for a column the insert can't cast.
                    file_columns=["id", "v", "_sdc_deleted_at", "extra"],
                    target_table_columns=["id", "v", "_sdc_deleted_at"],
                    delete_marked_keys=["id"],
                    key_column_types=types,
                )
        finally:
            connector.close()
        assert _rows(lake) == [(1, "a"), (2, "b"), (3, "c")]
