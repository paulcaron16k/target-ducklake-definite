"""A stream's SCHEMA sent again during a run, changing it (DUCKLKD-124)."""

from __future__ import annotations

import io
import json
from typing import TYPE_CHECKING, Any
from unittest.mock import patch

import pytest

from target_ducklake.connector import DuckLakeConnector
from target_ducklake.target import Targetducklake

if TYPE_CHECKING:
    from tests.catalogs import Lake

NARROW = {"id": {"type": "integer"}, "v": {"type": ["null", "string"]}}
# NARROW with a property the tap found while paginating.
WIDE = {**NARROW, "w": {"type": ["null", "string"]}}
# Batches no record fills, so each sink still holds its records at the change.
UNFILLED = 1000


def _schema(
    properties: dict, keys: list[str] | None = None, stream: str = "orders"
) -> dict:
    return {
        "type": "SCHEMA",
        "stream": stream,
        "schema": {"type": "object", "properties": properties},
        "key_properties": ["id"] if keys is None else keys,
    }


def _records(
    rows: list[dict], version: int | None = None, stream: str = "orders"
) -> list[dict]:
    extra = {} if version is None else {"version": version}
    return [{"type": "RECORD", "stream": stream, "record": r, **extra} for r in rows]


def _activate(version: int) -> dict:
    return {"type": "ACTIVATE_VERSION", "stream": "orders", "version": version}


def _run(lake: Lake, messages: list[dict], **config: Any) -> None:
    """Load ``messages`` in one run, with the lake's config and ``config``."""
    target = Targetducklake(config={**lake.config(), **config})
    try:
        target.listen(
            file_input=io.StringIO("".join(json.dumps(m) + "\n" for m in messages))
        )
    finally:
        target.target_connector.close()


def _rows(lake: Lake, columns: str = "id, v, w") -> list[tuple]:
    return lake.query(f"SELECT {columns} FROM lake.s.orders ORDER BY ALL")  # noqa: S608


def _widened(first: list[dict], then: list[dict]) -> list[dict]:
    """``first`` under NARROW, then a SCHEMA adding ``w``, then ``then``."""
    return [_schema(NARROW), *_records(first), _schema(WIDE), *_records(then)]


def _load_earlier(lake: Lake) -> None:
    """A table an earlier run loaded, with a row this run doesn't send."""
    _run(lake, [_schema(NARROW), *_records([{"id": 7, "v": "earlier"}])])


class TestWiderSchemaMidRun:
    """A SCHEMA adding a property adds the column, and every record loads."""

    @pytest.mark.parametrize("load_method", ["append", "merge", "overwrite"])
    @pytest.mark.parametrize("batch_size_rows", [UNFILLED, 1])
    def test_records_on_both_sides_of_the_change_load(
        self, lake: Lake, load_method: str, batch_size_rows: int
    ):
        """Rows buffered under the old schema load, as do the later ones into w."""
        _load_earlier(lake)
        _run(
            lake,
            _widened(
                [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}],
                [{"id": 3, "v": "c", "w": "x"}],
            ),
            load_method=load_method,
            batch_size_rows=batch_size_rows,
        )
        loaded = [(1, "a", None), (2, "b", None), (3, "c", "x")]
        earlier = [] if load_method == "overwrite" else [(7, "earlier", None)]
        assert _rows(lake) == loaded + earlier

    @pytest.mark.parametrize("batch_size_rows", [UNFILLED, 2])
    def test_merge_keeps_the_later_record_of_a_key(
        self, lake: Lake, batch_size_rows: int
    ):
        """A key sent before and after the change keeps the later record.

        With batch_size_rows 2, the new sink fills and drains before the end of
        the stream: the old sink's records must be in the table before it.
        """
        _run(
            lake,
            _widened(
                [{"id": 1, "v": "old"}, {"id": 2, "v": "b"}][: batch_size_rows - 1],
                [{"id": 1, "v": "new", "w": "x"}, {"id": 3, "v": "c", "w": "y"}],
            ),
            batch_size_rows=batch_size_rows,
        )
        assert (1, "new", "x") in _rows(lake)
        assert (1, "old", None) not in _rows(lake)

    @pytest.mark.parametrize("batch_size_rows", [UNFILLED, 1])
    def test_overwrite_replaces_the_table_once_with_every_row(
        self, lake: Lake, batch_size_rows: int
    ):
        """One replace, after the stream, holds the rows of both schemas.

        max_record_age_minutes near 0 drains every sink after each record, as a
        long run does now and then; the old sink's staged batches survive it.
        """
        _load_earlier(lake)
        with patch.object(
            DuckLakeConnector,
            "replace_table_contents",
            autospec=True,
            side_effect=DuckLakeConnector.replace_table_contents,
        ) as replace:
            _run(
                lake,
                _widened(
                    [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}],
                    [{"id": 3, "v": "c", "w": "x"}],
                ),
                load_method="overwrite",
                batch_size_rows=batch_size_rows,
                max_record_age_minutes=1e-9,
            )
        assert replace.call_count == 1
        assert _rows(lake) == [(1, "a", None), (2, "b", None), (3, "c", "x")]

    def test_overwrite_without_keys_then_with_keys(self, lake: Lake):
        """An overwrite_if_no_pk stream that gains keys: its keyless rows are kept.

        The rows sent before the change replace the table when the stream is
        rebuilt as a merge; the later ones merge into them.
        """
        _load_earlier(lake)
        _run(
            lake,
            [
                _schema(NARROW, keys=[]),
                *_records([{"id": 1, "v": "a"}, {"id": 2, "v": "b"}]),
                _schema(WIDE, keys=["id"]),
                *_records([{"id": 2, "v": "B", "w": "x"}]),
            ],
            overwrite_if_no_pk=True,
            batch_size_rows=UNFILLED,
        )
        assert _rows(lake) == [(1, "a", None), (2, "B", "x")]

    def test_activate_version_after_the_change_retires_other_versions(self, lake: Lake):
        """The version announced to the old sink still retires the earlier rows."""
        _load_earlier(lake)
        _run(
            lake,
            [
                _schema(NARROW),
                _activate(5),
                *_records([{"id": 1, "v": "a"}], version=5),
                _schema(WIDE),
                _activate(5),
            ],
            add_record_metadata=True,
            batch_size_rows=UNFILLED,
        )
        assert _rows(lake, "id, _sdc_deleted_at IS NOT NULL") == [
            (1, False),
            (7, True),
        ]


class TestColumnAddedByAnotherSink:
    """A sink whose column list predates a column another sink added."""

    @pytest.mark.parametrize("load_method", ["append", "merge"])
    def test_values_load_into_their_own_columns(self, lake: Lake, load_method: str):
        """Each value is inserted by column name, the new column left NULL.

        Streams "s-orders" and "orders" load one table (DUCKLKD-44): the
        second adds w while the first still holds its records.
        """
        _run(
            lake,
            [
                _schema(NARROW, stream="s-orders"),
                *_records([{"id": 1, "v": "a"}], stream="s-orders"),
                _schema(WIDE, stream="orders"),
                *_records([{"id": 2, "v": "b", "w": "x"}], stream="orders"),
            ],
            load_method=load_method,
            batch_size_rows=UNFILLED,
        )
        assert _rows(lake) == [(1, "a", None), (2, "b", "x")]


class TestSameSchemaMidRun:
    """A SCHEMA sent again unchanged, as many taps do, changes nothing."""

    @pytest.mark.parametrize("load_method", ["append", "merge"])
    def test_one_sink_one_commit(self, lake: Lake, load_method: str):
        """The records stay in one batch, loaded in one commit."""
        method = "insert_into_table" if load_method == "append" else "merge_into_table"
        with patch.object(
            DuckLakeConnector,
            method,
            autospec=True,
            side_effect=getattr(DuckLakeConnector, method),
        ) as load:
            _run(
                lake,
                [
                    _schema(NARROW),
                    *_records([{"id": 1, "v": "a"}]),
                    _schema(NARROW),
                    *_records([{"id": 2, "v": "b"}]),
                ],
                load_method=load_method,
                batch_size_rows=UNFILLED,
            )
        assert load.call_count == 1
        assert _rows(lake, "id, v") == [(1, "a"), (2, "b")]
