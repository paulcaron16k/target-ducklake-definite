"""An overwrite with keys holds one row per key across batches (DUCKLKD-136)."""

from __future__ import annotations

import io
import json
from typing import TYPE_CHECKING, Any

import pytest

from target_ducklake.target import Targetducklake

if TYPE_CHECKING:
    from tests.catalogs import Lake

PROPERTIES = {"id": {"type": "integer"}, "v": {"type": ["null", "string"]}}
# Declared by every stream, so delete_rows_with_deleted_at applies when on.
DELETED_AT = {"_sdc_deleted_at": {"type": ["null", "string"], "format": "date-time"}}
# Every record its own batch, or every record in one batch.
BATCH_SIZES = [1, 1000]
# delete_rows_with_deleted_at off and on.
DELETE_MARKED = [False, True]


def _schema(
    properties: dict | None = None, keys: list[str] | None = None
) -> dict[str, Any]:
    return {
        "type": "SCHEMA",
        "stream": "t",
        "schema": {
            "type": "object",
            "properties": {**(properties or PROPERTIES), **DELETED_AT},
        },
        "key_properties": ["id"] if keys is None else keys,
    }


def _records(rows: list[dict]) -> list[dict[str, Any]]:
    return [{"type": "RECORD", "stream": "t", "record": row} for row in rows]


def _run(lake: Lake, messages: list[dict], **config: object) -> None:
    config = {"load_method": "overwrite", **config}
    target = Targetducklake(config={**lake.config(), **config})
    try:
        target.listen(
            file_input=io.StringIO("".join(json.dumps(m) + "\n" for m in messages))
        )
    finally:
        target.target_connector.close()


def _rows(lake: Lake, columns: str = "id, v") -> list[tuple]:
    return lake.query(f"SELECT {columns} FROM lake.s.t ORDER BY ALL")  # noqa: S608


@pytest.mark.parametrize("batch_size_rows", BATCH_SIZES)
@pytest.mark.parametrize("delete_marked", DELETE_MARKED)
class TestOneRowPerKey:
    """Each key's last record decides, whatever batch it landed in."""

    def test_a_key_sent_twice(
        self, lake: Lake, batch_size_rows: int, *, delete_marked: bool
    ):
        """The issue's case: (1, a) then (1, a2) leave (1, a2) only."""
        records = [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}, {"id": 1, "v": "a2"}]
        _run(
            lake,
            [_schema(), *_records(records)],
            batch_size_rows=batch_size_rows,
            delete_rows_with_deleted_at=delete_marked,
        )
        assert _rows(lake) == [(1, "a2"), (2, "b")]

    def test_a_composite_key(
        self, lake: Lake, batch_size_rows: int, *, delete_marked: bool
    ):
        """Rows match on every key column; sharing one isn't enough."""
        properties = {**PROPERTIES, "k": {"type": "string"}}
        records = [
            {"id": 1, "k": "x", "v": "a"},
            {"id": 1, "k": "y", "v": "b"},
            {"id": 1, "k": "x", "v": "a2"},
        ]
        _run(
            lake,
            [_schema(properties, ["id", "k"]), *_records(records)],
            batch_size_rows=batch_size_rows,
            delete_rows_with_deleted_at=delete_marked,
        )
        assert _rows(lake, "id, k, v") == [(1, "x", "a2"), (1, "y", "b")]

    def test_null_key_values_are_one_key(
        self, lake: Lake, batch_size_rows: int, *, delete_marked: bool
    ):
        """NULL matches NULL, as in a merge (IS NOT DISTINCT FROM)."""
        properties = {**PROPERTIES, "k": {"type": ["null", "string"]}}
        records: list[dict] = [
            {"id": 1, "k": None, "v": "a"},
            {"id": 1, "k": "x", "v": "b"},
            {"id": 1, "k": None, "v": "a2"},
        ]
        _run(
            lake,
            [_schema(properties, ["id", "k"]), *_records(records)],
            batch_size_rows=batch_size_rows,
            delete_rows_with_deleted_at=delete_marked,
        )
        assert _rows(lake, "id, k, v") == [(1, "x", "b"), (1, None, "a2")]

    def test_one_instant_at_two_offsets_is_one_key(
        self, lake: Lake, batch_size_rows: int, *, delete_marked: bool
    ):
        """A date-time key is compared as the instant it names."""
        properties = {
            "ts": {"type": "string", "format": "date-time"},
            "v": {"type": ["null", "string"]},
        }
        records = [
            {"ts": "2024-01-01T01:00:00+01:00", "v": "a"},
            {"ts": "2024-01-01T00:00:00Z", "v": "a2"},
        ]
        _run(
            lake,
            [_schema(properties, ["ts"]), *_records(records)],
            batch_size_rows=batch_size_rows,
            delete_rows_with_deleted_at=delete_marked,
        )
        assert _rows(lake, "v") == [("a2",)]

    def test_a_keyless_overwrite_keeps_every_row(
        self, lake: Lake, batch_size_rows: int, *, delete_marked: bool
    ):
        """overwrite_if_no_pk: without keys, no two records are the same row."""
        records = [{"id": 1, "v": "a"}, {"id": 1, "v": "a"}, {"id": 1, "v": "a2"}]
        _run(
            lake,
            [_schema(keys=[]), *_records(records)],
            load_method="merge",
            overwrite_if_no_pk=True,
            batch_size_rows=batch_size_rows,
            delete_rows_with_deleted_at=delete_marked,
        )
        assert _rows(lake) == [(1, "a"), (1, "a"), (1, "a2")]


@pytest.mark.parametrize("batch_size_rows", BATCH_SIZES)
def test_batches_staged_under_two_schemas(lake: Lake, batch_size_rows: int):
    """A SCHEMA adding a column mid-run: the batches still dedupe by key."""
    wide = {**PROPERTIES, "w": {"type": ["null", "string"]}}
    messages = [
        _schema(),
        *_records([{"id": 1, "v": "a"}, {"id": 2, "v": "b"}]),
        _schema(wide),
        *_records([{"id": 1, "v": "a2", "w": "x"}]),
    ]
    _run(lake, messages, batch_size_rows=batch_size_rows)
    assert _rows(lake, "id, v, w") == [(1, "a2", "x"), (2, "b", None)]


@pytest.mark.parametrize("batch_size_rows", BATCH_SIZES)
@pytest.mark.parametrize("delete_marked", DELETE_MARKED)
class TestKeysChangedMidRun:
    """A SCHEMA that changes the keys: each batch dedupes by its own keys.

    Rows staged under different keys can't be matched, so each set of
    batches keeps the last row per its keys, and all of them load.
    """

    def test_records_under_both_keys(
        self, lake: Lake, batch_size_rows: int, *, delete_marked: bool
    ):
        """Rows staged by id aren't collapsed by k, the key that follows."""
        properties = {**PROPERTIES, "k": {"type": "string"}}
        messages = [
            _schema(properties, ["id"]),
            *_records(
                [
                    {"id": 1, "k": "x", "v": "a"},
                    {"id": 2, "k": "x", "v": "b"},
                    {"id": 1, "k": "x", "v": "a2"},
                ]
            ),
            _schema(properties, ["k"]),
            *_records([{"id": 3, "k": "y", "v": "c"}, {"id": 4, "k": "y", "v": "d"}]),
        ]
        _run(
            lake,
            messages,
            batch_size_rows=batch_size_rows,
            delete_rows_with_deleted_at=delete_marked,
        )
        assert _rows(lake, "id, k, v") == [(1, "x", "a2"), (2, "x", "b"), (4, "y", "d")]

    def test_a_new_key_no_record_has(
        self, lake: Lake, batch_size_rows: int, *, delete_marked: bool
    ):
        """The keys move to a new column and no record follows.

        The rows staged before still dedupe by their own key, rather than
        by a column none of them has.
        """
        wide = {**PROPERTIES, "w": {"type": ["null", "string"]}}
        messages = [
            _schema(),
            *_records([{"id": 1, "v": "a"}, {"id": 2, "v": "b"}, {"id": 1, "v": "a2"}]),
            _schema(wide, ["w"]),
        ]
        _run(
            lake,
            messages,
            batch_size_rows=batch_size_rows,
            delete_rows_with_deleted_at=delete_marked,
        )
        assert _rows(lake) == [(1, "a2"), (2, "b")]


def test_rows_count_the_staged_records(lake: Lake):
    """The commit's rows are the staged rows, as a merge's: before the dedupe."""
    records = [{"id": 1, "v": "a"}, {"id": 1, "v": "a2"}]
    _run(lake, [_schema(), *_records(records)], batch_size_rows=1)
    (info,) = lake.query(
        "SELECT commit_extra_info FROM lake.snapshots() "
        "WHERE commit_extra_info IS NOT NULL"
    )
    assert json.loads(info[0])["rows"] == 2  # noqa: PLR2004
    assert _rows(lake) == [(1, "a2")]
