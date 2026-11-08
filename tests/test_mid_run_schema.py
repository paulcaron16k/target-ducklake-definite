"""A stream's SCHEMA sent again during a run, changing it (DUCKLKD-124)."""

from __future__ import annotations

import io
import json
import logging
from typing import TYPE_CHECKING, Any, ClassVar
from unittest.mock import patch

import pytest

from target_ducklake.connector import DuckLakeConnector
from target_ducklake.target import Targetducklake

if TYPE_CHECKING:
    from collections.abc import Iterator

    from tests.catalogs import Lake

NARROW = {"id": {"type": "integer"}, "v": {"type": ["null", "string"]}}
# NARROW with a property the tap found while paginating.
WIDE = {**NARROW, "w": {"type": ["null", "string"]}}
# WIDE with one more property, for a second change.
WIDER = {**WIDE, "z": {"type": ["null", "integer"]}}
DELETED_AT = {"type": ["null", "string"], "format": "date-time"}
DELETED = "2024-01-01T00:00:00Z"
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


class TestMergeThenOverwrite:
    """A merge stream that loses its keys under overwrite_if_no_pk mid-run."""

    MESSAGES: ClassVar[list[dict]] = [
        _schema(NARROW, keys=["id"]),
        *_records([{"id": 1, "v": "a"}, {"id": 2, "v": "b"}]),
        _schema(WIDE, keys=[]),
        *_records([{"id": 3, "v": "c", "w": "x"}]),
    ]

    def test_the_buffered_rows_join_the_overwrite(
        self, lake: Lake, warnings: pytest.LogCaptureFixture
    ):
        """The records held at the change join the one replace, without a warning.

        They are staged rather than merged, so the replace holds every row.
        """
        _load_earlier(lake)
        _run(lake, self.MESSAGES, overwrite_if_no_pk=True, batch_size_rows=UNFILLED)
        assert _rows(lake) == [(1, "a", None), (2, "b", None), (3, "c", "x")]
        assert not [r for r in warnings.records if "overwrite" in r.getMessage()]

    def test_rows_committed_before_the_change_are_replaced_with_a_warning(
        self, lake: Lake, warnings: pytest.LogCaptureFixture
    ):
        """A batch merged before the change is replaced, with a warning.

        It is in the table already, so the replace removes it.
        """
        _load_earlier(lake)
        _run(lake, self.MESSAGES, overwrite_if_no_pk=True, batch_size_rows=1)
        assert _rows(lake) == [(3, "c", "x")]
        (warning,) = [
            r.getMessage() for r in warnings.records if "overwrite" in r.getMessage()
        ]
        assert "'orders'" in warning
        assert "2 row(s) in 2 batch(es)" in warning


class TestOtherSchemaChanges:
    """Changes beyond one added property, for each load method."""

    @pytest.mark.parametrize("batch_size_rows", [UNFILLED, 1])
    def test_overwrite_with_two_changes(self, lake: Lake, batch_size_rows: int):
        """Batches of three schemas: one replace, each row with its own columns."""
        _load_earlier(lake)
        _run(
            lake,
            [
                *_widened([{"id": 1, "v": "a"}], [{"id": 2, "v": "b", "w": "x"}]),
                _schema(WIDER),
                *_records([{"id": 3, "v": "c", "w": "y", "z": 5}]),
            ],
            load_method="overwrite",
            batch_size_rows=batch_size_rows,
            max_record_age_minutes=1e-9,
        )
        assert _rows(lake, "id, v, w, z") == [
            (1, "a", None, None),
            (2, "b", "x", None),
            (3, "c", "y", 5),
        ]

    @pytest.mark.parametrize("batch_size_rows", [UNFILLED, 1])
    def test_overwrite_deletes_across_batches_of_different_columns(
        self, lake: Lake, batch_size_rows: int
    ):
        """A key marked deleted after the change removes the row staged before it."""
        narrow = {**NARROW, "_sdc_deleted_at": DELETED_AT}
        _load_earlier(lake)
        _run(
            lake,
            [
                _schema(narrow),
                *_records([{"id": 1, "v": "a"}, {"id": 2, "v": "b"}]),
                _schema({**narrow, "w": {"type": ["null", "string"]}}),
                *_records(
                    [
                        {"id": 2, "_sdc_deleted_at": DELETED},
                        {"id": 3, "v": "c", "w": "x"},
                    ]
                ),
            ],
            load_method="overwrite",
            delete_rows_with_deleted_at=True,
            batch_size_rows=batch_size_rows,
        )
        assert _rows(lake) == [(1, "a", None), (3, "c", "x")]

    @pytest.mark.parametrize("load_method", ["append", "merge", "overwrite"])
    @pytest.mark.parametrize("batch_size_rows", [UNFILLED, 1])
    def test_a_struct_gains_a_field(
        self, lake: Lake, load_method: str, batch_size_rows: int
    ):
        """The STRUCT column gets the field; the earlier row has it NULL."""

        def shaped(fields: dict) -> dict:
            return {
                "id": {"type": "integer"},
                "obj": {"type": ["null", "object"], "properties": fields},
            }

        a = {"a": {"type": ["null", "integer"]}}
        _run(
            lake,
            [
                _schema(shaped(a)),
                *_records([{"id": 1, "obj": {"a": 1}}]),
                _schema(shaped({**a, "b": {"type": ["null", "string"]}})),
                *_records([{"id": 2, "obj": {"a": 2, "b": "x"}}]),
            ],
            load_method=load_method,
            batch_size_rows=batch_size_rows,
        )
        assert _rows(lake, "id, obj.a, obj.b") == [(1, 1, None), (2, 2, "x")]

    @pytest.mark.parametrize("load_method", ["append", "merge", "overwrite"])
    def test_a_removed_property(self, lake: Lake, load_method: str):
        """The column stays: the earlier row keeps its value, the later one is NULL."""
        _run(
            lake,
            [
                _schema(WIDE),
                *_records([{"id": 1, "v": "a", "w": "x"}]),
                _schema(NARROW),
                *_records([{"id": 2, "v": "b"}]),
            ],
            load_method=load_method,
            batch_size_rows=UNFILLED,
        )
        assert _rows(lake) == [(1, "a", "x"), (2, "b", None)]

    def test_merge_on_the_new_keys(self, lake: Lake):
        """A second SCHEMA with other key properties merges on them."""
        _run(
            lake,
            [
                _schema(NARROW, keys=["id"]),
                *_records([{"id": 1, "v": "a"}]),
                _schema(WIDE, keys=["id", "v"]),
                *_records(
                    [{"id": 1, "v": "b", "w": "x"}, {"id": 1, "v": "a", "w": "y"}]
                ),
            ],
            batch_size_rows=UNFILLED,
        )
        assert _rows(lake) == [(1, "a", "y"), (1, "b", "x")]


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
