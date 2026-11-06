"""Streams that load the same table (DUCKLKD-44)."""

from __future__ import annotations

import io
import json
import logging
import threading
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any
from unittest.mock import patch

import pytest

from target_ducklake.collisions import TableCollisionError
from target_ducklake.target import Targetducklake

if TYPE_CHECKING:
    from collections.abc import Iterator

    from tests.catalogs import Lake

PROPERTIES = {"id": {"type": "integer"}, "v": {"type": ["null", "string"]}}
COLLIDING = "both load table"


@pytest.fixture
def warnings(caplog: pytest.LogCaptureFixture) -> Iterator[pytest.LogCaptureFixture]:
    """The target's warnings, caught on its own logger.

    The singer-sdk configures logging when a target starts, which replaces
    the root handler caplog listens on.
    """
    logger = logging.getLogger("target-ducklake")
    logger.addHandler(caplog.handler)
    caplog.handler.setLevel(logging.WARNING)
    try:
        yield caplog
    finally:
        logger.removeHandler(caplog.handler)


def _collision_warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [r.getMessage() for r in caplog.records if COLLIDING in r.getMessage()]


def _schema(
    stream: str, keys: list[str] | None, properties: dict | None = None
) -> dict:
    message: dict[str, Any] = {
        "type": "SCHEMA",
        "stream": stream,
        "schema": {"type": "object", "properties": properties or PROPERTIES},
    }
    if keys is not None:
        message["key_properties"] = keys
    return message


def _records(stream: str, rows: list[dict], version: int | None = None) -> list[dict]:
    extra = {} if version is None else {"version": version}
    return [{"type": "RECORD", "stream": stream, "record": r, **extra} for r in rows]


def _activate(stream: str, version: int) -> dict:
    return {"type": "ACTIVATE_VERSION", "stream": stream, "version": version}


def _run(lake: Lake, messages: list[dict], **config: object) -> None:
    """Load ``messages`` in one run, with the lake's config and ``config``.

    A setting given as None is left out.
    """
    settings = {**lake.config(), **config}
    target = Targetducklake(
        config={key: value for key, value in settings.items() if value is not None}
    )
    try:
        target.listen(
            file_input=io.StringIO("".join(json.dumps(m) + "\n" for m in messages))
        )
    finally:
        target.target_connector.close()


def _two_streams(
    first: str,
    second: str,
    keys: tuple[list[str] | None, list[str] | None] = (["id"], ["id"]),
) -> list[dict]:
    """``first`` loads ids 1 and 2, then ``second`` ids 1 (changed) and 3."""
    return [
        _schema(first, keys[0]),
        *_records(first, [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}]),
        _schema(second, keys[1]),
        *_records(second, [{"id": 1, "v": "x"}, {"id": 3, "v": "c"}]),
    ]


def _rows(lake: Lake, table: str = "orders", schema: str = "s") -> list[tuple]:
    return lake.query(f'SELECT id, v FROM lake.{schema}."{table}" ORDER BY ALL')  # noqa: S608


SHARED_ALIAS = {
    "stream_maps": {"eu": {"__alias__": "orders"}, "us": {"__alias__": "orders"}}
}
# What _two_streams leaves when the second stream merges by id over the first.
MERGED = [(1, "x"), (2, "b"), (3, "c")]
# A table loaded by an earlier run, which a refused run must leave alone.
EARLIER = [(7, "kept")]


def _load_earlier(lake: Lake, stream: str = "orders") -> None:
    _run(lake, [_schema(stream, ["id"]), *_records(stream, [{"id": 7, "v": "kept"}])])


class TestCollisionsWarn:
    """Streams combined in one table, which is allowed, are warned about once."""

    @pytest.mark.parametrize(
        ("first", "second", "config"),
        [
            ("eu", "us", SHARED_ALIAS),
            ("orders", "us", {"stream_maps": {"us": {"__alias__": "orders"}}}),
            ("Orders", "orders", {}),
            ("s-orders", "orders", {}),
        ],
        ids=["shared-alias", "alias-onto-a-stream", "case-only", "schema-table-name"],
    )
    def test_same_keys_warn_and_both_streams_load(
        self,
        lake: Lake,
        warnings: pytest.LogCaptureFixture,
        first: str,
        second: str,
        config: dict,
    ):
        """Both streams' rows land, merged by their common key, with one warning."""
        _run(lake, _two_streams(first, second), **config)

        assert _rows(lake) == MERGED
        (warning,) = _collision_warnings(warnings)
        assert repr(first) in warning
        assert repr(second) in warning
        assert "s.orders" in warning.lower()
        assert "['id']" in warning

    def test_catalog_part_of_a_stream_name_is_dropped(
        self, lake: Lake, warnings: pytest.LogCaptureFixture
    ):
        """``a-s-orders`` and ``b-s-orders`` both load s.orders."""
        _run(
            lake,
            _two_streams("a-s-orders", "b-s-orders"),
            default_target_schema=None,
        )

        assert _rows(lake) == MERGED
        assert len(_collision_warnings(warnings)) == 1

    def test_append_and_merge_may_share_a_table(
        self, lake: Lake, warnings: pytest.LogCaptureFixture
    ):
        """Neither replaces the other's rows; the warning names both load methods."""
        _run(
            lake,
            _two_streams("Orders", "orders"),
            stream_overrides={"Orders": {"load_method": "append"}},
        )

        assert _rows(lake) == MERGED
        (warning,) = _collision_warnings(warnings)
        assert "'Orders' by append" in warning
        assert "'orders' by merge" in warning

    def test_keys_are_compared_unordered(
        self, lake: Lake, warnings: pytest.LogCaptureFixture
    ):
        """A composite key declared in another order is the same key."""
        _run(lake, _two_streams("Orders", "orders", (["id", "v"], ["v", "id"])))

        assert len(_collision_warnings(warnings)) == 1

    def test_keys_are_compared_by_column_name(
        self, lake: Lake, warnings: pytest.LogCaptureFixture
    ):
        """``Order ID`` and ``order_id`` are one column with conform_column_names."""
        messages = [
            _schema("Orders", ["Order ID"], {"Order ID": {"type": "integer"}}),
            *_records("Orders", [{"Order ID": 1}]),
            _schema("orders", ["order_id"], {"order_id": {"type": "integer"}}),
            *_records("orders", [{"order_id": 2}]),
        ]
        _run(lake, messages, conform_column_names=True)

        assert lake.query("SELECT order_id FROM lake.s.orders ORDER BY ALL") == [
            (1,),
            (2,),
        ]
        assert len(_collision_warnings(warnings)) == 1

    def test_one_stream_configuring_the_layout_is_not_a_conflict(
        self, lake: Lake, warnings: pytest.LogCaptureFixture
    ):
        """A stream without a partition keeps the table's, so nothing flip-flops."""
        _run(
            lake,
            _two_streams("Orders", "orders"),
            stream_overrides={"Orders": {"partition": [{"column": "id"}]}},
        )

        assert _rows(lake) == MERGED
        assert len(_collision_warnings(warnings)) == 1


class TestCollisionsFail:
    """Streams that would undo each other's rows or settings fail the run."""

    @pytest.mark.parametrize(
        ("first", "second", "config"),
        [("eu", "us", SHARED_ALIAS), ("Orders", "orders", {})],
        ids=["shared-alias", "case-only"],
    )
    def test_different_keys(self, lake: Lake, first: str, second: str, config: dict):
        """Refused before either stream loads a row, naming streams, table and keys."""
        _load_earlier(lake)

        with pytest.raises(TableCollisionError) as error:
            _run(lake, _two_streams(first, second, (["id"], ["v"])), **config)

        message = str(error.value)
        assert repr(first) in message
        assert repr(second) in message
        assert "s.orders" in message.lower()
        assert "['id'] and ['v']" in message
        assert _rows(lake) == EARLIER

    def test_a_keyless_stream_and_a_keyed_one(self, lake: Lake):
        """No keys is a different key set: a merge would re-key the other's rows."""
        with pytest.raises(TableCollisionError, match=r"none and \['id'\]"):
            _run(lake, _two_streams("Orders", "orders", (None, ["id"])))

    @pytest.mark.parametrize(
        ("config", "reason"),
        [
            (
                {"stream_overrides": {"Orders": {"load_method": "overwrite"}}},
                "load_method overwrite",
            ),
            (
                {"stream_overrides": {"orders": {"load_method": "overwrite"}}},
                "load_method overwrite",
            ),
            ({"overwrite_if_no_pk": True}, "overwrite_if_no_pk"),
        ],
        ids=["first-overwrites", "second-overwrites", "overwrite-if-no-pk"],
    )
    def test_an_overwrite(self, lake: Lake, config: dict, reason: str):
        """Refused before the overwrite replaces anything."""
        keys = (None, None) if "overwrite_if_no_pk" in config else (["id"], ["id"])
        _load_earlier(lake)

        with pytest.raises(TableCollisionError, match="full refresh") as error:
            _run(lake, _two_streams("Orders", "orders", keys), **config)

        assert reason in str(error.value)
        assert _rows(lake) == EARLIER

    def test_activate_version_after_the_other_stream_arrives(self, lake: Lake):
        """Refused before any row is retired, or the refreshing stream's loaded."""
        _load_earlier(lake)
        messages = [
            _schema("Orders", ["id"]),
            _schema("orders", ["id"]),
            *_records("orders", [{"id": 3, "v": "c"}]),
            *_records("Orders", [{"id": 1, "v": "a"}], version=5),
            _activate("Orders", 5),
        ]

        with pytest.raises(TableCollisionError, match="ACTIVATE_VERSION"):
            _run(lake, messages, add_record_metadata=True)

        assert _rows(lake) == EARLIER

    def test_activate_version_before_the_other_stream_arrives(self, lake: Lake):
        """The other stream fails when it arrives, before it loads a row.

        The target sees a stream only when its SCHEMA arrives, so rows the
        refresh retired before then are retired.
        """
        messages = [
            _schema("Orders", ["id"]),
            _activate("Orders", 5),
            *_records("Orders", [{"id": 1, "v": "a"}], version=5),
            _schema("orders", ["id"]),
            *_records("orders", [{"id": 3, "v": "c"}]),
        ]

        with pytest.raises(TableCollisionError, match="'Orders' does a full refresh"):
            _run(lake, messages, add_record_metadata=True)

    def test_different_partition_keys(self, lake: Lake):
        """A table has one partitioning, which each stream would set back."""
        overrides = {
            "Orders": {"partition": [{"column": "id"}]},
            "orders": {"partition": [{"column": "v"}]},
        }
        with pytest.raises(TableCollisionError, match="different partition keys"):
            _run(lake, _two_streams("Orders", "orders"), stream_overrides=overrides)

    def test_different_inlining_limits(self, lake: Lake):
        """A table has one data_inlining_row_limit, which each would set back."""
        overrides = {"orders": {"data_inlining_row_limit": 10}}
        with pytest.raises(TableCollisionError, match="data_inlining_row_limit"):
            _run(lake, _two_streams("Orders", "orders"), stream_overrides=overrides)


class TestNoFalsePositives:
    """A stream never collides with itself, nor tables in different schemas."""

    def test_a_schema_sent_again(self, lake: Lake, warnings: pytest.LogCaptureFixture):
        """A tap re-sending a stream's SCHEMA is the same stream."""
        messages = [
            _schema("orders", ["id"]),
            *_records("orders", [{"id": 1, "v": "a"}]),
            _schema("orders", ["id"]),
            *_records("orders", [{"id": 2, "v": "b"}]),
        ]
        _run(lake, messages, load_method="overwrite")

        assert _rows(lake) == [(1, "a"), (2, "b")]
        assert not _collision_warnings(warnings)

    def test_a_sink_rebuilt_for_new_keys(
        self, lake: Lake, warnings: pytest.LogCaptureFixture
    ):
        """New keys rebuild the stream's sink, which replaces its own claim."""
        messages = [
            _schema("orders", ["id"]),
            *_records("orders", [{"id": 1, "v": "a"}]),
            _schema("orders", ["v"]),
            *_records("orders", [{"id": 2, "v": "b"}]),
        ]
        _run(lake, messages)

        assert _rows(lake) == [(1, "a"), (2, "b")]
        assert not _collision_warnings(warnings)

    def test_one_table_name_in_two_schemas(
        self, lake: Lake, warnings: pytest.LogCaptureFixture
    ):
        """``eu-orders`` and ``us-orders`` are two tables, even with different keys."""
        _run(
            lake,
            _two_streams("eu-orders", "us-orders", (["id"], ["v"])),
            default_target_schema=None,
        )

        assert _rows(lake, schema="eu") == [(1, "a"), (2, "b")]
        assert _rows(lake, schema="us") == [(1, "x"), (3, "c")]
        assert not _collision_warnings(warnings)


class TestDrains:
    """Sinks that load one table drain one after another."""

    def test_sinks_of_one_table_drain_in_turn(self):
        """Two merges of one table, committed at once, would each miss the other's rows.

        On Postgres, both streams' key 1 was then in the table twice. Sinks of
        other tables still drain in parallel.
        """
        target = Targetducklake(
            config={"catalog_url": "x", "data_path": "/x", "storage_type": "local"}
        )
        sinks = [
            SimpleNamespace(name=name, target_schema="s", target_table=table)
            for name, table in [
                ("Orders", "Orders"),
                ("other", "other"),
                ("orders", "orders"),
            ]
        ]
        lock = threading.Lock()
        running: set[str] = set()
        overlapped: list[set[str]] = []
        order: list[str] = []

        def drain_one(sink: SimpleNamespace) -> None:
            with lock:
                running.add(sink.name)
                overlapped.append(set(running))
                order.append(sink.name)
            threading.Event().wait(0.2)
            with lock:
                running.discard(sink.name)

        with patch.object(target, "drain_one", side_effect=drain_one):
            target._drain_all(sinks, parallelism=3)  # noqa: SLF001

        assert not any({"Orders", "orders"} <= names for names in overlapped)
        assert any({"Orders", "other"} <= names for names in overlapped)
        assert order.index("Orders") < order.index("orders")
