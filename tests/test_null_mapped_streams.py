"""A stream mapped to null leaves its table alone (DUCKLKD-127).

``stream_maps: {orders: null}`` (or ``__else__: null``) is the singer-sdk's way
to remove a stream: its records are dropped. The target must not touch its
table either: no CREATE, no overwrite, no ACTIVATE_VERSION, no STATE held back
and no writeCompleted event. A stream map that filters every record out
(``__filter__: false``) is not a removal: its overwrite still empties the
table.
"""

from __future__ import annotations

import io
import json
from typing import TYPE_CHECKING, Any

import pytest

from target_ducklake import target
from target_ducklake.target import Targetducklake

if TYPE_CHECKING:
    from tests.catalogs import Lake

PROPERTIES = {"id": {"type": "integer"}, "v": {"type": ["null", "string"]}}
# The rows an earlier run loaded, which a removed stream must leave alone.
EARLIER = [(7, "kept")]
STATE = {"type": "STATE", "value": {"bookmarks": {"orders": {"id": 2}}}}


def _schema(stream: str, keys: list[str] | None = None) -> dict:
    message: dict[str, Any] = {
        "type": "SCHEMA",
        "stream": stream,
        "schema": {"type": "object", "properties": PROPERTIES},
    }
    if keys is not None:
        message["key_properties"] = keys
    return message


def _records(stream: str, rows: list[dict], version: int | None = None) -> list[dict]:
    extra = {} if version is None else {"version": version}
    return [{"type": "RECORD", "stream": stream, "record": r, **extra} for r in rows]


def _activate(stream: str, version: int) -> dict:
    return {"type": "ACTIVATE_VERSION", "stream": stream, "version": version}


def _run(lake: Lake, messages: list[dict], **config: object) -> Targetducklake:
    """Load ``messages`` in one run, with the lake's config and ``config``."""
    loader = Targetducklake(config={**lake.config(), **config})
    try:
        loader.listen(
            file_input=io.StringIO("".join(json.dumps(m) + "\n" for m in messages))
        )
    finally:
        loader.target_connector.close()
    return loader


def _load_earlier(lake: Lake, *, version: int | None = None) -> None:
    """Load ``EARLIER`` into ``s.orders``, of ``version`` if given."""
    _run(
        lake,
        [
            _schema("orders", ["id"]),
            *_records("orders", [{"id": 7, "v": "kept"}], version),
        ],
        add_record_metadata=version is not None,
    )


def _rows(lake: Lake, table: str = "orders") -> list[tuple]:
    return lake.query(f"SELECT id, v FROM lake.s.{table} ORDER BY ALL")  # noqa: S608


def _snapshot_count(lake: Lake) -> int:
    return lake.query("SELECT count(*) FROM lake.snapshots()")[0][0]


def _tables(lake: Lake) -> list[str]:
    return [
        row[0]
        for row in lake.query(
            "SELECT table_name FROM duckdb_tables() WHERE database_name = 'lake' "
            "ORDER BY ALL"
        )
    ]


ORDERS = [_schema("orders"), *_records("orders", [{"id": 1, "v": "a"}])]
# Every way the target can come to replace a table's rows wholesale.
FULL_REFRESHES = [
    pytest.param({"load_method": "overwrite"}, id="overwrite"),
    pytest.param({"overwrite_if_no_pk": True}, id="overwrite_if_no_pk"),
    pytest.param(
        {"stream_overrides": {"orders": {"load_method": "overwrite"}}},
        id="stream_override",
    ),
]


class TestRemovedStream:
    """A stream mapped to null never creates, writes to or replaces its table."""

    @pytest.mark.parametrize("config", FULL_REFRESHES)
    def test_an_overwrite_leaves_the_table(self, lake: Lake, config: dict):
        """The removed stream's end-of-run overwrite doesn't empty its table."""
        _load_earlier(lake)
        snapshots = _snapshot_count(lake)

        _run(lake, ORDERS, stream_maps={"orders": None}, **config)

        assert _rows(lake) == EARLIER
        assert _snapshot_count(lake) == snapshots

    @pytest.mark.parametrize("config", [*FULL_REFRESHES, pytest.param({}, id="merge")])
    def test_the_table_is_not_created(self, lake: Lake, config: dict):
        """Not even an empty table, nor a schema, for a removed stream."""
        _run(lake, ORDERS, stream_maps={"orders": None}, **config)

        assert _tables(lake) == []
        assert _snapshot_count(lake) == 1  # The catalog's own first snapshot.

    def test_removed_by_else(self, lake: Lake):
        """``__else__: null`` removes every stream the map doesn't name."""
        _load_earlier(lake)
        messages = [
            *ORDERS,
            _schema("items", ["id"]),
            *_records("items", [{"id": 2, "v": "b"}]),
        ]

        _run(
            lake,
            messages,
            stream_maps={"items": {}, "__else__": None},
            load_method="overwrite",
        )

        assert _rows(lake) == EARLIER
        assert _rows(lake, "items") == [(2, "b")]

    def test_activate_version_retires_nothing(self, lake: Lake):
        """The removed stream's ACTIVATE_VERSION doesn't retire the table's rows.

        The version is announced first, then activated after its records: a
        full-table sync, which removes (hard_delete) every row of another
        version.
        """
        _load_earlier(lake, version=1)
        messages = [
            _schema("orders", ["id"]),
            _activate("orders", 2),
            *_records("orders", [{"id": 1, "v": "a"}], version=2),
            _activate("orders", 2),
        ]

        _run(
            lake,
            messages,
            stream_maps={"orders": None},
            add_record_metadata=True,
            hard_delete=True,
        )

        assert _rows(lake) == EARLIER

    def test_activate_version_spares_the_stream_taking_its_name(self, lake: Lake):
        """A stream aliased onto the removed one's name keeps its rows.

        The removed stream's ACTIVATE_VERSION is its own: the table belongs to
        ``orders_v2``, whose rows are of another version.
        """
        settings = {
            "stream_maps": {"orders": None, "orders_v2": {"__alias__": "orders"}},
            "add_record_metadata": True,
            "hard_delete": True,
        }
        _run(
            lake,
            [
                _schema("orders_v2", ["id"]),
                *_records("orders_v2", [{"id": 2, "v": "new"}], version=5),
            ],
            **settings,
        )
        messages = [
            _schema("orders_v2", ["id"]),
            _schema("orders", ["id"]),
            _activate("orders", 9),
            *_records("orders", [{"id": 1, "v": "old"}], version=9),
            _activate("orders", 9),
        ]

        _run(lake, messages, **settings)

        assert _rows(lake) == [(2, "new")]

    def test_its_schema_leaves_the_sink_of_the_stream_taking_its_name(self, lake: Lake):
        """The removed stream's SCHEMA doesn't rebuild ``orders_v2``'s sink.

        Rebuilt with the removed stream's schema and keys (none: a null map
        drops them), the sink would append ``orders_v2``'s records rather
        than merge them by id.
        """
        stream_maps = {"orders": None, "orders_v2": {"__alias__": "orders"}}
        orders_v2 = [_schema("orders_v2", ["id"])]
        _run(
            lake,
            [*orders_v2, *_records("orders_v2", [{"id": 2, "v": "old"}])],
            stream_maps=stream_maps,
        )
        messages = [
            *orders_v2,
            _schema("orders", ["id"]),
            *_records("orders_v2", [{"id": 2, "v": "new"}]),
        ]

        _run(lake, messages, stream_maps=stream_maps)

        assert _rows(lake) == [(2, "new")]

    def test_a_copy_of_the_removed_stream_loads(self, lake: Lake):
        """``__source__`` copies a removed stream into a table of its own."""
        _load_earlier(lake)
        stream_maps = {"orders": None, "orders_copy": {"__source__": "orders"}}

        _run(lake, ORDERS, stream_maps=stream_maps, load_method="overwrite")

        assert _rows(lake) == EARLIER
        assert _rows(lake, "orders_copy") == [(1, "a")]

    def test_state_is_not_held_back(
        self, lake: Lake, capsys: pytest.CaptureFixture[str]
    ):
        """A removed overwrite stream never commits, so it holds no STATE.

        Every message drains here (max_record_age_minutes near zero), and the
        run then fails: the bookmark of the merged stream is out before.
        """
        messages = [
            *ORDERS,
            _schema("items", ["id"]),
            *_records("items", [{"id": 1, "v": "a"}]),
            STATE,
            *_records("items", [{"id": 2, "v": "b"}]),
            *_records("items", [{"id": "abc", "v": "bad"}]),
        ]
        capsys.readouterr()

        with pytest.raises(ValueError, match="whole numbers"):
            _run(
                lake,
                messages,
                stream_maps={"orders": None},
                overwrite_if_no_pk=True,
                max_record_age_minutes=1e-9,
            )

        assert '"bookmarks"' in capsys.readouterr().out

    def test_no_write_completed_event(
        self, lake: Lake, monkeypatch: pytest.MonkeyPatch
    ):
        """Only the tables the run loaded get a writeCompleted event."""
        published: list[dict] = []
        monkeypatch.setattr(
            target, "publish", lambda batch, *_args: published.extend(batch)
        )
        _load_earlier(lake)
        messages = [
            *ORDERS,
            _schema("items", ["id"]),
            *_records("items", [{"id": 2, "v": "b"}]),
        ]
        nats = {"servers": "nats://127.0.0.1:1", "subject": "lake.write.completed"}

        _run(
            lake,
            messages,
            stream_maps={"orders": None},
            load_method="overwrite",
            nats=nats,
        )

        assert [event["data"]["table"] for event in published] == ["items"]


class TestFilteredStream:
    """A stream map filtering every record out still loads its stream."""

    def test_an_overwrite_empties_the_table(self, lake: Lake):
        """No rows came, so a full refresh leaves none: as configured."""
        _load_earlier(lake)
        stream_maps = {"orders": {"__filter__": "False"}}

        _run(lake, ORDERS, stream_maps=stream_maps, load_method="overwrite")

        assert _rows(lake) == []

    def test_a_merge_leaves_the_table(self, lake: Lake):
        """No rows came, so a merge changes nothing."""
        _load_earlier(lake)
        stream_maps = {"orders": {"__filter__": "False"}}

        _run(lake, ORDERS, stream_maps=stream_maps)

        assert _rows(lake) == EARLIER
