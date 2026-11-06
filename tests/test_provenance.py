"""Load provenance on DuckLake snapshots (DUCKLKD-90)."""

from __future__ import annotations

import io
import json
import logging
from typing import TYPE_CHECKING, Any

import duckdb
import pytest

from target_ducklake import sinks
from target_ducklake.connector import DuckLakeConnector
from target_ducklake.provenance import CommitProvenance
from target_ducklake.target import Targetducklake

if TYPE_CHECKING:
    from collections.abc import Iterator

    from tests.catalogs import Lake

PROPERTIES = {"id": {"type": "integer"}, "v": {"type": ["null", "string"]}}


def _schema(stream: str, keys: list[str] | None) -> dict:
    message: dict[str, Any] = {
        "type": "SCHEMA",
        "stream": stream,
        "schema": {"type": "object", "properties": PROPERTIES},
    }
    if keys is not None:
        message["key_properties"] = keys
    return message


def _listen(lake: Lake, messages: list[dict], **config: object) -> Targetducklake:
    """Run the target over the messages; return it, for its run id."""
    loader = Targetducklake(config={**lake.config(), **config})
    try:
        loader.listen(
            file_input=io.StringIO("".join(json.dumps(m) + "\n" for m in messages))
        )
    finally:
        loader.target_connector.close()
    return loader


def _run(
    lake: Lake,
    streams: dict[str, list[dict]],
    *,
    keys: list[str] | None = None,
    **config: object,
) -> Targetducklake:
    """Load each stream's records in one run; every stream declares ``keys``."""
    messages: list[dict] = []
    for stream, records in streams.items():
        messages.append(_schema(stream, keys))
        messages.extend(
            {"type": "RECORD", "stream": stream, "record": r} for r in records
        )
    return _listen(lake, messages, **config)


def _commits(lake: Lake) -> list[tuple[str, dict]]:
    """The target's snapshots, oldest first: (commit message, extra info)."""
    rows = lake.query(
        "SELECT commit_message, commit_extra_info FROM lake.snapshots() "
        "WHERE author = 'target-ducklake' ORDER BY snapshot_id"
    )
    return [(message, json.loads(info)) for message, info in rows]


def _run_id(loader: Targetducklake) -> str:
    run_id = loader._connector().run_id  # noqa: SLF001
    assert run_id
    return run_id


RECORDS = [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}]


class TestCommitProvenance:
    """The message and extra info of one commit."""

    def test_a_merge(self):
        """Stream, load method and rows, in the message and the extra info."""
        commit = CommitProvenance("orders", "merge", rows=120)
        assert commit.message() == "orders: merge, 120 rows"
        assert json.loads(commit.extra_info("r1")) == {
            "run_id": "r1",
            "stream": "orders",
            "load_method": "merge",
            "rows": 120,
        }

    def test_an_operation_without_rows(self):
        """Unset keys are left out, not written as null."""
        commit = CommitProvenance("orders", "merge", operation="activate-version")
        assert commit.message() == "orders: merge (activate-version)"
        assert json.loads(commit.extra_info(None)) == {
            "stream": "orders",
            "load_method": "merge",
            "operation": "activate-version",
        }

    def test_zero_rows_are_recorded(self):
        """An empty overwrite loads 0 rows, which is not unknown."""
        commit = CommitProvenance("t", "overwrite", rows=0, operation="full-refresh")
        assert json.loads(commit.extra_info("r"))["rows"] == 0
        assert commit.message() == "t: overwrite (full-refresh), 0 rows"


class TestThroughListen:
    """Read back from the catalog's snapshots (SQLite, and Postgres when set)."""

    def test_a_merge_names_the_stream_load_method_and_rows(self, lake: Lake):
        """The commit names the run, the stream, the load method and its rows."""
        loader = _run(lake, {"orders": RECORDS}, keys=["id"])
        assert _commits(lake) == [
            (
                "orders: merge, 2 rows",
                {
                    "run_id": _run_id(loader),
                    "stream": "orders",
                    "load_method": "merge",
                    "rows": 2,
                },
            )
        ]

    def test_an_append(self, lake: Lake):
        """Without keys, the load method applied is append."""
        _run(lake, {"events": RECORDS})
        ((message, info),) = _commits(lake)
        assert message == "events: append, 2 rows"
        assert (info["load_method"], info["rows"]) == ("append", 2)
        assert "operation" not in info

    def test_each_batch_is_its_own_commit(self, lake: Lake):
        """Rows are each commit's, not the stream's."""
        records = [{"id": i} for i in range(5)]
        _run(lake, {"t": records}, keys=["id"], batch_size_rows=2)
        assert [info["rows"] for _, info in _commits(lake)] == [2, 2, 1]

    @pytest.mark.parametrize(
        "config",
        [
            {"load_method": "overwrite"},
            {"stream_overrides": {"snap": {"write_mode": "overwrite"}}},
            {"overwrite_if_no_pk": True},
        ],
        ids=["load-method", "write-mode", "overwrite-if-no-pk"],
    )
    def test_an_overwrite_is_marked_a_full_refresh(self, lake: Lake, config: dict):
        """However the stream comes to overwrite, its replace is a full refresh."""
        _run(lake, {"snap": RECORDS}, **config)
        ((message, info),) = _commits(lake)
        assert message == "snap: overwrite (full-refresh), 2 rows"
        assert (info["load_method"], info["operation"], info["rows"]) == (
            "overwrite",
            "full-refresh",
            2,
        )

    def test_an_empty_overwrite_empties_the_table_with_0_rows(self, lake: Lake):
        """A run without records still replaces the table: 0 rows, not unknown."""
        _run(lake, {"snap": RECORDS}, load_method="overwrite")
        _run(lake, {"snap": []}, load_method="overwrite")
        (_, info) = _commits(lake)[-1]
        assert (info["operation"], info["rows"]) == ("full-refresh", 0)

    @pytest.mark.parametrize("hard_delete", [False, True], ids=["soft", "hard"])
    def test_activate_version(self, lake: Lake, *, hard_delete: bool):
        """The merge, then ACTIVATE_VERSION's retirement, which loads no rows."""
        messages = [
            _schema("m", ["id"]),
            {"type": "RECORD", "stream": "m", "record": {"id": 2}, "version": 0},
            {"type": "RECORD", "stream": "m", "record": {"id": 1}, "version": 1},
            {"type": "ACTIVATE_VERSION", "stream": "m", "version": 1},
        ]
        _listen(lake, messages, add_record_metadata=True, hard_delete=hard_delete)
        assert [info.get("operation") for _, info in _commits(lake)] == [
            None,
            "activate-version",
        ]
        (message, info) = _commits(lake)[-1]
        assert message == "m: merge (activate-version)"
        assert "rows" not in info

    def test_an_inlined_flush(self, lake: Lake):
        """The inlined insert, then the flush that writes its file."""
        _run(
            lake,
            {"t": RECORDS},
            data_inlining_row_limit=10,
            inlined_data_flush_rows=1,
        )
        assert [message for message, _ in _commits(lake)] == [
            "t: append, 2 rows",
            "t: append (flush-inlined-data)",
        ]

    def test_one_run_one_run_id_per_run(self, lake: Lake):
        """Every commit of a run names it; the next run has its own."""
        first = _run(lake, {"a": RECORDS, "b": RECORDS}, keys=["id"])
        second = _run(lake, {"a": RECORDS}, keys=["id"])
        run_ids = [info["run_id"] for _, info in _commits(lake)]
        assert run_ids == [_run_id(first)] * 2 + [_run_id(second)]
        assert _run_id(first) != _run_id(second)

    def test_a_stream_map_alias_is_the_stream(self, lake: Lake):
        """The stream as it loads the table: the alias a stream map gives it."""
        _run(
            lake,
            {"raw": RECORDS},
            keys=["id"],
            stream_maps={"raw": {"__alias__": "clean"}},
        )
        ((message, info),) = _commits(lake)
        assert info["stream"] == "clean"
        assert message.startswith("clean: ")

    def test_quotes_are_escaped(self, lake: Lake):
        """The message and extra info are SQL literals, quoted safely.

        Run on the catalog directly: a stream whose table name has a quote
        doesn't load yet (DUCKLKD-128).
        """
        _run(lake, {"t": RECORDS}, keys=["id"])
        connector = DuckLakeConnector.__new__(DuckLakeConnector)
        connector.catalog_name = "lake"
        connector.run_id = "r'1"
        begin = connector._begin_transaction(  # noqa: SLF001
            CommitProvenance("it's", "append", rows=1)
        )
        con = duckdb.connect()
        con.execute(lake.attach_sql())
        con.execute(f"{begin}\nINSERT INTO lake.s.t (id) VALUES (3);\nCOMMIT;")
        con.close()
        (message, info) = _commits(lake)[-1]
        assert message == "it's: append, 1 row"
        assert (info["stream"], info["run_id"]) == ("it's", "r'1")

    def test_schema_changes_are_not_tagged(self, lake: Lake):
        """Only data commits carry provenance; CREATE TABLE is DuckLake's own."""
        _run(lake, {"t": RECORDS}, keys=["id"])
        untagged = lake.query(
            "SELECT count(*) FROM lake.snapshots() WHERE author IS NULL "
            "AND snapshot_id > 0"
        )[0][0]
        assert untagged >= 1
        assert len(_commits(lake)) == 1


@pytest.fixture
def warnings(caplog: pytest.LogCaptureFixture) -> Iterator[pytest.LogCaptureFixture]:
    """The target's warnings, caught on its own logger."""
    logger = logging.getLogger("target-ducklake")
    logger.addHandler(caplog.handler)
    caplog.handler.setLevel(logging.WARNING)
    try:
        yield caplog
    finally:
        logger.removeHandler(caplog.handler)


def test_rows_that_cant_be_counted_are_left_out(
    lake: Lake,
    monkeypatch: pytest.MonkeyPatch,
    warnings: pytest.LogCaptureFixture,
):
    """Counting is advisory: the commit goes ahead, without rows, and warns."""

    def unreadable(_path: str) -> object:
        msg = "no footer"
        raise OSError(msg)

    monkeypatch.setattr(sinks.pq, "read_metadata", unreadable)
    _run(lake, {"t": RECORDS}, keys=["id"])
    ((message, info),) = _commits(lake)
    assert message == "t: merge"
    assert "rows" not in info
    assert lake.query("SELECT count(*) FROM lake.s.t") == [(2,)]
    assert "Couldn't count the rows of a batch of t" in warnings.text
