"""writeCompleted events over NATS (DUCKLKD-86)."""

from __future__ import annotations

import asyncio
import dataclasses
import importlib.util
import io
import json
import logging
import os
import shutil
import socket
import ssl
import subprocess
import sys
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

import duckdb
import pytest
from singer_sdk.exceptions import ConfigValidationError

from target_ducklake import nats_publish, target
from target_ducklake.connector import DuckLakeConnector, _changes_table
from target_ducklake.nats_publish import (
    NatsSettings,
    PlaintextRefusedError,
    nats_settings,
)
from target_ducklake.target import Targetducklake
from target_ducklake.write_completed import TableSnapshots, WriteLedger, event_id

if TYPE_CHECKING:
    from collections.abc import Iterator

    from tests.catalogs import Lake

# A NATS server with JetStream (nats:2-alpine -js) for the end-to-end test,
# as a URL. Unset, that test is skipped.
NATS_ENV = "TARGET_DUCKLAKE_TEST_NATS"
NATS = {"servers": "nats://127.0.0.1:1", "subject": "lake.write.completed"}
PROPERTIES = {"id": {"type": "integer"}, "v": {"type": ["null", "string"]}}


def _messages(streams: dict[str, list[dict]], keys: list[str] | None) -> str:
    messages: list[dict[str, Any]] = []
    for stream, records in streams.items():
        schema: dict[str, Any] = {
            "type": "SCHEMA",
            "stream": stream,
            "schema": {"type": "object", "properties": PROPERTIES},
        }
        if keys is not None:
            schema["key_properties"] = keys
        messages.append(schema)
        messages.extend(
            {"type": "RECORD", "stream": stream, "record": r} for r in records
        )
    return "".join(json.dumps(m) + "\n" for m in messages)


def _run(
    lake: Lake,
    streams: dict[str, list[dict]],
    *,
    keys: list[str] | None = None,
    **config: object,
) -> Targetducklake:
    """Load the streams in one run, with events on unless config says otherwise."""
    loader = Targetducklake(config={**lake.config(), "nats": NATS, **config})
    try:
        loader.listen(file_input=io.StringIO(_messages(streams, keys)))
    finally:
        loader.target_connector.close()
    return loader


@pytest.fixture
def warnings(caplog: pytest.LogCaptureFixture) -> Iterator[pytest.LogCaptureFixture]:
    """The target's warnings, caught on its own logger (see test_batch_size)."""
    logger = logging.getLogger("target-ducklake")
    logger.addHandler(caplog.handler)
    caplog.handler.setLevel(logging.WARNING)
    try:
        yield caplog
    finally:
        logger.removeHandler(caplog.handler)


@pytest.fixture
def published(monkeypatch: pytest.MonkeyPatch) -> list[dict]:
    """The events the target hands to the publisher, instead of publishing."""
    events: list[dict] = []

    def capture(batch: list[dict], *_args: object, **_kwargs: object) -> bool:
        events.extend(batch)
        return True

    monkeypatch.setattr(target, "publish", capture)
    return events


class TestPayload:
    """The event of each table, from a real run."""

    def test_one_event_for_a_merged_table(self, lake: Lake, published: list[dict]):
        """Identity, counts and the catalog's snapshot, in the contract's shape."""
        before = datetime.now(timezone.utc)
        _run(lake, {"orders": [{"id": 1}, {"id": 2}, {"id": 2}]}, keys=["id"])
        (event,) = published
        data = event["data"]
        warehouse = lake.data_path.rstrip("/")
        snapshot_id, uuid_ = lake.query(
            "SELECT max(snapshot_id), "
            "(SELECT CAST(table_uuid AS VARCHAR) FROM ducklake_table_info('lake')) "
            "FROM lake.snapshots()"
        )[0]
        assert event["type"] == "writeCompleted"
        assert event["source"] == f"uri:target-ducklake:{socket.gethostname()}"
        assert event["id"] == f"{warehouse}/s/orders@{snapshot_id}"
        assert data["warehouse"] == warehouse
        assert (data["namespace"], data["table"]) == (["s"], "orders")
        assert data["snapshot_id"] == snapshot_id
        assert data["table_uuid"] == uuid_
        # The batch's duplicate key is loaded once.
        assert data["rows"] == 2  # noqa: PLR2004
        assert (data["load_method"], data["full_refresh"]) == ("upsert", False)
        assert data["streams"] == ["orders"]
        assert (data["snapshot_count"], data["data_files_added"]) == (1, 1)
        files_bytes = lake.query(
            "SELECT sum(file_size_bytes) FROM ducklake_table_info('lake')"
        )[0][0]
        assert data["bytes_added"] == files_bytes
        started = datetime.fromisoformat(data["started_at"])
        completed = datetime.fromisoformat(data["completed_at"])
        assert before <= started <= completed <= datetime.now(timezone.utc)
        assert "catalog_uri" not in data
        assert "partitions_changed" not in data

    def test_counts_only_this_runs_commits(self, lake: Lake, published: list[dict]):
        """A second run reports its own batches, files and snapshots."""
        _run(lake, {"orders": [{"id": 1}]}, keys=["id"])
        published.clear()
        _run(
            lake,
            {"orders": [{"id": 1}, {"id": 2}, {"id": 3}]},
            keys=["id"],
            batch_size_rows=2,
        )
        (event,) = published
        assert event["data"]["rows"] == 3  # noqa: PLR2004
        assert event["data"]["snapshot_count"] == 2  # noqa: PLR2004
        assert event["data"]["data_files_added"] == 2  # noqa: PLR2004

    def test_one_event_for_a_table_written_by_two_streams(
        self, lake: Lake, published: list[dict]
    ):
        """Both streams are listed, and their rows summed."""
        _run(
            lake,
            {"a-orders": [{"id": 1}], "b-orders": [{"id": 2}, {"id": 3}]},
            load_method="append",
        )
        (event,) = published
        assert event["data"]["table"] == "orders"
        # In first-commit order, which parallel drains (Postgres) make vary.
        assert sorted(event["data"]["streams"]) == ["a-orders", "b-orders"]
        assert event["data"]["rows"] == 3  # noqa: PLR2004
        assert event["data"]["load_method"] == "append-only"

    def test_overwrite_is_a_full_refresh(self, lake: Lake, published: list[dict]):
        """An overwrite reports one commit that replaced the table."""
        _run(lake, {"t": [{"id": 1}, {"id": 2}]}, load_method="overwrite")
        (event,) = published
        assert (event["data"]["load_method"], event["data"]["full_refresh"]) == (
            "overwrite",
            True,
        )
        assert event["data"]["rows"] == 2  # noqa: PLR2004

    def test_warehouse_setting_names_the_warehouse(
        self, lake: Lake, published: list[dict]
    ):
        """target-iceberg's warehouse setting, in a shared config."""
        _run(lake, {"t": [{"id": 1}]}, warehouse="acme")
        (event,) = published
        assert event["data"]["warehouse"] == "acme"
        assert event["id"].startswith("acme/s/t@")

    def test_no_event_for_a_table_with_no_commit(
        self, lake: Lake, published: list[dict]
    ):
        """A stream with no records commits nothing, so has no event."""
        _run(lake, {"empty": [], "t": [{"id": 1}]})
        assert [event["data"]["table"] for event in published] == ["t"]

    def test_off_without_servers_and_subject(self, lake: Lake, published: list[dict]):
        """Either one alone leaves events off, and nothing is recorded."""
        loader = _run(lake, {"t": [{"id": 1}]}, nats={"subject": "s"})
        assert loader.write_ledger is None
        assert published == []


class TestRunTagging:
    """Only this run's tagged commits count (review of #121, should-fix 2)."""

    def test_another_writers_commit_is_not_counted(
        self,
        lake: Lake,
        published: list[dict],
        monkeypatch: pytest.MonkeyPatch,
    ):
        """A foreign insert between the run's commit and its event changes nothing."""
        build = Targetducklake.write_completed_events

        def foreign_commit_first(self: Targetducklake) -> list[dict]:
            lake.query("INSERT INTO lake.s.t VALUES (99, 'foreign')")
            return build(self)

        monkeypatch.setattr(
            Targetducklake, "write_completed_events", foreign_commit_first
        )
        _run(lake, {"t": [{"id": 1}]}, keys=["id"])
        (event,) = published
        ours = lake.query(
            "SELECT snapshot_id FROM lake.snapshots() WHERE author = 'target-ducklake'"
        )
        latest = lake.query("SELECT max(snapshot_id) FROM lake.snapshots()")[0][0]
        assert ours == [(event["data"]["snapshot_id"],)]
        assert event["data"]["snapshot_id"] < latest
        assert event["data"]["snapshot_count"] == 1
        assert event["data"]["data_files_added"] == 1
        assert event["data"]["rows"] == 1

    def test_every_data_transaction_is_tagged(
        self, lake: Lake, published: list[dict], monkeypatch: pytest.MonkeyPatch
    ):
        """Merge, ACTIVATE_VERSION, overwrite: each BEGIN carries the run's tag."""
        transactions: list[str] = []
        execute = DuckLakeConnector.execute

        def recording(
            self: DuckLakeConnector, query: str, *args: Any, **kwargs: Any
        ) -> object:
            if "BEGIN TRANSACTION" in query:
                transactions.append(query)
            return execute(self, query, *args, **kwargs)

        monkeypatch.setattr(DuckLakeConnector, "execute", recording)
        schema = {"type": "object", "properties": PROPERTIES}
        messages = [
            {
                "type": "SCHEMA",
                "stream": "m",
                "schema": schema,
                "key_properties": ["id"],
            },
            {"type": "RECORD", "stream": "m", "record": {"id": 2}, "version": 0},
            {"type": "RECORD", "stream": "m", "record": {"id": 1}, "version": 1},
            {"type": "ACTIVATE_VERSION", "stream": "m", "version": 1},
            {"type": "SCHEMA", "stream": "o", "schema": schema},
            {"type": "RECORD", "stream": "o", "record": {"id": 1}},
        ]
        loader = Targetducklake(
            config={
                **lake.config(),
                "nats": NATS,
                "add_record_metadata": True,
                "stream_overrides": {"o": {"load_method": "overwrite"}},
            }
        )
        try:
            loader.listen(
                file_input=io.StringIO("".join(json.dumps(m) + "\n" for m in messages))
            )
        finally:
            loader.target_connector.close()
        run_id = loader._connector().run_id  # noqa: SLF001
        kinds = {"MERGE": "DELETE FROM", "VERSION": "UPDATE", "REPLACE": "INSERT"}
        assert all(any(kind in q for q in transactions) for kind in kinds.values())
        for query in transactions:
            assert f"set_commit_message('target-ducklake', 'run {run_id}'" in query
        tagged = lake.query(
            "SELECT DISTINCT json_extract_string(commit_extra_info, '$.run_id') "
            "FROM lake.snapshots() WHERE author = 'target-ducklake'"
        )
        assert tagged == [(run_id,)]
        by_table = {event["data"]["table"]: event["data"] for event in published}
        # The merge, then ACTIVATE_VERSION's update.
        assert by_table["m"]["snapshot_count"] == 2  # noqa: PLR2004
        assert by_table["o"]["snapshot_count"] == 1

    def test_a_retried_commit_keeps_its_tag(
        self, lake: Lake, published: list[dict], monkeypatch: pytest.MonkeyPatch
    ):
        """The tag is part of the transaction's SQL, so execute's retry repeats it."""
        failed: list[str] = []
        real_connection = DuckLakeConnector.connection

        class FlakyCursor:
            def __init__(self, cursor: duckdb.DuckDBPyConnection) -> None:
                self._cursor = cursor

            def execute(
                self, query: str, parameters: object = None
            ) -> duckdb.DuckDBPyConnection:
                if "INSERT INTO" in query and not failed:
                    failed.append(query)
                    msg = "connection dropped"
                    raise duckdb.IOException(msg)
                return self._cursor.execute(query, parameters)

        class FlakyConnection:
            def __init__(self, connection: duckdb.DuckDBPyConnection) -> None:
                self._connection = connection

            def cursor(self) -> FlakyCursor:
                return FlakyCursor(self._connection.cursor())

            def __getattr__(self, name: str) -> object:
                return getattr(self._connection, name)

        monkeypatch.setattr(
            DuckLakeConnector,
            "connection",
            property(lambda self: FlakyConnection(real_connection.fget(self))),  # type: ignore[attr-defined]
        )
        monkeypatch.setattr("target_ducklake.connector.time.sleep", lambda _s: None)
        _run(lake, {"t": [{"id": 1}]}, load_method="append")
        assert failed
        assert "set_commit_message" in failed[0]
        (event,) = published
        assert event["data"]["snapshot_count"] == 1
        assert lake.query(
            "SELECT count(*) FROM lake.snapshots() WHERE author = 'target-ducklake'"
        ) == [(1,)]

    def test_an_inlined_flush_is_this_runs_commit(
        self, lake: Lake, published: list[dict]
    ):
        """Inlined insert, then the flush that writes its file: both counted."""
        _run(
            lake,
            {"t": [{"id": 1}, {"id": 2}]},
            data_inlining_row_limit=10,
            inlined_data_flush_rows=1,
        )
        (event,) = published
        assert event["data"]["snapshot_count"] == 2  # noqa: PLR2004
        assert event["data"]["data_files_added"] == 1
        assert event["data"]["rows"] == 2  # noqa: PLR2004

    def test_tables_without_events_are_not_tagged(self, lake: Lake):
        """With events off, commits carry no tag, as before."""
        _run(lake, {"t": [{"id": 1}]}, nats={})
        assert lake.query(
            "SELECT count(*) FROM lake.snapshots() WHERE author IS NOT NULL"
        ) == [(0,)]


class TestWithTypeSource:
    """A type_source stream's events (DUCKLKD-43 with DUCKLKD-86)."""

    @staticmethod
    def _load(lake: Lake, records: list[dict], **config: object) -> Targetducklake:
        text = {"type": ["string", "null"]}
        schema = {
            "type": "object",
            "properties": {"id": {"type": "string"}, "qty": text, "day": text},
        }
        messages = [
            {
                "type": "SCHEMA",
                "stream": "t",
                "schema": schema,
                "key_properties": ["id"],
            },
            *({"type": "RECORD", "stream": "t", "record": r} for r in records),
        ]
        loader = Targetducklake(
            config={
                **lake.config(),
                "nats": NATS,
                "stream_defaults": {
                    "type_source": {"id": "BIGINT", "qty": "int", "day": "date"},
                    "on_error": "null",
                },
                **config,
            }
        )
        try:
            loader.listen(
                file_input=io.StringIO("".join(json.dumps(m) + "\n" for m in messages))
            )
        finally:
            loader.target_connector.close()
        return loader

    @pytest.mark.parametrize(
        "config",
        [
            {},
            {"load_method": "overwrite"},
            {"data_inlining_row_limit": 10, "inlined_data_flush_rows": 1},
        ],
        ids=["merge", "overwrite", "inlined-then-flushed"],
    )
    def test_coerced_rows_are_counted_and_tagged(
        self, lake: Lake, published: list[dict], config: dict
    ):
        """Coerced values load, a bad one as NULL, and the event counts every row."""
        records: list[dict] = [
            {"id": "1", "qty": "3", "day": "2026-01-02"},
            {"id": "2", "qty": "lots", "day": None},
        ]
        loader = self._load(lake, records, **config)
        assert lake.query("SELECT id, qty, day::VARCHAR FROM lake.s.t ORDER BY id") == [
            (1, 3, "2026-01-02"),
            (2, None, None),
        ]
        (event,) = published
        assert event["data"]["rows"] == 2  # noqa: PLR2004
        run_id = loader._connector().run_id  # noqa: SLF001
        tagged = lake.query(
            "SELECT count(*) FROM lake.snapshots() "  # noqa: S608 (a uuid hex)
            "WHERE json_extract_string(commit_extra_info, '$.run_id') = "
            f"'{run_id}'"
        )[0][0]
        assert event["data"]["snapshot_count"] == tagged
        assert event["data"]["full_refresh"] == ("load_method" in config)


class TestLoadMethodNames:
    """applied_load_method, by the contract's names."""

    @pytest.mark.parametrize(
        ("keys", "config", "expected"),
        [
            (["id"], {"load_method": "append"}, "append-only"),
            (None, {}, "append-only"),
            (["id"], {}, "upsert"),
            (None, {"overwrite_if_no_pk": True}, "overwrite"),
        ],
        ids=["keyed-append", "keyless-merge", "keyed-merge", "overwrite-if-no-pk"],
    )
    def test_names(
        self,
        lake: Lake,
        published: list[dict],
        keys: list[str] | None,
        config: dict,
        expected: str,
    ):
        """A merge without keys appends, so it is named append-only."""
        _run(lake, {"t": [{"id": 1}]}, keys=keys, **config)
        (event,) = published
        assert event["data"]["load_method"] == expected


class TestCompletedRunOnly:
    """Events go out only when the whole run completed."""

    def test_a_failed_commit_emits_nothing(
        self,
        lake: Lake,
        published: list[dict],
        monkeypatch: pytest.MonkeyPatch,
    ):
        """One table committed, another failed: the run publishes nothing."""
        merge = DuckLakeConnector.merge_into_table

        def fail_on_b(self: DuckLakeConnector, **kwargs: Any) -> None:
            if kwargs["table_name"] == "b":
                msg = "commit failed"
                raise RuntimeError(msg)
            merge(self, **kwargs)

        monkeypatch.setattr(DuckLakeConnector, "merge_into_table", fail_on_b)
        with pytest.raises(RuntimeError, match="commit failed"):
            _run(lake, {"a": [{"id": 1}], "b": [{"id": 1}]}, keys=["id"])
        assert lake.query("SELECT count(*) FROM lake.s.a") == [(1,)]
        assert published == []

    def test_unreadable_snapshots_warn(
        self,
        lake: Lake,
        published: list[dict],
        monkeypatch: pytest.MonkeyPatch,
        warnings: pytest.LogCaptureFixture,
    ):
        """Reading the catalog after the run is advisory too."""

        def fail(*_args: object) -> None:
            msg = "catalog gone"
            raise RuntimeError(msg)

        monkeypatch.setattr(DuckLakeConnector, "table_snapshots", fail)
        _run(lake, {"t": [{"id": 1}]})
        assert published == []
        assert "RuntimeError: catalog gone" in warnings.text


class TestAccountingNeverFailsTheRun:
    """Reads made only for events warn instead of failing (should-fix 5)."""

    def test_uncountable_rows_are_left_out(
        self,
        lake: Lake,
        published: list[dict],
        monkeypatch: pytest.MonkeyPatch,
        warnings: pytest.LogCaptureFixture,
    ):
        """A parquet footer that can't be read: the event has no rows field."""

        def unreadable(*_args: object) -> None:
            msg = "footer unreadable"
            raise OSError(msg)

        monkeypatch.setattr("target_ducklake.sinks.pq.read_metadata", unreadable)
        _run(lake, {"t": [{"id": 1}]})
        assert lake.query("SELECT count(*) FROM lake.s.t") == [(1,)]
        (event,) = published
        assert "rows" not in event["data"]
        assert "footer unreadable" in warnings.text

    def test_one_unreadable_table_skips_only_its_event(
        self,
        lake: Lake,
        published: list[dict],
        monkeypatch: pytest.MonkeyPatch,
        warnings: pytest.LogCaptureFixture,
    ):
        """The other table's event still goes out."""
        snapshots = DuckLakeConnector.table_snapshots

        def fail_on_a(
            self: DuckLakeConnector, schema: str, table: str
        ) -> TableSnapshots | None:
            if table == "a":
                msg = "catalog gone"
                raise RuntimeError(msg)
            return snapshots(self, schema, table)

        monkeypatch.setattr(DuckLakeConnector, "table_snapshots", fail_on_a)
        _run(lake, {"a": [{"id": 1}], "b": [{"id": 1}]})
        assert [event["data"]["table"] for event in published] == ["b"]
        assert "No writeCompleted event for s.a" in warnings.text

    def test_catalog_secrets_are_redacted(
        self,
        lake: Lake,
        monkeypatch: pytest.MonkeyPatch,
        warnings: pytest.LogCaptureFixture,
    ):
        """An error echoing the catalog's password or a secret key is redacted."""

        def echoing(*_args: object) -> None:
            msg = "could not connect: host=db password=cat-secret key=sk-secret"
            raise RuntimeError(msg)

        monkeypatch.setattr(DuckLakeConnector, "table_snapshots", echoing)
        loader = Targetducklake(config={**lake.config(), "nats": NATS})
        loader._config["catalog_url"] = "host=db password=cat-secret"  # noqa: SLF001
        loader._config["secret_key"] = "sk-secret"  # noqa: SLF001, S105
        loader.write_ledger.record(  # type: ignore[union-attr]
            "s",
            "t",
            "t",
            rows=1,
            load_method="append",
            started_at=datetime.now(timezone.utc),
            completed_at=datetime.now(timezone.utc),
        )
        assert loader.write_completed_events() == []
        assert "could not connect" in warnings.text
        assert "cat-secret" not in warnings.text
        assert "sk-secret" not in warnings.text


class _FakeServer:
    """A TCP server that sends ``greeting`` (none: silent), recording what it gets."""

    def __init__(self, greeting: bytes = b"") -> None:
        self.greeting = greeting
        self.received = b""
        self.socket = socket.create_server(("127.0.0.1", 0))
        self.port = self.socket.getsockname()[1]
        self._accepted: list[socket.socket] = []
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self) -> None:
        try:
            while True:
                connection, _ = self.socket.accept()
                self._accepted.append(connection)
                threading.Thread(
                    target=self._serve, args=(connection,), daemon=True
                ).start()
        except OSError:
            return

    def _serve(self, connection: socket.socket) -> None:
        try:
            if self.greeting:
                connection.sendall(self.greeting)
            while data := connection.recv(4096):
                self.received += data
        except OSError:
            return

    def close(self) -> None:
        for connection in self._accepted:
            connection.close()
        self.socket.close()


@pytest.fixture
def silent_server() -> Iterator[_FakeServer]:
    """A silent TCP server, closed after the test."""
    server = _FakeServer()
    yield server
    server.close()


@pytest.fixture
def plaintext_server() -> Iterator[_FakeServer]:
    """A server whose INFO does not require TLS, as a plain NATS server's."""
    server = _FakeServer(b'INFO {"server_id":"x","tls_required":false}\r\n')
    yield server
    server.close()


EVENT = {"id": "w/s/t@1", "type": "writeCompleted"}


class TestPublishFailure:
    """A failure to publish is one warning, bounded in time, without secrets."""

    def test_refused_connection_warns(self, caplog: pytest.LogCaptureFixture):
        """A broker that refuses the connection."""
        settings = NatsSettings(servers=("nats://127.0.0.1:1",), subject="x")
        with caplog.at_level(logging.WARNING):
            assert not nats_publish.publish([EVENT], settings, logging.getLogger())
        assert "writeCompleted events were not published" in caplog.text
        assert "nats://127.0.0.1:1" in caplog.text

    def test_silent_server_is_bounded(
        self, silent_server: _FakeServer, caplog: pytest.LogCaptureFixture
    ):
        """A server that never answers costs at most the timeout."""
        settings = NatsSettings(
            servers=(f"nats://127.0.0.1:{silent_server.port}",), subject="x"
        )
        started = time.monotonic()
        with caplog.at_level(logging.WARNING):
            assert not nats_publish.publish(
                [EVENT], settings, logging.getLogger(), timeout=1
            )
        assert time.monotonic() - started < 5  # noqa: PLR2004
        assert "were not published" in caplog.text

    def test_a_run_succeeds_when_publishing_fails(
        self, lake: Lake, warnings: pytest.LogCaptureFixture
    ):
        """The run's data is committed and the run ends normally."""
        _run(lake, {"t": [{"id": 1}]})
        assert lake.query("SELECT count(*) FROM lake.s.t") == [(1,)]
        assert "writeCompleted events were not published" in warnings.text

    def test_credentials_are_redacted(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ):
        """URL credentials, passwords and tokens never reach the log."""

        async def echoing(*_args: object, **_kwargs: object) -> None:
            msg = (
                "auth failed for nats://alice:url-secret@host with pw-secret tok-secret"
            )
            raise RuntimeError(msg)

        monkeypatch.setattr(nats_publish._Attempt, "_publish", echoing)  # noqa: SLF001
        settings = NatsSettings(
            servers=("nats://alice:url-secret@host:4222", "other:4222"),
            subject="x",
            username="bob",
            password="pw-secret",  # noqa: S106
            token="tok-secret",  # noqa: S106
        )
        with caplog.at_level(logging.WARNING):
            nats_publish.publish([EVENT], settings, logging.getLogger())
        for secret in ("url-secret", "pw-secret", "tok-secret", "alice"):
            assert secret not in caplog.text
        assert "nats://***:***@host:4222, other:4222" in caplog.text


class TestTransportSafety:
    """TLS is never downgraded, and the bound holds through name resolution."""

    def test_tls_refuses_a_plaintext_server(
        self, plaintext_server: _FakeServer, caplog: pytest.LogCaptureFixture
    ):
        """Nothing, credentials included, is sent to a server without TLS."""
        settings = NatsSettings(
            servers=(f"nats://127.0.0.1:{plaintext_server.port}",),
            subject="x",
            password="pw-secret",  # noqa: S106
            username="u",
            tls=True,
        )
        with caplog.at_level(logging.WARNING):
            assert not nats_publish.publish([EVENT], settings, logging.getLogger())
        assert "PlaintextRefusedError" in caplog.text
        time.sleep(0.2)
        assert plaintext_server.received == b""

    def test_a_server_requiring_tls_passes_the_check(self):
        """The check reads INFO's tls_required."""
        server = _FakeServer(b'INFO {"tls_required":true}\r\n')
        try:
            asyncio.run(nats_publish._require_tls(f"nats://127.0.0.1:{server.port}"))  # noqa: SLF001
        finally:
            server.close()
        plain = _FakeServer(b"INFO {}\r\n")
        try:
            with pytest.raises(PlaintextRefusedError):
                asyncio.run(nats_publish._require_tls(f"nats://127.0.0.1:{plain.port}"))  # noqa: SLF001
        finally:
            plain.close()

    def test_slow_name_resolution_is_bounded(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ):
        """A resolver that hangs costs the timeout, not the resolver's time."""
        resolve = socket.getaddrinfo

        def hanging(*args: Any, **kwargs: Any) -> object:
            time.sleep(10)
            return resolve(*args, **kwargs)

        monkeypatch.setattr(socket, "getaddrinfo", hanging)
        settings = NatsSettings(servers=("nats://example.invalid:4222",), subject="x")
        started = time.monotonic()
        with caplog.at_level(logging.WARNING):
            assert not nats_publish.publish(
                [EVENT], settings, logging.getLogger(), timeout=1
            )
        assert time.monotonic() - started < 3  # noqa: PLR2004
        assert "no answer within 1s" in caplog.text

    def test_the_creds_file_client_is_installed(self):
        """nats-py reads a .creds file with nkeys, which the extra installs."""
        assert importlib.util.find_spec("nkeys") is not None


class _FakeNats:
    """A minimal NATS server: INFO, then PONG for each PING, recording PUBs.

    With ``tls`` (a server ``SSLContext``), INFO requires TLS and the
    connection is upgraded after it, as nats-server does.
    """

    def __init__(self, tls: ssl.SSLContext | None = None) -> None:
        self.tls = tls
        self.published: list[tuple[str, bytes]] = []
        self.socket = socket.create_server(("127.0.0.1", 0))
        self.port = self.socket.getsockname()[1]
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self) -> None:
        try:
            while True:
                connection, _ = self.socket.accept()
                threading.Thread(
                    target=self._serve, args=(connection,), daemon=True
                ).start()
        except OSError:
            return

    def _serve(self, connection: socket.socket) -> None:
        info = {"server_id": "fake", "max_payload": 1048576, "headers": True}
        if self.tls is not None:
            info["tls_required"] = True
        try:
            connection.sendall(b"INFO " + json.dumps(info).encode() + b"\r\n")
            if self.tls is not None:
                connection = self.tls.wrap_socket(connection, server_side=True)
            stream = connection.makefile("rb")
            while line := stream.readline():
                if line.startswith(b"PING"):
                    connection.sendall(b"PONG\r\n")
                elif line.startswith(b"PUB"):
                    _, subject, *_, size = line.split()
                    body = stream.read(int(size) + 2)[: int(size)]
                    self.published.append((subject.decode(), body))
        except (OSError, ssl.SSLError):
            return

    def close(self) -> None:
        self.socket.close()


def _certificates(directory: Path) -> tuple[Path, Path, Path]:
    """A CA, and a server certificate for localhost it signed, made with openssl."""
    if shutil.which("openssl") is None:
        pytest.skip("openssl is needed to make a test certificate")
    ca_key, ca, key, csr, cert = (
        directory / name for name in ("ca.key", "ca.pem", "s.key", "s.csr", "s.pem")
    )
    ext = directory / "ext.cnf"
    ext.write_text("subjectAltName=DNS:localhost\n")
    for command in (
        ["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", ca_key,
         "-out", ca, "-days", "2", "-subj", "/CN=test-ca"],
        ["req", "-newkey", "rsa:2048", "-nodes", "-keyout", key, "-out", csr,
         "-subj", "/CN=localhost"],
        ["x509", "-req", "-in", csr, "-CA", ca, "-CAkey", ca_key,
         "-CAcreateserial", "-out", cert, "-days", "2", "-extfile", ext],
    ):  # fmt: skip
        subprocess.run(["openssl", *map(str, command)], check=True, capture_output=True)  # noqa: S603, S607
    return ca, cert, key


class TestOutcomeReporting:
    """Only a real publish is reported as one (review of #121, round 3)."""

    def test_a_loop_that_cannot_start_is_a_failure(
        self, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
    ):
        """No event loop, no publish: a warning, never "Published"."""

        def no_loop() -> None:
            msg = "Too many open files"
            raise OSError(msg)

        monkeypatch.setattr(nats_publish, "_ResolvingLoop", no_loop)
        settings = NatsSettings(servers=("nats://127.0.0.1:1",), subject="x")
        with caplog.at_level(logging.INFO):
            assert not nats_publish.publish([EVENT], settings, logging.getLogger())
        assert "Published" not in caplog.text
        assert "OSError: Too many open files" in caplog.text

    def test_a_silent_server_names_the_connect_timeout(
        self, silent_server: _FakeServer, caplog: pytest.LogCaptureFixture
    ):
        """It fails on the 2 s connect timeout, not the 10 s bound, and says so."""
        settings = NatsSettings(
            servers=(f"nats://127.0.0.1:{silent_server.port}",), subject="x"
        )
        started = time.monotonic()
        with caplog.at_level(logging.WARNING):
            assert not nats_publish.publish([EVENT], settings, logging.getLogger())
        assert time.monotonic() - started < 5  # noqa: PLR2004
        assert "within 10s" not in caplog.text
        assert "no answer within 2s" in caplog.text

    def test_failover_passes_a_silent_server(self, silent_server: _FakeServer):
        """A server that accepts but stays silent; the next one in the list is tried."""
        live = _FakeNats()
        try:
            settings = NatsSettings(
                servers=(
                    f"nats://127.0.0.1:{silent_server.port}",
                    f"nats://127.0.0.1:{live.port}",
                ),
                subject="x",
            )
            assert nats_publish.publish([EVENT], settings, logging.getLogger())
        finally:
            live.close()
        assert any(subject == "x" for subject, _ in live.published)


class TestConnectByName:
    """nats-py connects by host name (review of #121, round 2)."""

    def test_a_tls_server_verifies_by_name_without_the_tls_setting(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        """A tls:// server, nats.tls unset: TLS verifies the name, and it publishes."""
        ca, cert, key = _certificates(tmp_path)
        server_tls = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
        server_tls.load_cert_chain(cert, key)
        monkeypatch.setenv("SSL_CERT_FILE", str(ca))
        server = _FakeNats(tls=server_tls)
        try:
            settings = NatsSettings(
                servers=(f"tls://localhost:{server.port}",), subject="x"
            )
            assert nats_publish.publish([EVENT], settings, logging.getLogger())
            settings = dataclasses.replace(settings, tls=True)
            assert nats_publish.publish([EVENT], settings, logging.getLogger())
        finally:
            server.close()
        events = [
            json.loads(body) for subject, body in server.published if subject == "x"
        ]
        assert events == [EVENT, EVENT]

    def test_an_unreachable_first_address_is_skipped(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        """A name resolving to a dead address first, then a live one, publishes."""
        server = _FakeNats()
        resolve = socket.getaddrinfo

        def dead_first(host: str, port: int, *args: Any, **kwargs: Any) -> object:
            if host != "lake-broker.test":
                return resolve(host, port, *args, **kwargs)
            tcp = socket.SOCK_STREAM, socket.IPPROTO_TCP, ""
            return [
                (socket.AF_INET6, *tcp, ("::1", port, 0, 0)),
                (socket.AF_INET, *tcp, ("127.0.0.1", port)),
            ]

        monkeypatch.setattr(socket, "getaddrinfo", dead_first)
        try:
            settings = NatsSettings(
                servers=(f"nats://lake-broker.test:{server.port}",), subject="x"
            )
            assert nats_publish.publish([EVENT], settings, logging.getLogger())
        finally:
            server.close()
        assert server.published

    def test_every_servers_error_is_reported(self, caplog: pytest.LogCaptureFixture):
        """When no server connects, the warning gives each one's reason."""
        settings = NatsSettings(
            servers=("nats://127.0.0.1:1", "nats://127.0.0.1:2"), subject="x"
        )
        with caplog.at_level(logging.WARNING):
            assert not nats_publish.publish([EVENT], settings, logging.getLogger())
        assert "127.0.0.1', 1)" in caplog.text
        assert "127.0.0.1', 2)" in caplog.text

    def test_a_hung_resolver_does_not_hold_exit(self, tmp_path: Path):
        """The interpreter exits within the bound while the resolver still hangs."""
        script = tmp_path / "hang.py"
        script.write_text(
            "import logging, socket, time\n"
            "from target_ducklake import nats_publish as n\n"
            "socket.getaddrinfo = lambda *a, **k: time.sleep(30)\n"
            "s = n.NatsSettings(servers=('nats://example.invalid:1',), subject='x')\n"
            "n.publish([{'id': 'a'}], s, logging.getLogger(), timeout=1)\n"
        )
        started = time.monotonic()
        subprocess.run(  # noqa: S603
            [sys.executable, str(script)],
            check=True,
            timeout=20,
            cwd=Path(__file__).parents[1],
            env={**os.environ, "PYTHONPATH": str(Path(__file__).parents[1])},
        )
        assert time.monotonic() - started < 10  # noqa: PLR2004


class TestSettings:
    """The nats setting: target-iceberg's keys."""

    def test_both_keys_turn_events_on(self):
        """Servers are split and trimmed; the other keys are read as given."""
        settings = nats_settings(
            {
                "nats": {
                    "servers": " nats://a:4222 , nats://b:4222 ",
                    "subject": "x",
                    "jetstream": "true",
                    "tls": True,
                }
            }
        )
        assert settings == NatsSettings(
            servers=("nats://a:4222", "nats://b:4222"),
            subject="x",
            jetstream="true",
            tls=True,
        )

    @pytest.mark.parametrize(
        "block", [None, {}, {"servers": "nats://a"}, {"subject": "x"}]
    )
    def test_off_unless_both_are_set(self, block: dict | None):
        """Either key alone leaves events off."""
        assert nats_settings({"nats": block}) is None

    def test_jetstream_mode_is_checked(self, lake: Lake):
        """The config schema refuses a mode other than auto, true or false."""
        with pytest.raises(ConfigValidationError, match="'sometimes' is not one of"):
            Targetducklake(
                config={**lake.config(), "nats": {**NATS, "jetstream": "sometimes"}}
            )

    def test_missing_client_fails_at_startup(
        self, lake: Lake, monkeypatch: pytest.MonkeyPatch
    ):
        """Events configured without the nats extra: say how to install it."""
        find_spec = importlib.util.find_spec
        monkeypatch.setattr(
            importlib.util,
            "find_spec",
            lambda name, *a: None if name == "nats" else find_spec(name, *a),
        )
        with pytest.raises(ConfigValidationError, match=r"target-ducklake\[nats\]"):
            Targetducklake(config={**lake.config(), "nats": NATS})

    def test_repr_is_redacted(self):
        """A log or traceback showing the settings shows no credential."""
        settings = NatsSettings(
            servers=("nats://alice:url-secret@host:4222",),
            subject="x",
            password="pw-secret",  # noqa: S106
            token="tok-secret",  # noqa: S106
        )
        shown = repr(settings)
        for secret in ("url-secret", "pw-secret", "tok-secret"):
            assert secret not in shown
        assert "subject='x'" in shown

    @pytest.mark.parametrize(
        ("block", "missing"),
        [({"servers": "nats://a"}, "subject"), ({"subject": "x"}, "servers")],
    )
    def test_only_one_key_warns(
        self,
        lake: Lake,
        warnings: pytest.LogCaptureFixture,
        block: dict,
        missing: str,
    ):
        """Events stay off, as in target-iceberg, and the warning names the gap."""
        loader = Targetducklake(config={**lake.config(), "nats": block})
        assert loader.write_ledger is None
        assert f"nats.{missing} is not" in warnings.text

    def test_an_unset_warehouse_is_logged(
        self, lake: Lake, caplog: pytest.LogCaptureFixture
    ):
        """One INFO line names the warehouse events will carry."""
        logger = logging.getLogger("target-ducklake")
        logger.addHandler(caplog.handler)
        caplog.handler.setLevel(logging.INFO)
        try:
            Targetducklake(config={**lake.config(), "nats": NATS})
        finally:
            logger.removeHandler(caplog.handler)
        assert "warehouse is not set" in caplog.text
        assert lake.data_path.rstrip("/") in caplog.text

    def test_auth_precedence(self):
        """A creds file, then a token, then user/password."""
        full = NatsSettings(
            servers=("nats://a",),
            subject="x",
            username="u",
            password="p",  # noqa: S106
            token="t",  # noqa: S106
            creds_file="f.creds",
        )
        options = nats_publish.connect_options(full)
        assert options["user_credentials"] == "f.creds"
        assert "token" not in options
        assert "user" not in options
        options = nats_publish.connect_options(
            dataclasses.replace(full, creds_file=None)
        )
        assert options["token"] == "t"  # noqa: S105
        assert "user" not in options
        options = nats_publish.connect_options(
            dataclasses.replace(full, creds_file=None, token=None)
        )
        assert (options["user"], options["password"]) == ("u", "p")


class TestLedger:
    """What the sinks report, and how the catalog's changes are read."""

    def test_record_merges_commits(self):
        """Commits to one table add up into one entry."""
        ledger = WriteLedger()
        early = datetime(2026, 1, 1, tzinfo=timezone.utc)
        late = datetime(2026, 1, 2, tzinfo=timezone.utc)
        ledger.record(
            "s",
            "t",
            "b",
            rows=2,
            load_method="merge",
            started_at=late,
            completed_at=late,
        )
        ledger.record(
            "s",
            "t",
            "a",
            rows=3,
            load_method="append",
            started_at=early,
            completed_at=early,
        )
        ledger.record(
            "s",
            "t",
            "b",
            rows=0,
            load_method="merge",
            started_at=late,
            completed_at=late,
        )
        (write,) = ledger.tables()
        assert write.streams == ["b", "a"]
        assert write.rows == 5  # noqa: PLR2004
        assert write.load_methods == {"merge", "append"}
        assert (write.started_at, write.completed_at) == (early, late)

    def test_unknown_rows_stay_unknown(self):
        """One batch that couldn't be counted leaves the table's rows unknown."""
        ledger = WriteLedger()
        moment = datetime(2026, 1, 1, tzinfo=timezone.utc)
        for rows in (2, None, 3):
            ledger.record(
                "s",
                "t",
                "a",
                rows=rows,
                load_method="append",
                started_at=moment,
                completed_at=moment,
            )
        (write,) = ledger.tables()
        assert write.rows is None

    def test_changes_name_tables_by_id_except_creations(self):
        """Created objects are named; every other change is by id."""
        assert _changes_table({"tables_inserted_into": ["2", "3"]}, 3)
        assert _changes_table({"flushed_inlined": ["3"]}, 3)
        assert not _changes_table({"tables_created": ["3"]}, 3)
        assert not _changes_table({"tables_altered": ["13"]}, 3)

    def test_event_id_joins_namespace_levels(self):
        """The contract's id joins the namespace levels with slashes."""
        assert event_id("w", ["a", "b"], "t", 7) == "w/a/b/t@7"


@pytest.mark.skipif(not os.environ.get(NATS_ENV), reason=f"set {NATS_ENV}")
class TestNatsServer:
    """End to end, on a real NATS server with JetStream."""

    @staticmethod
    async def _stored(url: str, subject: str) -> list[Any]:
        """The messages the subject's stream holds."""
        import nats  # noqa: PLC0415

        connection = await nats.connect(url)
        try:
            js = connection.jetstream()
            info = await js.stream_info(subject.replace(".", "_"))
            return [
                await js.get_msg(info.config.name, seq)
                for seq in range(1, info.state.messages + 1)
            ]
        finally:
            await connection.close()

    @staticmethod
    async def _add_stream(url: str, subject: str) -> None:
        import nats  # noqa: PLC0415

        connection = await nats.connect(url)
        try:
            await connection.jetstream().add_stream(
                name=subject.replace(".", "_"), subjects=[subject]
            )
        finally:
            await connection.close()

    def test_a_creds_file_connects(self, tmp_path: Path):
        """A .creds file (a JWT and an nkey seed) loads and connects."""
        import nkeys  # noqa: PLC0415

        seed = nkeys.encode_seed(os.urandom(32), nkeys.PREFIX_BYTE_USER).decode()
        creds = tmp_path / "user.creds"
        creds.write_text(
            "-----BEGIN NATS USER JWT-----\neyJhbGciOiJlZDI1NTE5In0.e30.c2ln\n"
            "------END NATS USER JWT------\n\n"
            f"-----BEGIN USER NKEY SEED-----\n{seed}\n------END USER NKEY SEED------\n"
        )
        settings = NatsSettings(
            servers=(os.environ[NATS_ENV],), subject="x", creds_file=str(creds)
        )
        assert nats_publish.publish([EVENT], settings, logging.getLogger())

    def test_jetstream_dedups_on_the_event_id(self, lake: Lake):
        """Auto finds the stream; a re-published event is dropped as a duplicate."""
        url = os.environ[NATS_ENV]
        subject = f"ducklkd.test.{uuid.uuid4().hex[:8]}"
        asyncio.run(self._add_stream(url, subject))
        nats_config = {"servers": url, "subject": subject}
        _run(lake, {"t": [{"id": 1}]}, nats=nats_config)
        settings = nats_settings({"nats": nats_config})
        assert settings is not None
        (first,) = asyncio.run(self._stored(url, subject))
        # Publish the run's event again: same id, so the server drops it.
        assert nats_publish.publish(
            [json.loads(first.data)], settings, logging.getLogger()
        )
        (message,) = asyncio.run(self._stored(url, subject))
        event = json.loads(message.data)
        assert message.headers["Nats-Msg-Id"] == event["id"]
        assert event["data"]["table"] == "t"
        assert event["data"]["rows"] == 1
