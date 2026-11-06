"""writeCompleted events over NATS (DUCKLKD-86)."""

from __future__ import annotations

import asyncio
import importlib.util
import io
import json
import logging
import os
import socket
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

import pytest
from singer_sdk.exceptions import ConfigValidationError

from target_ducklake import nats_publish, target
from target_ducklake.connector import DuckLakeConnector, _changes_table
from target_ducklake.nats_publish import NatsError, NatsSettings, nats_settings
from target_ducklake.target import Targetducklake
from target_ducklake.write_completed import WriteLedger, event_id

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
        assert event["data"]["streams"] == ["a-orders", "b-orders"]
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


class _SilentServer:
    """A TCP server that accepts connections and never answers."""

    def __init__(self) -> None:
        self.socket = socket.create_server(("127.0.0.1", 0))
        self.port = self.socket.getsockname()[1]
        self._accepted: list[socket.socket] = []
        self._thread = threading.Thread(target=self._accept, daemon=True)
        self._thread.start()

    def _accept(self) -> None:
        try:
            while True:
                connection, _ = self.socket.accept()
                self._accepted.append(connection)
        except OSError:
            return

    def close(self) -> None:
        for connection in self._accepted:
            connection.close()
        self.socket.close()


@pytest.fixture
def silent_server() -> Iterator[_SilentServer]:
    """A silent TCP server, closed after the test."""
    server = _SilentServer()
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
        self, silent_server: _SilentServer, caplog: pytest.LogCaptureFixture
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

        monkeypatch.setattr(nats_publish, "_publish", echoing)
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
        with pytest.raises(NatsError, match=r"target-ducklake\[nats\]"):
            Targetducklake(config={**lake.config(), "nats": NATS})

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
        options = nats_publish.connect_options(full._replace(creds_file=None))
        assert options["token"] == "t"  # noqa: S105
        assert "user" not in options
        options = nats_publish.connect_options(
            full._replace(creds_file=None, token=None)
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

    def test_start_reads_the_snapshot_once(self):
        """Later sinks keep the first sink's start snapshot."""
        ledger = WriteLedger()
        reads = iter([4, 9])
        ledger.start(lambda: next(reads))
        ledger.start(lambda: next(reads))
        assert ledger.start_snapshot == 4  # noqa: PLR2004

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
