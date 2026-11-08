"""A SQLite catalog whose COMMIT fails with 'database is locked' (DUCKLKD-82).

When another process holds a read lock on the catalog file past the busy
timeout, the SQLite COMMIT of a DuckLake transaction fails with
``database is locked``. DuckDB's sqlite extension then keeps that SQLite
connection, still inside the transaction SQLite leaves open after a busy
COMMIT, so the attachment holds the catalog's write lock and every later
statement fails the same way. ``DuckLakeConnector.execute`` detaches the
catalog, which closes that connection and rolls the transaction back, and
attaches it again before the next attempt.

The lock is held by a separate process: POSIX locks don't conflict within
one process, so a holder in the test process would not block DuckDB.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap
from types import SimpleNamespace
from typing import TYPE_CHECKING
from unittest.mock import Mock

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from target_ducklake.connector import DuckLakeConnector, DuckLakeConnectorError

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from pathlib import Path

# DuckDB's default SQLite busy timeout is 5 s; the tests wait 100 ms instead.
BUSY_TIMEOUT = "META_BUSY_TIMEOUT 100"
LOCKED = "database is locked"


class ReadLockHolder:
    """Another process with an open read transaction on a SQLite file.

    Its SHARED lock lets a writer reserve the file but not commit: the
    writer's COMMIT waits out its busy timeout and fails.
    """

    def __init__(self, path: Path) -> None:
        """Start the process and wait until it holds the lock."""
        script = textwrap.dedent(
            """
            import sqlite3, sys
            con = sqlite3.connect(sys.argv[1], isolation_level=None)
            con.execute("BEGIN")
            con.execute("SELECT count(*) FROM sqlite_master").fetchall()
            print("held", flush=True)
            sys.stdin.readline()
            con.execute("ROLLBACK")
            """
        )
        self._process = subprocess.Popen(  # noqa: S603 (this interpreter, our script)
            [sys.executable, "-c", script, str(path)],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
        )
        assert self._process.stdout is not None
        assert self._process.stdout.readline().strip() == "held"

    def release(self) -> None:
        """End the read transaction and the process; idempotent."""
        if self._process.poll() is None:
            assert self._process.stdin is not None
            self._process.stdin.write("\n")
            self._process.stdin.flush()
            self._process.wait(timeout=10)


def other_process_can_write(path: Path) -> bool:
    """Whether another process can take the SQLite file's write lock now."""
    script = textwrap.dedent(
        """
        import sqlite3, sys
        con = sqlite3.connect(sys.argv[1], timeout=0.1, isolation_level=None)
        con.execute("BEGIN IMMEDIATE")
        con.execute("ROLLBACK")
        """
    )
    result = subprocess.run(  # noqa: S603 (this interpreter, our script)
        [sys.executable, "-c", script, str(path)], capture_output=True, check=False
    )
    return result.returncode == 0


def _on_backoff(monkeypatch: pytest.MonkeyPatch, action: Callable[[], None]) -> None:
    """Run ``action`` instead of each retry's sleep in execute().

    The connector's ``time`` module is replaced, not ``time.sleep`` itself,
    which the subprocess module uses too.
    """
    monkeypatch.setattr(
        "target_ducklake.connector.time", SimpleNamespace(sleep=lambda _s: action())
    )


@pytest.fixture
def catalog_path(tmp_path: Path) -> Path:
    """The SQLite catalog file."""
    return tmp_path / "catalog.sqlite"


@pytest.fixture
def connector(
    tmp_path: Path, catalog_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[DuckLakeConnector]:
    """A connector on a SQLite catalog with table main.t (i INTEGER).

    Its ATTACH, and every re-ATTACH, uses a 100 ms busy timeout; retries
    don't sleep.
    """
    created = DuckLakeConnector(
        {
            "catalog_type": "sqlite",
            "catalog_url": str(catalog_path),
            "data_path": f"{tmp_path / 'data'}/",
            "storage_type": "local",
        }
    )
    attach = created._build_attach_statement()  # noqa: SLF001 (patched below)
    assert "DATA_INLINING_ROW_LIMIT 0)" in attach
    monkeypatch.setattr(
        created,
        "_build_attach_statement",
        lambda: attach.replace(
            "DATA_INLINING_ROW_LIMIT 0)", f"DATA_INLINING_ROW_LIMIT 0, {BUSY_TIMEOUT})"
        ),
    )
    _on_backoff(monkeypatch, lambda: None)
    created.execute("CREATE TABLE ducklake_catalog.main.t (i INTEGER)")
    yield created
    created.close()


def _staged(tmp_path: Path, values: list[int]) -> str:
    """A parquet file of column i holding ``values``."""
    location = tmp_path / "staged.parquet"
    pq.write_table(pa.table({"i": pa.array(values, pa.int32())}), location)
    return str(location)


def _insert(connector: DuckLakeConnector, location: str) -> None:
    connector.insert_into_table(location, "main", "t", ["i"], ["i"])


def _rows(connector: DuckLakeConnector) -> list[tuple]:
    return connector.execute(
        "SELECT i FROM ducklake_catalog.main.t ORDER BY i"
    ).fetchall()


def _data_files(tmp_path: Path) -> list[Path]:
    return sorted((tmp_path / "data").rglob("*.parquet"))


class TestBusyCommitWedgesTheAttachment:
    """The upstream behaviour the target works around, on DuckDB alone.

    If DuckDB's sqlite extension starts closing the connection of a failed
    COMMIT, these tests fail and the workaround can be reconsidered.
    """

    def test_attachment_keeps_the_write_lock_until_detached(
        self, tmp_path: Path, catalog_path: Path
    ) -> None:
        """After a busy COMMIT, nothing reads the catalog until DETACH."""
        attach = (
            f"ATTACH 'ducklake:sqlite:{catalog_path}' AS lake "
            f"(DATA_PATH '{tmp_path / 'data'}/', DATA_INLINING_ROW_LIMIT 0, "
            f"{BUSY_TIMEOUT})"
        )
        con = duckdb.connect()
        con.execute(attach)
        con.execute("CREATE TABLE lake.t (i INTEGER)")
        con.execute("INSERT INTO lake.t VALUES (1)")

        holder = ReadLockHolder(catalog_path)
        try:
            with pytest.raises(duckdb.Error, match=r'"COMMIT": database is locked'):
                con.execute("BEGIN; INSERT INTO lake.t VALUES (2); COMMIT;")
        finally:
            holder.release()

        # The lock holder is gone, but the failed COMMIT's SQLite connection
        # still holds the write lock: this process can't read, and another
        # can't write.
        with pytest.raises(duckdb.Error, match=LOCKED):
            con.execute("SELECT count(*) FROM lake.t")
        assert not other_process_can_write(catalog_path)
        with pytest.raises(duckdb.Error, match="no transaction is active"):
            con.execute("ROLLBACK")

        con.execute("DETACH lake")
        assert other_process_can_write(catalog_path)
        con.execute(attach)
        # The busy COMMIT committed nothing, and DuckLake removed its file.
        assert con.execute("SELECT i FROM lake.t").fetchall() == [(1,)]
        assert len(_data_files(tmp_path)) == 1
        con.close()


class TestExecuteRecoversTheCatalog:
    """execute() re-attaches a SQLite catalog after 'database is locked'."""

    def test_busy_commit_is_retried_on_a_fresh_attachment(
        self,
        tmp_path: Path,
        catalog_path: Path,
        connector: DuckLakeConnector,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The lock is released during the backoff; the retry commits once."""
        holder = ReadLockHolder(catalog_path)
        _on_backoff(monkeypatch, holder.release)
        try:
            _insert(connector, _staged(tmp_path, [1, 2]))
        finally:
            holder.release()

        assert _rows(connector) == [(1,), (2,)]
        assert len(_data_files(tmp_path)) == 1
        assert other_process_can_write(catalog_path)

    def test_a_lock_that_stays_fails_naming_the_catalog(
        self, tmp_path: Path, catalog_path: Path, connector: DuckLakeConnector
    ) -> None:
        """Every attempt fails, naming the catalog; the catalog is left usable.

        Nothing of the failed query is applied, and the lock is released
        before the error is raised.
        """
        holder = ReadLockHolder(catalog_path)
        try:
            with pytest.raises(DuckLakeConnectorError) as raised:
                _insert(connector, _staged(tmp_path, [1]))
            # Released already: the attachment no longer holds the lock.
            holder.release()
            assert other_process_can_write(catalog_path)
        finally:
            holder.release()

        message = str(raised.value)
        assert "failed after 3 attempts" in message
        assert f"SQLite catalog {catalog_path}" in message
        assert LOCKED in message
        assert _rows(connector) == []
        assert _data_files(tmp_path) == []

        _insert(connector, _staged(tmp_path, [3]))
        assert _rows(connector) == [(3,)]


def _mock_connector(
    catalog_type: str, monkeypatch: pytest.MonkeyPatch
) -> tuple[DuckLakeConnector, Mock]:
    """A connector on ``catalog_type`` whose queries fail with 'database is locked'."""
    connector = DuckLakeConnector(
        {
            "catalog_type": catalog_type,
            "catalog_url": "catalog",
            "data_path": "/tmp/data/",  # noqa: S108 (never written)
            "storage_type": "local",
        }
    )
    cursor = Mock()
    cursor.execute.side_effect = duckdb.TransactionException(
        'Failed to commit: Failed to execute query "COMMIT": database is locked'
    )
    connection = Mock()
    connection.cursor.return_value = cursor
    monkeypatch.setattr(
        "target_ducklake.connector.duckdb.connect", lambda **_kwargs: connection
    )
    return connector, cursor


@pytest.mark.parametrize("catalog_type", ["postgres", "mysql", "duckdb"])
def test_other_catalogs_do_not_reattach(
    catalog_type: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Only a SQLite catalog is re-attached; elsewhere the error is raised at once."""
    connector, cursor = _mock_connector(catalog_type, monkeypatch)
    with pytest.raises(DuckLakeConnectorError, match=LOCKED):
        connector.execute("SELECT 1")
    assert [c.args[0] for c in cursor.execute.call_args_list] == ["SELECT 1"]


def test_sqlite_reattaches_before_each_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    """Each failure detaches the catalog; each retry attaches it first."""
    _on_backoff(monkeypatch, lambda: None)
    connector, cursor = _mock_connector("sqlite", monkeypatch)
    statements: list[str] = []

    def run(query: str, _parameters: object = None) -> None:
        statements.append(query.split(" ", 1)[0])
        if query == "SELECT 1":
            raise duckdb.TransactionException(LOCKED)

    cursor.execute.side_effect = run
    with pytest.raises(DuckLakeConnectorError, match="SQLite catalog catalog"):
        connector.execute("SELECT 1")
    assert statements == [
        "SELECT",
        "DETACH",
        "ATTACH",
        "SELECT",
        "DETACH",
        "ATTACH",
        "SELECT",
        "DETACH",
    ]
