"""writeCompleted events: one per table a run wrote, built at the end of the run.

The event is Zamboni's ``write.completed`` contract (docs/write-completed.md
in paulcaron16k/Zamboni), version ``CONTRACT_VERSION``: a CloudEvents 1.0
envelope telling a table maintainer that a writer has finished with a table
for this run. tests/contracts holds a pinned copy of its JSON Schema
(bin/refresh-write-completed-contract.sh), and
tests/test_write_completed_contract.py validates the events built here
against it.

Two sources fill an event. The sinks report each commit to a ``WriteLedger``
(which streams wrote the table, how many rows, under which load method, and
when); the catalog reports the snapshots this run committed to the table,
which it tags (``DuckLakeConnector.tag_commits``), and the data files they
added (``TableSnapshots``). Publishing is ``nats_publish``.
"""

from __future__ import annotations

import socket
import threading
from dataclasses import dataclass, field
from importlib.metadata import PackageNotFoundError, version
from typing import TYPE_CHECKING, Any, NamedTuple

from target_ducklake.collisions import table_id

if TYPE_CHECKING:
    from datetime import datetime

# The contract version these events are written against. The contract test
# checks it against the vendored spec's x-contract-version.
CONTRACT_VERSION = "1.0"
# The contract's document plus /<major>.<minor>, as the contract asks.
DATASCHEMA = (
    "https://github.com/paulcaron16k/Zamboni/blob/main/docs/write-completed.md/"
    f"{CONTRACT_VERSION}"
)
EVENT_TYPE = "writeCompleted"
PRODUCER = "target-ducklake"

# This target's load methods, by the names the contract uses.
CONTRACT_LOAD_METHODS = {
    "append": "append-only",
    "merge": "upsert",
    "overwrite": "overwrite",
}


@dataclass
class TableWrite:
    """What the run's sinks committed to one table.

    Attributes:
        schema: The table's schema, as the first stream to commit names it.
        table: The table's name, likewise. The event names the table as the
            catalog does (``TableSnapshots``).
        started_at: When the first commit to the table began.
        completed_at: When the last commit to the table ended.
        streams: The streams that wrote the table, in first-commit order (a
            stream map's alias, when one renames the stream).
        rows: Rows in the committed batches; None once a batch's count could
            not be read.
        load_methods: The load methods the commits used (``load_method``
            values: append, merge or overwrite).
    """

    schema: str
    table: str
    started_at: datetime
    completed_at: datetime
    streams: list[str] = field(default_factory=list)
    rows: int | None = 0
    load_methods: set[str] = field(default_factory=set)


class TableSnapshots(NamedTuple):
    """What the catalog holds of the run's commits to one table.

    Attributes:
        snapshot_id: The last snapshot this run committed to the table.
        snapshot_count: The snapshots this run committed to it.
        data_files_added: Data files those snapshots added to it.
        bytes_added: Their size in bytes.
        table_uuid: The table's DuckLake UUID.
        schema: The table's schema, as the catalog spells it.
        table: The table's name, as the catalog spells it: the event's, whatever
            the case of the streams that loaded it (DUCKLKD-135).
    """

    snapshot_id: int
    snapshot_count: int
    data_files_added: int
    bytes_added: int
    table_uuid: str | None
    schema: str
    table: str


class WriteLedger:
    """The tables a run committed to, as its sinks report them.

    A table is identified as DuckDB identifies it (``collisions.table_id``),
    so streams whose names differ only in case share one entry, under the
    first one's spelling (DUCKLKD-135). Sinks drain in parallel threads, so
    every update takes a lock.
    """

    def __init__(self) -> None:
        """Start an empty ledger."""
        self._lock = threading.Lock()
        self._tables: dict[tuple[str, str], TableWrite] = {}

    def record(  # noqa: PLR0913 (the table, then what one commit did)
        self,
        schema: str,
        table: str,
        stream: str,
        *,
        rows: int | None,
        load_method: str,
        started_at: datetime,
        completed_at: datetime,
    ) -> None:
        """Add one committed transaction to its table's entry.

        Args:
            schema: The table's schema.
            table: The table's name.
            stream: The stream whose sink committed.
            rows: Rows the commit loaded (0 for a delete), or None if they
                could not be counted.
            load_method: The sink's load method.
            started_at: When the commit began.
            completed_at: When it ended.
        """
        with self._lock:
            write = self._tables.setdefault(
                table_id(schema, table),
                TableWrite(schema, table, started_at, completed_at),
            )
            if stream not in write.streams:
                write.streams.append(stream)
            write.rows = (
                None if rows is None or write.rows is None else write.rows + rows
            )
            write.load_methods.add(load_method)
            write.started_at = min(write.started_at, started_at)
            write.completed_at = max(write.completed_at, completed_at)

    def tables(self) -> list[TableWrite]:
        """The tables committed to, in first-commit order."""
        with self._lock:
            return list(self._tables.values())


def event_id(warehouse: str, namespace: list[str], table: str, snapshot_id: int) -> str:
    """The contract's deterministic event id.

    ``<warehouse>/<namespace levels joined by />/<table>@<snapshot_id>``, so
    a re-published event has the same id (JetStream's Nats-Msg-Id).
    """
    return f"{warehouse}/{'/'.join(namespace)}/{table}@{snapshot_id}"


def build_event(
    write: TableWrite,
    snapshots: TableSnapshots,
    *,
    warehouse: str,
    host: str | None = None,
) -> dict[str, Any]:
    """Build the CloudEvents envelope for one table, in structured mode.

    Optional fields with nothing true to say are left out, not nulled:
    ``load_method`` when the table's streams used different ones, and
    ``rows`` when a batch's count could not be read, and
    ``partitions_changed``, ``warehouse_id`` and ``catalog_uri`` always (the
    contract says to omit a partition count rather than estimate it;
    DuckLake has no warehouse id; the catalog URL may hold a password).

    Args:
        write: What the sinks committed to the table.
        snapshots: What the catalog holds of those commits.
        warehouse: The warehouse name the consumer matches on.
        host: The host for ``source``; this host if omitted.

    Returns:
        The event, as a JSON-ready dict.
    """
    namespace = [snapshots.schema]
    data: dict[str, Any] = {
        "warehouse": warehouse,
        "namespace": namespace,
        "table": snapshots.table,
        "snapshot_id": snapshots.snapshot_id,
        "completed_at": write.completed_at.isoformat(),
        "started_at": write.started_at.isoformat(),
        "writer": f"{PRODUCER}/{_version()}",
        "full_refresh": "overwrite" in write.load_methods,
        "data_files_added": snapshots.data_files_added,
        "bytes_added": snapshots.bytes_added,
        "snapshot_count": snapshots.snapshot_count,
        "streams": list(write.streams),
    }
    if write.rows is not None:
        data["rows"] = write.rows
    if len(write.load_methods) == 1:
        (method,) = write.load_methods
        data["load_method"] = CONTRACT_LOAD_METHODS[method]
    if snapshots.table_uuid:
        data["table_uuid"] = snapshots.table_uuid
    return {
        "specversion": "1.0",
        "type": EVENT_TYPE,
        "source": f"uri:{PRODUCER}:{host or socket.gethostname()}",
        "id": event_id(warehouse, namespace, snapshots.table, snapshots.snapshot_id),
        "dataschema": DATASCHEMA,
        "datacontenttype": "application/json",
        "data": data,
    }


def _version() -> str:
    """This package's installed version, or ``unknown`` in an uninstalled tree."""
    try:
        return version(PRODUCER)
    except PackageNotFoundError:
        return "unknown"
