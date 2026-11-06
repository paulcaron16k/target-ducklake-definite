"""Streams that load the same table (DUCKLKD-44).

Two streams reach one table when their names resolve to the same schema and
table: a stream map aliases one onto the other (or both onto one name), the
names differ only in ASCII case (DuckDB identifiers ignore it), a
``schema-table`` stream name meets a plain one under ``default_target_schema``,
or two ``catalog-schema-table`` names differ only in their first part. Nothing
in the singer-sdk notices: its sinks are keyed by stream name.

Combining streams into one table is a legitimate thing to configure, so a
collision is reported with a warning, once per pair of streams. It fails the
run, before the second stream changes the table, when the two would undo each
other's rows or settings:

- **A full refresh**, by either stream: an overwrite (``load_method:
  overwrite``, or ``overwrite_if_no_pk`` for a keyless stream), or the
  ``ACTIVATE_VERSION`` of a full-table sync, which retires every row of the
  table that isn't of its version. Either replaces the other stream's rows.
- **Different key properties**, compared as sets of the key columns' final
  names (after ``conform_column_names`` and Postgres truncation, in DuckDB's
  case folding): each stream would merge, deduplicate and delete by columns
  that don't identify the other's rows.
- **A different table setting**: partition keys or sort order where both
  streams configure them, or ``data_inlining_row_limit`` (which always has a
  value). A table has one of each, so each stream's setup would change it
  back every run.

Different load methods (one stream appending, the other merging) and
different ``type_source`` declarations are allowed: neither replaces the
other's rows, and each stream's column types are already fitted to the
table's own, as for a table another run created.
"""

from __future__ import annotations

import contextlib
import dataclasses
from typing import TYPE_CHECKING

from target_ducklake.conform import fold_case
from target_ducklake.layout import same_keys

if TYPE_CHECKING:
    import logging
    from collections.abc import Iterator

    from target_ducklake.layout import TableLayout

__all__ = ["TableClaims", "TableCollisionError", "TableWriter", "table_id"]

# The reason a stream doing a full refresh by ACTIVATE_VERSION gives.
ACTIVATE_VERSION_REFRESH = (
    "ACTIVATE_VERSION, which retires every row of the table not of the "
    "activated version"
)


def table_id(schema: str, table: str) -> tuple[str, str]:
    """A table as DuckDB identifies it: ASCII case folded."""
    return fold_case(schema), fold_case(table)


class TableCollisionError(ValueError):
    """Two streams load one table in ways that would undo each other's rows."""


@dataclasses.dataclass(frozen=True)
class TableWriter:
    """What one stream does to its table, as far as other streams care.

    Attributes:
        stream: The stream as the tap names it.
        sink: The name it is loaded under: its stream map alias, or the
            stream's own name.
        schema: The table's schema.
        table: The table's name.
        keys: The key columns' final names, in the stream's order.
        load_method: ``merge``, ``append`` or ``overwrite``, as applied.
        full_refresh: Why the stream replaces the table's rows, or None.
        layout: The partition keys and sort order the stream configures.
        inlining_row_limit: The ``data_inlining_row_limit`` it configures.
    """

    stream: str
    sink: str
    schema: str
    table: str
    keys: tuple[str, ...]
    load_method: str
    full_refresh: str | None
    layout: TableLayout
    inlining_row_limit: int

    @property
    def table_id(self) -> tuple[str, str]:
        """The table as DuckDB identifies it (see ``table_id``)."""
        return table_id(self.schema, self.table)

    @property
    def key_set(self) -> frozenset[str]:
        """The key columns as DuckDB compares them, unordered."""
        return frozenset(fold_case(key) for key in self.keys)

    @property
    def label(self) -> str:
        """The stream for a message, with its alias if it has one."""
        if self.sink == self.stream:
            return repr(self.stream)
        return f"{self.stream!r} (as {self.sink!r})"

    @property
    def described_keys(self) -> str:
        """The keys for a message: the list, or ``none``."""
        return str(list(self.keys)) if self.keys else "none"


class TableClaims:
    """Every table the run's streams load, checked as each stream arrives.

    A stream claims its table when its sink is set up (before the sink
    changes the table), again after its SCHEMA message (when a stream map
    sends it into a sink another stream already has, which the singer-sdk
    reuses without setting it up), and when it starts a full refresh by
    ``ACTIVATE_VERSION``. A stream is identified by its own name and the name
    it is loaded under, so a stream whose SCHEMA is sent again, or whose sink
    is rebuilt, replaces its own claim rather than colliding with it.
    """

    def __init__(self, logger: logging.Logger) -> None:
        """Start with no claims."""
        self._logger = logger
        self._writers: dict[tuple[str, str], TableWriter] = {}
        self._warned: set[frozenset[tuple[str, str]]] = set()
        # The stream whose SCHEMA message is being processed (see for_stream).
        self._schema_stream: str | None = None

    @contextlib.contextmanager
    def for_stream(self, stream: str) -> Iterator[None]:
        """Attribute the sinks set up in the block to ``stream``.

        The singer-sdk sets a sink up for a SCHEMA message under the name of
        its stream map alias, which may be shared by several streams: only the
        message says which stream the sink is for.
        """
        self._schema_stream = stream
        try:
            yield
        finally:
            self._schema_stream = None

    @property
    def schema_stream(self) -> str | None:
        """The stream whose SCHEMA message is being processed, if any."""
        return self._schema_stream

    def claim(self, writer: TableWriter) -> None:
        """Record what ``writer`` does to its table, checked against the others.

        A stream's full refresh stays claimed for the rest of the run, even if
        its sink is rebuilt without one: the rows an ``ACTIVATE_VERSION``
        retired are gone, and the old sink still commits its overwrite.

        Raises:
            TableCollisionError: If another stream loads the table in a way
                that would undo this one's rows or settings, or this one's
                the other's.
        """
        key = (writer.stream, writer.sink)
        previous = self._writers.get(key)
        if previous is not None and writer.full_refresh is None:
            writer = dataclasses.replace(writer, full_refresh=previous.full_refresh)
        for other_key, other in self._writers.items():
            if other_key == key or other.table_id != writer.table_id:
                continue
            _check_compatible(other, writer)
            pair = frozenset((other_key, key))
            if pair not in self._warned:
                self._warned.add(pair)
                self._warn(other, writer)
        self._writers[key] = writer

    def claim_full_refresh(self, writer: TableWriter) -> None:
        """Claim ``writer``'s table for a full refresh by ``ACTIVATE_VERSION``.

        Raises:
            TableCollisionError: If another stream loads the table.
        """
        self.claim(dataclasses.replace(writer, full_refresh=ACTIVATE_VERSION_REFRESH))

    def _warn(self, first: TableWriter, second: TableWriter) -> None:
        load_methods = (
            f"both {first.load_method}"
            if first.load_method == second.load_method
            else f"{first.label} by {first.load_method}, "
            f"{second.label} by {second.load_method}"
        )
        self._logger.warning(
            "Streams %s and %s both load table %s.%s, so their rows are combined "
            "in it (%s). They agree on key properties (%s), so neither re-keys "
            "the other's rows. If they should be separate tables, give one of "
            "them its own name with a stream map (__alias__).",
            first.label,
            second.label,
            first.schema,
            first.table,
            load_methods,
            first.described_keys,
        )


def _check_compatible(first: TableWriter, second: TableWriter) -> None:
    """Refuse two streams loading one table that would undo each other.

    Raises:
        TableCollisionError: Naming both streams, the table and the conflict.
    """
    where = (
        f"Streams {first.label} and {second.label} both load table "
        f"{first.schema}.{first.table}"
    )
    fix = "Give each stream its own table with a stream map (__alias__)"
    for refresher, other in ((first, second), (second, first)):
        if refresher.full_refresh is not None:
            msg = (
                f"{where}, but {refresher.label} does a full refresh "
                f"({refresher.full_refresh}): it would replace the rows of "
                f"{other.label} too. {fix}, or load both with merge or append."
            )
            raise TableCollisionError(msg)
    if first.key_set != second.key_set:
        msg = (
            f"{where}, but with different key properties: "
            f"{first.described_keys} and {second.described_keys}. Each would "
            "merge, deduplicate and delete rows by columns that don't identify "
            f"the other's rows. {fix}, or make their key_properties agree "
            "(stream_overrides.<stream>.key_properties)."
        )
        raise TableCollisionError(msg)
    if conflict := next(_table_settings(first, second), None):
        setting, ours, theirs = conflict
        msg = (
            f"{where}, but configure different {setting}: {ours} and {theirs}. "
            "A table has one, so each stream would change it back every run. "
            f"{fix}, or configure the same {setting} for both (or for one only)."
        )
        raise TableCollisionError(msg)


def _table_settings(
    first: TableWriter, second: TableWriter
) -> Iterator[tuple[str, str, str]]:
    """The table settings both streams configure, and differently."""
    for setting, ours, theirs in (
        ("partition keys", first.layout.partition, second.layout.partition),
        ("sort orders", first.layout.sort, second.layout.sort),
    ):
        if ours is not None and theirs is not None and not same_keys(ours, theirs):
            yield (
                setting,
                str([key.sql() for key in ours]),
                str([key.sql() for key in theirs]),
            )
    if first.inlining_row_limit != second.inlining_row_limit:
        yield (
            "data_inlining_row_limit values",
            str(first.inlining_row_limit),
            str(second.inlining_row_limit),
        )
