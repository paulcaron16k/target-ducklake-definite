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
other's rows:

- **A full refresh**, by either stream: an overwrite (``load_method:
  overwrite``, or ``overwrite_if_no_pk`` for a keyless stream), or the
  ``ACTIVATE_VERSION`` of a full-table sync, which retires every row of the
  table that isn't of its version. Either replaces the other stream's rows.
- **Different key properties**, compared as sets of the key columns' final
  names (after ``conform_column_names`` and Postgres truncation, in DuckDB's
  case folding): each stream would merge, deduplicate and delete by columns
  that don't identify the other's rows.

Different table settings (partition keys, sort order or
``data_inlining_row_limit``, where both streams set them per stream) are named in the
warning: each stream's setup resets the table's setting, so the stream set up
last wins and the table's metadata changes every run, but no row is lost. A
config shared with target-iceberg, which has no such check, keeps working.

Different load methods (one stream appending, the other merging) and
different ``type_source`` declarations are allowed: neither replaces the
other's rows, and each stream's column types are already fitted to the
table's own, as for a table another run created.

A stream mapped to null (``stream_maps: {orders: null}``, or ``__else__:
null``) loads no rows, so it claims nothing. An overwrite stream mapped to
null still empties its table, though, as it did before (DUCKLKD-127).
"""

from __future__ import annotations

import contextlib
import dataclasses
from typing import TYPE_CHECKING

from singer_sdk.mapper import RemoveRecordTransform

from target_ducklake.conform import fold_case
from target_ducklake.layout import PartitionKey, same_keys

if TYPE_CHECKING:
    import logging
    from collections.abc import Iterator

    from singer_sdk.mapper import PluginMapper, StreamMap

    from target_ducklake.layout import SortKey, TableLayout

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
        inlining_row_limit: The ``data_inlining_row_limit`` set for the
            stream in ``stream_overrides`` or ``stream_defaults``, or None
            where it comes from the top level, which applies to every stream.
    """

    stream: str
    sink: str
    schema: str
    table: str
    keys: tuple[str, ...]
    load_method: str
    full_refresh: str | None
    layout: TableLayout
    inlining_row_limit: int | None

    @property
    def claimant(self) -> tuple[str, str]:
        """The claim's identity: the stream and the name it is loaded under."""
        return self.stream, self.sink

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

    def __init__(self, logger: logging.Logger, mapper: PluginMapper) -> None:
        """Start with no claims.

        Args:
            logger: For the collision warnings.
            mapper: The target's stream maps: which streams are mapped to
                null, and the schema each stream sends its sink.
        """
        self._logger = logger
        self._mapper = mapper
        # Each table's claims, by table_id, then by TableWriter.claimant.
        self._tables: dict[tuple[str, str], dict[tuple[str, str], TableWriter]] = {}
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
        retired are gone, and the old sink still commits its overwrite. A
        stream mapped to null claims nothing (an overwrite one still empties
        its table: DUCKLKD-127).

        Raises:
            TableCollisionError: If another stream loads the table in a way
                that would undo this one's rows, or this one the other's.
        """
        if isinstance(self._stream_map(writer), RemoveRecordTransform):
            return
        claims = self._tables.setdefault(writer.table_id, {})
        previous = claims.get(writer.claimant)
        if previous is not None and writer.full_refresh is None:
            writer = dataclasses.replace(writer, full_refresh=previous.full_refresh)
        for other in claims.values():
            if other.claimant == writer.claimant:
                continue
            _check_compatible(other, writer)
            pair = frozenset((other.claimant, writer.claimant))
            if pair not in self._warned:
                self._warned.add(pair)
                self._warn(other, writer)
        claims[writer.claimant] = writer

    def claim_full_refresh(self, writer: TableWriter) -> None:
        """Claim ``writer``'s table for a full refresh by ``ACTIVATE_VERSION``.

        Raises:
            TableCollisionError: If another stream loads the table.
        """
        self.claim(dataclasses.replace(writer, full_refresh=ACTIVATE_VERSION_REFRESH))

    def _stream_map(self, writer: TableWriter) -> StreamMap | None:
        """The stream map sending the writer's stream into its sink, if any."""
        for stream_map in self._mapper.stream_maps.get(writer.stream, ()):
            if stream_map.stream_alias == writer.sink:
                return stream_map
        return None

    def _send_different_schemas(self, first: TableWriter, second: TableWriter) -> bool:
        """Whether two streams sharing one sink send it different schemas."""
        if first.sink != second.sink:
            return False
        first_map, second_map = self._stream_map(first), self._stream_map(second)
        if first_map is None or second_map is None:
            return False
        return first_map.transformed_schema != second_map.transformed_schema

    def _warn(self, first: TableWriter, second: TableWriter) -> None:
        load_methods = (
            f"both {first.load_method}"
            if first.load_method == second.load_method
            else f"{first.label} by {first.load_method}, "
            f"{second.label} by {second.load_method}"
        )
        notes = [
            f" They set different {setting} ({ours} and {theirs}): each stream's "
            "setup resets the table's, so it follows the stream set up last, and "
            "its metadata changes every run."
            for setting, ours, theirs in _differing_settings(first, second)
        ]
        if self._send_different_schemas(first, second):
            notes.append(
                f" They send {first.sink!r} different schemas: the singer-sdk "
                "rebuilds that sink whenever either sends its SCHEMA, so records "
                "of one go through a sink built for the other's schema until it "
                "sends its SCHEMA again."
            )
        self._logger.warning(
            "Streams %s and %s both load %s, so their rows are combined in it "
            "(%s). They agree on key properties (%s), so neither re-keys the "
            "other's rows.%s If they should be separate tables, give one of them "
            "its own name with a stream map (__alias__).",
            first.label,
            second.label,
            _describe_table(first, second),
            load_methods,
            first.described_keys,
            "".join(notes),
        )


def _describe_table(first: TableWriter, second: TableWriter) -> str:
    """The table as each stream names it: ``table s.t``, or both spellings."""
    names = [f"{writer.schema}.{writer.table}" for writer in (first, second)]
    if names[0] == names[1]:
        return f"table {names[0]}"
    return f"one table ({names[0]} and {names[1]}: DuckDB ignores ASCII case)"


def _check_compatible(first: TableWriter, second: TableWriter) -> None:
    """Refuse two streams loading one table that would undo each other's rows.

    Raises:
        TableCollisionError: Naming both streams, the table and the conflict.
    """
    table = _describe_table(first, second)
    where = f"Streams {first.label} and {second.label} both load {table}"
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


def _differing_settings(
    first: TableWriter, second: TableWriter
) -> list[tuple[str, str, str]]:
    """The table settings both streams set, and set differently."""
    differing = [
        (setting, _described(ours), _described(theirs))
        for setting, ours, theirs in (
            ("partition keys", first.layout.partition, second.layout.partition),
            ("sort orders", first.layout.sort, second.layout.sort),
        )
        if ours is not None and theirs is not None and not same_keys(ours, theirs)
    ]
    ours, theirs = first.inlining_row_limit, second.inlining_row_limit
    if ours is not None and theirs is not None and ours != theirs:
        differing.append(("data_inlining_row_limit values", str(ours), str(theirs)))
    return differing


def _described(keys: tuple[PartitionKey, ...] | tuple[SortKey, ...]) -> str:
    """Partition or sort keys for a message, by plain column name.

    ``['id', 'day(created_at)']``, or ``['id', 'v DESC']`` where a sort key
    isn't ascending with NULLs last.
    """
    names = []
    for key in keys:
        if isinstance(key, PartitionKey):
            names.append(
                key.column
                if key.transform == "identity"
                else f"{key.transform}({key.column})"
            )
        else:
            words = [key.column]
            if key.direction != "ASC":
                words.append(key.direction)
            if key.nulls != "NULLS_LAST":
                words.append(key.nulls.replace("_", " "))
            names.append(" ".join(words))
    return str(names)
