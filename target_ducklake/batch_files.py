"""Staging Singer BATCH files with DuckDB, never row by row in Python (DUCKLKD-119).

A BATCH message names files (JSONL, plain or gzip, or Parquet; local or in
object storage) instead of carrying records. Each message's files are read
by DuckDB and written, in one ``COPY``, as one staged Parquet file at the
RECORD path's staging types (``batch_sql``), which the sink then loads as it
loads a batch of records. A stream with keys keeps the last row of each key,
in manifest and file order. A JSONL file is parsed once, into a temporary
table that the counts of changed values and the ``COPY`` read; a Parquet
file is read where it is.

The files are read on a DuckDB connection of their own, without the catalog:
a file in object storage is opened through its fsspec file system, given
``batch_config.storage.params``, which is registered with that connection
only, so the lake's own storage is never routed through fsspec.
"""

from __future__ import annotations

import contextlib
import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import duckdb
import fsspec
import pyarrow as pa
import pyarrow.parquet as pq
from fsspec.implementations.local import LocalFileSystem
from singer_sdk.helpers._batch import BaseBatchFileEncoding, BatchFileFormat

from target_ducklake.batch_sql import (
    JSON,
    JSON_TEXT,
    UUID_BYTES,
    VARCHAR,
    BatchColumn,
    Finding,
    JsonText,
    Source,
    SourceType,
    StagingContext,
    jsonl_read_types,
    projection,
)
from target_ducklake.connector import SESSION_TIME_ZONE
from target_ducklake.duckdb_types import (
    DuckDBList,
    DuckDBScalar,
    DuckDBStruct,
    DuckDBTypeTree,
    parse_type,
    quote_identifier,
    quote_literal,
)

if TYPE_CHECKING:
    import logging
    from collections.abc import Callable, Iterable, Mapping, Sequence

__all__ = [
    "BatchColumn",
    "FoundValues",
    "StagingContext",
    "batch_encoding",
    "stage_batch",
]

_FORMATS = (BatchFileFormat.JSONL, BatchFileFormat.PARQUET)
# The columns of a row's file and its position in it, while rows are ordered.
_FILE = quote_identifier("__ducklake_batch_file")
_ROW = quote_identifier("__ducklake_batch_row")
# The columns DuckDB adds for a row's position, which a property can't share.
_PARQUET_ROW = "file_row_number"
_JSONL_ROW = "ordinality"
# The lines of DuckDB's errors that suggest read_json options.
_READ_JSON_HINTS = ("Try setting ", " If this error occurred during read_json")
# How many values a finding shows.
_SAMPLE_VALUES = 3


def batch_encoding(encoding: Mapping[str, Any]) -> BaseBatchFileEncoding:
    """A BATCH message's ``encoding``, checked.

    Raises:
        ValueError: If the format isn't ``jsonl`` or ``parquet`` (the
            singer-sdk's decoder raises a bare KeyError).
    """
    fmt = encoding.get("format")
    if fmt not in _FORMATS:
        msg = (
            f"BATCH format {fmt!r} is not supported: use one of "
            f"{[f.value for f in _FORMATS]}."
        )
        raise ValueError(msg)
    return BaseBatchFileEncoding.from_dict(dict(encoding))


@dataclass(frozen=True)
class FoundValues:
    """A finding in one file: how many values it holds, and a few of them."""

    url: str
    finding: Finding
    count: int
    sample: list[str]


def stage_batch(  # noqa: PLR0913 (the files and their columns, then the options)
    urls: Sequence[str],
    encoding: BaseBatchFileEncoding,
    columns: Sequence[BatchColumn],
    staged_path: str,
    *,
    stream: str,
    context: StagingContext,
    storage_params: Mapping[str, Any] | None,
    settings: Mapping[str, str],
    logger: logging.Logger,
    on_findings: Callable[[list[FoundValues]], None],
) -> int:
    """Stage the rows of a BATCH message's files in one Parquet file.

    Values a cast would change (``batch_sql.Finding``) are counted first,
    and passed to ``on_findings``, which warns of them or raises; nothing
    is staged when it raises. A file without a key property fails; one
    without another property is warned of.

    Args:
        urls: The message's manifest.
        encoding: Its encoding.
        columns: The staged columns, in order.
        staged_path: Where to write the staged file.
        stream: The stream, for messages.
        context: The staging rules of the stream's columns; with keys, each
            key's last row is kept.
        storage_params: fsspec options for files in object storage.
        settings: DuckDB settings for the staging connection (its memory
            limit, threads and temporary directory).
        logger: The stream's logger.
        on_findings: Told of the values the files' casts would change.

    Returns:
        The rows staged.

    Raises:
        ValueError: For a value DuckDB can't load into its column, naming
            the stream, the file, the row, the column and the value; a file
            without a key property; or a file DuckDB can't read, naming the
            stream and the files.
    """
    keys = [column for column in columns if column.name.lower() in context.keys]
    connection = duckdb.connect()
    try:
        connection.execute(f"SET TimeZone = '{SESSION_TIME_ZONE}'")
        for name, value in settings.items():
            connection.execute(f"SET {name} = {quote_literal(value)}")
        files = _Files(connection, storage_params or {}, stream=stream, keys=keys)
        sources: list[Source] = []
        selects: list[list[str]] = []
        found: list[FoundValues] = []
        for url in urls:
            source = files.source(url, encoding, columns)
            where = f"stream {stream!r}, BATCH file {url}"
            staged = projection(source, columns, where=where, context=context)
            _check_absent(source, staged.absent, keys, stream, logger)
            sources.append(source)
            selects.append(staged.selects)
            found += _count(connection, source, staged.findings, stream)
        if found:
            on_findings(found)
        query = _staged_rows(sources, selects, [key.name for key in keys])
        copy = f"COPY ({query}) TO {quote_literal(staged_path)} (FORMAT parquet)"
        return connection.execute(copy).fetchone()[0]
    except duckdb.Error as error:
        raise _failed(error, stream, urls) from error
    finally:
        connection.close()


def _check_absent(
    source: Source,
    absent: list[str],
    keys: Sequence[BatchColumn],
    stream: str,
    logger: logging.Logger,
) -> None:
    """Refuse a file without a key property; warn of one without another.

    Raises:
        ValueError: Naming the stream, the file and the key properties, as
            a record without one is refused.
    """
    if not absent:
        return
    if missing := [key.name for key in keys if key.name in absent]:
        msg = (
            f"stream {stream!r}, BATCH file {source.url}: the file has no column "
            f"for key property(s) {missing}, so its rows would load without a "
            f"key. Its columns: {sorted(source.columns)}."
        )
        raise ValueError(msg)
    logger.warning(
        "stream %r, BATCH file %s: the file has no column for %s; loaded NULL.",
        stream,
        source.url,
        ", ".join(repr(name) for name in absent),
    )


class _Files:
    """The BATCH files of a message, opened on the staging connection."""

    def __init__(
        self,
        connection: duckdb.DuckDBPyConnection,
        params: Mapping[str, Any],
        *,
        stream: str,
        keys: Sequence[BatchColumn],
    ) -> None:
        self.connection = connection
        self.params = params
        self.stream = stream
        self.keys = keys
        self.registered: set[str] = set()
        self.parsed = 0

    def source(
        self,
        url: str,
        encoding: BaseBatchFileEncoding,
        columns: Sequence[BatchColumn],
    ) -> Source:
        """The file as DuckDB reads it: how to read it, its columns and types."""
        fs, path = fsspec.core.url_to_fs(url, **self.params)
        local = isinstance(fs, LocalFileSystem)
        if not local:
            self._register(fs)
        location = path if local else url
        if encoding.format == BatchFileFormat.PARQUET:
            return self._parquet(url, location, fs, path)
        return self._jsonl(url, location, encoding, columns)

    def _register(self, fs: fsspec.AbstractFileSystem) -> None:
        """Let DuckDB open the file system's URLs, once per protocol."""
        protocols = (fs.protocol,) if isinstance(fs.protocol, str) else fs.protocol
        if not self.registered.intersection(protocols):
            self.connection.register_filesystem(fs)
            self.registered.update(protocols)

    def _parquet(
        self, url: str, location: str, fs: fsspec.AbstractFileSystem, path: str
    ) -> Source:
        reader = f"read_parquet({quote_literal(location)}"
        described = self.connection.execute(
            f"DESCRIBE SELECT * FROM {reader})"  # noqa: S608 (a quoted literal)
        ).fetchall()
        types = {name: parse_type(column_type) for name, column_type, *_ in described}
        if _holds_blob(types.values()):
            # Bytes may be UUIDs, which only the file's Arrow schema says.
            with fs.open(path, "rb") as file:
                arrow = pq.read_schema(file)
            types = {
                name: _with_uuids(tree, arrow.field(name).type)
                if name in arrow.names
                else tree
                for name, tree in types.items()
            }
        _check_no_clash(url, types, _PARQUET_ROW)
        return Source(
            url=url,
            reader=f"{reader}, file_row_number = true)",
            columns=types,
            row=f"({_PARQUET_ROW} + 1)",
        )

    def _jsonl(
        self,
        url: str,
        location: str,
        encoding: BaseBatchFileEncoding,
        columns: Sequence[BatchColumn],
    ) -> Source:
        """A JSONL file, parsed once into a temporary table, read from there.

        Each line is parsed once (``read_json_objects``), and its properties
        taken as ``jsonl_read_types`` says: the counts, the samples and the
        staged rows then read the table, not the file. A number or boolean
        column's property is kept as its text beside its JSON type
        (``batch_sql.JsonText``), and each key property's presence beside
        it, so a key sent as null is told from one not sent. A line that
        isn't a JSON object fails the file, naming it and the row.
        """
        types = jsonl_read_types(columns) or {"__ducklake_no_column": JSON.name}
        _check_no_clash(url, types, _JSONL_ROW)
        structure = json.dumps(
            {
                name: VARCHAR.name if kind == VARCHAR.name else JSON.name
                for name, kind in types.items()
            }
        )
        obj = quote_identifier("__ducklake_object")
        selects: list[str] = []
        source_types: dict[str, SourceType] = {}
        for index, (name, kind) in enumerate(types.items()):
            value = f"{obj}.{quote_identifier(name)}"
            if kind == JSON_TEXT:
                kind_column = quote_identifier(f"__ducklake_json_type_{index}")
                # A string's text is its value; any other's is its JSON text,
                # which ->> would copy more slowly.
                text = (
                    f"CASE WHEN json_type({value}) = 'VARCHAR' THEN {value} ->> '$' "
                    f"ELSE CAST({value} AS VARCHAR) END"
                )
                selects += [
                    f"{text} AS {quote_identifier(name)}",
                    f"json_type({value}) AS {kind_column}",
                ]
                source_types[name] = JsonText(kind_column)
            else:
                selects.append(f"{value} AS {quote_identifier(name)}")
                source_types[name] = parse_type(kind)
        present = {}
        for index, key in enumerate(self.keys):
            if key.path is not None and len(key.path) == 1:
                flag = quote_identifier(f"__ducklake_has_key_{index}")
                path = quote_literal("$." + json.dumps(key.path[0]))
                selects.append(f"json_exists(json, {path}) AS {flag}")
                present[key.name] = flag
        not_object = quote_literal(f"stream {self.stream!r}, BATCH file {url}, row ")
        refusal = (
            f"error({not_object} || CAST({_JSONL_ROW} AS VARCHAR) || "
            "': the line isn''t a JSON object, so the file wasn''t loaded.')"
        )
        selects.append(
            f"CASE WHEN json_type(json) = 'OBJECT' THEN {_JSONL_ROW} "
            f"ELSE {refusal} END AS {_JSONL_ROW}"
        )
        compression = "gzip" if encoding.compression == "gzip" else "uncompressed"
        self.parsed += 1
        table = quote_identifier(f"__ducklake_batch_file_{self.parsed}")
        self.connection.execute(
            f"CREATE TEMPORARY TABLE {table} AS SELECT {', '.join(selects)} "  # noqa: S608
            f"FROM (SELECT json_transform(json, {quote_literal(structure)}) AS {obj}, "
            f"json, {_JSONL_ROW} FROM read_json_objects({quote_literal(location)}, "
            f"format = 'newline_delimited', compression = '{compression}') "
            "WITH ORDINALITY)"
        )
        return Source(
            url=url,
            reader=table,
            columns=source_types,
            row=_JSONL_ROW,
            has_every_column=True,
            present=present,
        )


def _holds_blob(trees: Iterable[DuckDBTypeTree]) -> bool:
    """Whether any of the types is, or holds, BLOB."""
    for tree in trees:
        if isinstance(tree, DuckDBStruct):
            if _holds_blob(part for _, part in tree.fields):
                return True
        elif isinstance(tree, DuckDBList):
            if _holds_blob([tree.element]):
                return True
        elif tree.name.upper() == "BLOB":
            return True
    return False


def _with_uuids(tree: DuckDBTypeTree, arrow: pa.DataType) -> DuckDBTypeTree:
    """The type with each BLOB the Arrow schema says is a ``uuid`` marked so."""
    if isinstance(arrow, pa.BaseExtensionType) and arrow.extension_name == "arrow.uuid":
        return UUID_BYTES if tree == DuckDBScalar("BLOB") else tree
    if isinstance(tree, DuckDBStruct) and pa.types.is_struct(arrow):
        fields = {field.name: field.type for field in arrow}
        return DuckDBStruct(
            tuple(
                (name, _with_uuids(part, fields[name]) if name in fields else part)
                for name, part in tree.fields
            )
        )
    if isinstance(tree, DuckDBList) and (
        pa.types.is_list(arrow)
        or pa.types.is_large_list(arrow)
        or pa.types.is_fixed_size_list(arrow)
    ):
        return DuckDBList(_with_uuids(tree.element, arrow.value_type), tree.size)
    return tree


def _check_no_clash(url: str, names: Iterable[str], row: str) -> None:
    """Refuse a file whose column has the name DuckDB gives a row's position."""
    if any(name.lower() == row for name in names):
        msg = (
            f"BATCH file {url} has a column named {row!r}, which DuckDB's reader "
            "uses for each row's position. Rename the property."
        )
        raise ValueError(msg)


def _count(
    connection: duckdb.DuckDBPyConnection,
    source: Source,
    findings: list[Finding],
    stream: str,
) -> list[FoundValues]:
    """How many values of each finding the file holds, and a sample of each.

    A JSONL file's key properties are checked too, in the same query: one no
    row has (null is had) is refused, as a file without the key's column is
    (``_check_absent``).

    Raises:
        ValueError: For a JSONL file none of whose rows has a key property.
    """
    totals = [f"sum(len({finding.values}))" for finding in findings]
    totals += [f"coalesce(bool_or({flag}), true)" for flag in source.present.values()]
    if not totals:
        return []
    query = f"SELECT {', '.join(totals)} FROM {source.reader}"  # noqa: S608 (built SQL)
    counts = connection.execute(query).fetchone()
    had = counts[len(findings) :]
    if missing := [
        name for name, has in zip(source.present, had, strict=True) if not has
    ]:
        msg = (
            f"stream {stream!r}, BATCH file {source.url}: no row of the file has "
            f"key property(s) {missing}, so its rows would load without a key."
        )
        raise ValueError(msg)
    found = []
    for finding, count in zip(findings, counts[: len(findings)], strict=True):
        if not count:
            continue
        sample = connection.execute(
            f"SELECT min(v, {_SAMPLE_VALUES}) FROM "  # noqa: S608 (built SQL)
            f"(SELECT unnest({finding.values}) AS v FROM {source.reader})"
        ).fetchone()[0]
        found.append(FoundValues(source.url, finding, int(count), sample))
    return found


def _staged_rows(
    sources: Sequence[Source], selects: Sequence[list[str]], keys: Sequence[str]
) -> str:
    """The SELECT of the rows to stage: every file's, or each key's last."""
    if not keys:
        return " UNION ALL ".join(
            f"SELECT {', '.join(columns)} FROM {source.reader}"  # noqa: S608
            for source, columns in zip(sources, selects, strict=True)
        )
    rows = " UNION ALL ".join(
        f"SELECT {', '.join(columns)}, {index} AS {_FILE}, "  # noqa: S608
        f"{source.row} AS {_ROW} FROM {source.reader}"
        for index, (source, columns) in enumerate(zip(sources, selects, strict=True))
    )
    partition = ", ".join(quote_identifier(key) for key in keys)
    return (
        f"SELECT * EXCLUDE ({_FILE}, {_ROW}) FROM ({rows}) "  # noqa: S608
        f"QUALIFY row_number() OVER (PARTITION BY {partition} "
        f"ORDER BY {_FILE} DESC, {_ROW} DESC) = 1"
    )


def _failed(error: duckdb.Error, stream: str, urls: Sequence[str]) -> ValueError:
    """A failed read or cast, with the stream and the files if it lacks them."""
    message = _message(error)
    if message.startswith(f"stream {stream!r}"):
        return ValueError(message)
    return ValueError(
        f"stream {stream!r}: loading BATCH file(s) {', '.join(urls)} failed: {message}"
    )


def _message(error: duckdb.Error) -> str:
    """DuckDB's message, without its error class and its hints for read_json's options.

    ``Invalid Input Error: `` is dropped, and the lines telling a user of
    ``read_json`` to try ``auto_detect`` or ``ignore_errors``, which the
    target sets itself.
    """
    text = "\n".join(
        line
        for line in str(error).splitlines()
        if not line.startswith(_READ_JSON_HINTS)
    ).strip()
    with contextlib.suppress(ValueError):
        kind, rest = text.split(": ", 1)
        if kind.endswith("Error"):
            return rest
    return text
