"""Reading Singer BATCH files as the records they hold (DUCKLKD-40).

A BATCH message points at files (JSONL or Parquet, local or in object
storage) instead of carrying records. The target reads them here, one chunk at
a time, and hands each record to the same path a RECORD message takes, so
stream maps, metadata columns, validation, flattening, coercion, merges and
deletes apply alike.

Parquet holds typed values where JSON has text, so each value is put in the
form ``json.loads(parse_float=Decimal)`` would give for the tap's JSON:
date-times, dates and times as ISO text, floats as ``Decimal``, bytes as
base64 text, maps as objects.
"""

from __future__ import annotations

import base64
import contextlib
import datetime as dt
import gzip
import math
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import pyarrow.parquet as pq
from singer_sdk.helpers._batch import BaseBatchFileEncoding, BatchFileFormat
from singer_sdk.singerlib.json import deserialize_json
from upath import UPath

if TYPE_CHECKING:
    from collections.abc import Iterator, Mapping

__all__ = ["batch_encoding", "batch_records", "json_compatible"]

_FORMATS = (BatchFileFormat.JSONL, BatchFileFormat.PARQUET)


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
            f"{[str(f) for f in _FORMATS]}."
        )
        raise ValueError(msg)
    return BaseBatchFileEncoding.from_dict(dict(encoding))


def batch_records(
    url: str,
    encoding: BaseBatchFileEncoding,
    *,
    storage_params: Mapping[str, Any] | None = None,
    chunk_rows: int = 10000,
) -> Iterator[dict[str, Any]]:
    """Yield the records of one BATCH file, reading it ``chunk_rows`` at a time.

    Args:
        url: The file, as the BATCH message's manifest names it: a path or a
            URL fsspec opens (``file://``, ``s3://``, ``gs://``, ...).
        encoding: The message's encoding: ``jsonl`` or ``parquet``, and
            ``gzip`` or ``none``. A gzip Parquet file is gzip inside (the
            singer-sdk writes it so), which Parquet readers handle.
        storage_params: fsspec options for the file system, such as
            credentials: ``batch_config.storage.params``.
        chunk_rows: The most Parquet rows held at once.
    """
    path = UPath(url, **(storage_params or {}))
    with path.open("rb") as file:
        if encoding.format == BatchFileFormat.JSONL:
            gzipped = encoding.compression == "gzip"
            with gzip.open(file) if gzipped else contextlib.nullcontext(file) as lines:
                for line in lines:
                    if line.strip():
                        yield deserialize_json(line)
            return
        for chunk in pq.ParquetFile(file).iter_batches(batch_size=chunk_rows):
            for row in chunk.to_pylist():
                yield {name: json_compatible(value) for name, value in row.items()}


def json_compatible(value: object) -> Any:  # noqa: ANN401, PLR0911 - a type dispatch
    """``value`` as decoding the tap's JSON would have given it.

    Raises:
        ValueError: For a value JSON has no form for, such as a duration.
    """
    if value is None or isinstance(value, (bool, int, str, Decimal)):
        return value
    if isinstance(value, float):
        # json.loads(parse_float=Decimal) reads the shortest repr; NaN and
        # infinities have no JSON form and stay floats.
        return Decimal(repr(value)) if math.isfinite(value) else value
    if isinstance(value, (dt.datetime, dt.date, dt.time)):
        return value.isoformat()
    if isinstance(value, (bytes, bytearray, memoryview)):
        return base64.b64encode(bytes(value)).decode("ascii")
    if isinstance(value, dict):
        return {key: json_compatible(item) for key, item in value.items()}
    if isinstance(value, list):
        # pyarrow gives a map column's values as (key, value) pairs.
        if value and all(isinstance(item, tuple) and len(item) == 2 for item in value):  # noqa: PLR2004
            return {key: json_compatible(item) for key, item in value}
        return [json_compatible(item) for item in value]
    msg = (
        f"A BATCH Parquet value of type {type(value).__name__} has no JSON form: "
        f"{value!r}. Write it as text or a number."
    )
    raise ValueError(msg)
