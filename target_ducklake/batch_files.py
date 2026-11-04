"""Reading Singer BATCH files as the records they hold (DUCKLKD-40).

A BATCH message points at files (JSONL or Parquet, local or in object
storage) instead of carrying records. The target reads them here, one chunk at
a time, and hands each record to the same path a RECORD message takes, so
stream maps, metadata columns, validation, flattening, coercion, merges and
deletes apply alike.

Parquet holds typed values where JSON has text, so each value is put in the
form ``json.loads(parse_float=Decimal)`` would give for the tap's JSON:
date-times, dates and times as ISO text, floats as ``Decimal`` of their
shortest text, bytes as base64 text, maps as objects with text keys. Columns
are converted by their Arrow type, so an empty map is still an object, and a
nanosecond timestamp keeps its nine digits.
"""

from __future__ import annotations

import base64
import contextlib
import datetime as dt
import gzip
import math
import re
import struct
from decimal import Decimal
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq
from singer_sdk.helpers._batch import BaseBatchFileEncoding, BatchFileFormat
from singer_sdk.singerlib.json import deserialize_json
from upath import UPath

if TYPE_CHECKING:
    from collections.abc import Generator, Mapping

__all__ = ["batch_encoding", "batch_records", "json_compatible"]

_FORMATS = (BatchFileFormat.JSONL, BatchFileFormat.PARQUET)
_EPOCH = dt.datetime(1970, 1, 1)  # noqa: DTZ001 - naive, as an Arrow timestamp without a tz
_OFFSET = re.compile(r"^([+-])(\d{2}):(\d{2})$")
_NANOS = 10**9


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
) -> Generator[dict[str, Any], None, None]:
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
            columns = {
                name: _values(chunk.column(index))
                for index, name in enumerate(chunk.schema.names)
            }
            for row in range(chunk.num_rows):
                yield {name: values[row] for name, values in columns.items()}


def _values(array: pa.Array) -> list[Any]:  # noqa: PLR0911 - a type dispatch
    """An Arrow column's values in their JSON forms, by the column's type."""
    typ = array.type
    if pa.types.is_dictionary(typ):
        return _values(array.dictionary_decode())
    valid = array.is_valid().to_pylist()
    if pa.types.is_map(typ):
        pairs = array.values.flatten()
        keys, items = _values(pairs[0]), _values(pairs[1])
        offsets = array.offsets.to_pylist()
        return [
            {_key(keys[i]): items[i] for i in range(start, end)} if ok else None
            for ok, start, end in zip(valid, offsets, offsets[1:], strict=False)
        ]
    if pa.types.is_list(typ) or pa.types.is_large_list(typ):
        values = _values(array.values)
        offsets = array.offsets.to_pylist()
        return [
            values[start:end] if ok else None
            for ok, start, end in zip(valid, offsets, offsets[1:], strict=False)
        ]
    if pa.types.is_struct(typ):
        fields = {
            typ.field(index).name: _values(child)
            for index, child in enumerate(array.flatten())
        }
        return [
            {name: values[row] for name, values in fields.items()} if ok else None
            for row, ok in enumerate(valid)
        ]
    if pa.types.is_timestamp(typ) and typ.unit == "ns":
        # to_pylist needs pandas for these, and would drop the nanoseconds.
        nanos = array.cast(pa.int64()).to_pylist()
        return [None if n is None else _nanosecond_text(n, typ.tz) for n in nanos]
    if pa.types.is_float32(typ):
        return [_float32_decimal(value) for value in array.to_pylist()]
    return [json_compatible(value) for value in array.to_pylist()]


def _key(key: object) -> str:
    """A map key as JSON writes an object's key: as text."""
    if isinstance(key, str):
        return key
    if isinstance(key, bool):
        return "true" if key else "false"
    return str(key)


def _float32_decimal(value: float | None) -> Decimal | float | None:
    """A float32 as the shortest text that reads back as it, as JSON writes it."""
    if value is None or not math.isfinite(value):
        return value
    single = struct.unpack("f", struct.pack("f", value))[0]
    for digits in range(1, 10):
        text = f"{value:.{digits}g}"
        if struct.unpack("f", struct.pack("f", float(text)))[0] == single:
            return Decimal(text)
    return Decimal(repr(value))


def _nanosecond_text(nanos: int, tz: str | None) -> str:
    """ISO text for a nanosecond timestamp, in its column's time zone."""
    seconds, fraction = divmod(nanos, _NANOS)
    moment = _EPOCH + dt.timedelta(seconds=seconds)
    if tz is not None:
        moment = moment.replace(tzinfo=dt.timezone.utc).astimezone(_zone(tz))
    text = moment.isoformat()
    return f"{text[:19]}.{fraction:09d}{text[19:]}"


def _zone(tz: str) -> dt.tzinfo:
    """An Arrow time zone: a fixed offset such as +02:00, or a zone name."""
    if match := _OFFSET.match(tz):
        sign, hours, minutes = match.groups()
        offset = dt.timedelta(hours=int(hours), minutes=int(minutes))
        return dt.timezone(-offset if sign == "-" else offset)
    return dt.timezone.utc if tz.upper() == "UTC" else ZoneInfo(tz)


def json_compatible(value: object) -> Any:  # noqa: ANN401 - any JSON value
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
    if isinstance(value, (list, tuple)):
        return [json_compatible(item) for item in value]
    msg = (
        f"A BATCH Parquet value of type {type(value).__name__} has no JSON form: "
        f"{value!r}. Write it as text or a number."
    )
    raise ValueError(msg)
