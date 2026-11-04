"""Singer BATCH messages, loaded as the RECORD messages they hold (DUCKLKD-40)."""

from __future__ import annotations

import base64
import datetime as dt
import gzip
import io
import json
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import fsspec
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from target_ducklake.batch_files import (
    _values,
    _zone,
    batch_encoding,
    batch_records,
    json_compatible,
)
from target_ducklake.target import Targetducklake

if TYPE_CHECKING:
    from pathlib import Path

    from tests.catalogs import Lake

PROPERTIES: dict[str, Any] = {
    "id": {"type": "integer"},
    "name": {"type": ["null", "string"]},
    "amount": {"type": ["null", "number"]},
    "price": {"type": ["null", "number"], "x-sql-datatype": "DECIMAL(10,2)"},
    "ts": {"type": ["null", "string"], "format": "date-time"},
    "day": {"type": ["null", "string"], "format": "date"},
    "blob": {"type": ["null", "string"], "contentEncoding": "base64"},
    "obj": {
        "type": ["null", "object"],
        "properties": {"a": {"type": "integer"}, "b": {"type": "string"}},
    },
    "tags": {"type": ["null", "array"], "items": {"type": "string"}},
}
# As a tap sends them in RECORD messages: JSON values.
RECORDS: list[dict[str, Any]] = [
    {
        "id": i,
        "name": f"n{i}",
        "amount": i + 0.5,
        "price": i + 0.25,
        "ts": f"2024-01-0{i}T10:00:00+02:00",
        "day": f"2024-02-0{i}",
        "blob": base64.b64encode(f"b{i}".encode()).decode(),
        "obj": {"a": i, "b": "x"},
        "tags": ["t", str(i)],
    }
    for i in range(1, 6)
]
# ts as text: fetching a TIMESTAMPTZ into Python needs pytz.
COLUMNS = "id, name, amount, price, ts::VARCHAR, day, blob, obj, tags"


def _schema(stream: str = "t", keys: list[str] | None = None) -> dict:
    return {
        "type": "SCHEMA",
        "stream": stream,
        "schema": {"type": "object", "properties": PROPERTIES},
        "key_properties": ["id"] if keys is None else keys,
    }


def _batch(
    urls: list[str], fmt: str, compression: str = "none", stream: str = "t"
) -> dict:
    return {
        "type": "BATCH",
        "stream": stream,
        "encoding": {"format": fmt, "compression": compression},
        "manifest": urls,
    }


def _records(records: list[dict], stream: str = "t") -> list[dict]:
    return [{"type": "RECORD", "stream": stream, "record": r} for r in records]


def _listen(lake: Lake, messages: list[dict], **config: object) -> None:
    target = Targetducklake(config={**lake.config(), **config})
    try:
        target.listen(
            file_input=io.StringIO(
                "".join(json.dumps(m, default=str) + "\n" for m in messages)
            )
        )
    finally:
        target.target_connector.close()


def _rows(lake: Lake, table: str = "t", columns: str = COLUMNS) -> list[tuple]:
    return lake.query(f"SELECT {columns} FROM lake.s.{table} ORDER BY id")  # noqa: S608


def _jsonl(path: Path, records: list[dict], *, gzipped: bool = False) -> str:
    text = "".join(json.dumps(r) + "\n" for r in records).encode()
    path.write_bytes(gzip.compress(text) if gzipped else text)
    return path.as_uri()


def _parquet(path: Path, table: pa.Table, compression: str = "snappy") -> str:
    pq.write_table(table, path, compression=compression)
    return path.as_uri()


def _typed_table(records: list[dict]) -> pa.Table:
    """The records with Parquet's own types: timestamps, dates, decimals, bytes."""
    return pa.table(
        {
            "id": pa.array([r["id"] for r in records], pa.int64()),
            "name": pa.array([r["name"] for r in records]),
            "amount": pa.array([r["amount"] for r in records], pa.float64()),
            "price": pa.array(
                [Decimal(str(r["price"])) for r in records], pa.decimal128(10, 2)
            ),
            "ts": pa.array(
                [dt.datetime.fromisoformat(r["ts"]) for r in records],
                pa.timestamp("us", tz="UTC"),
            ),
            "day": pa.array([dt.date.fromisoformat(r["day"]) for r in records]),
            "blob": pa.array([base64.b64decode(r["blob"]) for r in records]),
            "obj": pa.array([r["obj"] for r in records]),
            "tags": pa.array([r["tags"] for r in records]),
        }
    )


@pytest.fixture
def expected(lake: Lake) -> list[tuple]:
    """The table RECORD messages load: what every BATCH encoding must give."""
    _listen(lake, [_schema(), *_records(RECORDS)])
    rows = _rows(lake)
    lake.query("DROP TABLE lake.s.t")
    return rows


class TestSameAsRecords:
    """Each encoding loads exactly what RECORD messages load."""

    @pytest.mark.parametrize("gzipped", [False, True], ids=["plain", "gzip"])
    def test_jsonl(self, lake: Lake, tmp_path: Path, expected: list, *, gzipped: bool):
        """JSONL, plain and gzipped, in two files."""
        urls = [
            _jsonl(tmp_path / "a.jsonl", RECORDS[:2], gzipped=gzipped),
            _jsonl(tmp_path / "b.jsonl", RECORDS[2:], gzipped=gzipped),
        ]
        encoding = "gzip" if gzipped else "none"
        _listen(lake, [_schema(), _batch(urls, "jsonl", encoding)])
        assert _rows(lake) == expected

    def test_parquet_as_the_singer_sdk_writes_it(
        self, lake: Lake, tmp_path: Path, expected: list
    ):
        """A singer-sdk tap writes pa.Table.from_pylist of its JSON-ready records."""
        url = _parquet(tmp_path / "a.parquet.gz", pa.Table.from_pylist(RECORDS), "gzip")
        _listen(lake, [_schema(), _batch([url], "parquet", "gzip")])
        assert _rows(lake) == expected

    def test_parquet_with_its_own_types(
        self, lake: Lake, tmp_path: Path, expected: list
    ):
        """Timestamps, dates, decimals and bytes load as their JSON forms would."""
        url = _parquet(tmp_path / "a.parquet", _typed_table(RECORDS))
        _listen(lake, [_schema(), _batch([url], "parquet")])
        assert _rows(lake) == expected

    def test_object_storage(self, lake: Lake, expected: list):
        """A URL fsspec opens (memory:// standing in for s3:// or gs://)."""
        memory = fsspec.filesystem("memory")
        with memory.open("/batches/a.parquet", "wb") as file:
            pq.write_table(_typed_table(RECORDS), file)
        try:
            _listen(
                lake, [_schema(), _batch(["memory:///batches/a.parquet"], "parquet")]
            )
        finally:
            memory.rm("/batches", recursive=True)
        assert _rows(lake) == expected


class TestRecordSemantics:
    """What the RECORD path does to a record, BATCH does too."""

    def test_records_and_batches_mix_and_merge(self, lake: Lake, tmp_path: Path):
        """A later file's record for a key replaces the earlier one."""
        changed = {**RECORDS[1], "name": "changed"}
        messages = [
            _schema(),
            *_records(RECORDS[:2]),
            _batch([_jsonl(tmp_path / "a.jsonl", [changed, RECORDS[2]])], "jsonl"),
        ]
        _listen(lake, messages)
        assert _rows(lake, columns="id, name") == [(1, "n1"), (2, "changed"), (3, "n3")]

    def test_stream_maps_apply(self, lake: Lake, tmp_path: Path):
        """A filter and a dropped property, as on RECORD messages."""
        url = _jsonl(tmp_path / "a.jsonl", RECORDS)
        maps = {"t": {"__filter__": "id != 2", "name": "__NULL__"}}
        _listen(lake, [_schema(), _batch([url], "jsonl")], stream_maps=maps)
        assert lake.query("SELECT id FROM lake.s.t ORDER BY id") == [
            (1,),
            (3,),
            (4,),
            (5,),
        ]
        columns = lake.query(
            "SELECT column_name FROM duckdb_columns() "
            "WHERE database_name = 'lake' AND table_name = 't'"
        )
        assert ("name",) not in columns

    def test_an_aliased_stream(self, lake: Lake, tmp_path: Path):
        """A stream map's alias loads the file into the aliased table."""
        url = _jsonl(tmp_path / "a.jsonl", RECORDS[:2])
        maps = {"t": {"__alias__": "renamed"}}
        _listen(lake, [_schema(), _batch([url], "jsonl")], stream_maps=maps)
        assert _rows(lake, "renamed", "id") == [(1,), (2,)]

    def test_record_metadata(self, lake: Lake, tmp_path: Path):
        """add_record_metadata stamps BATCH records as RECORD ones."""
        url = _jsonl(tmp_path / "a.jsonl", RECORDS[:2])
        _listen(lake, [_schema(), _batch([url], "jsonl")], add_record_metadata=True)
        stamped = lake.query(
            "SELECT count(*) FROM lake.s.t WHERE _sdc_received_at IS NOT NULL"
        )
        assert stamped == [(2,)]

    def test_validation_of_typed_parquet(self, lake: Lake, tmp_path: Path):
        """Typed values validate as the JSON forms the schema declares."""
        url = _parquet(tmp_path / "a.parquet", _typed_table(RECORDS))
        _listen(lake, [_schema(), _batch([url], "parquet")], validate_records=True)
        assert _rows(lake, columns="id") == [(i,) for i in range(1, 6)]

    def test_the_messages_version_and_time_extracted(self, lake: Lake, tmp_path: Path):
        """Kept on its records, as on a RECORD message's, for ACTIVATE_VERSION."""
        url = _jsonl(tmp_path / "a.jsonl", RECORDS[1:3])
        batch = {
            **_batch([url], "jsonl"),
            "version": 5,
            "time_extracted": "2024-03-01T00:00:00Z",
        }
        messages = [
            _schema(),
            *({**m, "version": 5} for m in _records(RECORDS[:1])),
            batch,
            {"type": "ACTIVATE_VERSION", "stream": "t", "version": 5},
        ]
        _listen(lake, messages, add_record_metadata=True)
        assert lake.query(
            "SELECT id, _sdc_table_version, _sdc_extracted_at IS NOT NULL, "
            "_sdc_deleted_at IS NULL FROM lake.s.t ORDER BY id"
        ) == [(1, 5, False, True), (2, 5, True, True), (3, 5, True, True)]

    def test_flattening(self, lake: Lake, tmp_path: Path):
        """flatten_max_level splits objects into columns, as for RECORD."""
        url = _jsonl(tmp_path / "a.jsonl", RECORDS[:1])
        _listen(lake, [_schema(), _batch([url], "jsonl")], flatten_max_level=1)
        assert lake.query("SELECT obj__a, obj__b FROM lake.s.t") == [(1, "x")]

    def test_deletes(self, lake: Lake, tmp_path: Path):
        """A file's _sdc_deleted_at record deletes its key's row."""
        props = {**PROPERTIES, "_sdc_deleted_at": {"type": ["null", "string"]}}
        schema = {**_schema(), "schema": {"type": "object", "properties": props}}
        gone = {"id": 2, "_sdc_deleted_at": "2024-03-01T00:00:00Z"}
        url = _jsonl(tmp_path / "a.jsonl", [gone])
        _listen(
            lake,
            [schema, *_records(RECORDS[:3]), _batch([url], "jsonl")],
            delete_rows_with_deleted_at=True,
        )
        assert _rows(lake, columns="id") == [(1,), (3,)]

    def test_a_file_is_loaded_in_batches(self, lake: Lake, tmp_path: Path):
        """A file larger than a batch is drained as it is read, not held whole."""
        url = _parquet(tmp_path / "a.parquet", _typed_table(RECORDS))
        _listen(lake, [_schema(keys=[]), _batch([url], "parquet")], batch_size_rows=2)
        files = lake.query(
            "SELECT data_file FROM ducklake_list_files('lake', 't', schema => 's')"
        )
        assert len(files) == 3  # noqa: PLR2004
        assert _rows(lake, columns="id") == [(i,) for i in range(1, 6)]

    def test_an_unknown_format_fails(self, lake: Lake, tmp_path: Path):
        """Named, rather than skipped."""
        with pytest.raises(ValueError, match="BATCH format 'csv'"):
            _listen(lake, [_schema(), _batch([(tmp_path / "a.csv").as_uri()], "csv")])


def _parquet_rows(tmp_path: Path, table: pa.Table) -> list[dict]:
    url = _parquet(tmp_path / "t.parquet", table)
    return list(batch_records(url, batch_encoding({"format": "parquet"})))


class TestParquetTypes:
    """Values read by their Arrow type, as the tap's JSON would hold them."""

    def test_maps_including_empty_ones(self, tmp_path: Path):
        """An empty map is an empty object; keys are text, as in JSON."""
        typ = pa.map_(pa.int64(), pa.int64())
        table = pa.table({"m": pa.array([[], [(1, 2)], None], typ)})
        assert _parquet_rows(tmp_path, table) == [
            {"m": {}},
            {"m": {"1": 2}},
            {"m": None},
        ]

    def test_nested_maps(self, tmp_path: Path):
        """In a struct and in a list."""
        typ = pa.map_(pa.string(), pa.int64())
        table = pa.table(
            {
                "s": pa.array([{"m": []}], pa.struct([("m", typ)])),
                "l": pa.array([[[("a", 1)], []]], pa.list_(typ)),
            }
        )
        assert _parquet_rows(tmp_path, table) == [{"s": {"m": {}}, "l": [{"a": 1}, {}]}]

    def test_fixed_size_lists(self, tmp_path: Path):
        """Read by type like other lists: float32 and ns timestamps inside."""
        floats = pa.array([[0.1, 1.5], [2.5, 0.2]], pa.list_(pa.float32(), 2))
        stamps = pa.array([[0], [1]], pa.list_(pa.timestamp("ns"), 1))
        table = pa.table({"f": floats, "t": stamps})
        assert _parquet_rows(tmp_path, table) == [
            {
                "f": [Decimal("0.1"), Decimal("1.5")],
                "t": ["1970-01-01T00:00:00.000000000"],
            },
            {
                "f": [Decimal("2.5"), Decimal("0.2")],
                "t": ["1970-01-01T00:00:00.000000001"],
            },
        ]

    def test_a_sliced_fixed_size_list_with_nulls(self):
        """Its rows start at the array's offset; a null list stays null.

        A Parquet file can't hold such nulls, but Arrow arrays can.
        """
        lists = pa.FixedSizeListArray.from_arrays(
            pa.array([0.1, 1.5, 0, 0, 2.5, 0.2], pa.float32()),
            2,
            mask=pa.array([False, True, False]),
        )
        assert _values(lists.slice(1)) == [None, [Decimal("2.5"), Decimal("0.2")]]

    def test_an_unknown_zone_is_named(self):
        """Not a bare ZoneInfoNotFoundError."""
        with pytest.raises(ValueError, match="time zone 'Nowhere/Atlantis'"):
            _zone("Nowhere/Atlantis")

    def test_float32_as_its_shortest_text(self, tmp_path: Path):
        """float32 0.1 is written 0.1 in JSON, not 0.10000000149011612."""
        table = pa.table({"f": pa.array([0.1, 1.5, None], pa.float32())})
        assert _parquet_rows(tmp_path, table) == [
            {"f": Decimal("0.1")},
            {"f": Decimal("1.5")},
            {"f": None},
        ]

    @pytest.mark.parametrize(
        ("tz", "expected"),
        [
            (None, "2024-01-01T00:00:00.123456789"),
            ("UTC", "2024-01-01T00:00:00.123456789+00:00"),
            ("+02:00", "2024-01-01T02:00:00.123456789+02:00"),
            ("Europe/Paris", "2024-01-01T01:00:00.123456789+01:00"),
        ],
        ids=["naive", "utc", "offset", "zone"],
    )
    def test_nanosecond_timestamps(self, tmp_path: Path, tz: str | None, expected: str):
        """All nine digits, without pandas, in the column's time zone."""
        nanos = 1_704_067_200_123_456_789  # 2024-01-01T00:00:00.123456789Z
        table = pa.table({"ts": pa.array([nanos, None], pa.timestamp("ns", tz=tz))})
        assert _parquet_rows(tmp_path, table) == [{"ts": expected}, {"ts": None}]


class TestAbout:
    """The target declares the capability, and batch_config with it."""

    def test_batch_capability(self):
        """--about lists batch and the SDK's batch_config setting."""
        assert "batch" in [str(c) for c in Targetducklake.capabilities]
        assert "batch_config" in Targetducklake.config_jsonschema["properties"]


class TestJsonCompatible:
    """Parquet values in their JSON forms."""

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            (
                dt.datetime(2024, 1, 1, 8, tzinfo=dt.timezone.utc),
                "2024-01-01T08:00:00+00:00",
            ),
            (dt.datetime(2024, 1, 1, 8), "2024-01-01T08:00:00"),  # noqa: DTZ001
            (dt.date(2024, 1, 2), "2024-01-02"),
            (dt.time(8, 30), "08:30:00"),
            (0.1, Decimal("0.1")),
            (b"\x00\x01", "AAE="),
            ({"a": [dt.date(2024, 1, 2)]}, {"a": ["2024-01-02"]}),
            (None, None),
            (True, True),
            (3, 3),
        ],
        ids=[
            "aware",
            "naive",
            "date",
            "time",
            "float",
            "bytes",
            "nested",
            "null",
            "bool",
            "int",
        ],
    )
    def test_value(self, value: object, expected: object):
        """As json.loads(parse_float=Decimal) of a tap's JSON would give."""
        assert json_compatible(value) == expected

    def test_a_float_that_json_cant_hold_is_kept(self):
        """NaN and infinities have no JSON form; they stay floats."""
        assert json_compatible(float("inf")) == float("inf")

    def test_an_unsupported_value_is_named(self):
        """A duration has no JSON form a schema could declare."""
        with pytest.raises(ValueError, match="timedelta"):
            json_compatible(dt.timedelta(seconds=1))
