"""Singer BATCH messages, staged and cast by DuckDB (DUCKLKD-40, DUCKLKD-119).

``tests/test_batch_equivalence.py`` compares the tables RECORD and BATCH
messages load. These cover what is BATCH's own: encodings and storage, stream
maps, the values DuckDB refuses (named with the stream, the file and the
column) and those its casts change (warned of, or refused under strict
``value_type_checking``), and Parquet's own types.
"""

from __future__ import annotations

import base64
import datetime as dt
import gzip
import io
import json
import logging
import uuid
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import fsspec
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from singer_sdk.exceptions import StreamMapConfigError

from target_ducklake.layout import LayoutError
from target_ducklake.target import Targetducklake

if TYPE_CHECKING:
    from collections.abc import Iterator
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


def _schema(
    stream: str = "t",
    keys: list[str] | None = None,
    properties: dict[str, Any] | None = None,
) -> dict:
    return {
        "type": "SCHEMA",
        "stream": stream,
        "schema": {"type": "object", "properties": properties or PROPERTIES},
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


@pytest.fixture
def warnings(caplog: pytest.LogCaptureFixture) -> Iterator[pytest.LogCaptureFixture]:
    """The target's warnings (its logger doesn't propagate)."""
    logger = logging.getLogger("target-ducklake")
    logger.addHandler(caplog.handler)
    caplog.handler.setLevel(logging.WARNING)
    try:
        yield caplog
    finally:
        logger.removeHandler(caplog.handler)


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
        """Timestamps, dates, decimals, bytes and structs, cast by DuckDB."""
        url = _parquet(tmp_path / "a.parquet", _typed_table(RECORDS))
        _listen(lake, [_schema(), _batch([url], "parquet")])
        assert _rows(lake) == expected

    @pytest.mark.parametrize("fmt", ["parquet", "jsonl"])
    def test_object_storage(self, lake: Lake, expected: list, fmt: str):
        """A URL fsspec opens (memory:// standing in for s3:// or gs://)."""
        memory = fsspec.filesystem("memory")
        url = f"memory:///batches/a.{fmt}"
        with memory.open(url, "wb") as file:
            if fmt == "parquet":
                pq.write_table(_typed_table(RECORDS), file)
            else:
                file.write(gzip.compress(json.dumps(RECORDS[0]).encode() + b"\n"))
        try:
            compression = "none" if fmt == "parquet" else "gzip"
            _listen(lake, [_schema(), _batch([url], fmt, compression)])
        finally:
            memory.rm("/batches", recursive=True)
        assert _rows(lake) == (expected if fmt == "parquet" else expected[:1])


class TestRecordSemantics:
    """What the RECORD path does to a stream, BATCH does too."""

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

    def test_records_before_a_batch_load_first(self, lake: Lake, tmp_path: Path):
        """The sink is drained before the file, so the file's row of a key wins."""
        earlier = {**RECORDS[0], "name": "record"}
        url = _jsonl(tmp_path / "a.jsonl", [{**RECORDS[0], "name": "file"}])
        _listen(lake, [_schema(), *_records([earlier]), _batch([url], "jsonl")])
        assert _rows(lake, columns="id, name") == [(1, "file")]

    def test_the_last_row_of_a_key_wins(self, lake: Lake, tmp_path: Path):
        """In file order, and in manifest order across files."""
        first = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}, {"id": 1, "name": "c"}]
        second = [{"id": 2, "name": "d"}]
        for fmt in ("jsonl", "parquet"):
            if fmt == "jsonl":
                urls = [
                    _jsonl(tmp_path / "1.jsonl", first),
                    _jsonl(tmp_path / "2.jsonl", second),
                ]
            else:
                urls = [
                    _parquet(tmp_path / "1.parquet", pa.Table.from_pylist(first)),
                    _parquet(tmp_path / "2.parquet", pa.Table.from_pylist(second)),
                ]
            _listen(lake, [_schema(), _batch(urls, fmt)])
            assert _rows(lake, columns="id, name") == [(1, "c"), (2, "d")], fmt
            lake.query("DROP TABLE lake.s.t")

    def test_an_aliased_stream(self, lake: Lake, tmp_path: Path):
        """A stream map's alias loads the file into the aliased table."""
        url = _jsonl(tmp_path / "a.jsonl", RECORDS[:2])
        maps = {"t": {"__alias__": "renamed"}}
        _listen(lake, [_schema(), _batch([url], "jsonl")], stream_maps=maps)
        assert _rows(lake, "renamed", "id") == [(1,), (2,)]

    def test_a_copy_with_its_own_keys(self, lake: Lake, tmp_path: Path):
        """__source__ and __key_properties__ apply to the sink, not the rows."""
        url = _jsonl(tmp_path / "a.jsonl", [*RECORDS[:2], {**RECORDS[2], "name": "n1"}])
        maps = {"copy": {"__source__": "t", "__key_properties__": ["name"]}}
        _listen(lake, [_schema(), _batch([url], "jsonl")], stream_maps=maps)
        assert _rows(lake, "t", "id") == [(1,), (2,), (3,)]
        assert _rows(lake, "copy", "id, name") == [(2, "n2"), (3, "n1")]

    @pytest.mark.parametrize(
        ("maps", "named"),
        [
            ({"t": {"__filter__": "id != 2"}}, "stream_maps.t sets __filter__"),
            ({"t": {"name": "__NULL__"}}, "stream_maps.t sets name"),
            (
                {"t": {"__alias__": "u", "upper": "name.upper()"}},
                "stream_maps.t sets upper",
            ),
            ({"*": {"__else__": "__NULL__", "id": "id"}}, "stream_maps.* sets"),
        ],
        ids=["filter", "removed-property", "expression", "wildcard-else"],
    )
    def test_a_map_that_changes_records_fails_the_stream(
        self, lake: Lake, tmp_path: Path, maps: dict, named: str
    ):
        """Before any file is read: a map left out would load what it keeps out."""
        url = _jsonl(tmp_path / "a.jsonl", RECORDS)
        with pytest.raises(StreamMapConfigError, match=named):
            _listen(lake, [_schema(), _batch([url], "jsonl")], stream_maps=maps)

    def test_the_singer_sdk_flattening_fails_the_stream(
        self, lake: Lake, tmp_path: Path
    ):
        """flattening_enabled rewrites records in its stream maps."""
        url = _jsonl(tmp_path / "a.jsonl", RECORDS)
        config = {"flattening_enabled": True, "flattening_max_depth": 1}
        with pytest.raises(StreamMapConfigError, match="flattening_enabled is on"):
            _listen(lake, [_schema(), _batch([url], "jsonl")], **config)

    def test_type_source_fails_the_stream(self, lake: Lake, tmp_path: Path):
        """It converts each record's values in Python, which BATCH rows skip."""
        properties = {"id": {"type": "integer"}, "n": {"type": ["null", "string"]}}
        url = _jsonl(tmp_path / "a.jsonl", [{"id": 1, "n": "5"}])
        config = {"stream_overrides": {"t": {"type_source": {"n": "integer"}}}}
        with pytest.raises(LayoutError, match="type_source converts each record"):
            _listen(
                lake,
                [_schema(properties=properties), _batch([url], "jsonl")],
                **config,
            )

    def test_validate_records_doesnt_apply(self, lake: Lake, tmp_path: Path):
        """As in the singer-sdk: DuckDB's casts check the values, not the schema."""
        properties = {"id": {"type": "integer"}, "code": {"type": "string"}}
        url = _jsonl(tmp_path / "a.jsonl", [{"id": 1, "code": 42}])
        messages = [_schema(properties=properties), _batch([url], "jsonl")]
        _listen(lake, messages, validate_records=True)
        assert _rows(lake, columns="id, code") == [(1, "42")]

    def test_record_metadata(self, lake: Lake, tmp_path: Path):
        """add_record_metadata stamps BATCH rows as RECORD ones."""
        url = _jsonl(tmp_path / "a.jsonl", RECORDS[:2])
        _listen(lake, [_schema(), _batch([url], "jsonl")], add_record_metadata=True)
        stamped = lake.query(
            "SELECT count(*) FROM lake.s.t WHERE _sdc_received_at IS NOT NULL "
            "AND _sdc_batched_at IS NOT NULL AND _sdc_sequence IS NOT NULL "
            "AND _sdc_sync_started_at IS NOT NULL"
        )
        assert stamped == [(2,)]

    def test_the_messages_version_and_time_extracted(self, lake: Lake, tmp_path: Path):
        """Kept on its rows, as on a RECORD message's, for ACTIVATE_VERSION."""
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
            "SELECT id, _sdc_table_version, _sdc_extracted_at::VARCHAR, "
            "_sdc_deleted_at IS NULL FROM lake.s.t ORDER BY id"
        ) == [
            (1, 5, None, True),
            (2, 5, "2024-03-01 00:00:00+00", True),
            (3, 5, "2024-03-01 00:00:00+00", True),
        ]

    def test_flattening(self, lake: Lake, tmp_path: Path):
        """flatten_max_level splits objects into columns, as for RECORD."""
        url = _jsonl(tmp_path / "a.jsonl", RECORDS[:1])
        _listen(lake, [_schema(), _batch([url], "jsonl")], flatten_max_level=1)
        assert lake.query("SELECT obj__a, obj__b FROM lake.s.t") == [(1, "x")]

    def test_a_parquet_file_of_flattened_columns(self, lake: Lake, tmp_path: Path):
        """A file holding obj__a rather than obj loads it, as the RECORD path did."""
        table = pa.table({"id": [1], "obj__a": [7], "obj__b": ["y"]})
        url = _parquet(tmp_path / "a.parquet", table)
        _listen(lake, [_schema(), _batch([url], "parquet")], flatten_max_level=1)
        assert lake.query("SELECT obj__a, obj__b FROM lake.s.t") == [(7, "y")]

    def test_deletes(self, lake: Lake, tmp_path: Path):
        """A file's _sdc_deleted_at record deletes its key's row."""
        props = {**PROPERTIES, "_sdc_deleted_at": {"type": ["null", "string"]}}
        gone = {"id": 2, "_sdc_deleted_at": "2024-03-01T00:00:00Z"}
        url = _jsonl(tmp_path / "a.jsonl", [gone])
        _listen(
            lake,
            [_schema(properties=props), *_records(RECORDS[:3]), _batch([url], "jsonl")],
            delete_rows_with_deleted_at=True,
        )
        assert _rows(lake, columns="id") == [(1,), (3,)]

    def test_a_file_is_one_batch(self, lake: Lake, tmp_path: Path):
        """A message's files are staged and committed together, whatever their size.

        DuckDB stages them without holding them in Python, so batch_size_rows
        (which bounds the records held) doesn't split them.
        """
        url = _parquet(tmp_path / "a.parquet", _typed_table(RECORDS))
        _listen(lake, [_schema(keys=[]), _batch([url], "parquet")], batch_size_rows=2)
        files = lake.query(
            "SELECT data_file FROM ducklake_list_files('lake', 't', schema => 's')"
        )
        assert len(files) == 1
        assert _rows(lake, columns="id") == [(i,) for i in range(1, 6)]

    def test_an_empty_file_loads_nothing(self, lake: Lake, tmp_path: Path):
        """No commit, and no error."""
        (tmp_path / "a.jsonl").write_text("")
        url = (tmp_path / "a.jsonl").as_uri()
        _listen(lake, [_schema(), *_records(RECORDS[:1]), _batch([url], "jsonl")])
        snapshots = lake.query(
            "SELECT count(*) FROM ducklake_snapshots('lake') "
            "WHERE commit_message LIKE 't:%'"
        )
        assert snapshots == [(1,)]

    def test_an_empty_manifest_loads_nothing(self, lake: Lake):
        """No file, no commit, no error."""
        _listen(lake, [_schema(), *_records(RECORDS[:1]), _batch([], "jsonl")])
        assert _rows(lake, columns="id") == [(1,)]
        snapshots = lake.query(
            "SELECT count(*) FROM ducklake_snapshots('lake') "
            "WHERE commit_message LIKE 't:%'"
        )
        assert snapshots == [(1,)]

    @pytest.mark.parametrize(
        "text",
        [
            '{"id": 1, "name": "a"}\n\n{"id": 2, "name": "b"}\n\n',
            '{"id": 1, "name": "a"}\r\n{"id": 2, "name": "b"}\r\n',
        ],
        ids=["blank-lines", "crlf"],
    )
    def test_jsonl_lines(self, lake: Lake, tmp_path: Path, text: str):
        """Blank lines are skipped, and CRLF ends a line as LF does."""
        (tmp_path / "a.jsonl").write_bytes(text.encode())
        url = (tmp_path / "a.jsonl").as_uri()
        _listen(lake, [_schema(), _batch([url], "jsonl")])
        assert _rows(lake, columns="id, name") == [(1, "a"), (2, "b")]

    def test_an_unknown_format_fails(self, lake: Lake, tmp_path: Path):
        """Named, with the formats there are."""
        with pytest.raises(
            ValueError, match=r"BATCH format 'csv'.*\['jsonl', 'parquet'\]"
        ):
            _listen(lake, [_schema(), _batch([(tmp_path / "a.csv").as_uri()], "csv")])

    def test_a_property_named_as_duckdbs_row_number(self, lake: Lake, tmp_path: Path):
        """DuckDB's reader names a row's position; a property can't share it."""
        properties = {"id": {"type": "integer"}, "ordinality": {"type": "integer"}}
        url = _jsonl(tmp_path / "a.jsonl", [{"id": 1, "ordinality": 2}])
        with pytest.raises(ValueError, match="column named 'ordinality'"):
            _listen(lake, [_schema(properties=properties), _batch([url], "jsonl")])


class TestRefused:
    """A value DuckDB can't load fails the files, naming where it is."""

    @pytest.mark.parametrize("fmt", ["jsonl", "parquet"])
    def test_a_file_without_a_key_property(self, lake: Lake, tmp_path: Path, fmt: str):
        """Names are exact, as a record's keys are: ID isn't id."""
        rows = [{"ID": 1, "name": "a"}]
        if fmt == "jsonl":
            url = _jsonl(tmp_path / "a.jsonl", rows)
            named = "no row of the file has a value for key property"
        else:
            url = _parquet(tmp_path / "a.parquet", pa.Table.from_pylist(rows))
            named = "the file has no column for key property"
        with pytest.raises(ValueError, match=f"{named}" + r".*\['id'\]") as e:
            _listen(lake, [_schema(), _batch([url], fmt)])
        assert f"stream 't', BATCH file {url}" in str(e.value)

    def test_a_parquet_file_without_a_column(
        self, lake: Lake, tmp_path: Path, warnings: pytest.LogCaptureFixture
    ):
        """Loads NULL, warned of once, naming the file and the columns."""
        url = _parquet(tmp_path / "a.parquet", pa.table({"id": [1], "name": ["a"]}))
        _listen(lake, [_schema(), _batch([url], "parquet")])
        assert _rows(lake, columns="id, name, amount") == [(1, "a", None)]
        assert f"stream 't', BATCH file {url}: the file has no column for " in (
            warnings.text
        )
        assert "'amount'" in warnings.text

    def test_duckdbs_hints_are_left_out(self, lake: Lake, tmp_path: Path):
        """read_json's suggestions name options the target sets itself."""
        (tmp_path / "a.jsonl").write_text("[1, 2]\n")
        url = (tmp_path / "a.jsonl").as_uri()
        with pytest.raises(ValueError, match="stream 't': loading BATCH file") as e:
            _listen(lake, [_schema(), _batch([url], "jsonl")])
        assert "auto_detect" not in str(e.value)

    def test_a_value_the_cast_refuses(self, lake: Lake, tmp_path: Path):
        """The stream, the file, the column and the value; nothing is loaded."""
        url = _jsonl(tmp_path / "a.jsonl", [RECORDS[0], {"id": 2, "day": "someday"}])
        _listen(lake, [_schema(), *_records(RECORDS[2:3])])
        with pytest.raises(
            ValueError, match="value 'someday' can't be loaded as DATE"
        ) as e:
            _listen(lake, [_schema(), _batch([url], "jsonl")])
        assert f"stream 't', BATCH file {url}, row 2, column 'day'" in str(e.value)
        assert _rows(lake, columns="id") == [(3,)]

    def test_in_a_parquet_file(self, lake: Lake, tmp_path: Path):
        """Text a Parquet column holds, cast as JSONL's is."""
        url = _parquet(tmp_path / "a.parquet", pa.table({"id": [1], "ts": ["soon"]}))
        with pytest.raises(ValueError, match="column 'ts': value 'soon' can't be"):
            _listen(lake, [_schema(), _batch([url], "parquet")])

    def test_in_a_nested_field(self, lake: Lake, tmp_path: Path):
        """A STRUCT's field, by its path."""
        url = _jsonl(tmp_path / "a.jsonl", [{"id": 1, "obj": {"a": "x"}}])
        with pytest.raises(ValueError, match="column 'obj.a': value 'x'"):
            _listen(lake, [_schema(), _batch([url], "jsonl")])

    def test_a_value_of_the_wrong_shape(self, lake: Lake, tmp_path: Path):
        """A string where an object or an array is declared."""
        url = _jsonl(tmp_path / "a.jsonl", [{"id": 1, "tags": "t"}])
        with pytest.raises(
            ValueError, match="column 'tags': value '\"t\"' can't be loaded as an array"
        ):
            _listen(lake, [_schema(), _batch([url], "jsonl")])

    def test_text_that_isnt_base64(self, lake: Lake, tmp_path: Path):
        """A BLOB column's text."""
        url = _jsonl(tmp_path / "a.jsonl", [{"id": 1, "blob": "!!"}])
        with pytest.raises(
            ValueError, match="column 'blob': value '!!' can't be loaded as base64"
        ):
            _listen(lake, [_schema(), _batch([url], "jsonl")])

    def test_a_long_value_is_cut(self, lake: Lake, tmp_path: Path):
        """To 40 characters."""
        url = _jsonl(tmp_path / "a.jsonl", [{"id": "x" * 100}])
        with pytest.raises(ValueError, match="value '" + "x" * 40 + "' can't"):
            _listen(lake, [_schema(), _batch([url], "jsonl")])

    def test_a_parquet_value_of_another_type(self, lake: Lake, tmp_path: Path):
        """A date for an integer column, which DuckDB doesn't cast."""
        table = pa.table({"id": [dt.date(2024, 1, 1)]})
        url = _parquet(tmp_path / "a.parquet", table)
        with pytest.raises(
            ValueError, match="column 'id': value '2024-01-01' can't be loaded as"
        ):
            _listen(lake, [_schema(), _batch([url], "parquet")])

    def test_a_missing_file(self, lake: Lake, tmp_path: Path):
        """Named, with the stream."""
        url = (tmp_path / "missing.jsonl").as_uri()
        with pytest.raises(ValueError, match="stream 't': loading BATCH file"):
            _listen(lake, [_schema(), _batch([url], "jsonl")])


class TestChanged:
    """Values a cast changes: warned of, or refused under strict."""

    def test_a_rounded_number_is_warned_of(
        self, lake: Lake, tmp_path: Path, warnings: pytest.LogCaptureFixture
    ):
        """Counted, with a sample, and loaded rounded."""
        url = _jsonl(tmp_path / "a.jsonl", [{"id": 1, "price": 1.234}, {"id": 2}])
        _listen(lake, [_schema(), _batch([url], "jsonl")])
        assert _rows(lake, columns="id, price") == [
            (1, Decimal("1.23")),
            (2, None),
        ]
        assert (
            "column 'price' is DECIMAL(10,2), and these values were rounded to fit "
            "it (1 value(s)): '1.234'"
        ) in warnings.text

    def test_in_a_wide_decimal_column(
        self, lake: Lake, tmp_path: Path, warnings: pytest.LogCaptureFixture
    ):
        """DECIMAL(38, 9): a value wider than 18 digits loads exactly too."""
        wide = "12345678901234567890.123456789"
        rows = [{"id": 1, "amount": "1.0000000005"}, {"id": 2, "amount": wide}]
        url = _jsonl(tmp_path / "a.jsonl", rows)
        _listen(lake, [_schema(), _batch([url], "jsonl")])
        assert _rows(lake, columns="amount::VARCHAR") == [
            ("1.000000001",),
            (wide,),
        ]
        assert "(1 value(s)): '1.0000000005'" in warnings.text

    @pytest.mark.parametrize("strict", [False, True], ids=["permissive", "strict"])
    def test_a_long_json_number(
        self,
        lake: Lake,
        tmp_path: Path,
        warnings: pytest.LogCaptureFixture,
        *,
        strict: bool,
    ):
        """DuckDB reads it as a double: past 15 significant digits, it may change.

        The same digits sent as text are exact.
        """
        line = (
            '{"id": 1, "amount": 12345678901234567.125}\n'
            '{"id": 2, "amount": "12345678901234567.125"}\n'
        )
        (tmp_path / "a.jsonl").write_text(line)
        url = (tmp_path / "a.jsonl").as_uri()
        config = {"value_type_checking": "strict"} if strict else {}
        if strict:
            with pytest.raises(ValueError, match="more than 15 significant digits"):
                _listen(lake, [_schema(), _batch([url], "jsonl")], **config)
            return
        _listen(lake, [_schema(), _batch([url], "jsonl")], **config)
        assert _rows(lake, columns="amount::VARCHAR")[1] == (
            "12345678901234567.125000000",
        )
        assert (
            "column 'amount' is DECIMAL(38,9), and these JSON numbers have more "
            "than 15 significant digits"
        ) in warnings.text

    def test_json_booleans_into_a_number_column(self, lake: Lake, tmp_path: Path):
        """A JSON true is 1, as DuckDB's cast, Parquet and a record make it."""
        properties = {"id": {"type": "integer"}, "n": {"type": ["null", "integer"]}}
        url = _jsonl(
            tmp_path / "a.jsonl", [{"id": 1, "n": True}, {"id": 2, "n": False}]
        )
        _listen(lake, [_schema(properties=properties), _batch([url], "jsonl")])
        assert _rows(lake, columns="id, n") == [(1, 1), (2, 0)]

    def test_a_time_of_day_for_a_date_column(
        self, lake: Lake, tmp_path: Path, warnings: pytest.LogCaptureFixture
    ):
        """Lost in DuckDB's cast, as InsertCast says: warned of, refused if strict."""
        url = _jsonl(
            tmp_path / "a.jsonl", [{"id": 1, "day": "2024-01-01T23:00:00-05:00"}]
        )
        with pytest.raises(ValueError, match="lose their time of day"):
            _listen(
                lake,
                [_schema(), _batch([url], "jsonl")],
                value_type_checking="strict",
            )
        _listen(lake, [_schema(), _batch([url], "jsonl")])
        assert _rows(lake, columns="day::VARCHAR") == [("2024-01-01",)]
        assert "column 'day' is DATE, and these values lose their time of day" in (
            warnings.text
        )

    def test_a_rounded_key_says_rows_merge(
        self, lake: Lake, tmp_path: Path, warnings: pytest.LogCaptureFixture
    ):
        """1.5 and 2 are one key in a BIGINT column: the later row is kept."""
        table = pa.table({"id": [1.5, 2.0], "name": ["a", "b"]})
        url = _parquet(tmp_path / "a.parquet", table)
        _listen(lake, [_schema(), _batch([url], "parquet")])
        assert _rows(lake, columns="id, name") == [(2, "b")]
        assert "in a key column, so rows whose keys now match are merged" in (
            warnings.text
        )

    def test_strict_refuses_them(self, lake: Lake, tmp_path: Path):
        """Before anything is staged."""
        url = _jsonl(tmp_path / "a.jsonl", [{"id": 1, "price": "1.234"}])
        with pytest.raises(ValueError, match="value_type_checking is strict"):
            _listen(
                lake,
                [_schema(), _batch([url], "jsonl")],
                value_type_checking="strict",
            )
        assert _rows(lake, columns="id") == []

    @pytest.mark.parametrize("fmt", ["jsonl", "parquet"])
    def test_an_undeclared_key_is_dropped_and_warned_of(
        self, lake: Lake, tmp_path: Path, fmt: str, warnings: pytest.LogCaptureFixture
    ):
        """Its values are counted, by the key's path."""
        rows = [{"id": 1, "obj": {"a": 1, "b": "x", "z": 9}}]
        if fmt == "jsonl":
            url = _jsonl(tmp_path / "a.jsonl", rows)
        else:
            url = _parquet(tmp_path / "a.parquet", pa.Table.from_pylist(rows))
        _listen(lake, [_schema(), _batch([url], fmt)])
        assert _rows(lake, columns="id, obj") == [(1, {"a": 1, "b": "x"})]
        assert (
            "column 'obj' is a STRUCT without these fields of the file's objects, "
            "whose values were dropped (1 value(s)): 'obj.z'"
        ) in warnings.text

    def test_strict_refuses_an_undeclared_key(self, lake: Lake, tmp_path: Path):
        """In a list's objects too."""
        properties = {
            "id": {"type": "integer"},
            "items": {
                "type": "array",
                "items": {"type": "object", "properties": {"a": {"type": "integer"}}},
            },
        }
        url = _jsonl(tmp_path / "a.jsonl", [{"id": 1, "items": [{"a": 1, "z": 2}]}])
        with pytest.raises(ValueError, match=r"'items\[\]' is a STRUCT without"):
            _listen(
                lake,
                [_schema(properties=properties), _batch([url], "jsonl")],
                value_type_checking="strict",
            )


class TestParquetTypes:
    """Parquet's own types, cast by DuckDB."""

    @pytest.mark.parametrize("declared", ["uuid", "string"])
    def test_a_uuid_column(self, lake: Lake, tmp_path: Path, declared: str):
        """DUCKLKD-152: Arrow's uuid type, a UUID column or its canonical text."""
        value = uuid.UUID("123E4567-E89B-12D3-A456-426614174000")
        prop: dict[str, Any] = {"type": ["null", "string"]}
        if declared == "uuid":
            prop["format"] = "uuid"
        properties = {"id": {"type": "integer"}, "u": prop}
        table = pa.table({"id": [1, 2], "u": pa.array([value.bytes, None], pa.uuid())})
        url = _parquet(tmp_path / "a.parquet", table)
        _listen(lake, [_schema(properties=properties), _batch([url], "parquet")])
        assert _rows(lake, columns="id, u::VARCHAR, typeof(u)") == [
            (1, str(value), "UUID" if declared == "uuid" else "VARCHAR"),
            (2, None, "UUID" if declared == "uuid" else "VARCHAR"),
        ]

    @pytest.mark.parametrize(
        "prop",
        [
            {
                "type": ["null", "string"],
                "format": "x-singer.decimal",
                "precision": 6,
                "scale": 2,
            },
            {"type": ["null", "number"], "x-sql-datatype": "decimal(6,2)"},
        ],
        ids=["singer-decimal", "number"],
    )
    def test_a_decimal_column_with_validate_records(
        self, lake: Lake, tmp_path: Path, prop: dict
    ):
        """DUCKLKD-153: unchanged in its DECIMAL column, with validation on."""
        properties = {"id": {"type": "integer"}, "d": prop}
        values = [Decimal("1234.50"), Decimal("-0.01"), None]
        table = pa.table({"id": [1, 2, 3], "d": pa.array(values, pa.decimal128(6, 2))})
        url = _parquet(tmp_path / "a.parquet", table)
        _listen(
            lake,
            [_schema(properties=properties), _batch([url], "parquet")],
            validate_records=True,
            value_type_checking="strict",
        )
        assert _rows(lake, columns="d, typeof(d)") == [
            (value, "DECIMAL(6,2)") for value in values
        ]

    def test_a_float_is_its_shortest_text(self, lake: Lake, tmp_path: Path):
        """A float32 0.1 is 0.1 in a DECIMAL column, as JSON writes it."""
        table = pa.table({"id": [1], "amount": pa.array([0.1], pa.float32())})
        url = _parquet(tmp_path / "a.parquet", table)
        _listen(
            lake,
            [_schema(), _batch([url], "parquet")],
            value_type_checking="strict",
        )
        assert _rows(lake, columns="amount") == [(Decimal("0.100000000"),)]

    @pytest.mark.parametrize(
        ("tz", "expected"),
        [
            (None, "2024-01-01 00:00:00.123456+00"),
            ("UTC", "2024-01-01 00:00:00.123456+00"),
            ("Europe/Paris", "2024-01-01 00:00:00.123456+00"),
        ],
        ids=["naive", "utc", "zone"],
    )
    def test_nanosecond_timestamps(
        self, lake: Lake, tmp_path: Path, tz: str | None, expected: str
    ):
        """Cut to microseconds; a timestamp without a time zone is UTC."""
        nanos = 1_704_067_200_123_456_789  # 2024-01-01T00:00:00.123456789Z
        table = pa.table(
            {"id": [1, 2], "ts": pa.array([nanos, None], pa.timestamp("ns", tz=tz))}
        )
        url = _parquet(tmp_path / "a.parquet", table)
        _listen(lake, [_schema(), _batch([url], "parquet")])
        assert _rows(lake, columns="ts::VARCHAR") == [(expected,), (None,)]

    def test_a_map_into_a_json_column(self, lake: Lake, tmp_path: Path):
        """As an object, an empty map as {}."""
        properties = {"id": {"type": "integer"}, "m": {"type": ["null", "object"]}}
        typ = pa.map_(pa.string(), pa.int64())
        table = pa.table({"id": [1, 2, 3], "m": pa.array([[("a", 1)], [], None], typ)})
        url = _parquet(tmp_path / "a.parquet", table)
        _listen(lake, [_schema(properties=properties), _batch([url], "parquet")])
        assert _rows(lake, columns="m::VARCHAR") == [('{"a":1}',), ("{}",), (None,)]

    def test_bytes_into_a_text_column(self, lake: Lake, tmp_path: Path):
        """As base64 text, the JSON form of binary."""
        properties = {"id": {"type": "integer"}, "b": {"type": ["null", "string"]}}
        table = pa.table({"id": [1], "b": pa.array([b"\x00\x01"], pa.binary())})
        url = _parquet(tmp_path / "a.parquet", table)
        _listen(lake, [_schema(properties=properties), _batch([url], "parquet")])
        assert _rows(lake, columns="b") == [("AAE=",)]


class TestJsonText:
    """JSON text where an object or array is declared (DUCKLKD-123)."""

    def test_into_struct_list_and_json_columns(self, lake: Lake, tmp_path: Path):
        """Loaded as the object or array the text encodes."""
        properties = {
            **PROPERTIES,
            "doc": {"type": ["null", "object"]},
        }
        row = {
            "id": 1,
            "obj": json.dumps({"a": 1, "b": "x"}),
            "tags": json.dumps(["t"]),
            "doc": json.dumps({"k": [1]}),
        }
        url = _jsonl(tmp_path / "a.jsonl", [row])
        _listen(lake, [_schema(properties=properties), _batch([url], "jsonl")])
        assert _rows(lake, columns="obj, tags, doc::VARCHAR") == [
            ({"a": 1, "b": "x"}, ["t"], '{"k":[1]}')
        ]

    def test_flattened(self, lake: Lake, tmp_path: Path):
        """An object sent as its text still flattens into columns."""
        url = _jsonl(tmp_path / "a.jsonl", [{"id": 1, "obj": '{"a": 1, "b": "x"}'}])
        _listen(lake, [_schema(), _batch([url], "jsonl")], flatten_max_level=1)
        assert lake.query("SELECT obj__a, obj__b FROM lake.s.t") == [(1, "x")]


class TestAbout:
    """The target declares the capability, and batch_config with it."""

    def test_batch_capability(self):
        """--about lists batch and the SDK's batch_config setting."""
        assert "batch" in [str(c) for c in Targetducklake.capabilities]
        assert "batch_config" in Targetducklake.config_jsonschema["properties"]
