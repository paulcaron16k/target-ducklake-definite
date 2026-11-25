"""The same rows, as RECORD messages and as Parquet and JSONL BATCH files (DUCKLKD-119).

BATCH files are staged by DuckDB, never as records in Python. Each test loads
the same rows each way, into a fresh table, and compares the tables: their
column types, and their rows with every value as text (JSON values
normalised, as DuckDB writes JSON without the RECORD path's spaces).
"""

from __future__ import annotations

import io
import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from target_ducklake.target import Targetducklake

if TYPE_CHECKING:
    from pathlib import Path

    from tests.catalogs import Lake

WAYS = ("records", "parquet", "jsonl")
# Metadata the target sets when it receives a record: different every load.
VOLATILE = (
    "_sdc_received_at",
    "_sdc_batched_at",
    "_sdc_sequence",
    "_sdc_sync_started_at",
)
PROPERTIES: dict[str, Any] = {
    "id": {"type": "integer"},
    "name": {"type": ["null", "string"]},
    "amount": {"type": ["null", "number"]},
    "ts": {"type": ["null", "string"], "format": "date-time"},
    "day": {"type": ["null", "string"], "format": "date"},
    "price": {
        "type": ["null", "string"],
        "format": "x-singer.decimal",
        "precision": 12,
        "scale": 2,
    },
    "obj": {
        "type": ["null", "object"],
        "properties": {
            "a": {"type": ["null", "integer"]},
            "at": {"type": ["null", "string"], "format": "date-time"},
        },
    },
    "tags": {"type": ["null", "array"], "items": {"type": "string"}},
    "doc": {"type": ["null", "object"]},
}


def _row(i: int, **changes: object) -> dict[str, Any]:
    return {
        "id": i,
        "name": f"n{i}",
        "amount": i + 0.25,
        "ts": f"2024-01-0{i % 9 + 1}T10:00:00+02:00",
        "day": f"2024-02-0{i % 9 + 1}",
        "price": f"{i}.50",
        "obj": {"a": i, "at": f"2024-03-0{i % 9 + 1}T08:30:00Z"},
        "tags": ["t", str(i)],
        "doc": {"k": i, "list": ["a", "x"]},
        **changes,
    }


ROWS = [_row(i) for i in range(1, 6)]


@dataclass
class Load:
    """Rows sent together: a BATCH message's file, or one RECORD message each."""

    rows: list[dict[str, Any]]
    version: int | None = None


# What a test sends: loads of rows, and messages sent as they are.
Script = list[Load | dict[str, Any]]


def _schema(properties: dict, keys: list[str] | None = None, stream: str = "t") -> dict:
    return {
        "type": "SCHEMA",
        "stream": stream,
        "schema": {"type": "object", "properties": properties},
        "key_properties": ["id"] if keys is None else keys,
    }


def _messages(
    way: str, tmp_path: Path, script: Script, stream: str = "t"
) -> list[dict]:
    """The script's loads sent ``way``; its other items are messages as they are."""
    messages: list[dict] = []
    for index, item in enumerate(script):
        if isinstance(item, dict):
            messages.append(item)
            continue
        version = {} if item.version is None else {"version": item.version}
        if way == "records":
            messages += [
                {"type": "RECORD", "stream": stream, "record": row, **version}
                for row in item.rows
            ]
            continue
        path = tmp_path / f"{stream}-{way}-{index}.{way}"
        if way == "jsonl":
            path.write_text("".join(json.dumps(row) + "\n" for row in item.rows))
        else:
            pq.write_table(_table_of(item.rows), path)
        messages.append(
            {
                "type": "BATCH",
                "stream": stream,
                "encoding": {"format": way, "compression": "none"},
                "manifest": [path.as_uri()],
                **version,
            }
        )
    return messages


def _table_of(rows: list[dict[str, Any]]) -> pa.Table:
    """The rows as a singer-sdk tap writes them to Parquet, every property in each.

    ``from_pylist`` takes its columns from the first row alone.
    """
    names = list(dict.fromkeys(name for row in rows for name in row))
    return pa.Table.from_pylist(
        [{name: row.get(name) for name in names} for row in rows]
    )


def _listen(lake: Lake, messages: list[dict], **config: object) -> None:
    target = Targetducklake(config={**lake.config(), **config})
    try:
        target.listen(
            file_input=io.StringIO("".join(json.dumps(m) + "\n" for m in messages))
        )
    finally:
        target.target_connector.close()


def _table(
    lake: Lake, table: str, presence: tuple[str, ...] = ()
) -> tuple[list[tuple], list[tuple]]:
    """The table's column types, and its rows as text, sorted.

    A nested or JSON value is its JSON, written by DuckDB; a ``presence``
    column is whether it is set, for a value the target takes from the clock.
    """
    described = lake.query(f"DESCRIBE lake.s.{table}")
    types = [(name, kind) for name, kind, *_ in described if name not in VOLATILE]
    selected = []
    for name, kind in types:
        column = f'"{name}"'
        if name in presence:
            selected.append(f"{column} IS NOT NULL")
        elif kind == "JSON" or kind.startswith("STRUCT") or kind.endswith("]"):
            selected.append(f"CAST(to_json({column}) AS VARCHAR)")
        else:
            selected.append(f"CAST({column} AS VARCHAR)")
    rows = lake.query(
        f"SELECT {', '.join(selected)} FROM lake.s.{table} ORDER BY ALL"  # noqa: S608
    )
    return types, rows


def _each_way(  # noqa: PLR0913 (the script, then the setup and options)
    lake: Lake,
    tmp_path: Path,
    script: Script,
    *,
    table: str = "t",
    before: list[dict] | None = None,
    presence: tuple[str, ...] = (),
    **config: object,
) -> dict[str, tuple[list[tuple], list[tuple]]]:
    """The table the script leaves, each way; ``before`` runs first, as RECORDs."""
    tables = {}
    for way in WAYS:
        lake.query(f"DROP TABLE IF EXISTS lake.s.{table}")
        if before:
            _listen(lake, before, **config)
        _listen(lake, _messages(way, tmp_path, script), **config)
        tables[way] = _table(lake, table, presence)
    return tables


def _assert_same(tables: dict[str, tuple[list[tuple], list[tuple]]]) -> None:
    for way in WAYS[1:]:
        assert tables[way] == tables["records"], way
    assert tables["records"][1], "the table has rows"


class TestLoadMethods:
    """Append, merge and overwrite, over rows a run loaded before."""

    @pytest.mark.parametrize("load_method", ["append", "merge", "overwrite"])
    def test_load_method(self, lake: Lake, tmp_path: Path, load_method: str):
        """The later file's row of a key replaces the earlier one, where keyed.

        A batch of records holds 3, as each file does: within a batch, each
        key's last row is kept, whatever the load method.
        """
        before = [_schema(PROPERTIES), *_messages("records", tmp_path, [Load(ROWS)])]
        changed = [_row(2, name="changed"), _row(6), _row(2, name="last")]
        script: Script = [_schema(PROPERTIES), Load(ROWS[:3]), Load(changed)]
        tables = _each_way(
            lake,
            tmp_path,
            script,
            before=before,
            load_method=load_method,
            batch_size_rows=3,
        )
        _assert_same(tables)

    def test_append_without_keys(self, lake: Lake, tmp_path: Path):
        """Every row is kept, duplicates too."""
        script: Script = [_schema(PROPERTIES, keys=[]), Load([*ROWS, ROWS[0]])]
        _assert_same(_each_way(lake, tmp_path, script))


class TestVersionsAndDeletes:
    """ACTIVATE_VERSION, and a CDC tap's _sdc_deleted_at."""

    @pytest.mark.parametrize("hard_delete", [False, True], ids=["soft", "hard"])
    def test_activate_version(self, lake: Lake, tmp_path: Path, *, hard_delete: bool):
        """Rows of other versions are retired, the message's version on its rows."""
        script: Script = [
            _schema(PROPERTIES),
            Load(ROWS, version=1),
            {"type": "ACTIVATE_VERSION", "stream": "t", "version": 1},
            Load(ROWS[1:3], version=2),
            {"type": "ACTIVATE_VERSION", "stream": "t", "version": 2},
        ]
        tables = _each_way(
            lake,
            tmp_path,
            script,
            presence=("_sdc_deleted_at",),
            add_record_metadata=True,
            hard_delete=hard_delete,
        )
        _assert_same(tables)

    @pytest.mark.parametrize("load_method", ["append", "merge", "overwrite"])
    def test_deleted_at_deletes(self, lake: Lake, tmp_path: Path, load_method: str):
        """A row marked deleted deletes its key's row, earlier in the run or not."""
        properties = {
            **PROPERTIES,
            "_sdc_deleted_at": {"type": ["null", "string"], "format": "date-time"},
        }
        gone = {**_row(2), "_sdc_deleted_at": "2024-05-01T00:00:00Z"}
        before = [_schema(properties), *_messages("records", tmp_path, [Load(ROWS)])]
        script: Script = [_schema(properties), Load([ROWS[0], gone, _row(7)])]
        tables = _each_way(
            lake,
            tmp_path,
            script,
            before=before,
            load_method=load_method,
            delete_rows_with_deleted_at=True,
        )
        _assert_same(tables)


def _nested(depth: int) -> dict[str, Any]:
    """An object holding a number and the object one level down."""
    if not depth:
        return {"type": ["null", "integer"]}
    return {
        "type": ["null", "object"],
        "properties": {"v": {"type": ["null", "integer"]}, "n": _nested(depth - 1)},
    }


def _nested_value(depth: int) -> object:
    return {"v": depth, "n": _nested_value(depth - 1)} if depth else 7


class TestShapes:
    """Flattened objects, native STRUCT and LIST columns, and JSON columns."""

    def test_deeply_nested_objects(self, lake: Lake, tmp_path: Path):
        """Ten levels of STRUCTs, and lists of them four deep, load each way.

        A JSONL object's conversion is written once per level, so the SQL
        grows with the depth (it grew as a power of it, and depth 6 never
        finished planning).
        """
        lists: dict[str, Any] = {"type": ["null", "integer"]}
        listed: object = 1
        for _ in range(4):
            lists = {
                "type": ["null", "array"],
                "items": {"type": "object", "properties": {"n": lists}},
            }
            listed = [{"n": listed}]
        properties = {"id": {"type": "integer"}, "o": _nested(10), "l": lists}
        rows: list[dict[str, Any]] = [
            {"id": i, "o": _nested_value(10), "l": listed} for i in range(3)
        ]
        _assert_same(_each_way(lake, tmp_path, [_schema(properties), Load(rows)]))

    @pytest.mark.parametrize("nested_types", ["native", "json"])
    def test_nested_types(self, lake: Lake, tmp_path: Path, nested_types: str):
        """STRUCT, LIST and JSON columns, and NULLs and empty lists in them."""
        rows: list[dict[str, Any]] = [
            *ROWS,
            _row(8, obj=None, tags=[], doc=None),
            _row(9, obj={"a": None, "at": None}, tags=None),
        ]
        script: Script = [_schema(PROPERTIES), Load(rows)]
        _assert_same(_each_way(lake, tmp_path, script, nested_types=nested_types))

    def test_flattening(self, lake: Lake, tmp_path: Path):
        """flatten_max_level splits objects into columns, at any depth.

        ``doc``, an object with no declared properties, loads whole as JSON
        each way (DUCKLKD-155): the RECORD path split its values into
        columns the table didn't have, and loaded it NULL.
        """
        properties = {
            **PROPERTIES,
            "deep": {
                "type": ["null", "object"],
                "properties": {
                    "inner": {
                        "type": ["null", "object"],
                        "properties": {"x": {"type": ["null", "integer"]}},
                    },
                    "y": {"type": ["null", "string"]},
                },
            },
        }
        rows: list[dict[str, Any]] = [
            _row(i, deep={"inner": {"x": i}, "y": f"y{i}"}) for i in range(1, 4)
        ] + [_row(4, deep=None), _row(5, deep={"inner": None, "y": "z"})]
        script: Script = [_schema(properties), Load(rows)]
        tables = _each_way(lake, tmp_path, script, flatten_max_level=2)
        _assert_same(tables)
        names = [name for name, _ in tables["records"][0]]
        assert {"deep__inner__x", "deep__y", "obj__a", "doc"} <= set(names)

    def test_untyped_object_loads_whole(self, lake: Lake, tmp_path: Path):
        """An object without declared properties is one JSON column (DUCKLKD-155).

        At any flatten_max_level, as in a BATCH file: its keys aren't split
        into columns of their own.
        """
        properties = {"id": {"type": "integer"}, "o": {"type": ["null", "object"]}}
        rows: list[dict[str, Any]] = [{"id": 1, "o": {"a": 1}}, {"id": 2, "o": None}]
        script: Script = [_schema(properties), Load(rows)]
        tables = _each_way(lake, tmp_path, script, flatten_max_level=1)
        _assert_same(tables)
        assert tables["records"] == (
            [("id", "BIGINT"), ("o", "JSON")],
            [("1", '{"a":1}'), ("2", None)],
        )


class TestNames:
    """Conformed and truncated column names."""

    def test_conform_column_names(self, lake: Lake, tmp_path: Path):
        """Names conformed alike, keys too."""
        properties = {
            "Order ID": {"type": "integer"},
            "Customer Name": {"type": ["null", "string"]},
            "Obj": {
                "type": ["null", "object"],
                "properties": {"Inner Value": {"type": ["null", "integer"]}},
            },
        }
        rows: list[dict[str, Any]] = [
            {"Order ID": i, "Customer Name": f"c{i}", "Obj": {"Inner Value": i}}
            for i in range(1, 4)
        ]
        script: Script = [
            _schema(properties, keys=["Order ID"]),
            Load([*rows, rows[0]]),
        ]
        tables = _each_way(
            lake, tmp_path, script, conform_column_names=True, flatten_max_level=1
        )
        _assert_same(tables)
        assert [name for name, _ in tables["records"][0]] == [
            "customer_name",
            "obj__inner_value",
            "order_id",
        ]

    def test_truncated_names(self, lake: Lake, tmp_path: Path):
        """Names past 63 characters are cut on a Postgres catalog, keys too."""
        long_key, long_other = "k" * 70, "v" * 64 + "_one"
        properties = {
            long_key: {"type": "integer"},
            long_other: {"type": ["null", "string"]},
            "v" * 64 + "_two": {"type": ["null", "string"]},
        }
        rows: list[dict[str, Any]] = [
            {long_key: i, long_other: f"a{i}", "v" * 64 + "_two": f"b{i}"}
            for i in range(1, 4)
        ]
        script: Script = [_schema(properties, keys=[long_key]), Load([*rows, rows[1]])]
        tables = _each_way(lake, tmp_path, script)
        _assert_same(tables)
        if lake.kind == "postgres":
            names = [name for name, _ in tables["records"][0]]
            assert all(len(name) <= 63 for name in names)  # noqa: PLR2004


class TestExistingColumns:
    """A property whose column the table already has as another type (DUCKLKD-137)."""

    @pytest.mark.parametrize("load_method", ["append", "merge", "overwrite"])
    def test_text_into_a_number_column(
        self, lake: Lake, tmp_path: Path, load_method: str
    ):
        """Text of the column's type loads, cast by the INSERT, keys too."""
        earlier = {"id": {"type": "integer"}, "n": {"type": ["null", "integer"]}}
        before = [
            _schema(earlier),
            *_messages("records", tmp_path, [Load([{"id": 1, "n": 5}])]),
        ]
        now = {"id": {"type": "string"}, "n": {"type": ["null", "string"]}}
        rows: list[dict[str, Any]] = [
            {"id": "1", "n": "7"},
            {"id": "2", "n": "8"},
            {"id": "3", "n": None},
        ]
        script: Script = [_schema(now), Load(rows)]
        tables = _each_way(
            lake, tmp_path, script, before=before, load_method=load_method
        )
        _assert_same(tables)
        assert tables["records"][0] == [("id", "BIGINT"), ("n", "BIGINT")]

    @pytest.mark.parametrize(
        ("earlier", "now", "values"),
        [
            (
                {"format": "date"},
                {"format": "date-time"},
                ["2024-01-01T23:00:00-05:00", "2024-01-01T10:00:00", None],
            ),
            (
                {"format": "time"},
                {"format": "date-time"},
                ["2024-01-01T23:00:00-05:00", "2024-01-01T10:00:00Z", None],
            ),
            ({"type": ["null", "boolean"]}, {}, ["true", "false", None]),
            (
                {"format": "uuid"},
                {},
                ["123E4567-E89B-12D3-A456-426614174000", None],
            ),
        ],
        ids=["instant-into-date", "instant-into-time", "boolean", "uuid"],
    )
    def test_text_into_a_typed_column(
        self, lake: Lake, tmp_path: Path, earlier: dict, now: dict, values: list
    ):
        """An instant is staged as UTC text for a DATE or TIME column, as for RECORD.

        DuckDB's cast of text to a DATE or TIME ignores its offset, so
        ``23:00-05:00`` is the next day's 04:00 only once it is UTC text.
        """
        string = {"type": ["null", "string"]}
        before_schema = {"id": {"type": "integer"}, "c": {**string, **earlier}}
        before = [
            _schema(before_schema),
            *_messages("records", tmp_path, [Load([{"id": 0, "c": None}])]),
        ]
        rows: list[dict[str, Any]] = [
            {"id": i, "c": value} for i, value in enumerate(values, 1)
        ]
        later = {"id": {"type": "integer"}, "c": {**string, **now}}
        script: Script = [_schema(later), Load(rows)]
        _assert_same(_each_way(lake, tmp_path, script, before=before))

    def test_an_instant_into_a_nested_date_field(self, lake: Lake, tmp_path: Path):
        """A STRUCT leaf the table has as DATE, as UTC text too."""

        def schema(fmt: str) -> dict:
            leaf = {"type": ["null", "string"], "format": fmt}
            return {
                "id": {"type": "integer"},
                "o": {"type": ["null", "object"], "properties": {"at": leaf}},
            }

        before = [
            _schema(schema("date")),
            *_messages("records", tmp_path, [Load([{"id": 0, "o": {"at": None}}])]),
        ]
        rows: list[dict[str, Any]] = [
            {"id": 1, "o": {"at": "2024-01-01T23:00:00-05:00"}},
            {"id": 2, "o": {"at": "2024-01-01T10:00:00"}},
        ]
        script: Script = [_schema(schema("date-time")), Load(rows)]
        tables = _each_way(lake, tmp_path, script, before=before)
        _assert_same(tables)
        assert ("2", '{"at":"2024-01-01"}') in [
            (row[0], row[1]) for row in tables["records"][1]
        ]

    def test_a_value_the_cast_refuses(self, lake: Lake, tmp_path: Path):
        """Fails the batch each way, the table unchanged."""
        earlier = {"id": {"type": "integer"}, "n": {"type": ["null", "integer"]}}
        now = {"id": {"type": "integer"}, "n": {"type": ["null", "string"]}}
        for way in WAYS:
            lake.query("DROP TABLE IF EXISTS lake.s.t")
            _listen(
                lake,
                [
                    _schema(earlier),
                    *_messages("records", tmp_path, [Load([{"id": 1, "n": 5}])]),
                ],
            )
            script: Script = [_schema(now), Load([{"id": 2, "n": "seven"}])]
            with pytest.raises(ValueError, match="'n' is BIGINT, and 1 value"):
                _listen(lake, _messages(way, tmp_path, script))
            assert lake.query("SELECT id, n FROM lake.s.t") == [(1, 5)], way


class TestDateTimes:
    """Date-times with offsets, into either date_time_type."""

    @pytest.mark.parametrize("date_time_type", ["timestamptz", "timestamp"])
    def test_offsets(self, lake: Lake, tmp_path: Path, date_time_type: str):
        """With an offset, without one, and with Z."""
        properties = {
            "id": {"type": "integer"},
            "at": {"type": ["null", "string"], "format": "date-time"},
        }
        values = [
            "2024-01-01T23:00:00-05:00",
            "2024-06-01T10:00:00+05:30",
            "2024-01-01T10:00:00",
            "2024-01-01T10:00:00.123456Z",
            None,
        ]
        rows: list[dict[str, Any]] = [
            {"id": i, "at": value} for i, value in enumerate(values, 1)
        ]
        script: Script = [_schema(properties), Load(rows)]
        _assert_same(_each_way(lake, tmp_path, script, date_time_type=date_time_type))


class TestLayout:
    """Partitioning applies to the files a BATCH loads."""

    def test_partitioned(self, lake: Lake, tmp_path: Path):
        """The same rows, in the same partitions."""
        config = {"stream_overrides": {"t": {"partition": [{"column": "name"}]}}}
        script: Script = [_schema(PROPERTIES), Load(ROWS)]
        partitions = {}
        tables = {}
        for way in WAYS:
            lake.query("DROP TABLE IF EXISTS lake.s.t")
            _listen(lake, _messages(way, tmp_path, script), **config)
            tables[way] = _table(lake, "t")
            files = lake.query(
                "SELECT data_file FROM ducklake_list_files('lake', 't', schema => 's')"
            )
            partitions[way] = sorted(
                path.split("/")[-2] for (path,) in files if "name=" in path
            )
        _assert_same(tables)
        assert partitions["records"] == [f"name=n{i}" for i in range(1, 6)]
        assert partitions["parquet"] == partitions["jsonl"] == partitions["records"]
