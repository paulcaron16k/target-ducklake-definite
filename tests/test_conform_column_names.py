"""Column names conformed as the singer-sdk's SQL targets conform them (DUCKLKD-87)."""

from __future__ import annotations

import io
import json
import logging
from typing import TYPE_CHECKING, Any, cast

import pytest
from singer_sdk.exceptions import ConformedNameClashException
from singer_sdk.sinks import SQLSink

from target_ducklake import target as target_module
from target_ducklake.conform import conform_name, conform_names, is_lossy
from target_ducklake.connector import DuckLakeConnector
from target_ducklake.layout import (
    LayoutError,
    PartitionKey,
    SortKey,
    check_layout_config,
    resolve_conform_column_names,
)
from target_ducklake.target import Targetducklake
from target_ducklake.type_source import CoercionError

if TYPE_CHECKING:
    from collections.abc import Iterator

    from tests.catalogs import Lake

# Names a tap may send, each a way conforming can go: case, separators,
# surrounding and inner whitespace, dropped characters, leading digits,
# names already conformed, and the singer-sdk's own metadata columns.
CORPUS = [
    "id",
    "Id",
    "ID",
    "camelCase",
    "PascalCase",
    "snake_case",
    "UPPER_SNAKE",
    "first name",
    "First Name",
    "  padded  ",
    "tab\tinside",
    "a.b",
    "a.b.c",
    "kebab-case",
    "mixed.-_ sep",
    "a__b",
    "A__B",
    "_leading_underscore",
    "__dunder__",
    "_sdc_deleted_at",
    "_sdc_table_version",
    "2fa",
    "0zero",
    "9nine",
    "123",
    " 1padded",
    "a1",
    "café",
    "naïve",
    "größe",
    "price ($)",
    "50%",
    "#hash",
    "e-mail@domain",
    "日本語_name",
    "😀smile",
    "é1",
    "order",
    "select",
    "c024_revenue",
    "already_conformed",
]
# Names with nothing left to conform; the singer-sdk fails on them.
EMPTY = ["", "日本語", "$%&", "😀"]


def _sdk_conform_name(name: str) -> str:
    """The singer-sdk's ``SQLSink.conform_name``, which uses nothing of its sink."""
    return SQLSink.conform_name(cast("SQLSink", None), name)


class TestParityWithSingerSdk:
    """The rules are the singer-sdk's, which target-iceberg follows too."""

    @pytest.mark.parametrize("name", CORPUS)
    def test_conform_name_matches_the_sdk(self, name: str):
        """Byte for byte, name by name."""
        assert conform_name(name) == _sdk_conform_name(name)

    @pytest.mark.parametrize("name", CORPUS)
    def test_conformed_names_are_conformed(self, name: str):
        """Conforming twice changes nothing, so either form names a column."""
        assert conform_name(conform_name(name)) == conform_name(name)

    @pytest.mark.parametrize("name", EMPTY)
    def test_nothing_left_is_refused(self, name: str):
        """Where the singer-sdk fails with an IndexError, the stream is refused."""
        with pytest.raises(IndexError):
            _sdk_conform_name(name)
        assert conform_name(name) == ""
        with pytest.raises(ValueError, match="conform to an empty name"):
            conform_names(["id", name])

    def test_clash_is_refused_as_the_sdk_refuses_it(self):
        """Two names for one column raise the singer-sdk's exception."""
        with pytest.raises(ConformedNameClashException, match="'my_col'"):
            conform_names(["my.col", "MY-COL", "other"])

    def test_only_changed_names_are_mapped(self):
        """The mapping holds only the names that change."""
        assert conform_names(["id", "First Name", "_sdc_deleted_at"]) == {
            "First Name": "first_name"
        }

    @pytest.mark.parametrize(
        ("name", "lossy"),
        [
            ("First Name", False),
            ("a.b-c", False),
            ("CamelCase", False),
            ("2fa", True),
            (" 1padded", True),
            ("café", True),
            ("price ($)", True),
            ("already_conformed", False),
        ],
    )
    def test_lossy(self, name: str, *, lossy: bool):
        """Dropped characters and a replaced digit lose information; case doesn't."""
        assert is_lossy(name) is lossy


class TestSetting:
    """conform_column_names, at the top level or per stream."""

    @pytest.mark.parametrize(
        ("config", "expected"),
        [
            ({}, False),
            ({"conform_column_names": True}, True),
            (
                {
                    "conform_column_names": True,
                    "stream_defaults": {"conform_column_names": False},
                },
                False,
            ),
            (
                {
                    "stream_defaults": {"conform_column_names": False},
                    "stream_overrides": {"t": {"conform_column_names": True}},
                },
                True,
            ),
            ({"stream_overrides": {"u": {"conform_column_names": True}}}, False),
        ],
        ids=["default-off", "top-level", "defaults", "override", "other-stream"],
    )
    def test_precedence(self, config: dict, *, expected: bool):
        """Override, then stream_defaults, then the top level, then off."""
        assert resolve_conform_column_names(config, "t") is expected

    def test_not_a_boolean_is_refused_at_startup(self):
        """A per-stream value is checked when the target starts."""
        config: dict[str, Any] = {
            "stream_overrides": {"t": {"conform_column_names": "yes"}}
        }
        with pytest.raises(LayoutError, match="conform_column_names must be true"):
            check_layout_config(config)


PROPERTIES = {
    "Id": {"type": "integer"},
    "Order ID": {"type": "integer"},
    "First Name": {"type": ["null", "string"]},
    "e-mail": {"type": ["null", "string"]},
    "2fa": {"type": ["null", "boolean"]},
    "café": {"type": ["null", "string"]},
    "Created.At": {"type": ["null", "string"], "format": "date-time"},
}


def _run(
    lake: Lake,
    records: list[dict],
    *,
    properties: dict | None = None,
    keys: list[str] | None = None,
    **config: object,
) -> None:
    """Load ``records`` into s.t in one run."""
    messages: list[dict[str, Any]] = [
        {
            "type": "SCHEMA",
            "stream": "t",
            "schema": {"type": "object", "properties": properties or PROPERTIES},
            "key_properties": keys or [],
        },
        *({"type": "RECORD", "stream": "t", "record": r} for r in records),
    ]
    target = Targetducklake(config={**lake.config(), **config})
    try:
        target.listen(
            file_input=io.StringIO("".join(json.dumps(m) + "\n" for m in messages))
        )
    finally:
        target.target_connector.close()


def _columns(lake: Lake) -> list[str]:
    return [row[0] for row in lake.query("DESCRIBE lake.s.t")]


def _record(key: int, name: str) -> dict:
    return {
        "Id": key,
        "Order ID": key * 10,
        "First Name": name,
        "e-mail": f"{name}@example.com",
        "2fa": True,
        "café": "noir",
        "Created.At": "2026-11-06T10:00:00Z",
    }


@pytest.fixture
def warnings(caplog: pytest.LogCaptureFixture) -> Iterator[pytest.LogCaptureFixture]:
    """The target's warnings, caught on its own logger (see test_batch_size)."""
    logger = logging.getLogger("target-ducklake")
    logger.addHandler(caplog.handler)
    caplog.handler.setLevel(logging.WARNING)
    try:
        yield caplog
    finally:
        logger.removeHandler(caplog.handler)


class TestLoad:
    """Loading through the target, on a real catalog."""

    def test_off_by_default_keeps_the_names(self, lake: Lake):
        """Without the setting, columns are named as the tap names them."""
        _run(lake, [_record(1, "a")])
        assert sorted(_columns(lake)) == sorted(PROPERTIES)

    def test_columns_are_conformed(
        self, lake: Lake, warnings: pytest.LogCaptureFixture
    ):
        """Records land in the conformed columns; lossy renames are warned about."""
        _run(lake, [_record(1, "a")], conform_column_names=True)
        assert sorted(_columns(lake)) == [
            "caf",
            "cfa",
            "created_at",
            "e_mail",
            "first_name",
            "id",
            "order_id",
        ]
        assert lake.query(
            "SELECT id, order_id, first_name, e_mail, cfa, caf FROM lake.s.t"
        ) == [(1, 10, "a", "a@example.com", True, "noir")]
        (warning,) = [r.getMessage() for r in warnings.records]
        assert "'2fa' → 'cfa'" in warning
        assert "'café' → 'caf'" in warning
        assert "First Name" not in warning

    @pytest.mark.parametrize("conform", [False, True], ids=["off", "on"])
    def test_a_key_with_a_space_merges(self, lake: Lake, *, conform: bool):
        """The singer-sdk conformed keys, but not columns: 'Order ID' failed."""
        _run(lake, [_record(1, "a")], keys=["Order ID"], conform_column_names=conform)
        _run(
            lake,
            [_record(1, "b"), _record(2, "c")],
            keys=["Order ID"],
            conform_column_names=conform,
        )
        name = "first_name" if conform else '"First Name"'
        assert lake.query(f"SELECT {name} FROM lake.s.t ORDER BY 1") == [  # noqa: S608
            ("b",),
            ("c",),
        ]

    @pytest.mark.parametrize("key", ["Order ID", "order_id"])
    def test_configured_key_may_name_either_form(self, lake: Lake, key: str):
        """A key_properties setting names the tap's property or its column."""
        config: dict[str, Any] = {
            "conform_column_names": True,
            "stream_overrides": {"t": {"key_properties": [key]}},
        }
        _run(lake, [_record(1, "a")], **config)
        _run(lake, [_record(1, "b")], **config)
        assert lake.query("SELECT order_id, first_name FROM lake.s.t") == [(10, "b")]

    def test_configured_key_naming_a_property_twice_is_refused(self, lake: Lake):
        """Both forms of one name in key_properties are a mistake."""
        config: dict[str, Any] = {
            "conform_column_names": True,
            "stream_overrides": {"t": {"key_properties": ["Order ID", "order_id"]}},
        }
        with pytest.raises(LayoutError, match="name a property twice"):
            _run(lake, [_record(1, "a")], **config)

    def test_conformed_key_is_unknown_with_the_setting_off(self, lake: Lake):
        """Without the setting, there is no conformed name to give."""
        config: dict[str, Any] = {
            "stream_overrides": {"t": {"key_properties": ["order_id"]}}
        }
        with pytest.raises(LayoutError, match=r"\['order_id'\] are not properties"):
            _run(lake, [_record(1, "a")], **config)

    @pytest.mark.parametrize(
        ("partition", "sort"),
        [("Created.At", '"First Name" DESC'), ("created_at", "first_name DESC")],
        ids=["tap-names", "conformed-names"],
    )
    def test_partition_and_sort_name_either_form(
        self, lake: Lake, partition: str, sort: str
    ):
        """A shared config names layout columns as the tap or as the table does."""
        _run(
            lake,
            [_record(1, "a")],
            conform_column_names=True,
            stream_overrides={
                "t": {
                    "partition": [{"column": partition, "transform": "day"}],
                    "sort_order": [sort],
                }
            },
        )
        connector = DuckLakeConnector(config=lake.config())
        try:
            layout = connector.get_table_layout("s", "t")
        finally:
            connector.close()
        assert layout.partition == (PartitionKey("created_at", "day"),)
        assert layout.sort == (SortKey("first_name", "DESC", "NULLS_LAST"),)

    def test_clash_fails_the_stream(self, lake: Lake):
        """Two properties for one column fail the stream before anything loads."""
        properties = {"my.col": {"type": "integer"}, "MY-COL": {"type": "integer"}}
        with pytest.raises(ConformedNameClashException, match="'my_col'"):
            _run(
                lake,
                [{"my.col": 1, "MY-COL": 2}],
                properties=properties,
                conform_column_names=True,
            )

    def test_names_only_ascii_case_tells_apart_are_refused(self, lake: Lake):
        """DuckDB would fail creating the table, without naming the stream."""
        properties = {"Id": {"type": "integer"}, "id": {"type": "integer"}}
        with pytest.raises(ValueError, match="would share a column"):
            _run(lake, [{"Id": 1, "id": 2}], properties=properties)

    @pytest.mark.parametrize("keys", [[], ["id"]], ids=["keyless", "keyed"])
    def test_non_ascii_case_is_two_columns(self, lake: Lake, keys: list[str]):
        """DuckDB folds ASCII case only: 'É' and 'é' are two columns, as before."""
        properties = {
            "id": {"type": "integer"},
            "É": {"type": "integer"},
            "é": {"type": "string"},
        }
        rows = [{"id": 1, "É": 1, "é": "x"}, {"id": 1, "É": 2, "é": "y"}]
        _run(lake, rows, properties=properties, keys=keys)
        assert sorted(_columns(lake)) == ["id", "É", "é"]
        expected = [(1, 2, "y")] if keys else [(1, 1, "x"), (1, 2, "y")]
        assert lake.query('SELECT id, "É", "é" FROM lake.s.t ORDER BY ALL') == expected

    def test_flattened_names_are_conformed_but_struct_fields_are_not(self, lake: Lake):
        """As in target-iceberg: columns are conformed, a STRUCT's fields kept."""
        properties = {
            "Id": {"type": "integer"},
            "Home Address": {
                "type": ["null", "object"],
                "properties": {"Street Name": {"type": ["null", "string"]}},
            },
            "Billing": {
                "type": ["null", "object"],
                "properties": {"Zip-Code": {"type": ["null", "string"]}},
            },
        }
        record = {
            "Id": 1,
            "Home Address": {"Street Name": "Main"},
            "Billing": {"Zip-Code": "02139"},
        }
        _run(
            lake,
            [record],
            properties=properties,
            conform_column_names=True,
            stream_overrides={"t": {"key_properties": ["id"]}},
        )
        assert sorted(_columns(lake)) == ["billing", "home_address", "id"]
        assert lake.query(
            """SELECT home_address."Street Name", billing."Zip-Code" FROM lake.s.t"""
        ) == [("Main", "02139")]
        _run(
            lake,
            [record],
            properties=properties,
            conform_column_names=True,
            flatten_max_level=1,
            nested_types="json",
            stream_overrides={"t": {"key_properties": ["id"]}},
        )
        assert {"home_address__street_name", "billing__zip_code"} <= set(_columns(lake))

    def test_deleted_at_still_deletes(self, lake: Lake):
        """The singer-sdk's metadata names are already conformed."""
        properties = {
            **PROPERTIES,
            "_sdc_deleted_at": {"type": ["null", "string"], "format": "date-time"},
        }
        config: dict[str, Any] = {
            "conform_column_names": True,
            "delete_rows_with_deleted_at": True,
        }
        _run(
            lake,
            [_record(1, "a"), _record(2, "b")],
            properties=properties,
            keys=["Order ID"],
            **config,
        )
        _run(
            lake,
            [{**_record(1, "a"), "_sdc_deleted_at": "2026-11-06T11:00:00Z"}],
            properties=properties,
            keys=["Order ID"],
            **config,
        )
        assert lake.query("SELECT order_id FROM lake.s.t") == [(20,)]

    def test_turning_it_on_adds_the_renamed_columns(
        self, lake: Lake, warnings: pytest.LogCaptureFixture
    ):
        """What the README says happens to an existing table.

        A name that only changes case is the same DuckDB column, which keeps
        its spelling; any other renamed property becomes a new column, with a
        warning, and the old column is NULL in the rows loaded from then on.
        """
        properties = {
            "Id": {"type": "integer"},
            "First Name": {"type": ["null", "string"]},
        }
        _run(lake, [{"Id": 1, "First Name": "a"}], properties=properties, keys=["Id"])
        _run(
            lake,
            [{"Id": 1, "First Name": "b"}, {"Id": 2, "First Name": "c"}],
            properties=properties,
            keys=["Id"],
            conform_column_names=True,
        )
        assert sorted(_columns(lake)) == ["First Name", "Id", "first_name"]
        assert lake.query(
            'SELECT "Id", "First Name", first_name FROM lake.s.t ORDER BY 1'
        ) == [(1, None, "b"), (2, None, "c")]
        (warning,) = [r.getMessage() for r in warnings.records]
        assert "'First Name' → 'first_name'" in warning

    def test_turning_it_on_refuses_a_renamed_key(self, lake: Lake):
        """A merge on the new, empty key column would load every row again."""
        properties = {"Order ID": {"type": "integer"}, "v": {"type": "string"}}
        rows = [{"Order ID": 1, "v": "a"}, {"Order ID": 2, "v": "b"}]
        _run(lake, rows, properties=properties, keys=["Order ID"])
        with pytest.raises(ValueError, match="'Order ID' → 'order_id'"):
            _run(
                lake,
                [{"Order ID": 1, "v": "a2"}],
                properties=properties,
                keys=["Order ID"],
                conform_column_names=True,
            )
        assert sorted(_columns(lake)) == ["Order ID", "v"]
        assert lake.query('SELECT "Order ID", v FROM lake.s.t ORDER BY 1') == [
            (1, "a"),
            (2, "b"),
        ]

    @pytest.mark.parametrize(
        ("config", "message", "rename"),
        [
            (
                {
                    "stream_defaults": {
                        "partition": [{"column": "created_at", "transform": "month"}]
                    }
                },
                "column 'created_at' is not in this stream's columns",
                "'Created.At' → 'created_at'",
            ),
            (
                {"stream_defaults": {"sort_order": ["first_name"]}},
                "column 'first_name' is not in this stream's columns",
                "'First Name' → 'first_name'",
            ),
            (
                {
                    "partition_fields": {
                        "t": [{"column_name": "e_mail", "type": "identifier"}]
                    }
                },
                "column 'e_mail' is not in this stream's columns",
                "'e-mail' → 'e_mail'",
            ),
            (
                {"stream_overrides": {"t": {"key_properties": ["order_id"]}}},
                r"key_properties \['order_id'\] are not properties",
                "'Order ID' → 'order_id'",
            ),
        ],
        ids=["partition", "sort_order", "partition_fields", "key_properties"],
    )
    def test_conformed_name_with_the_setting_off_names_it(
        self, lake: Lake, config: dict, message: str, rename: str
    ):
        """A config written for target-iceberg is told which setting it needs."""
        with pytest.raises(LayoutError, match=message) as raised:
            _run(lake, [_record(1, "a")], **config)
        assert str(raised.value).endswith(
            "Set conform_column_names: true to name columns by their conformed "
            f"names ({rename})."
        )

    def test_not_found_lists_each_column_once(self, lake: Lake):
        """Aliases for conformed names aren't listed as columns of their own."""
        config: dict[str, Any] = {"stream_defaults": {"sort_order": ["missing"]}}
        with pytest.raises(LayoutError) as raised:
            _run(lake, [_record(1, "a")], conform_column_names=True, **config)
        columns = ["caf", "cfa", "created_at", "e_mail", "first_name", "id", "order_id"]
        assert f"not in this stream's columns {columns}." in str(raised.value)

    def test_long_key_is_truncated_and_merges(self, lake: Lake):
        """Postgres catalogs truncate names; a truncated key merged by its full name.

        The singer-sdk checks every record for its keys by the records' own
        names, which the truncated key was no longer.
        """
        key = "Customer Account Identifier " + "x" * 50
        properties = {key: {"type": "integer"}, "v": {"type": ["null", "string"]}}
        for value in ("a", "b"):
            _run(
                lake,
                [{key: 1, "v": value}],
                properties=properties,
                keys=[key],
                conform_column_names=True,
            )
        assert lake.query("SELECT * FROM lake.s.t") == [(1, "b")]
        expected = conform_name(key)
        if lake.kind == "postgres":
            expected = expected[:63]
        assert _columns(lake)[0] == expected

    def test_long_conformed_names_truncated_apart(self, lake: Lake):
        """Two conformed names that truncate alike get suffixes, and still merge."""
        stem = "Customer Account " + "x" * 50
        first, second = f"{stem} Id", f"{stem}-Name"
        properties = {first: {"type": "integer"}, second: {"type": ["null", "string"]}}
        for value in ("a", "b"):
            _run(
                lake,
                [{first: 1, second: value}],
                properties=properties,
                keys=[first],
                conform_column_names=True,
            )
        assert lake.query("SELECT * FROM lake.s.t") == [(1, "b")]
        columns = _columns(lake)
        if lake.kind == "postgres":
            prefix = conform_name(stem)[:61]
            assert columns == [f"{prefix}_0", f"{prefix}_1"]
        else:
            assert columns == [conform_name(first), conform_name(second)]


def test_setup_reads_the_table_once(lake: Lake, monkeypatch: pytest.MonkeyPatch):
    """The checks and prepare_table share one read of the existing table."""
    calls: list[str] = []
    read = DuckLakeConnector.existing_column_types
    get = DuckLakeConnector.get_table_column_types

    def counted_read(self: DuckLakeConnector, schema: str, table: str) -> dict:
        calls.append("existing_column_types")
        return read(self, schema, table)

    def counted_get(self: DuckLakeConnector, schema: str, table: str) -> dict:
        calls.append("get_table_column_types")
        return get(self, schema, table)

    properties = {
        "Id": {"type": "integer"},
        "Region": {"type": ["null", "string"]},
        "Home Address": {
            "type": ["null", "object"],
            "properties": {"Street": {"type": ["null", "string"]}},
        },
    }
    config: dict[str, Any] = {
        "conform_column_names": True,
        "stream_defaults": {"partition": [{"column": "Region"}]},
    }
    record = {"Id": 1, "Region": "eu", "Home Address": {"Street": "Main"}}
    _run(lake, [record], properties=properties, keys=["Id"], **config)
    monkeypatch.setattr(DuckLakeConnector, "existing_column_types", counted_read)
    monkeypatch.setattr(DuckLakeConnector, "get_table_column_types", counted_get)
    _run(lake, [record], properties=properties, keys=["Id"], **config)
    # One read before the checks (which reads the columns), one after
    # prepare_table changed the table.
    assert calls == [
        "existing_column_types",
        "get_table_column_types",
        "get_table_column_types",
    ]


TYPED = {
    "Order ID": {"type": "string"},
    "Qty-Count": {"type": ["null", "string"]},
}


def _types(lake: Lake) -> dict[str, str]:
    return {row[0]: row[1] for row in lake.query("DESCRIBE lake.s.t")}


class TestWithTypeSource:
    """type_source (DUCKLKD-43) with conformed column names."""

    @pytest.mark.parametrize(
        "type_source",
        [
            {"Order ID": "bigint", "Qty-Count": "integer"},
            {"order_id": "bigint", "qty_count": "integer"},
        ],
        ids=["tap-names", "conformed-names"],
    )
    def test_entries_type_the_conformed_columns(self, lake: Lake, type_source: dict):
        """Either name types the column, as for key_properties and the layout."""
        config: dict[str, Any] = {
            "conform_column_names": True,
            "stream_overrides": {"t": {"type_source": type_source}},
        }
        records = [{"Order ID": "1", "Qty-Count": "3"}]
        _run(lake, records, properties=TYPED, keys=["Order ID"], **config)
        _run(lake, records, properties=TYPED, keys=["Order ID"], **config)
        assert _types(lake) == {"order_id": "BIGINT", "qty_count": "BIGINT"}
        assert lake.query("SELECT order_id, qty_count FROM lake.s.t") == [(1, 3)]

    def test_conformed_names_are_unknown_with_the_setting_off(
        self, lake: Lake, warnings: pytest.LogCaptureFixture
    ):
        """Off, a conformed name is a column the stream doesn't have."""
        config: dict[str, Any] = {
            "stream_overrides": {"t": {"type_source": {"order_id": "bigint"}}}
        }
        _run(lake, [{"Order ID": "1"}], properties=TYPED, **config)
        assert _types(lake)["Order ID"] == "VARCHAR"
        assert any("['order_id']" in r.getMessage() for r in warnings.records)

    def test_both_names_of_a_property_are_refused(self, lake: Lake):
        """An entry by each name of one property is a mistake."""
        config: dict[str, Any] = {
            "conform_column_names": True,
            "stream_overrides": {
                "t": {"type_source": {"Order ID": "bigint", "order_id": "bigint"}}
            },
        }
        with pytest.raises(LayoutError, match="names a property twice"):
            _run(lake, [{"Order ID": "1"}], properties=TYPED, **config)

    @pytest.mark.parametrize("conform", [False, True], ids=["off", "on"])
    def test_a_bad_key_value_fails_under_null(self, lake: Lake, *, conform: bool):
        """The key rule applies to the key property whatever its column's name."""
        config: dict[str, Any] = {
            "conform_column_names": conform,
            "stream_overrides": {
                "t": {"type_source": {"Order ID": "bigint"}, "on_error": "null"}
            },
        }
        with pytest.raises(CoercionError, match="key column"):
            _run(
                lake,
                [{"Order ID": "x", "Qty-Count": "3"}],
                properties=TYPED,
                keys=["Order ID"],
                **config,
            )


def test_write_completed_counts_rows_with_conformed_names(
    lake: Lake, monkeypatch: pytest.MonkeyPatch
):
    """The writeCompleted event doesn't depend on column names (DUCKLKD-86)."""
    events: list[dict] = []

    def capture(batch: list[dict], *_args: object, **_kwargs: object) -> bool:
        events.extend(batch)
        return True

    monkeypatch.setattr(target_module, "publish", capture)
    config: dict[str, Any] = {
        "conform_column_names": True,
        "nats": {"servers": "nats://127.0.0.1:1", "subject": "lake.write.completed"},
    }
    records = [_record(1, "a"), _record(2, "b"), _record(2, "c")]
    _run(lake, records, keys=["Order ID"], **config)
    (event,) = events
    data = event["data"]
    assert (data["table"], data["rows"], data["load_method"]) == ("t", 2, "upsert")
    tagged = lake.query(
        "SELECT count(*) FROM lake.snapshots() "
        "WHERE json_extract_string(commit_extra_info, '$.run_id') IS NOT NULL"
    )[0][0]
    assert data["snapshot_count"] == tagged


class TestRoundedKeyValues:
    """value_type_checking's key wording, for keys whose column was renamed."""

    @staticmethod
    def _load(lake: Lake, key: str, **config: object) -> None:
        properties = {key: {"type": "number"}, "v": {"type": ["null", "string"]}}
        _run(
            lake,
            [{key: 7.5, "v": "a"}, {key: 7.2, "v": "b"}],
            properties=properties,
            keys=[key],
            **config,
        )

    @staticmethod
    def _existing_integer_key(lake: Lake, key: str, **config: object) -> None:
        """A table whose key column is an integer, so a fraction is rounded."""
        properties = {key: {"type": "integer"}, "v": {"type": ["null", "string"]}}
        _run(lake, [{key: 1, "v": "a"}], properties=properties, keys=[key], **config)

    @pytest.mark.parametrize(
        ("key", "config"),
        [
            ("Order ID", {"conform_column_names": True}),
            ("Long Key " * 8, {}),
        ],
        ids=["conformed", "truncated"],
    )
    def test_strict_names_the_key(self, lake: Lake, key: str, config: dict):
        """The error says the column is a key."""
        if "Long" in key and lake.kind != "postgres":
            pytest.skip("names are truncated on Postgres catalogs only")
        self._existing_integer_key(lake, key, **config)
        with pytest.raises(ValueError, match="the column is a key"):
            self._load(lake, key, value_type_checking="strict", **config)

    @pytest.mark.parametrize(
        ("key", "config"),
        [
            ("Order ID", {"conform_column_names": True}),
            ("Long Key " * 8, {}),
        ],
        ids=["conformed", "truncated"],
    )
    def test_permissive_warns_that_keys_merged(
        self,
        lake: Lake,
        capfd: pytest.CaptureFixture[str],
        key: str,
        config: dict,
    ):
        """The warning says key values were rounded, so records merged."""
        if "Long" in key and lake.kind != "postgres":
            pytest.skip("names are truncated on Postgres catalogs only")
        column = "order_id" if config else key[:63]
        self._existing_integer_key(lake, key, **config)
        capfd.readouterr()
        self._load(lake, key, **config)
        warning = capfd.readouterr().err
        assert f"2 key value(s) in column {column!r}" in warning
        assert "merged into one row" in warning
