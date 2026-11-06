"""Column names conformed as the singer-sdk's SQL targets conform them (DUCKLKD-87)."""

from __future__ import annotations

import io
import json
import logging
from typing import TYPE_CHECKING, Any, cast

import pytest
from singer_sdk.exceptions import ConformedNameClashException
from singer_sdk.sinks import SQLSink

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
        assert "'2fa': 'cfa'" in warning
        assert "'café': 'caf'" in warning
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

    def test_names_only_case_tells_apart_are_refused(self, lake: Lake):
        """DuckDB would fail creating the table, without naming the stream."""
        properties = {"Id": {"type": "integer"}, "id": {"type": "integer"}}
        with pytest.raises(ValueError, match="would share a column"):
            _run(lake, [{"Id": 1, "id": 2}], properties=properties)

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

    def test_turning_it_on_adds_the_renamed_columns(self, lake: Lake):
        """What the README says happens to an existing table.

        A name that only changes case is the same DuckDB column, which keeps
        its spelling; any other renamed property becomes a new column, and
        the old column is NULL in the rows loaded from then on.
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
